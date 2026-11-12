package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	"tresorbot/internal/agents"
	"tresorbot/internal/agents/hostjs"
	"tresorbot/internal/config"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthexec"
	"tresorbot/internal/earthjournal"
	"tresorbot/internal/earthui"
	"tresorbot/internal/ownerauth"
)

var version = "dev"

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(a []string) error {
	// `agent` manages the adapter catalogue and needs no config, so it is
	// dispatched before flag parsing: its own subcommands have their own flags.
	if len(a) > 0 && a[0] == "agent" {
		return runAgent(a[1:])
	}
	f := flag.NewFlagSet("tresorbot-earth", flag.ContinueOnError)
	p := f.String("config", "", "YAML config path")
	c := f.Bool("check-config", false, "validate config and exit")
	v := f.Bool("version", false, "print version and exit")
	if e := f.Parse(a); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", f.Args())
	}
	if *v {
		fmt.Println(version)
		return nil
	}
	if *p == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, e := config.Load(*p, "earth")
	if e != nil {
		return e
	}
	if cfg.LegacyAgents {
		log.Printf("earth.yaml uses node_path/pi_path/pi_cli_path/pi_tools; these still work but should move under agents.settings.pi")
	}
	if *c {
		fmt.Println("config ok")
		return nil
	}
	if cfg.UIDisabled {
		if e = cfg.ValidateEarthRuntime(); e != nil {
			return e
		}
	}
	if e = cfg.EnsureEarthWorkspace(); e != nil {
		return fmt.Errorf("create default Earth workspace: %w", e)
	}
	if e = os.MkdirAll(cfg.StateDir, 0755); e != nil {
		return e
	}
	journal, e := earthjournal.Open(filepath.Join(cfg.StateDir, "earth.db"), journalCapacity(config.AgentsMaxSessions))
	if e != nil {
		return e
	}
	defer journal.Close()
	agentsDir, e := config.AgentsDir()
	if e != nil {
		return e
	}
	// The host ships inside this binary, so there is nothing to install for Earth
	// itself to run. Only adapters are fetched, and only the ones a user chooses.
	hostEntry, e := hostjs.Materialize(filepath.Join(cfg.StateDir, "agent-host"))
	if e != nil {
		return fmt.Errorf("extract agent host: %w", e)
	}
	pool, e := agents.NewPool(agents.PoolConfig{
		Entry:         hostEntry,
		AgentsDir:     agentsDir,
		StateDir:      filepath.Join(cfg.StateDir, "agent-host"),
		Hosts:         config.AgentsHosts,
		MaxSessions:   config.AgentsMaxSessions,
		CancelTimeout: config.AgentsCancelTimeout,
		Logf:          log.Printf,
	})
	if e != nil {
		return fmt.Errorf("agent adapters: %w", e)
	}
	defer pool.Close(context.Background())
	gateway, e := earthexec.NewToolGateway(context.Background(), cfg.SunURL, cfg.RunnerID, cfg.RunnerToken)
	if e != nil {
		return fmt.Errorf("tool gateway: %w", e)
	}
	defer gateway.Close()
	settings := &settingsState{cfg: cfg, connection: "disconnected", adapters: pool, tools: gateway}
	client, e := earth.New(earth.Options{SunURL: cfg.SunURL, RunnerID: cfg.RunnerID, Token: cfg.RunnerToken, Journal: journal, Executor: settings, DefaultWorkspace: settings.workspace, ConnectionChanged: settings.connectionChanged, ValidateRuntime: settings.validateRuntime, Adapters: settings.advertisedAdapters})
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	results := make(chan error, 2)
	var server *http.Server
	if !cfg.UIDisabled {
		sessions, err := ownerauth.Open(filepath.Join(cfg.StateDir, "earth-owner.db"), cfg.AdminPassword)
		if err != nil {
			return err
		}
		defer sessions.Close()
		opts := settings.uiOptions(*p)
		opts.Auth = sessions
		listener, err := net.Listen("tcp", cfg.BindAddr)
		if err != nil {
			return err
		}
		server = &http.Server{Handler: earthui.New(opts), ReadHeaderTimeout: 10 * time.Second}
		log.Printf("Earth setup: http://%s", listener.Addr())
		go func() {
			err := server.Serve(listener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			results <- err
		}()
		defer func() {
			shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = server.Shutdown(shutdown)
		}()
	}
	if e != nil {
		if cfg.UIDisabled {
			return e
		}
		settings.connectionChanged(false, "Earth enrollment incomplete; configure sun_url, runner_id and runner_token in earth.yaml and restart")
		select {
		case <-ctx.Done():
			return nil
		case err := <-results:
			return err
		}
	}
	settings.mu.Lock()
	settings.client = client
	settings.mu.Unlock()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		err := client.Run(ctx)
		if err != nil && !cfg.UIDisabled {
			settings.connectionChanged(false, err.Error())
			<-ctx.Done()
		}
		results <- err
	}()
	select {
	case <-ctx.Done():
		e = nil
	case e = <-results:
	}
	cancel()
	<-clientDone
	if errors.Is(e, context.Canceled) {
		return nil
	}
	return e
}

// journalCapacity sizes the durable assignment journal. A wide fleet can have
// many admitted assignments between acknowledgements, so the capacity follows
// the configured session ceiling instead of a fixed 1000.
func journalCapacity(maxSessions int) int {
	if maxSessions < 1000 {
		return 1000
	}
	return maxSessions * 2
}
