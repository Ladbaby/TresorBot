package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"tresorbot/internal/config"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthexec"
	"tresorbot/internal/earthjournal"
)

var version = "dev"

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(a []string) error {
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
	if *c {
		fmt.Println("config ok")
		return nil
	}
	if e = cfg.ValidateEarthRuntime(); e != nil {
		return e
	}
	if e = os.MkdirAll(cfg.StateDir, 0755); e != nil {
		return e
	}
	journal, e := earthjournal.Open(filepath.Join(cfg.StateDir, "earth.db"), 1000)
	if e != nil {
		return e
	}
	defer journal.Close()
	executor := earthexec.New(earthexec.Config{NodePath: cfg.NodePath, CLIPath: cfg.PiCLIPath, Tools: cfg.PiTools, StateDir: cfg.StateDir, RequireGatewayScope: true, Authorize: earthexec.AssignmentAuthorization})
	client, e := earth.New(earth.Options{SunURL: cfg.SunURL, RunnerID: cfg.RunnerID, Token: cfg.RunnerToken, Journal: journal, Executor: executor})
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	e = client.Run(ctx)
	if errors.Is(e, context.Canceled) {
		return nil
	}
	return e
}
