package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tresorbot/internal/config"
	"tresorbot/internal/environments"
	"tresorbot/internal/sun"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	// `input` manages the input catalogue and needs no config, so it is dispatched
	// before flag parsing: its own subcommands have their own flags.
	if len(args) > 0 && args[0] == "environment" {
		return runEnvironment(args[1:])
	}
	fs := flag.NewFlagSet("tresorbot-sun", flag.ContinueOnError)
	path := fs.String("config", "", "YAML config path")
	check := fs.Bool("check-config", false, "validate config and exit")
	ver := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *ver {
		fmt.Println(version)
		return nil
	}
	if *path == "" {
		return fmt.Errorf("--config is required")
	}
	c, err := config.Load(*path, "sun")
	if err != nil {
		return err
	}
	if *check {
		fmt.Println("config ok")
		return nil
	}
	// Reserve the listener before recovery changes durable task state.
	listener, err := net.Listen("tcp", c.BindAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	if c.AdminPassword == "" {
		address, ok := listener.Addr().(*net.TCPAddr)
		if !ok || !address.IP.IsLoopback() {
			return fmt.Errorf("password-free owner access requires a loopback listener")
		}
	}
	app, err := sun.Open(c)
	if err != nil {
		return err
	}
	defer app.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: c.BindAddr, Handler: app.Handler, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	// Webhook ingress gets its own listener when configured, so the port an
	// upstream posts to can be exposed without also exposing the owner dashboard
	// and the integration bridge that share bind_addr.
	var ingressSrv *http.Server
	if c.Environments != nil && c.Environments.Enabled && c.Environments.IngressAddr != "" {
		ingressListener, err := net.Listen("tcp", c.Environments.IngressAddr)
		if err != nil {
			return fmt.Errorf("listen ingress: %w", err)
		}
		defer ingressListener.Close()
		ingressSrv = &http.Server{Addr: c.Environments.IngressAddr, Handler: app.IngressHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
		go func() {
			if err := ingressSrv.Serve(ingressListener); err != nil && err != http.ErrServerClosed {
				log.Printf("ingress listener stopped: %v", err)
			}
		}()
		fmt.Fprintf(os.Stderr, "webhook ingress listening on %s%s\n", c.Environments.IngressAddr, environments.IngressPrefix)
	}
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if ingressSrv != nil {
			_ = ingressSrv.Shutdown(shutdownCtx)
		}
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			return err
		}
		err := <-done
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}
