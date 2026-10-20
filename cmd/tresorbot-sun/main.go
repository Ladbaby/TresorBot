package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tresorbot/internal/config"
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
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
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
