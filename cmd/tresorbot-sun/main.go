package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tresorbot/internal/config"
	"tresorbot/internal/store"
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
	s, err := store.Open(c.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()
	if err = s.Ping(); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	srv := &http.Server{Addr: c.BindAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
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
			return err
		}
		err := <-done
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}
