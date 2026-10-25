// Package sun composes durable owner services. Runner transport is deliberately separate.
package sun

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"tresorbot/internal/api"
	"tresorbot/internal/config"
	"tresorbot/internal/memory"
	"tresorbot/internal/ownerauth"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

type Application struct {
	Handler   http.Handler
	store     *store.Store
	memory    *memory.Store
	sessions  *ownerauth.Store
	hub       *runnertransport.Hub
	dispatch  *dispatchLoop
	closeOnce sync.Once
	closeErr  error
}

func Open(c config.Config) (*Application, error) {
	if err := c.ValidateIntegration(); err != nil {
		return nil, err
	}
	if c.Role != "" && c.Role != "sun" {
		return nil, fmt.Errorf("Sun requires sun configuration")
	}
	if c.DBPath == "" || c.MemoryRoot == "" {
		return nil, fmt.Errorf("Sun requires database and memory paths")
	}
	s, err := store.Open(c.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open task store: %w", err)
	}
	m, err := memory.Open(c.MemoryRoot, func(botID string) (bool, error) {
		b, err := s.GetBot(botID)
		if err != nil {
			return false, err
		}
		if !b.Enabled {
			return false, store.ErrDisabled
		}
		return b.SharedWrite, nil
	})
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("open memory: %w", err)
	}
	if err = s.RecoverActiveTasks(); err != nil {
		m.Close()
		s.Close()
		return nil, fmt.Errorf("recover tasks: %w", err)
	}
	authPath := c.DBPath + ".owner-sessions.db"
	if c.DBPath == ":memory:" {
		authPath = ":memory:"
	}
	sessions, err := ownerauth.Open(authPath, c.AdminPassword)
	if err != nil {
		m.Close()
		s.Close()
		return nil, fmt.Errorf("open owner sessions: %w", err)
	}
	app := &Application{store: s, memory: m, sessions: sessions}
	registry, service, err := app.integration(c)
	if err != nil {
		sessions.Close()
		m.Close()
		s.Close()
		return nil, err
	}
	var cleanup func(context.Context) error
	if registry != nil {
		cleanup = registry.Cleanup
	}
	app.hub = runnertransport.New(runnertransport.Options{AuthorizeAssignment: app.authorizeGateway(registry), Store: s, Reconciler: reconcileStore{s}, Changed: func() { app.dispatch.Wake() }, Disconnected: func(id string) { app.dispatch.Wake() }})
	app.dispatch = newDispatchLoop(s, app.hub, func(ctx context.Context, a store.Assignment) error {
		return app.hub.Cancel(ctx, a.RunnerID, a.ID, a.Epoch)
	}, cleanup)
	h := api.New(s, m, api.Options{OwnerAuth: sessions, AdminPassword: c.AdminPassword, BindAddr: c.BindAddr, MemoryRoot: c.MemoryRoot, RunnerStatus: app.hub.Status, RunnerChanged: app.hub.Invalidate, TasksChanged: app.dispatch.Wake})
	mux := http.NewServeMux()
	if service != nil {
		mux.Handle("/integration/", service)
	}
	mux.Handle("/api/runner/connect", app.hub)
	mux.Handle("/", h)
	app.Handler = mux
	// Retry durable cleanup after restart even before a runner reconnects.
	app.dispatch.Wake()
	return app, nil
}

func (a *Application) Close() error {
	a.closeOnce.Do(func() {
		hubErr := a.hub.Close()
		a.dispatch.Close()
		a.closeErr = errors.Join(hubErr, a.sessions.Close(), a.memory.Close(), a.store.Close())
	})
	return a.closeErr
}
