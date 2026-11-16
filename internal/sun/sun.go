// Package sun composes durable owner services. Runner transport is deliberately separate.
package sun

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"tresorbot/internal/api"
	"tresorbot/internal/bottools"
	"tresorbot/internal/config"
	"tresorbot/internal/environments"
	"tresorbot/internal/memory"
	"tresorbot/internal/ownerauth"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

type Application struct {
	Handler      http.Handler
	store        *store.Store
	memory       *memory.Store
	bottools     *bottools.Handler
	sessions     *ownerauth.Store
	hub          *runnertransport.Hub
	dispatch     *dispatchLoop
	environments *environments.Manager
	closeOnce    sync.Once
	closeErr     error
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
	// Seed the built-in Worker bot and guarantee it is a contact of every
	// enabled bot before any request is served.
	if err = s.EnsureWorkerForAll(); err != nil {
		s.Close()
		return nil, fmt.Errorf("seed worker: %w", err)
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
	if err = s.RecoverDirectParents(); err != nil {
		m.Close()
		s.Close()
		return nil, fmt.Errorf("recover direct parents: %w", err)
	}
	if err = s.RecoverOrphanedChildren(); err != nil {
		m.Close()
		s.Close()
		return nil, fmt.Errorf("recover orphaned children: %w", err)
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
	app := &Application{store: s, memory: m, sessions: sessions, bottools: bottools.New(s, m)}
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
	app.hub = runnertransport.New(runnertransport.Options{Store: s, Reconciler: reconcileStore{s}, Changed: func() { app.dispatch.Wake() }, Disconnected: func(id string) { app.dispatch.Wake() }})
	app.dispatch = newDispatchLoop(s, app.hub, func(ctx context.Context, a store.Assignment) error {
		return app.hub.Cancel(ctx, a.RunnerID, a.ID, a.Epoch)
	}, cleanup)
	// Inputs come up after the store, recovery and the dispatcher, because intake
	// admits tasks and the reply pump reads settled ones. A failure here is logged
	// and the subsystem stays off: a missing node binary or a broken input
	// catalogue must not stop the owner's dashboard from starting.
	app.environments, err = environments.Open(environments.OptionsFromConfig(c, s, app.dispatch.Wake))
	if err != nil {
		log.Printf("inputs disabled: %v", err)
		app.environments = nil
	}

	h := api.New(s, m, api.Options{OwnerAuth: sessions, AdminPassword: c.AdminPassword, BindAddr: c.BindAddr, MemoryRoot: c.MemoryRoot, RunnerStatus: app.hub.Status, RunnerChanged: app.hub.Invalidate, TasksChanged: app.dispatch.Wake, Environments: app.environments, EnvironmentsDir: environmentDir(c)})
	mux := http.NewServeMux()
	if service != nil {
		mux.Handle("/integration/", service)
	}
	if app.environments != nil {
		mux.Handle(environments.IngressPrefix, app.environments.Ingress)
	}
	mux.Handle("/api/runner/tools/", app.nativeTools())
	mux.Handle("/api/runner/connect", app.hub)
	mux.Handle("/", h)
	app.Handler = mux
	// Retry durable cleanup after restart even before a runner reconnects.
	app.dispatch.Wake()
	if app.environments != nil {
		app.environments.Start(context.Background())
	}
	return app, nil
}

// IngressHandler serves only the webhook ingress prefix, for the optional
// dedicated listener.
//
// A separate listener exists so the port an upstream posts to can be exposed to
// the internet without exposing the owner dashboard and the integration bridge: a
// listener that mounted the full handler would quietly undo that, so this one
// answers 404 for everything else.
func (a *Application) IngressHandler() http.Handler {
	mux := http.NewServeMux()
	if a.environments != nil {
		mux.Handle(environments.IngressPrefix, a.environments.Ingress)
	}
	return mux
}

func (a *Application) Close() error {
	a.closeOnce.Do(func() {
		// Inputs stop first: the host holds upstream connections and in-flight
		// admissions that read the store, so tearing the store down underneath them
		// would turn an orderly shutdown into a burst of misleading errors.
		var environmentsErr error
		if a.environments != nil {
			ctx, cancel := context.WithTimeout(context.Background(), environmentsShutdownTimeout)
			environmentsErr = a.environments.Close(ctx)
			cancel()
		}
		hubErr := a.hub.Close()
		a.dispatch.Close()
		a.closeErr = errors.Join(environmentsErr, hubErr, a.sessions.Close(), a.memory.Close(), a.store.Close())
	})
	return a.closeErr
}

// environmentsShutdownTimeout bounds adapter shutdown. An adapter that ignores its stop
// notification is killed rather than allowed to hold Sun open.
const environmentsShutdownTimeout = 20 * time.Second

// environmentDir reports the input catalogue path for the dashboard, without requiring
// inputs to be enabled.
func environmentDir(c config.Config) string {
	if c.Environments != nil && c.Environments.Dir != "" {
		return c.Environments.Dir
	}
	dir, err := environments.Dir()
	if err != nil {
		return ""
	}
	return dir
}
