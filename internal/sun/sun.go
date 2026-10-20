// Package sun composes durable owner services. Runner transport is deliberately separate.
package sun

import (
	"errors"
	"fmt"
	"net/http"

	"tresorbot/internal/api"
	"tresorbot/internal/config"
	"tresorbot/internal/memory"
	"tresorbot/internal/store"
)

type Application struct {
	Handler http.Handler
	store   *store.Store
	memory  *memory.Store
}

func Open(c config.Config) (*Application, error) {
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
	h := api.New(s, m, api.Options{AdminPassword: c.AdminPassword, BindAddr: c.BindAddr, MemoryRoot: c.MemoryRoot})
	return &Application{Handler: h, store: s, memory: m}, nil
}

func (a *Application) Close() error { return errors.Join(a.memory.Close(), a.store.Close()) }
