package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"tresorbot/internal/agent"
	"tresorbot/internal/agents"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthexec"
	"tresorbot/internal/runnertransport"
)

// settingsState captures settings per assignment, never changes an active executor.
type settingsState struct {
	mu         sync.RWMutex
	cfg        config.Config
	connected  bool
	connection string
	client     *earth.Client
	// adapters is shared by every snapshot: the pool owns processes, while each
	// snapshot freezes the configuration a turn runs with.
	adapters *agents.Pool
	// tools is the one loopback gateway for this Earth.
	tools *earthexec.ToolGateway
}

func (s *settingsState) snapshot() config.Config { s.mu.RLock(); defer s.mu.RUnlock(); return s.cfg }

// pool returns the shared adapter runtime.
func (s *settingsState) pool() *agents.Pool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adapters
}

func (s *settingsState) workspace() string {
	c := s.snapshot()
	if !filepath.IsAbs(c.DefaultWorkspace) {
		return ""
	}
	info, e := os.Stat(c.DefaultWorkspace)
	if e != nil || !info.IsDir() {
		return ""
	}
	return c.DefaultWorkspace
}
func (s *settingsState) connectionChanged(connected bool, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = connected
	s.connection = state
}
func (s *settingsState) SnapshotExecutor() earth.Executor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &settingsState{cfg: s.cfg, adapters: s.adapters, tools: s.tools}
}
func (s *settingsState) Execute(ctx context.Context, a contracts.Assign, progress func(int, string) error, started func(agent.Progress)) (agent.Result, error) {
	c := s.snapshot()
	if err := c.ValidateEarthRuntime(); err != nil {
		return agent.Result{}, err
	}
	var input runnertransport.ExecutionInput
	var bot struct {
		Workspace string `json:"workspace"`
	}
	if json.Unmarshal(a.Input, &input) != nil || json.Unmarshal(input.Snapshot, &bot) != nil {
		return agent.Result{}, errors.New("invalid execution workspace snapshot")
	}
	info, e := os.Stat(bot.Workspace)
	if !filepath.IsAbs(bot.Workspace) || e != nil || !info.IsDir() {
		return agent.Result{}, errors.New("task workspace must be an existing absolute directory on Earth")
	}
	ex := earthexec.New(earthexec.Config{
		StateDir:    c.StateDir,
		SunURL:      c.SunURL,
		RunnerID:    c.RunnerID,
		RunnerToken: c.RunnerToken,
		Adapters:    s.pool(),
		Tools:       s.tools,
	})
	return ex.Execute(ctx, a, progress, started)
}

// advertisedAdapters reports the adapters this Earth can run, in the wire shape
// Sun stores. It degrades to an empty list rather than failing the connection.
func (s *settingsState) advertisedAdapters(ctx context.Context) ([]contracts.RunnerAdapter, error) {
	pool := s.pool()
	if pool == nil {
		return nil, nil
	}
	list, err := pool.Adapters(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]contracts.RunnerAdapter, 0, len(list))
	for _, a := range list {
		out = append(out, contracts.RunnerAdapter{
			Name:        a.Name,
			Version:     a.Version,
			DisplayName: a.DisplayName,
			Healthy:     a.Healthy,
		})
	}
	return out, nil
}
