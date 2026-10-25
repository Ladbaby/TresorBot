package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthexec"
	"tresorbot/internal/piadapter"
	"tresorbot/internal/runnertransport"
)

// settingsState captures settings per assignment, never changes an active executor.
type settingsState struct {
	mu         sync.RWMutex
	cfg        config.Config
	connected  bool
	connection string
	client     *earth.Client
}

func (s *settingsState) snapshot() config.Config { s.mu.RLock(); defer s.mu.RUnlock(); return s.cfg }
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
func (s *settingsState) SnapshotExecutor() earth.Executor { return &settingsState{cfg: s.snapshot()} }
func (s *settingsState) Execute(ctx context.Context, a contracts.Assign, progress func(int, string) error, started func(piadapter.Progress)) (piadapter.Result, error) {
	c := s.snapshot()
	if err := c.ValidateEarthRuntime(); err != nil {
		return piadapter.Result{}, err
	}
	var input runnertransport.ExecutionInput
	var bot struct {
		Workspace string `json:"workspace"`
	}
	if json.Unmarshal(a.Input, &input) != nil || json.Unmarshal(input.Snapshot, &bot) != nil {
		return piadapter.Result{}, errors.New("invalid execution workspace snapshot")
	}
	info, e := os.Stat(bot.Workspace)
	if !filepath.IsAbs(bot.Workspace) || e != nil || !info.IsDir() {
		return piadapter.Result{}, errors.New("task workspace must be an existing absolute directory on Earth")
	}
	ex := earthexec.New(earthexec.Config{NodePath: c.NodePath, PiPath: c.PiPath, CLIPath: c.PiCLIPath, Tools: c.PiTools, StateDir: c.StateDir, RequireGatewayScope: true, Authorize: earthexec.AssignmentAuthorization})
	return ex.Execute(ctx, a, progress, started)
}
