package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"tresorbot/internal/config"
	"tresorbot/internal/earthui"
)

func (s *settingsState) validateRuntime() error {
	c := s.snapshot()
	if err := c.ValidateEarthRuntime(); err != nil {
		return err
	}
	// The adapter host resolves the agent itself; Earth only needs a usable Node
	// to run the host, so a missing pi CLI is reported by the host at dispatch
	// time rather than blocking Earth's own startup.
	return probeAgentHost()
}

func (s *settingsState) uiOptions(path string) earthui.Options {
	return earthui.Options{
		Password: s.snapshot().AdminPassword, PasswordRequired: s.snapshot().AdminPassword != "",
		Status: func() any {
			c := s.snapshot()
			s.mu.RLock()
			connected, state := s.connected, s.connection
			s.mu.RUnlock()
			out := map[string]any{"earth_id": c.RunnerID, "connected": connected, "connection": state, "default_workspace": s.workspace()}
			if dir, err := config.AgentsDir(); err == nil {
				out["agents_dir"] = dir
				out["adapters"] = installedAdapters(dir)
			}
			if err := probeAgentHost(); err != nil {
				out["runtime_error"] = err.Error()
			}
			if setupErr := c.ValidateEarthRuntime(); setupErr != nil {
				out["setup_error"] = setupErr.Error()
			}
			if s.workspace() == "" {
				out["workspace_error"] = "Choose an existing absolute default workspace, or set a bot workspace override."
			}
			return out
		},
		Load: func() (earthui.Settings, string, error) {
			before, err := config.FileRevision(path)
			if err != nil {
				return earthui.Settings{}, "", err
			}
			c, err := config.Load(path, "earth")
			if err != nil {
				return earthui.Settings{}, "", err
			}
			revision, err := config.FileRevision(path)
			if err == nil && revision != before {
				return earthui.Settings{}, "", earthui.ErrConflict
			}
			dir, _ := config.AgentsDir()
			return earthui.Settings{DefaultWorkspace: c.DefaultWorkspace, AgentsDir: dir}, revision, err
		},
		Save: func(in earthui.Settings, revision string) (string, error) {
			c, next, err := config.UpdateEarthSettings(path, revision, map[string]string{"default_workspace": in.DefaultWorkspace})
			if err != nil {
				if errors.Is(err, config.ErrConfigConflict) {
					return "", earthui.ErrConflict
				}
				return "", err
			}
			s.mu.Lock()
			s.cfg = c
			client := s.client
			s.mu.Unlock()
			if client != nil {
				client.RefreshSettings()
			}
			return next, nil
		},
	}
}

// probeAgentHost reports whether the adapter host can be started at all. It
// deliberately does not require any particular adapter: an Earth with no
// adapters is a configuration problem for the bots, not a startup failure.
// probeAgentHost reports whether Earth can run agents at all. The host is
// embedded, so this only fails when no adapter is installed — which is a normal
// first-run state, not a broken installation.
func probeAgentHost() error {
	dir, err := config.AgentsDir()
	if err != nil {
		return err
	}
	if names := installedAdapters(dir); len(names) == 0 {
		return fmt.Errorf("no agent adapters installed; run `tresorbot-earth agent install <url> --sha256 <digest>`")
	}
	return nil
}

// installedAdapters lists adapter directories that have a manifest.
func installedAdapters(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, entry.Name(), "agent.json")); err == nil {
			names = append(names, entry.Name())
		}
	}
	return names
}
