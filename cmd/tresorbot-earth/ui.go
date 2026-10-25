package main

import (
	"errors"
	"tresorbot/internal/config"
	"tresorbot/internal/earthui"
	"tresorbot/internal/runtimepath"
)

func (s *settingsState) validateRuntime() error {
	c := s.snapshot()
	if err := c.ValidateEarthRuntime(); err != nil {
		return err
	}
	_, err := runtimepath.Resolve(runtimepath.Options{NodePath: c.NodePath, PiPath: c.PiPath, PiCLIPath: c.PiCLIPath})
	return err
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
			resolved, err := runtimepath.Resolve(runtimepath.Options{NodePath: c.NodePath, PiPath: c.PiPath, PiCLIPath: c.PiCLIPath})
			if err != nil {
				out["runtime_error"] = err.Error()
			} else {
				out["node_path"] = resolved.NodePath
				out["pi_cli_path"] = resolved.CLIPath
			}
			if err = c.ValidateEarthRuntime(); err != nil {
				out["setup_error"] = err.Error()
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
			return earthui.Settings{DefaultWorkspace: c.DefaultWorkspace, NodePath: c.NodePath, PiPath: c.PiPath, PiCLIPath: c.PiCLIPath}, revision, err
		},
		Save: func(in earthui.Settings, revision string) (string, error) {
			c, next, err := config.UpdateEarthSettings(path, revision, map[string]string{"default_workspace": in.DefaultWorkspace, "node_path": in.NodePath, "pi_path": in.PiPath, "pi_cli_path": in.PiCLIPath})
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
