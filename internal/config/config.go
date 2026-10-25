package config

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// ValidateEarthRuntime checks execution-only requirements without creating state.
// Load permits incomplete enrollment for owner configuration preparation.
func (c Config) ValidateEarthRuntime() error {
	if c.Role != "earth" {
		return fmt.Errorf("Earth runtime requires earth configuration")
	}
	if strings.TrimSpace(c.RunnerToken) == "" {
		return fmt.Errorf("Earth requires runner_token; issue a credential from Sun's Earth tab")
	}
	if c.PiCLIPath != "" && c.PiPath != "" {
		return fmt.Errorf("choose pi_path or pi_cli_path, not both")
	}
	if c.PiCLIPath != "" {
		info, err := os.Stat(c.PiCLIPath)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("pi_cli_path must identify an installed Pi CLI file")
		}
	}
	return nil
}

type Config struct {
	Role                string   `yaml:"role"`
	BindAddr            string   `yaml:"bind_addr"`
	DBPath              string   `yaml:"db_path"`
	SunURL              string   `yaml:"sun_url"`
	RunnerID            string   `yaml:"runner_id"`
	StateDir            string   `yaml:"state_dir"`
	MemoryRoot          string   `yaml:"memory_root"`
	AdminPassword       string   `yaml:"admin_password"`
	RunnerToken         string   `yaml:"runner_token"`
	NodePath            string   `yaml:"node_path"`
	PiCLIPath           string   `yaml:"pi_cli_path"`
	PiPath              string   `yaml:"pi_path"`
	DefaultWorkspace    string   `yaml:"default_workspace"`
	UIDisabled          bool     `yaml:"ui_disabled"`
	PiTools             []string `yaml:"pi_tools"`
	IntegrationToken    string   `yaml:"integration_token"`
	GatewayURL          string   `yaml:"gateway_url"`
	GatewayBindingID    string   `yaml:"gateway_binding_id"` // Deprecated: accepted for old YAML; resolved IDs are never taken from this field.
	GatewayControlToken string   `yaml:"gateway_control_token"`
}

func (c Config) ValidateIntegration() error {
	configured := c.IntegrationToken != "" || c.GatewayURL != "" || c.GatewayControlToken != ""
	if !configured {
		return nil
	}
	if c.IntegrationToken == "" || c.GatewayURL == "" || c.GatewayControlToken == "" {
		return fmt.Errorf("incomplete gateway integration configuration")
	}
	if len(c.IntegrationToken) > 4096 || len(c.GatewayControlToken) > 4096 {
		return fmt.Errorf("integration credential or binding exceeds limit")
	}
	if c.IntegrationToken == c.GatewayControlToken {
		return fmt.Errorf("integration and control credentials must be distinct")
	}
	for _, v := range []string{c.IntegrationToken, c.GatewayControlToken} {
		if strings.TrimSpace(v) != v || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("invalid integration credential or binding")
		}
	}
	u, e := url.Parse(c.GatewayURL)
	if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid gateway_url")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("remote gateway_url requires HTTPS")
	}
	return nil
}

func Load(path, role string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	d := yaml.NewDecoder(strings.NewReader(string(b)))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("decode config: expected exactly one valid YAML document")
	}
	if role != "sun" && role != "earth" {
		return Config{}, fmt.Errorf("unknown role %q", role)
	}
	if c.Role != "" && c.Role != role {
		return Config{}, fmt.Errorf("config role %q does not match %q", c.Role, role)
	}
	c.Role = role
	if c.BindAddr == "" {
		if role == "earth" {
			c.BindAddr = "127.0.0.1:11521"
		} else {
			c.BindAddr = "127.0.0.1:11520"
		}
	}
	if c.DBPath == "" {
		c.DBPath = "tresorbot.db"
	}
	if !filepath.IsAbs(c.DBPath) && c.DBPath != ":memory:" {
		c.DBPath = filepath.Join(filepath.Dir(path), c.DBPath)
	}
	if role == "sun" {
		if err := c.ValidateIntegration(); err != nil {
			return Config{}, err
		}
		if c.MemoryRoot == "" {
			c.MemoryRoot = "memory"
		}
		if !filepath.IsAbs(c.MemoryRoot) {
			c.MemoryRoot = filepath.Join(filepath.Dir(path), c.MemoryRoot)
		}
		host, port, err := net.SplitHostPort(c.BindAddr)
		n, e := strconv.Atoi(port)
		if err != nil || e != nil || n < 1 || n > 65535 || strings.ContainsAny(host, " \r\n") {
			return Config{}, fmt.Errorf("invalid bind_addr")
		}
		ip := net.ParseIP(host)
		loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
		if !loopback && c.AdminPassword == "" {
			return Config{}, fmt.Errorf("non-loopback bind_addr requires admin_password")
		}
	}
	if role == "earth" {
		if strings.TrimSpace(c.SunURL) == "" {
			return Config{}, fmt.Errorf("earth config requires sun_url")
		}
		u, err := url.Parse(c.SunURL)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
			return Config{}, fmt.Errorf("invalid sun_url")
		}
		if u.Scheme == "http" {
			host := u.Hostname()
			ip := net.ParseIP(host)
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
				return Config{}, fmt.Errorf("remote sun_url requires HTTPS")
			}
		}
		if _, err := uuid.Parse(c.RunnerID); err != nil {
			return Config{}, fmt.Errorf("earth config requires a UUID runner_id")
		}
		if strings.TrimSpace(c.RunnerToken) != c.RunnerToken || strings.ContainsAny(c.RunnerToken, "\r\n\x00") {
			return Config{}, fmt.Errorf("invalid runner_token")
		}
		if !c.UIDisabled {
			host, port, err := net.SplitHostPort(c.BindAddr)
			n, e := strconv.Atoi(port)
			if err != nil || e != nil || n < 1 || n > 65535 {
				return Config{}, fmt.Errorf("invalid Earth bind_addr")
			}
			ip := net.ParseIP(host)
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) && c.AdminPassword == "" {
				return Config{}, fmt.Errorf("non-loopback Earth bind_addr requires admin_password")
			}
		}
		for _, p := range []*string{&c.NodePath, &c.PiCLIPath, &c.PiPath} {
			if strings.ContainsAny(*p, "\r\n\x00") {
				return Config{}, fmt.Errorf("invalid runtime path")
			}
			if *p != "" && !filepath.IsAbs(*p) && strings.ContainsAny(*p, "/\\") {
				*p = filepath.Join(filepath.Dir(path), *p)
			}
		}
		if strings.ContainsAny(c.DefaultWorkspace, "\r\n\x00") || len(c.DefaultWorkspace) > 4096 {
			return Config{}, fmt.Errorf("invalid default_workspace")
		}
		if c.DefaultWorkspace != "" && !filepath.IsAbs(c.DefaultWorkspace) {
			return Config{}, fmt.Errorf("default_workspace must be absolute")
		}
		allowed := map[string]bool{"read": true, "bash": true, "powershell": true, "edit": true, "write": true, "grep": true, "find": true, "ls": true}
		seen := map[string]bool{}
		for _, tool := range c.PiTools {
			if !allowed[tool] || seen[tool] {
				return Config{}, fmt.Errorf("invalid or duplicate pi_tools entry %q", tool)
			}
			seen[tool] = true
		}
		if strings.TrimSpace(c.StateDir) == "" {
			return Config{}, fmt.Errorf("earth config requires state_dir")
		}
		if !filepath.IsAbs(c.StateDir) {
			c.StateDir = filepath.Join(filepath.Dir(path), c.StateDir)
		}
	}
	return c, nil
}
