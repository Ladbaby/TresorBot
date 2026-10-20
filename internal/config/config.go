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

type Config struct {
	Role          string `yaml:"role"`
	BindAddr      string `yaml:"bind_addr"`
	DBPath        string `yaml:"db_path"`
	SunURL        string `yaml:"sun_url"`
	RunnerID      string `yaml:"runner_id"`
	StateDir      string `yaml:"state_dir"`
	MemoryRoot    string `yaml:"memory_root"`
	AdminPassword string `yaml:"admin_password"`
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
		c.BindAddr = "127.0.0.1:11520"
	}
	if c.DBPath == "" {
		c.DBPath = "tresorbot.db"
	}
	if !filepath.IsAbs(c.DBPath) && c.DBPath != ":memory:" {
		c.DBPath = filepath.Join(filepath.Dir(path), c.DBPath)
	}
	if role == "sun" {
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
		if strings.TrimSpace(c.StateDir) == "" {
			return Config{}, fmt.Errorf("earth config requires state_dir")
		}
		if !filepath.IsAbs(c.StateDir) {
			c.StateDir = filepath.Join(filepath.Dir(path), c.StateDir)
		}
	}
	return c, nil
}
