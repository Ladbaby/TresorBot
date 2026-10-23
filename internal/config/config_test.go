package config

import (
	"os"
	"path/filepath"
	"testing"
)

func loadText(t *testing.T, role, text string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(text), 0644); err != nil {
		t.Fatal(err)
	}
	return Load(path, role)
}
func TestDefaultsAndRelativePaths(t *testing.T) {
	c, err := loadText(t, "sun", "role: sun\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.BindAddr != "127.0.0.1:11520" || filepath.Base(c.DBPath) != "tresorbot.db" || !filepath.IsAbs(c.DBPath) {
		t.Fatal(c)
	}
	if !filepath.IsAbs(c.MemoryRoot) || filepath.Base(c.MemoryRoot) != "memory" {
		t.Fatal("memory root must be config relative", c.MemoryRoot)
	}
	c, err = loadText(t, "earth", "sun_url: http://127.0.0.1:11520\nrunner_id: 11111111-1111-4111-8111-111111111111\nstate_dir: state\n")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(c.StateDir) {
		t.Fatal(c)
	}
}
func TestRemoteOwnerRequiresPassword(t *testing.T) {
	c, err := loadText(t, "sun", "bind_addr: 0.0.0.0:11520\nadmin_password: owner-secret\nmemory_root: knowledge\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminPassword != "owner-secret" || filepath.Base(c.MemoryRoot) != "knowledge" {
		t.Fatal("configuration not retained")
	}
}

func TestLoopbackAddressesSupportLocalPasswordFreeMode(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:11520", "localhost:11520", "[::1]:11520"} {
		if _, err := loadText(t, "sun", "bind_addr: '"+addr+"'\n"); err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
	}
	for _, addr := range []string{"[::]:11520", "192.168.1.2:11520", "public.example:11520"} {
		if _, err := loadText(t, "sun", "bind_addr: '"+addr+"'\n"); err == nil {
			t.Fatal("remote address accepted without owner password", addr)
		}
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ role, text string }{
		{"sun", "bind_addr: 0.0.0.0:11520"}, {"sun", "bind_addr: :11520"},
		{"sun", "unknown: true"}, {"sun", "role: earth"}, {"sun", "bind_addr: invalid"}, {"sun", "bind_addr: localhost:99999"}, {"sun", "role: sun\n---\nrole: sun"}, {"sun", "role: sun\n---\n["}, {"sun", "["}, {"other", "role: other"},
		{"earth", "{}"}, {"earth", "sun_url: ftp://localhost\nrunner_id: bad\nstate_dir: state"}, {"earth", "sun_url: http://remote.example\nrunner_id: 11111111-1111-4111-8111-111111111111\nstate_dir: state"}, {"earth", "sun_url: https://example.com\nrunner_id: bad\nstate_dir: state"}, {"earth", "sun_url: https://example.com\nrunner_id: 11111111-1111-4111-8111-111111111111"},
	} {
		t.Run(tc.role+tc.text, func(t *testing.T) {
			if _, err := loadText(t, tc.role, tc.text); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}
