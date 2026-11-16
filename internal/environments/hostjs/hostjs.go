// Package hostjs carries the input host's JavaScript source inside the Sun
// binary.
//
// The host is part of Sun's runtime, not part of the input catalogue: an input is
// a plugin an operator installs, while the host is the thing that runs them.
// Keeping it here means `go build` produces a working Sun with no repository to
// clone, and the host can never drift out of version with the binary that speaks
// to it.
//
// This mirrors internal/agents/hostjs deliberately. The extraction rules are the
// same because the failure modes are the same: a half-written module that fails
// to import, a stale extraction running last version's protocol, and a renamed
// file lingering beside its replacement.
package hostjs

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

//go:embed protocol.mjs registry.mjs environment-host.mjs
var files embed.FS

// Names lists the modules in load order, so a materialized copy is complete
// before anything tries to import it.
var Names = []string{"protocol.mjs", "registry.mjs", "environment-host.mjs"}

// Entry is the module Sun runs.
const Entry = "environment-host.mjs"

// EnvironmentsDirEnv names the environment variable that tells the host where the input
// catalogue is. The host reads it once at start, so Sun and the host cannot
// disagree about which directory is authoritative.
const EnvironmentsDirEnv = "TRESORBOT_ENVIRONMENTS_DIR"

// Source returns one embedded module.
func Source(name string) ([]byte, error) { return files.ReadFile(name) }

// Digest identifies the embedded host build. It is written beside the
// materialized copy so an upgraded binary replaces a stale extraction instead of
// running last version's host against this version's protocol.
func Digest() string {
	h := sha256.New()
	for _, name := range Names {
		data, err := files.ReadFile(name)
		if err != nil {
			continue
		}
		fmt.Fprintf(h, "%d:%s%d:%s", len(name), name, len(data), data)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Materialize writes the host into dir and returns the path to its entry module.
//
// The copy is skipped when the digest marker already matches, so restarting Sun
// does not rewrite files under a running host. Writing is atomic per file, so a
// crash mid-extraction leaves the previous copy intact rather than a half-written
// module that fails to import.
func Materialize(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("input host directory is required")
	}
	marker := filepath.Join(dir, ".version")
	digest := Digest()
	if current, err := os.ReadFile(marker); err == nil && string(current) == digest {
		return filepath.Join(dir, Entry), nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	for _, name := range Names {
		data, err := files.ReadFile(name)
		if err != nil {
			return "", err
		}
		if err := writeAtomic(filepath.Join(dir, name), data); err != nil {
			return "", err
		}
	}
	// Written last: a marker that exists implies every module is already in place.
	if err := writeAtomic(marker, []byte(digest)); err != nil {
		return "", err
	}
	return filepath.Join(dir, Entry), nil
}

func writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".staging-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
