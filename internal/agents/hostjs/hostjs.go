// Package hostjs carries the agent host's JavaScript source inside the Go
// binary.
//
// The host is part of Earth's runtime, not part of the adapter catalogue: an
// adapter is a plugin a user installs, while the host is the thing that runs
// them. Keeping it here means `go build` produces a working Earth with no
// repository to clone, and the host can never drift out of version with the
// binary that speaks to it.
//
// The adapter repository holds only adapters, which are fetched on demand.
package hostjs

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

//go:embed protocol.mjs registry.mjs host.mjs tresorbot-mcp.mjs
var files embed.FS

// Names lists the modules in load order, so a materialized copy is complete
// before anything tries to import it.
var Names = []string{"protocol.mjs", "registry.mjs", "host.mjs"}

// Entry is the module Earth runs.
const Entry = "host.mjs"

// MCP is the stdio MCP server an adapter launches when its agent speaks MCP to
// an external client.
//
// It lives here, beside the host, for the same reason the host does: it adapts
// Earth's *gateway* to MCP, so it tracks Earth's own HTTP contract rather than
// any individual adapter's needs. Shipping it inside the binary means it cannot
// drift out of version with the gateway it talks to. Adapter bundles
// deliberately do not carry it; they locate it at run time through MCPPathEnv.
//
// It is extracted alongside the host but is not listed in Names, because Names
// describes the host's own import order.
const MCP = "tresorbot-mcp.mjs"

// extracted lists everything Materialize writes. Digest and Materialize both
// range over it, so a file cannot be extracted without also being covered by the
// marker that decides whether to re-extract.
func extracted() []string {
	return append(append([]string{}, Names...), MCP)
}

// MCPPathEnv names the environment variable that carries the extracted bridge
// path to the host, and through it to every adapter.
//
// The host inherits its own environment, so this is the one channel that
// reliably reaches an adapter. Adapters fall back to a file beside themselves so
// a bundle carrying its own copy still works.
const MCPPathEnv = "TRESORBOT_MCP"

// Source returns one embedded module.
func Source(name string) ([]byte, error) { return files.ReadFile(name) }

// Digest identifies the embedded host build. It is written next to the
// materialized copy so an upgraded binary replaces a stale extraction instead of
// running last version's host against this version's protocol.
//
// The bridge is included: changing it must invalidate the extraction, or an
// upgraded binary would keep launching the previous bridge.
func Digest() string {
	h := sha256.New()
	for _, name := range extracted() {
		data, err := files.ReadFile(name)
		if err != nil {
			continue
		}
		fmt.Fprintf(h, "%d:%s%d:%s", len(name), name, len(data), data)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// retired names modules this package used to extract under a different name.
//
// The MCP bridge was first shipped as tresor-mcp-bridge.mjs: "Tresor" is a
// sibling product, not this one, so it was renamed. A host directory upgraded in
// place would otherwise keep the old copy next to the live one, and anything
// resolving the bridge by a sibling path would find the stale file.
var retired = []string{"tresor-mcp-bridge.mjs", "tresorbot-mcp-bridge.mjs"}

// pruneRetired removes retired modules from an existing host directory.
//
// Only names on the retired list are touched, never a directory scan: this
// directory also holds conversation state that must not be disturbed.
func pruneRetired(dir string) error {
	for _, name := range retired {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Materialize writes the host into dir and returns the path to its entry module.
//
// The copy is skipped when the digest marker already matches, so restarting Earth
// does not rewrite files under a running host. Writing is atomic per file: a
// crash mid-extraction leaves the previous copy intact rather than a half-written
// module that fails to import.
func Materialize(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("host directory is required")
	}
	marker := filepath.Join(dir, ".version")
	digest := Digest()
	if current, err := os.ReadFile(marker); err == nil && string(current) == digest {
		// Pruning still runs on the skip path: a retired module can be present in
		// a directory whose current files all match, which is exactly the state an
		// in-place upgrade from a renamed module leaves behind.
		if err := pruneRetired(dir); err != nil {
			return "", err
		}
		return filepath.Join(dir, Entry), nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	for _, name := range extracted() {
		data, err := files.ReadFile(name)
		if err != nil {
			return "", err
		}
		if err := writeAtomic(filepath.Join(dir, name), data); err != nil {
			return "", err
		}
	}
	// A module this binary no longer ships is removed, so a renamed file cannot
	// linger beside its replacement. Only known host modules are considered: the
	// directory also holds conversation state and the host's own bookkeeping.
	if err := pruneRetired(dir); err != nil {
		return "", err
	}
	// Written last: a marker that exists implies every module is already in place.
	if err := writeAtomic(marker, []byte(digest)); err != nil {
		return "", err
	}
	return filepath.Join(dir, Entry), nil
}

// MCPPath returns where Materialize put the MCP server inside dir.
//
// Callers pass this to the host via MCPPathEnv. It is derived from dir rather
// than returned by Materialize so existing callers keep one return value.
func MCPPath(dir string) string { return filepath.Join(dir, MCP) }

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
