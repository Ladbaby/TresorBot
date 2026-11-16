package environments

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tresorbot/internal/settingspec"
)

// ManifestName is the descriptor file inside each input directory.
//
// It differs from the agent side's "agent.json" on purpose: both catalogues can
// then live in one directory tree without a file name collision forcing one to
// win, and a human reading a directory listing can tell which kind it is.
const ManifestName = "environment.json"

// CapabilityWebhook marks an adapter that can receive HTTP requests through
// Sun's generic ingress route. Sun routes on this alone — it must never guess
// from the adapter's name whether a POST can be forwarded to it.
const CapabilityWebhook = "webhook"

// Manifest is an input adapter's on-disk descriptor. It mirrors the agent
// manifest field for field where the concept carries over, so an author who has
// written one has written the other.
type Manifest struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	// Entry is the adapter module, relative to the manifest directory.
	Entry string `json:"entry"`
	// Protocol is the host protocol the adapter was written against.
	Protocol int `json:"protocol"`
	// Capabilities are labels Sun acts on. Only CapabilityWebhook changes
	// behaviour today; anything else is advisory and rendered in the dashboard.
	Capabilities []string `json:"capabilities,omitempty"`
	// Requires names the Node packages the adapter imports, so a missing
	// dependency is reported at install time rather than on the first event.
	Requires []string `json:"requires,omitempty"`
	// Settings declares the configuration Sun collects and validates.
	Settings []settingspec.Descriptor `json:"settings,omitempty"`
	// Limits lets an adapter declare its own ceiling rather than having Sun guess
	// one: an IMAP poller and a busy webhook receiver have very different shapes.
	Limits *ManifestLimits `json:"limits,omitempty"`
}

// ManifestLimits is an adapter's declared resource ceiling.
type ManifestLimits struct {
	// MaxInstances caps how many configured instances of this adapter may run.
	// Zero or absent means "no adapter-specific limit".
	MaxInstances int `json:"max_instances,omitempty"`
}

// LoadedEnvironment is a validated manifest plus provenance.
type LoadedEnvironment struct {
	Manifest
	// Dir is the absolute input directory.
	Dir string
	// EntryPath is the absolute entry module.
	EntryPath string
	// Digest fingerprints the manifest and entry module so a reload can tell
	// whether anything actually changed.
	Digest string
	// ModTime is the newest mtime of the files that make up the input.
	ModTime time.Time
}

// SupportsWebhook reports whether the adapter declared the webhook capability.
func (m Manifest) SupportsWebhook() bool {
	for _, c := range m.Capabilities {
		if c == CapabilityWebhook {
			return true
		}
	}
	return false
}

// ValidateEnvironmentName mirrors the adapter slug rule so a name survives a round
// trip through a directory name, a manifest and a JSON field without quoting.
func ValidateEnvironmentName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}

// Validate checks a manifest's structure. It must be strict: an input is loaded
// from a directory the operator chose, and a silent typo would surface as a
// confusing failure on the first event instead.
func (m Manifest) Validate() error {
	if !ValidateEnvironmentName(m.Name) {
		return fmt.Errorf("invalid input name %q", m.Name)
	}
	if strings.TrimSpace(m.Version) == "" || len(m.Version) > 64 {
		return errors.New("input version is required and must be at most 64 characters")
	}
	if m.Protocol != ProtocolVersion {
		return fmt.Errorf("input protocol %d does not match %d", m.Protocol, ProtocolVersion)
	}
	if strings.TrimSpace(m.Entry) == "" {
		return errors.New("input entry is required")
	}
	if filepath.IsAbs(m.Entry) || strings.Contains(m.Entry, "..") {
		return errors.New("input entry must be a relative path inside the input directory")
	}
	seen := map[string]bool{}
	for _, s := range m.Settings {
		if err := s.Validate(); err != nil {
			return err
		}
		if seen[s.Key] {
			return fmt.Errorf("duplicate setting %q", s.Key)
		}
		seen[s.Key] = true
	}
	seenCapability := map[string]bool{}
	for _, c := range m.Capabilities {
		if strings.TrimSpace(c) == "" || len(c) > 64 {
			return fmt.Errorf("invalid capability %q", c)
		}
		if seenCapability[c] {
			return fmt.Errorf("duplicate capability %q", c)
		}
		seenCapability[c] = true
	}
	if m.Limits != nil && m.Limits.MaxInstances < 0 {
		return errors.New("limits.max_instances cannot be negative")
	}
	return nil
}

// Discover scans dir for input adapters and returns the valid ones. Invalid
// adapters are reported through problems instead of aborting, so one broken
// adapter never prevents Sun from starting — the same contract as
// agents.Discover.
func Discover(dir string) (loaded []LoadedEnvironment, problems map[string]error, err error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	problems = map[string]error{}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		environmentDir := filepath.Join(dir, e.Name())
		loadedEnvironment, loadErr := loadEnvironment(environmentDir)
		if loadErr != nil {
			problems[e.Name()] = loadErr
			continue
		}
		loaded = append(loaded, loadedEnvironment)
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Name < loaded[j].Name })
	return loaded, problems, nil
}

func loadEnvironment(dir string) (LoadedEnvironment, error) {
	manifestPath := filepath.Join(dir, ManifestName)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return LoadedEnvironment{}, fmt.Errorf("read %s: %w", ManifestName, err)
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&m); err != nil {
		return LoadedEnvironment{}, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	if err = m.Validate(); err != nil {
		return LoadedEnvironment{}, err
	}
	entry := filepath.Join(dir, filepath.FromSlash(m.Entry))
	info, err := os.Stat(entry)
	if err != nil || !info.Mode().IsRegular() {
		return LoadedEnvironment{}, fmt.Errorf("entry module %q is missing", m.Entry)
	}
	entryRaw, err := os.ReadFile(entry)
	if err != nil {
		return LoadedEnvironment{}, err
	}
	h := sha256.New()
	h.Write(raw)
	h.Write([]byte{0})
	h.Write(entryRaw)
	newest := info.ModTime()
	if fi, e := os.Stat(manifestPath); e == nil && fi.ModTime().After(newest) {
		newest = fi.ModTime()
	}
	return LoadedEnvironment{
		Manifest:  m,
		Dir:       dir,
		EntryPath: entry,
		Digest:    hex.EncodeToString(h.Sum(nil)),
		ModTime:   newest,
	}, nil
}

// Dir resolves the input catalogue root. It mirrors config.AgentsDir so both
// catalogues live under the same per-user root and are backed up together.
func Dir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve inputs directory: %w", err)
	}
	return filepath.Join(home, ".tresorbot", "inputs"), nil
}

// NormalizeSettings validates an instance's values against the manifest and
// resolves "env:NAME" references from getenv.
//
// Resolution happens here rather than in the adapter for one reason: a secret
// must never be written to the database, echoed by the API, or logged. Sun holds
// the value only long enough to hand it to the host in a config frame. A
// sensitive setting whose reference cannot be resolved is a configuration error,
// not an empty string, because starting an adapter with a blank token produces a
// far more confusing failure later.
func NormalizeSettings(m Manifest, values map[string]any, getenv func(string) string) (map[string]any, error) {
	normalized, err := settingspec.Normalize(m.Settings, values)
	if err != nil {
		return nil, err
	}
	sensitive := map[string]bool{}
	for _, d := range m.Settings {
		sensitive[d.Key] = d.Sensitive
	}
	for key, value := range normalized {
		str, ok := value.(string)
		if !ok {
			continue
		}
		name, isRef := settingspec.ResolveEnvRef(str)
		if !isRef {
			continue
		}
		if getenv == nil {
			return nil, fmt.Errorf("setting %q references %s but no environment is available", key, name)
		}
		resolved := getenv(name)
		if strings.TrimSpace(resolved) == "" {
			return nil, fmt.Errorf("setting %q references unset environment variable %s", key, name)
		}
		normalized[key] = resolved
	}
	return normalized, nil
}

// Redact replaces sensitive setting values so a config can be shown to the owner
// without exposing a secret it never stored anyway.
func Redact(m Manifest, values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	sensitive := map[string]bool{}
	for _, d := range m.Settings {
		sensitive[d.Key] = d.Sensitive
	}
	for k, v := range values {
		if sensitive[k] {
			out[k] = ""
			continue
		}
		out[k] = v
	}
	return out
}
