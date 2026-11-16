package agents

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

// ManifestName is the descriptor file inside each adapter directory.
const ManifestName = "agent.json"

// Manifest is an adapter's on-disk descriptor. It is read by Earth for validation
// and by the host for discovery; both must agree, so the shape is versioned with
// the protocol.
type Manifest struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
	// Entry is the adapter module, relative to the manifest directory.
	Entry string `json:"entry"`
	// Protocol is the host protocol the adapter was written against.
	Protocol int `json:"protocol"`
	// Capabilities are advisory labels Earth exposes to Sun.
	Capabilities []string `json:"capabilities,omitempty"`
	// Requires names the Node package the adapter imports, so a mismatch is
	// reported at load time rather than mid-turn.
	Requires []string `json:"requires,omitempty"`
	// Settings declares the configuration Earth should collect and validate.
	Settings []SettingDescriptor `json:"settings,omitempty"`
	// Limits lets an adapter declare its own resource ceiling. Per-session cost
	// is a property of the agent — a pi session is ~0.4 MB in-process while a
	// Claude Code session spawns a ~131 MB child — so the cap belongs with the
	// adapter rather than in Earth's config.
	Limits *ManifestLimits `json:"limits,omitempty"`
}

// ManifestLimits is an adapter's declared resource ceiling.
type ManifestLimits struct {
	// MaxSessions caps concurrent sessions for this adapter alone. Zero or
	// absent means "use the host ceiling".
	MaxSessions int `json:"max_sessions,omitempty"`
}

// LoadedAdapter is a validated manifest plus provenance.
type LoadedAdapter struct {
	Manifest
	// Dir is the absolute adapter directory.
	Dir string
	// EntryPath is the absolute entry module.
	EntryPath string
	// Digest fingerprints the manifest and entry module so a hot reload can tell
	// whether anything actually changed.
	Digest string
	// ModTime is the newest mtime of the files that make up the adapter.
	ModTime time.Time
}

// Discover scans dir for adapters and returns the valid ones. Invalid adapters
// are reported through problems instead of aborting, so one broken adapter never
// prevents Earth from starting.
func Discover(dir string) (loaded []LoadedAdapter, problems map[string]error, err error) {
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
		adapterDir := filepath.Join(dir, e.Name())
		a, aerr := loadAdapter(adapterDir)
		if aerr != nil {
			problems[e.Name()] = aerr
			continue
		}
		loaded = append(loaded, a)
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].Name < loaded[j].Name })
	return loaded, problems, nil
}

func loadAdapter(dir string) (LoadedAdapter, error) {
	manifestPath := filepath.Join(dir, ManifestName)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return LoadedAdapter{}, fmt.Errorf("read %s: %w", ManifestName, err)
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&m); err != nil {
		return LoadedAdapter{}, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	if err = m.Validate(); err != nil {
		return LoadedAdapter{}, err
	}
	entry := filepath.Join(dir, filepath.FromSlash(m.Entry))
	info, err := os.Stat(entry)
	if err != nil || !info.Mode().IsRegular() {
		return LoadedAdapter{}, fmt.Errorf("entry module %q is missing", m.Entry)
	}
	entryRaw, err := os.ReadFile(entry)
	if err != nil {
		return LoadedAdapter{}, err
	}
	h := sha256.New()
	h.Write(raw)
	h.Write([]byte{0})
	h.Write(entryRaw)
	newest := info.ModTime()
	if fi, e := os.Stat(manifestPath); e == nil && fi.ModTime().After(newest) {
		newest = fi.ModTime()
	}
	return LoadedAdapter{
		Manifest:  m,
		Dir:       dir,
		EntryPath: entry,
		Digest:    hex.EncodeToString(h.Sum(nil)),
		ModTime:   newest,
	}, nil
}

// Validate checks a manifest's structure. It must be strict: an adapter is
// loaded from a directory the operator chose, and a silent typo would surface as
// a confusing mid-turn failure instead.
func (m Manifest) Validate() error {
	if !ValidAdapterName(m.Name) {
		return fmt.Errorf("invalid adapter name %q", m.Name)
	}
	if strings.TrimSpace(m.Version) == "" || len(m.Version) > 64 {
		return errors.New("adapter version is required and must be at most 64 characters")
	}
	if m.Protocol != ProtocolVersion {
		return fmt.Errorf("adapter protocol %d does not match %d", m.Protocol, ProtocolVersion)
	}
	if strings.TrimSpace(m.Entry) == "" {
		return errors.New("adapter entry is required")
	}
	if filepath.IsAbs(m.Entry) || strings.Contains(m.Entry, "..") {
		return errors.New("adapter entry must be a relative path inside the adapter directory")
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
	return nil
}

// ValidAdapterName reports whether name is a loadable adapter name.
func ValidAdapterName(name string) bool {
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

// Setting validation and normalisation live in internal/settingspec, shared with
// the input-side extension kind. These wrappers keep the agent package's callers
// unchanged while leaving exactly one definition of what a setting is.

// ValidateSet checks a list of descriptors, reporting the first invalid one.
func ValidateSet(descriptors []SettingDescriptor) error {
	return settingspec.ValidateSet(descriptors)
}

// Normalize validates values against the declared descriptors and fills in
// defaults. Unknown keys are rejected so a typo is reported instead of silently
// ignored.
func Normalize(descriptors []SettingDescriptor, values map[string]any) (map[string]any, error) {
	return settingspec.Normalize(descriptors, values)
}
