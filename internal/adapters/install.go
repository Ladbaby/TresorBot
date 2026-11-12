package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// MaxBundleBytes bounds a downloaded adapter package. Adapters are small — the
// pi adapter is 17 KB — so anything near this limit is not an adapter.
const MaxBundleBytes = 4 << 20

// Bundle is the published form of an adapter, mirroring Tresor's plugin
// packages: a JSON object of relative path to file contents. Shipping one JSON
// file rather than a tarball keeps installation to a single verified fetch with
// no archive parser in the trust path.
type Bundle struct {
	Files map[string]string `json:"files"`
}

// InstallInput is one installation request.
type InstallInput struct {
	URL string
	// SHA256 is the expected canonical source digest. Optional, like Tresor's
	// `plugin install-url --sha256`: HTTPS alone does not establish that the bytes
	// are the adapter you meant to install, so supplying it is what turns a
	// download into a verified install.
	SHA256 string
	// Dir is the adapter catalogue root. Empty uses the default.
	Dir string
	// Client overrides the HTTP client, so a test can trust its own TLS
	// certificate without weakening the default one.
	Client *http.Client
}

// Installed describes one adapter present in the catalogue.
type Installed struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	DisplayName string `json:"display_name,omitempty"`
	Digest      string `json:"digest"`
	Path        string `json:"path"`
	// Verified reports that the bundle matched a digest the caller supplied.
	Verified bool `json:"verified"`
}

// SourceDigest identifies a bundle's contents, matching Tresor's scheme so the
// digest an author publishes is the digest an operator passes to install.
//
// Lengths are prefixed so that moving bytes between files changes the digest;
// without them, renaming a file or shifting a boundary would collide.
func SourceDigest(files map[string]string) (string, error) {
	if len(files) == 0 || len(files) > 256 {
		return "", errors.New("bundle has an invalid file count")
	}
	names := make([]string, 0, len(files))
	seen := map[string]bool{}
	total := 0
	for name, body := range files {
		total += len(name) + len(body)
		if !safeRelativePath(name) {
			return "", fmt.Errorf("unsafe file path %q", name)
		}
		lower := strings.ToLower(name)
		if seen[lower] {
			return "", fmt.Errorf("duplicate file %q", name)
		}
		seen[lower] = true
		names = append(names, name)
	}
	if total > MaxBundleBytes {
		return "", errors.New("bundle exceeds the source limit")
	}
	if _, ok := files["agent.json"]; !ok {
		return "", errors.New("agent.json is required")
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%d:%s%d:%s", len(name), name, len(files[name]), files[name])
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// safeRelativePath rejects absolute paths, traversal, and NUL bytes, so a bundle
// cannot write outside its own adapter directory.
func safeRelativePath(name string) bool {
	if name == "" || len(name) > 256 || strings.ContainsRune(name, 0) {
		return false
	}
	if strings.ContainsAny(name, "\\") || strings.HasPrefix(name, "/") {
		return false
	}
	if filepath.Clean(name) != name {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." || part == "." || part == "" {
			return false
		}
	}
	return true
}

// manifest is the subset of agent.json that installation needs.
type manifest struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	// Requires lists npm packages the adapter needs at runtime. A single string is
	// also accepted, because a one-dependency adapter reads better that way.
	Requires stringList `json:"requires,omitempty"`
}

// stringList accepts either a JSON string or an array of strings.
type stringList []string

func (s *stringList) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*s = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

var adapterNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9/_-]{0,63}$`)

// Install downloads, verifies, and writes an adapter, then installs any npm
// dependency it declares.
//
// Verification happens before anything is written: a bundle whose digest does
// not match is never extracted, so a failed install cannot leave a half-written
// adapter that the host would then try to load.
func Install(ctx context.Context, in InstallInput) (Installed, error) {
	if in.SHA256 != "" {
		raw, err := hex.DecodeString(in.SHA256)
		if err != nil || len(raw) != 32 || strings.ToLower(in.SHA256) != in.SHA256 {
			return Installed{}, errors.New("--sha256 must be lowercase hexadecimal")
		}
	}
	u, err := url.Parse(in.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return Installed{}, errors.New("adapter URL must be public HTTPS without credentials or fragment")
	}
	dir := in.Dir
	if dir == "" {
		return Installed{}, errors.New("adapter directory is required")
	}

	body, err := fetch(ctx, in.URL, in.Client)
	if err != nil {
		return Installed{}, err
	}
	var bundle Bundle
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return Installed{}, fmt.Errorf("invalid adapter bundle: %w", err)
	}
	actual, err := SourceDigest(bundle.Files)
	if err != nil {
		return Installed{}, err
	}
	// Compared only when the caller pinned a digest, which is the only case where
	// there is something to compare against.
	if in.SHA256 != "" && actual != in.SHA256 {
		return Installed{}, fmt.Errorf("digest mismatch: bundle is %s, expected %s", actual, in.SHA256)
	}
	var m manifest
	if err := json.Unmarshal([]byte(bundle.Files["agent.json"]), &m); err != nil {
		return Installed{}, fmt.Errorf("invalid agent.json: %w", err)
	}
	if !adapterNamePattern.MatchString(m.Name) {
		return Installed{}, fmt.Errorf("invalid adapter name %q", m.Name)
	}
	if strings.TrimSpace(m.Version) == "" {
		return Installed{}, errors.New("agent.json must declare a version")
	}

	target := filepath.Join(dir, filepath.FromSlash(m.Name))
	if err := os.MkdirAll(target, 0700); err != nil {
		return Installed{}, err
	}
	for name, content := range bundle.Files {
		path := filepath.Join(target, filepath.FromSlash(name))
		// Defence in depth: SourceDigest already rejected traversal, and this
		// re-checks the joined path stays inside the adapter directory.
		if !strings.HasPrefix(path, target+string(os.PathSeparator)) {
			return Installed{}, fmt.Errorf("file %q escapes the adapter directory", name)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return Installed{}, err
		}
		if err := writeAtomic(path, []byte(content)); err != nil {
			return Installed{}, err
		}
	}

	out := Installed{Name: m.Name, Version: m.Version, Digest: actual, Path: target, Verified: in.SHA256 != ""}
	if raw, ok := bundle.Files["agent.json"]; ok {
		var full struct {
			DisplayName string `json:"display_name"`
		}
		if json.Unmarshal([]byte(raw), &full) == nil {
			out.DisplayName = full.DisplayName
		}
	}
	if len(m.Requires) > 0 {
		if err := npmInstall(ctx, target, m.Requires); err != nil {
			return out, fmt.Errorf("adapter installed but its dependency failed: %w", err)
		}
	}
	return out, nil
}

// npmInstall fetches an adapter's runtime dependency into its own directory, so
// adapters cannot interfere with each other's dependency versions.
func npmInstall(ctx context.Context, dir string, packages []string) error {
	npm, err := exec.LookPath("npm")
	if err != nil {
		return fmt.Errorf("npm is required to install %s: %w", strings.Join(packages, ", "), err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	args := append([]string{"install", "--omit=dev", "--no-audit", "--no-fund"}, packages...)
	cmd := exec.CommandContext(ctx, npm, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "npm_config_yes=true")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// The output is what tells an operator whether this was a network problem
		// or a missing package, so it is worth surfacing rather than a bare code.
		return fmt.Errorf("%w: %s", err, tail(string(output), 500))
	}
	return nil
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

func fetch(ctx context.Context, rawURL string, client *http.Client) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{
			// A redirect could move the download to a host the digest was not chosen
			// for; the digest still protects the bytes, but refusing keeps the trust
			// decision in one place.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed: %s", res.Status)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, MaxBundleBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxBundleBytes {
		return nil, errors.New("bundle exceeds the download limit")
	}
	return body, nil
}

// List reports the adapters present in a catalogue directory.
func List(dir string) ([]Installed, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := []Installed{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(filepath.Join(path, "agent.json"))
		if err != nil {
			continue
		}
		var m manifest
		if json.Unmarshal(raw, &m) != nil || m.Name == "" {
			continue
		}
		var full struct {
			DisplayName string `json:"display_name"`
		}
		_ = json.Unmarshal(raw, &full)
		out = append(out, Installed{
			Name: m.Name, Version: m.Version, DisplayName: full.DisplayName, Path: path,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Remove deletes an installed adapter. The name is validated first so a crafted
// argument cannot reach outside the catalogue.
func Remove(dir, name string) error {
	if !adapterNamePattern.MatchString(name) {
		return fmt.Errorf("invalid adapter name %q", name)
	}
	path := filepath.Join(dir, filepath.FromSlash(name))
	if path != filepath.Clean(dir) && !strings.HasPrefix(path, filepath.Clean(dir)+string(os.PathSeparator)) {
		return errors.New("adapter path escapes the catalogue")
	}
	if _, err := os.Stat(filepath.Join(path, "agent.json")); err != nil {
		return fmt.Errorf("adapter %q is not installed", name)
	}
	return os.RemoveAll(path)
}

// writeAtomic replaces a file in one step, so a crash cannot leave a partial
// module for the host to import.
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
