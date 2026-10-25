// Package runtimepath resolves installation metadata without executing launchers.
package runtimepath

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

type Options struct{ NodePath, PiPath, PiCLIPath string }
type Runtime struct{ NodePath, CLIPath string }

func regular(p string) bool { i, e := os.Stat(p); return e == nil && i.Mode().IsRegular() }
func executable(p string) (string, error) {
	if p == "" {
		return "", errors.New("empty executable")
	}
	p, e := exec.LookPath(p)
	if e != nil {
		return "", e
	}
	return filepath.Abs(p)
}
func packageCLI(root string) (string, error) {
	b, e := os.ReadFile(filepath.Join(root, "package.json"))
	if e != nil {
		return "", e
	}
	var manifest struct {
		Bin json.RawMessage `json:"bin"`
	}
	if e = json.Unmarshal(b, &manifest); e != nil {
		return "", e
	}
	var entry string
	if json.Unmarshal(manifest.Bin, &entry) != nil {
		var bins map[string]string
		if e = json.Unmarshal(manifest.Bin, &bins); e != nil {
			return "", e
		}
		entry = bins["pi"]
	}
	if entry == "" || filepath.IsAbs(entry) {
		return "", errors.New("invalid Pi package bin")
	}
	p := filepath.Join(root, filepath.FromSlash(entry))
	rel, e := filepath.Rel(root, p)
	if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.New("Pi bin escapes package")
	}
	real, e := filepath.EvalSymlinks(p)
	if e != nil {
		return "", e
	}
	realRoot, e := filepath.EvalSymlinks(root)
	if e != nil {
		return "", e
	}
	rel, e = filepath.Rel(realRoot, real)
	if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || !regular(real) {
		return "", errors.New("invalid Pi bin file")
	}
	return real, nil
}

var versionPattern = regexp.MustCompile(`^[0-9A-Za-z._+\-]+$`)

func managed(root string) (string, error) {
	b, e := os.ReadFile(filepath.Join(root, "install", "current-version"))
	if e != nil {
		return "", e
	}
	v := strings.TrimSpace(string(b))
	if v == "" || v == "." || v == ".." || !versionPattern.MatchString(v) {
		return "", errors.New("invalid managed Pi version")
	}
	return packageCLI(filepath.Join(root, "install", "releases", v, "node_modules", "@earendil-works", "pi-coding-agent"))
}
func fromPi(p string) (string, error) {
	abs, e := filepath.Abs(p)
	if e != nil {
		return "", e
	}
	if !regular(abs) {
		return "", fmt.Errorf("Pi executable not found: %s", abs)
	}
	dir := filepath.Dir(abs)
	if _, e = os.Stat(filepath.Join(dir, "pi-launcher.js")); e == nil {
		if cli, err := managed(filepath.Dir(dir)); err == nil {
			return cli, nil
		} else {
			return "", err
		}
	}
	real, e := filepath.EvalSymlinks(abs)
	if e == nil && strings.HasSuffix(strings.ToLower(real), ".js") {
		return real, nil
	}
	roots := []string{filepath.Join(dir, "node_modules", "@earendil-works", "pi-coding-agent"), filepath.Join(filepath.Dir(dir), "lib", "node_modules", "@earendil-works", "pi-coding-agent")}
	for _, root := range roots {
		if cli, err := packageCLI(root); err == nil {
			return cli, nil
		}
	}
	return "", errors.New("unrecognized Pi wrapper; set pi_cli_path to its JavaScript entry or pi_path to a managed/npm Pi installation")
}
func activeNodeBin(home string)string{
 root:=os.Getenv("NVM_DIR");if root==""{root=filepath.Join(home,".nvm")}
 if regular(filepath.Join(root,"current","bin","node")){return filepath.Join(root,"current","bin")}
 b,e:=os.ReadFile(filepath.Join(root,"alias","default"));if e!=nil{return ""};v:=strings.TrimSpace(string(b));if !regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(v){return ""};if !strings.HasPrefix(v,"v"){v="v"+v};dir:=filepath.Join(root,"versions","node",v,"bin");if regular(filepath.Join(dir,"node")){return dir};return ""
}

func Resolve(o Options) (Runtime, error) {
	var r Runtime
	var e error
	if o.PiPath != "" && o.PiCLIPath != "" {
		return r, errors.New("choose pi_path or pi_cli_path, not both")
	}
	if o.PiCLIPath != "" {
		r.CLIPath, e = filepath.Abs(o.PiCLIPath)
		if e != nil || !regular(r.CLIPath) {
			return r, errors.New("pi_cli_path must name an installed Pi JavaScript file")
		}
	} else if o.PiPath != "" {
		p, err := exec.LookPath(o.PiPath)
		if err != nil {
			return r, err
		}
		r.CLIPath, e = fromPi(p)
		if e != nil {
			return r, e
		}
	} else {
		candidates := []string{}
		if p, err := exec.LookPath("pi"); err == nil {
			candidates = append(candidates, p)
		}
		home, _ := os.UserHomeDir()
        if active:=activeNodeBin(home);active!=""{candidates=append(candidates,filepath.Join(active,"pi"),filepath.Join(active,"pi.cmd"))}
		candidates = append(candidates, filepath.Join(home, ".pi", "agent", "bin", "pi"), filepath.Join(home, ".local", "bin", "pi"),filepath.Join(home,".npm-global","bin","pi"), "/opt/homebrew/bin/pi", "/usr/local/bin/pi")
		if runtime.GOOS == "windows" {
			candidates = append(candidates, filepath.Join(home, ".pi", "agent", "bin", "pi.cmd"), filepath.Join(os.Getenv("APPDATA"), "npm", "pi.cmd"))
		}
		for _, p := range candidates {
			if cli, err := fromPi(p); err == nil {
				r.CLIPath = cli
				break
			}
		}
		if r.CLIPath == "" {
			return r, errors.New("Pi not found; install Pi, add it to PATH, or set pi_path to the stable Pi command")
		}
	}
	if o.NodePath != "" {
		r.NodePath, e = executable(o.NodePath)
		if e != nil {
			return r, fmt.Errorf("node_path: %w", e)
		}
		return r, nil
	}
	r.NodePath, e = executable("node")
	if e == nil {
		return r, nil
	}
	home, _ := os.UserHomeDir()
	names := []string{filepath.Join(filepath.Dir(r.CLIPath), "node"), filepath.Join(home, ".local", "bin", "node"), filepath.Join(home, ".nvm", "current", "bin", "node"), "/opt/homebrew/bin/node", "/usr/local/bin/node", "/usr/bin/node"}
	if runtime.GOOS == "windows" {
		names = append(names, filepath.Join(os.Getenv("NVM_SYMLINK"), "node.exe"), filepath.Join(os.Getenv("ProgramFiles"), "nodejs", "node.exe"))
	}
	if active:=activeNodeBin(home);active!=""{names=append([]string{filepath.Join(active,"node"),filepath.Join(active,"node.exe")},names...)}
    // npm's versioned installation may live beside its Node binary.
    for dir:=filepath.Dir(r.CLIPath);dir!=filepath.Dir(dir);dir=filepath.Dir(dir){if filepath.Base(dir)=="lib"{names=append([]string{filepath.Join(filepath.Dir(dir),"bin","node")},names...);break}}
	for _, p := range names {
		if regular(p) {
			if r.NodePath, e = executable(p); e == nil {
				return r, nil
			}
		}
	}
	return r, errors.New("Node.js not found; add node to PATH or set node_path")
}
