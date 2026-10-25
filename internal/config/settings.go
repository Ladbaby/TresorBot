package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrConfigConflict = errors.New("configuration changed; reload settings before saving")
var configWriteMu sync.Mutex

func FileRevision(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// UpdateEarthSettings preserves enrollment and unknown/commented YAML content.
func UpdateEarthSettings(path, revision string, fields map[string]string) (Config, string, error) {
	configWriteMu.Lock()
	defer configWriteMu.Unlock()
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, "", err
	}
	sum := sha256.Sum256(b)
	if revision != hex.EncodeToString(sum[:]) {
		return Config{}, "", ErrConfigConflict
	}
	var doc yaml.Node
	if err = yaml.Unmarshal(b, &doc); err != nil {
		return Config{}, "", err
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Config{}, "", errors.New("configuration must be a YAML mapping")
	}
	mapping := doc.Content[0]
	for key, value := range fields {
		switch key {
		case "default_workspace", "node_path", "pi_path", "pi_cli_path":
		default:
			return Config{}, "", errors.New("unsupported Earth setting")
		}
		if strings.ContainsAny(value, "\r\n\x00") {
			return Config{}, "", errors.New("invalid setting value")
		}
		if key == "default_workspace" && value != "" {
			info, e := os.Stat(value)
			if !filepath.IsAbs(value) || e != nil || !info.IsDir() {
				return Config{}, "", errors.New("default workspace must be an existing absolute directory on Earth")
			}
		}
		found := false
		for i := 0; i < len(mapping.Content); i += 2 {
			if mapping.Content[i].Value == key {
				mapping.Content[i+1].Kind = yaml.ScalarNode
				mapping.Content[i+1].Tag = "!!str"
				mapping.Content[i+1].Value = value
				found = true
				break
			}
		}
		if !found {
			mapping.Content = append(mapping.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
		}
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return Config{}, "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".earth-settings-*.yaml")
	if err != nil {
		return Config{}, "", err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(out); err != nil {
		tmp.Close()
		return Config{}, "", err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return Config{}, "", err
	}
	if err = tmp.Close(); err != nil {
		return Config{}, "", err
	}
	cfg, err := Load(name, "earth")
	if err != nil {
		return Config{}, "", err
	}
	if cfg.PiPath != "" && cfg.PiCLIPath != "" {
		return Config{}, "", errors.New("choose Pi path or Pi CLI path, not both")
	}
	// Catch external modifications during validation as well as stale browser drafts.
	current, err := FileRevision(path)
	if err != nil {
		return Config{}, "", err
	}
	if current != revision {
		return Config{}, "", ErrConfigConflict
	}
	if err = os.Rename(name, path); err != nil {
		return Config{}, "", err
	}
	next := sha256.Sum256(out)
	return cfg, hex.EncodeToString(next[:]), nil
}
