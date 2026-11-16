// Package settingspec defines the one description of a plugin setting shared by
// every extension kind in TresorBot.
//
// Agent adapters and input adapters both declare their configuration the same
// way: a list of typed descriptors that the host validates, defaults and renders
// without knowing what the plugin does with the values. Keeping that vocabulary
// in one package means a new type or a validation rule cannot exist in one
// extension kind and be missing in the other.
package settingspec

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Type enumerates the value shapes a host can validate and render.
type Type string

const (
	String     Type = "string"
	Bool       Type = "bool"
	Int        Type = "int"
	Path       Type = "path"
	StringList Type = "string_list"
	Enum       Type = "enum"
)

// Descriptor declares one plugin setting. The host validates against this
// without knowing what the plugin does with the value.
type Descriptor struct {
	Key         string   `json:"key"`
	Type        Type     `json:"type"`
	Required    bool     `json:"required,omitempty"`
	Title       string   `json:"title,omitempty"`
	Description string   `json:"description,omitempty"`
	Default     any      `json:"default,omitempty"`
	Values      []string `json:"values,omitempty"`
	// Sensitive marks a value the host must not echo back to a client or write
	// to a log: tokens, passwords and signing secrets. It changes nothing about
	// validation, only about how the value is reported.
	Sensitive bool `json:"sensitive,omitempty"`
}

// Validate checks one setting descriptor.
func (s Descriptor) Validate() error {
	if s.Key == "" || len(s.Key) > 64 {
		return fmt.Errorf("invalid setting key %q", s.Key)
	}
	switch s.Type {
	case String, Bool, Int, Path, StringList:
	case Enum:
		if len(s.Values) == 0 {
			return fmt.Errorf("setting %q is an enum without values", s.Key)
		}
	default:
		return fmt.Errorf("setting %q has unknown type %q", s.Key, s.Type)
	}
	return nil
}

// ValidateSet checks every descriptor in a list, so a broken manifest is
// reported once at load time rather than on the first value that uses it.
func ValidateSet(descriptors []Descriptor) error {
	for _, d := range descriptors {
		if err := d.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Normalize validates values against the declared descriptors and fills in
// defaults. Unknown keys are rejected so a typo is reported instead of silently
// ignored.
//
// A declared default is only applied when the caller omitted the key: an absent
// setting means "do not override the plugin's own behaviour", which is a
// different thing from a setting whose value happens to equal the default.
func Normalize(descriptors []Descriptor, values map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(descriptors))
	known := make(map[string]Descriptor, len(descriptors))
	for _, d := range descriptors {
		if err := d.Validate(); err != nil {
			return nil, err
		}
		known[d.Key] = d
	}
	for k := range values {
		if _, ok := known[k]; !ok {
			return nil, fmt.Errorf("unknown setting %q", k)
		}
	}
	for _, d := range descriptors {
		v, present := values[d.Key]
		if !present || v == nil {
			if d.Default != nil {
				out[d.Key] = d.Default
				continue
			}
			if d.Required {
				return nil, fmt.Errorf("setting %q is required", d.Key)
			}
			continue
		}
		coerced, err := d.coerce(v)
		if err != nil {
			return nil, err
		}
		out[d.Key] = coerced
	}
	return out, nil
}

func (s Descriptor) coerce(v any) (any, error) {
	switch s.Type {
	case String, Path:
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("setting %q must be a string", s.Key)
		}
		if s.Type == Path && str != "" && !filepath.IsAbs(str) {
			return nil, fmt.Errorf("setting %q must be an absolute path", s.Key)
		}
		return str, nil
	case Bool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("setting %q must be a boolean", s.Key)
		}
		return b, nil
	case Int:
		switch n := v.(type) {
		case int:
			return n, nil
		case int64:
			return n, nil
		case float64:
			if float64(int64(n)) != n {
				return nil, fmt.Errorf("setting %q must be an integer", s.Key)
			}
			return int64(n), nil
		default:
			return nil, fmt.Errorf("setting %q must be an integer", s.Key)
		}
	case StringList:
		list, ok := v.([]any)
		if !ok {
			if ls, ok := v.([]string); ok {
				out := make([]string, len(ls))
				copy(out, ls)
				return out, nil
			}
			return nil, fmt.Errorf("setting %q must be a list of strings", s.Key)
		}
		out := make([]string, 0, len(list))
		for _, item := range list {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("setting %q must be a list of strings", s.Key)
			}
			out = append(out, str)
		}
		return out, nil
	case Enum:
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("setting %q must be a string", s.Key)
		}
		for _, allowed := range s.Values {
			if str == allowed {
				return str, nil
			}
		}
		return nil, fmt.Errorf("setting %q must be one of %s", s.Key, strings.Join(s.Values, ", "))
	}
	return nil, fmt.Errorf("setting %q has unknown type", s.Key)
}

// EnvRefPrefix marks a string value that names an environment variable instead
// of carrying the secret itself ("env:SLACK_BOT_TOKEN").
//
// This is how a secret reaches a plugin without being stored: the operator's
// systemd EnvironmentFile holds the value, the host resolves the reference at
// configuration time, and no database row, API response or log line ever
// contains it.
const EnvRefPrefix = "env:"

// ResolveEnvRef reports the variable name when a string is an environment
// reference, and whether it is one at all.
func ResolveEnvRef(value string) (string, bool) {
	if !strings.HasPrefix(value, EnvRefPrefix) {
		return "", false
	}
	name := strings.TrimSpace(strings.TrimPrefix(value, EnvRefPrefix))
	if name == "" {
		return "", false
	}
	return name, true
}

// IsEnvRef reports whether a value is an environment reference.
func IsEnvRef(value string) bool {
	_, ok := ResolveEnvRef(value)
	return ok
}
