package environments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// ToolDefaultTimeout bounds one tool call made on behalf of a model turn.
//
// Shorter than the host's own ceiling so Sun reports the failure rather than
// waiting for the host to time out: an error the model can read is more useful
// than a turn that appears to hang.
const ToolDefaultTimeout = 25 * time.Second

// Schema limits. An adapter may describe its arguments only within a subset Sun
// validates and providers accept everywhere.
//
// The subset exists because of a failure recorded in another project: a tool
// schema using `allOf`/`if`-`then` was accepted by Sun, rejected wholesale by a
// strict-mode provider, and silently ended every turn that included it. The
// blast radius of a clever schema is the whole turn, so Sun refuses to pass one
// on rather than discovering the problem when a bot stops answering.
const (
	// MaxToolProperties bounds one tool's argument count.
	MaxToolProperties = 32
	// MaxToolEnumValues bounds an enum, which is rendered into the prompt in full.
	MaxToolEnumValues = 64
	// MaxToolDescriptionBytes bounds a tool description.
	MaxToolDescriptionBytes = 1024
	// MaxToolNameLength mirrors the adapter slug rule so a name survives a round
	// trip through a JSON field, a dashboard row and a model's tool call.
	MaxToolNameLength = 40
	// MaxToolsPerInstance bounds how many tools one instance may publish. A cron
	// adapter exposes a handful; a hundred means something is wrong.
	MaxToolsPerInstance = 32
)

// toolNameRe is the only tool-name shape Sun forwards.
//
// Lowercase and underscore rather than the environment rule's hyphen: the name is
// concatenated into a model-facing id ("<instance>.<tool>"), and a hyphen there
// reads as a subtraction to a model that is guessing at syntax.
var toolNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ErrInvalidToolSchema marks a schema Sun will never forward. It is a permanent
// fault in the adapter, not a transient failure, so it is reported at install and
// discovery time rather than retried per call.
var ErrInvalidToolSchema = errors.New("invalid tool schema")

// ValidToolName reports whether an adapter may publish this tool name.
func ValidToolName(name string) bool {
	return len(name) <= MaxToolNameLength && toolNameRe.MatchString(name)
}

// ValidateToolSchema checks that a JSON Schema stays inside the subset Sun
// forwards, and returns a description of the first violation.
//
// Deliberately structural rather than schema-library-based: the subset is small
// enough to check by hand, and a general validator would accept constructs this
// code must reject.
func ValidateToolSchema(raw json.RawMessage) error {
	if len(raw) == 0 {
		// A tool with no declared parameters takes nothing, which is legitimate.
		return nil
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return fmt.Errorf("%w: parameters must be a JSON object", ErrInvalidToolSchema)
	}
	return validateSchemaNode(schema, "parameters", true)
}

// ValidateMetaToolSchema is the same check with one documented exception: the
// root may carry a single open-object property named `arguments`.
//
// The exception exists for TresorBotToolCall, whose whole purpose is to carry an
// argument object shaped by an adapter's schema — a schema Sun has already
// validated separately. It is kept separate from ValidateToolSchema so an adapter
// can never obtain it: adapters that need structured input must flatten it.
func ValidateMetaToolSchema(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return fmt.Errorf("%w: parameters must be a JSON object", ErrInvalidToolSchema)
	}
	return validateSchemaNodeWithOpenArgument(schema)
}

// validateSchemaNodeWithOpenArgument permits `properties.arguments` to be an open
// object at the root only, then delegates the rest of the walk unchanged.
func validateSchemaNodeWithOpenArgument(root map[string]any) error {
	properties, _ := root["properties"].(map[string]any)
	if arguments, ok := properties["arguments"].(map[string]any); ok {
		if kind, _ := arguments["type"].(string); kind == "object" {
			// Only the two keys the exception needs. Anything else on this node is
			// still checked, so the exemption cannot carry a banned construct in
			// alongside it.
			for key := range arguments {
				switch key {
				case "type", "description", "additionalProperties":
				default:
					return fmt.Errorf("%w: parameters.properties.arguments may only declare type, description and additionalProperties", ErrInvalidToolSchema)
				}
			}
			if additional, ok := arguments["additionalProperties"]; ok && additional != true {
				return fmt.Errorf("%w: parameters.properties.arguments must allow additionalProperties", ErrInvalidToolSchema)
			}
			// Remove it for the strict walk, then restore it: the remaining
			// properties and every other rule still apply.
			delete(properties, "arguments")
			err := validateSchemaNode(root, "parameters", true)
			properties["arguments"] = arguments
			return err
		}
	}
	return validateSchemaNode(root, "parameters", true)
}

// validateSchemaNode walks one node. root distinguishes the tool's own object
// from a nested property, because only the root must be an object.
func validateSchemaNode(node map[string]any, path string, root bool) error {
	// Anything that makes a provider reinterpret the schema is refused outright.
	for _, banned := range []string{"allOf", "anyOf", "oneOf", "not", "$ref", "patternProperties", "dependentSchemas", "$defs", "definitions", "if", "then", "else"} {
		if _, ok := node[banned]; ok {
			return fmt.Errorf("%w: %s uses %q, which strict providers reject outright", ErrInvalidToolSchema, path, banned)
		}
	}
	if raw, ok := node["type"]; ok {
		kind, ok := raw.(string)
		if !ok {
			return fmt.Errorf("%w: %s has a non-string type", ErrInvalidToolSchema, path)
		}
		switch kind {
		case "object":
			if !root {
				// Nested objects are refused because provider support for them
				// varies; a flat argument list is expressible without them.
				return fmt.Errorf("%w: %s is a nested object, which this subset does not allow", ErrInvalidToolSchema, path)
			}
		case "string", "number", "integer", "boolean":
		case "array":
			items, ok := node["items"].(map[string]any)
			if !ok {
				return fmt.Errorf("%w: %s is an array without an items schema", ErrInvalidToolSchema, path)
			}
			if err := validateSchemaNode(items, path+"[]", false); err != nil {
				return err
			}
			if _, ok := items["type"].(string); !ok {
				return fmt.Errorf("%w: %s[] has no type", ErrInvalidToolSchema, path)
			}
			if items["type"] == "object" || items["type"] == "array" {
				return fmt.Errorf("%w: %s[] is an array of %v, which this subset does not allow", ErrInvalidToolSchema, path, items["type"])
			}
		default:
			return fmt.Errorf("%w: %s has unsupported type %q", ErrInvalidToolSchema, path, kind)
		}
	} else if root {
		return fmt.Errorf("%w: %s must declare type \"object\"", ErrInvalidToolSchema, path)
	}
	if additional, ok := node["additionalProperties"]; ok {
		if allowed, ok := additional.(bool); !ok || allowed {
			return fmt.Errorf("%w: %s must set additionalProperties: false", ErrInvalidToolSchema, path)
		}
	}
	if values, ok := node["enum"]; ok {
		list, ok := values.([]any)
		if !ok {
			return fmt.Errorf("%w: %s has a non-array enum", ErrInvalidToolSchema, path)
		}
		if len(list) > MaxToolEnumValues {
			return fmt.Errorf("%w: %s has %d enum values, above %d", ErrInvalidToolSchema, path, len(list), MaxToolEnumValues)
		}
		for _, value := range list {
			switch value.(type) {
			case string, float64, bool:
			default:
				return fmt.Errorf("%w: %s has a non-scalar enum value", ErrInvalidToolSchema, path)
			}
		}
	}
	if description, ok := node["description"]; ok {
		text, ok := description.(string)
		if !ok {
			return fmt.Errorf("%w: %s has a non-string description", ErrInvalidToolSchema, path)
		}
		if len(text) > MaxToolDescriptionBytes {
			return fmt.Errorf("%w: %s description exceeds %d bytes", ErrInvalidToolSchema, path, MaxToolDescriptionBytes)
		}
	}
	properties, ok := node["properties"]
	if !ok {
		if root {
			// A parameterless tool is normal; an absent properties map is treated
			// as empty rather than as an error.
			return nil
		}
		return nil
	}
	props, ok := properties.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: %s.properties must be an object", ErrInvalidToolSchema, path)
	}
	if len(props) > MaxToolProperties {
		return fmt.Errorf("%w: %s has %d properties, above %d", ErrInvalidToolSchema, path, len(props), MaxToolProperties)
	}
	// Sorted so the reported violation is deterministic when a schema has several.
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		child, ok := props[name].(map[string]any)
		if !ok {
			return fmt.Errorf("%w: %s.properties.%s must be an object", ErrInvalidToolSchema, path, name)
		}
		if err := validateSchemaNode(child, path+".properties."+name, false); err != nil {
			return err
		}
	}
	if required, ok := node["required"]; ok {
		list, ok := required.([]any)
		if !ok {
			return fmt.Errorf("%w: %s.required must be an array", ErrInvalidToolSchema, path)
		}
		for _, entry := range list {
			name, ok := entry.(string)
			if !ok {
				return fmt.Errorf("%w: %s.required must be strings", ErrInvalidToolSchema, path)
			}
			if _, declared := props[name]; !declared {
				return fmt.Errorf("%w: %s.required names %q, which is not a property", ErrInvalidToolSchema, path, name)
			}
		}
	}
	return nil
}

// CatalogEntry is one callable tool: an instance, the adapter tool behind it, and
// the validated schema the model will eventually see.
type CatalogEntry struct {
	// Instance is the configured environment instance.
	Instance string
	// Environment is the adapter name, shown so a model can tell two instances of
	// the same adapter apart.
	Environment string
	// Tool is the adapter's own tool name.
	Tool string
	// Description is what the model reads when choosing.
	Description string
	// Parameters is the validated schema.
	Parameters json.RawMessage
	// ID is the stable identifier the model names in a call. It encodes the
	// instance because two instances of one adapter publish the same tool names.
	ID string
}

// Name is the model-facing tool name, "<instance>.<tool>".
//
// The dot form is used rather than the wire ID because it is what a model will
// guess at, and guessing right costs no round trip.
func (e CatalogEntry) Name() string { return e.Instance + "." + e.Tool }

// Signature renders a compact one-line argument summary for search results.
//
// This is the whole point of progressive disclosure: the model sees enough to
// decide "that is the one" without paying for the full schema of every tool.
func (e CatalogEntry) Signature() string {
	if len(e.Parameters) == 0 {
		return "()"
	}
	var schema struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if json.Unmarshal(e.Parameters, &schema) != nil || len(schema.Properties) == 0 {
		return "()"
	}
	required := map[string]bool{}
	for _, name := range schema.Required {
		required[name] = true
	}
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		kind := schema.Properties[name].Type
		if kind == "" {
			kind = "any"
		}
		if !required[name] {
			kind += "?"
		}
		parts = append(parts, name+": "+kind)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// ToolSource is what discovery needs from the host. An interface rather than a
// concrete *Host so the catalog can be built and tested without a Node process.
type ToolSource interface {
	Tools(ctx context.Context, instance string) ([]ToolDefinition, error)
}

// Catalog holds the tools every instance currently offers.
//
// Guarded by its own mutex because discovery runs on start/reload while a model
// turn may be searching concurrently: a half-built catalog must never be visible.
type Catalog struct {
	mu           sync.RWMutex
	entries      []CatalogEntry
	environments map[string]string // instance -> adapter name, for messages
	problems     map[string]string // instance -> why discovery failed
}

// NewCatalog returns an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{environments: map[string]string{}, problems: map[string]string{}}
}

// Entries returns a copy of the current catalog, in stable order.
func (c *Catalog) Entries() []CatalogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]CatalogEntry, len(c.entries))
	copy(out, c.entries)
	return out
}

// Problems reports instances whose tools could not be discovered.
func (c *Catalog) Problems() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.problems))
	for k, v := range c.problems {
		out[k] = v
	}
	return out
}

// Find resolves a model-facing name or a wire id to one entry.
//
// Both spellings are accepted because a model may echo either: the short name it
// saw in a search result, or the id from a describe. Accepting only one would
// turn a correct guess into an error.
func (c *Catalog) Find(name string) (CatalogEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, entry := range c.entries {
		if entry.ID == name || entry.Name() == name {
			return entry, true
		}
	}
	return CatalogEntry{}, false
}

// Instance is one configured environment that may publish tools.
type ToolInstance struct {
	Name        string
	Environment string
}

// Refresh rebuilds the catalog from the given instances.
//
// Every instance is asked every time rather than diffed against the previous
// catalog: an adapter may change its tool set for reasons Sun cannot see (a
// setting that enables a feature, say), and the cost is one round trip per
// enabled instance on start or reload, not per model turn.
func (c *Catalog) Refresh(ctx context.Context, source ToolSource, instances []ToolInstance) {
	entries := make([]CatalogEntry, 0, len(instances))
	problems := map[string]string{}
	known := map[string]bool{}
	seen := map[string]string{} // model-facing name -> owning instance

	for _, instance := range instances {
		if !known[instance.Environment] {
			known[instance.Environment] = true
		}
		tools, err := source.Tools(ctx, instance.Name)
		if err != nil {
			// One instance failing must not empty the catalog: its siblings keep
			// working, and the failure is reported rather than silently dropped.
			problems[instance.Name] = err.Error()
			continue
		}
		if len(tools) > MaxToolsPerInstance {
			problems[instance.Name] = fmt.Sprintf("published %d tools, above the %d limit", len(tools), MaxToolsPerInstance)
			continue
		}
		taken := map[string]bool{}
		for _, tool := range tools {
			if !ValidToolName(tool.Name) {
				problems[instance.Name] = fmt.Sprintf("tool name %q is not lowercase [a-z][a-z0-9_]* (max %d chars)", tool.Name, MaxToolNameLength)
				break
			}
			if taken[tool.Name] {
				problems[instance.Name] = fmt.Sprintf("duplicate tool name %q", tool.Name)
				break
			}
			taken[tool.Name] = true
			if err := ValidateToolSchema(tool.Parameters); err != nil {
				problems[instance.Name] = fmt.Sprintf("tool %q: %v", tool.Name, err)
				break
			}
			if len(tool.Description) > MaxToolDescriptionBytes {
				problems[instance.Name] = fmt.Sprintf("tool %q description exceeds %d bytes", tool.Name, MaxToolDescriptionBytes)
				break
			}
			entry := CatalogEntry{
				Instance:    instance.Name,
				Environment: instance.Environment,
				Tool:        tool.Name,
				Description: strings.TrimSpace(tool.Description),
				Parameters:  tool.Parameters,
				ID:          "environment:" + instance.Name + ":" + tool.Name,
			}
			// A name collision can only happen across instances sharing a name,
			// which the store forbids, but the check is cheap and the alternative
			// is a model calling an arbitrary one of two identically named tools.
			if previous, clash := seen[entry.Name()]; clash && previous != instance.Name {
				problems[instance.Name] = fmt.Sprintf("tool name %q collides with another instance", tool.Name)
				break
			}
			seen[entry.Name()] = instance.Name
			entries = append(entries, entry)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Instance != entries[j].Instance {
			return entries[i].Instance < entries[j].Instance
		}
		return entries[i].Tool < entries[j].Tool
	})

	c.mu.Lock()
	c.entries = entries
	c.problems = problems
	c.environments = map[string]string{}
	for _, instance := range instances {
		c.environments[instance.Name] = instance.Environment
	}
	c.mu.Unlock()
}
