package bottools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"tresorbot/internal/environments"
)

// The three meta-tools that make an environment's tools callable without
// advertising every one of them.
//
// Why three and not one: a model that can only call by name cannot discover, and
// one that can only search cannot recover the full schema it needs to fill in
// arguments correctly. Openclaw arrived at the same split (tool_search /
// tool_describe / dispatch_action); lingtai avoids the problem by never deferring
// at all, which is not available here because environments are third-party.
const (
	ToolSearchName   = "TresorBotToolSearch"
	ToolDescribeName = "TresorBotToolDescribe"
	ToolCallName     = "TresorBotToolCall"
)

// ToolBridge is what the meta-tools need from Sun's environment subsystem.
//
// An interface rather than a concrete *environments.Manager so this package does
// not import the runtime, and so a nil bridge cleanly means "no environments
// enabled" instead of forcing every caller to nil-check.
type ToolBridge interface {
	// Catalogue returns the tools currently offered, already validated.
	Catalogue() []environments.CatalogEntry
	// Call runs one tool on behalf of a bot.
	//
	// tool is the caller's name for the tool. It is passed separately from entry
	// because it is what the model actually wrote: entry is the catalogue's match,
	// and the two can differ ("morning.send" against a row for "send"). Logging or
	// refusing on the matched name alone would misreport what was attempted.
	Call(ctx context.Context, botID string, tool string, entry environments.CatalogEntry, arguments json.RawMessage) (json.RawMessage, error)
}

// ErrNoEnvironments is returned when an environment tool is used on a Sun with
// no environment subsystem configured.
var ErrNoEnvironments = errors.New("no environments are enabled")

// EnvironmentDefinitions returns the three meta-tools.
//
// They are advertised unconditionally, exactly like the builtin memory tools, and
// they never change shape. That stability is what keeps the MCP bridge's
// process-lifetime cache of /definitions correct while the underlying catalog
// changes freely underneath it.
func EnvironmentDefinitions() []map[string]any {
	return []map[string]any{
		{
			"name":        ToolSearchName,
			"description": "Search installed environments for a tool that does something. Call this before assuming you cannot act on a chat, schedule, repository or webhook. Returns matching tool names with their argument summary; follow up with TresorBotToolDescribe for the full schema, then TresorBotToolCall.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "maxLength": 200, "description": "Words describing the capability you need, for example 'schedule' or 'post message'."},
				},
				"required":             []string{"query"},
				"additionalProperties": false,
			},
			"operation": "execute-tool",
			"label":     ToolSearchName,
		},
		{
			"name":        ToolDescribeName,
			"description": "Show the full argument schema for one environment tool found with TresorBotToolSearch, before calling it.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tool": map[string]any{"type": "string", "maxLength": 128, "description": "A tool name from TresorBotToolSearch, for example 'cron.list_jobs'."},
				},
				"required":             []string{"tool"},
				"additionalProperties": false,
			},
			"operation": "execute-tool",
			"label":     ToolDescribeName,
		},
		{
			"name":        ToolCallName,
			"description": "Run one environment tool. Describe it first with TresorBotToolDescribe so the arguments match its schema. Effects happen in the outside world, so prefer read operations when you are unsure.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tool":      map[string]any{"type": "string", "maxLength": 128, "description": "A tool name from TresorBotToolSearch, for example 'cron.list_jobs'."},
					"arguments": map[string]any{"type": "object", "description": "Arguments matching the tool's schema.", "additionalProperties": true},
				},
				"required":             []string{"tool"},
				"additionalProperties": false,
			},
			"operation": "execute-tool",
			"label":     ToolCallName,
		},
	}
}

// IsEnvironmentTool reports whether a tool name belongs to the meta-tool set.
func IsEnvironmentTool(name string) bool {
	switch name {
	case ToolSearchName, ToolDescribeName, ToolCallName:
		return true
	}
	return false
}

// searchEnvironments ranks catalog entries against a query.
//
// Deliberately substring-and-prefix matching rather than BM25: the catalog is a
// handful of entries, and ranking machinery costs more attention than it earns at
// this size (openclaw's own docs note compaction is not always cheaper for small
// catalogs). Name matches outrank description matches so an exact tool name
// always surfaces first.
func searchEnvironments(entries []environments.CatalogEntry, query string) []environments.CatalogEntry {
	terms := strings.Fields(strings.ToLower(strings.TrimSpace(query)))
	if len(terms) == 0 {
		return entries
	}
	type scored struct {
		entry environments.CatalogEntry
		score int
	}
	results := make([]scored, 0, len(entries))
	for _, entry := range entries {
		haystack := strings.ToLower(entry.Name() + " " + entry.Tool + " " + entry.Environment + " " + entry.Description)
		score := 0
		for _, term := range terms {
			switch {
			case strings.Contains(strings.ToLower(entry.Tool), term):
				score += 10
			case strings.Contains(strings.ToLower(entry.Name()), term):
				score += 8
			case strings.Contains(haystack, term):
				score += 2
			}
		}
		if score > 0 {
			results = append(results, scored{entry, score})
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].score != results[j].score {
			return results[i].score > results[j].score
		}
		return results[i].entry.Name() < results[j].entry.Name()
	})
	out := make([]environments.CatalogEntry, 0, len(results))
	for _, result := range results {
		out = append(out, result.entry)
	}
	return out
}

// describeTool renders one entry's full schema for the model.
func describeTool(entry environments.CatalogEntry) string {
	var b strings.Builder
	b.WriteString(entry.Name())
	if entry.Environment != "" {
		b.WriteString("  (environment: ")
		b.WriteString(entry.Environment)
		b.WriteString(")")
	}
	b.WriteString("\n")
	if entry.Description != "" {
		b.WriteString(entry.Description)
		b.WriteString("\n")
	}
	b.WriteString("Call id: ")
	b.WriteString(entry.ID)
	b.WriteString("\nArguments: ")
	// The schema is already validated, so it is JSON the model can read directly.
	// Rendered raw rather than reformatted: an adapter's wording is its own.
	if len(entry.Parameters) == 0 {
		b.WriteString("{}")
	} else {
		b.WriteString(string(entry.Parameters))
	}
	return b.String()
}

// SearchCatalogue answers TresorBotToolSearch.
func SearchCatalogue(bridge ToolBridge, query string) (string, error) {
	if bridge == nil {
		return "", ErrNoEnvironments
	}
	entries := bridge.Catalogue()
	if len(entries) == 0 {
		return "No environment tools are available. No environment is installed and enabled, or none of them exposes tools.", nil
	}
	matched := searchEnvironments(entries, query)
	if len(matched) == 0 {
		// Naming the closest names costs nothing and saves a second search with
		// different words, which is what an empty result usually triggers.
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		return fmt.Sprintf("No tool matched %q. Available tools: %s", query, strings.Join(names, ", ")), nil
	}
	text, omitted := RenderCatalogue(matched)
	return WithOmittedFooter(text, omitted), nil
}

// DescribeCatalogue answers TresorBotToolDescribe.
func DescribeCatalogue(bridge ToolBridge, tool string) (string, error) {
	if bridge == nil {
		return "", ErrNoEnvironments
	}
	entries := bridge.Catalogue()
	entry, ok := findEntry(entries, tool)
	if !ok {
		return "", unknownToolError(entries, tool)
	}
	return describeTool(entry), nil
}

// findEntry resolves a name or an id, tolerating surrounding whitespace and the
// case a model may have changed.
func findEntry(entries []environments.CatalogEntry, tool string) (environments.CatalogEntry, bool) {
	want := strings.TrimSpace(tool)
	if want == "" {
		return environments.CatalogEntry{}, false
	}
	for _, entry := range entries {
		if entry.ID == want || entry.Name() == want {
			return entry, true
		}
	}
	lower := strings.ToLower(want)
	for _, entry := range entries {
		if strings.ToLower(entry.Name()) == lower || strings.ToLower(entry.ID) == lower {
			return entry, true
		}
	}
	return environments.CatalogEntry{}, false
}

// unknownToolError lists near-matches so a mistyped name costs one retry rather
// than a dead end.
func unknownToolError(entries []environments.CatalogEntry, tool string) error {
	near := make([]string, 0, 8)
	lower := strings.ToLower(strings.TrimSpace(tool))
	// A wrong tool name usually gets the *instance* right ("cron.list_job" for
	// "cron.list_jobs"), so a shared instance prefix is the most useful signal.
	instance, _, hasPrefix := strings.Cut(lower, ".")
	for _, entry := range entries {
		name := strings.ToLower(entry.Name())
		matched := strings.Contains(name, lower) ||
			strings.Contains(lower, strings.ToLower(entry.Tool)) ||
			(hasPrefix && instance != "" && strings.HasPrefix(name, instance+"."))
		if matched {
			near = append(near, entry.Name())
		}
		if len(near) == 8 {
			break
		}
	}
	if len(near) == 0 {
		return fmt.Errorf("%w: %q is not an environment tool; use %s to find one", ErrUnknownTool, tool, ToolSearchName)
	}
	return fmt.Errorf("%w: %q is not an environment tool. Did you mean: %s?", ErrUnknownTool, tool, strings.Join(near, ", "))
}

// ErrUnknownTool marks a name the catalog does not contain.
var ErrUnknownTool = errors.New("unknown tool")
