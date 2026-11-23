package bottools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"tresorbot/internal/environments"
	"tresorbot/internal/store"
)

// executeEnvironmentTool runs one of the three meta-tools.
//
// Kept apart from the memory switch because the argument discipline differs: those
// tools take only `scope`-style flat strings validated by an allowlist, while these
// take an adapter-defined schema and a free-form argument object. Sharing the
// allowlist would either weaken it or make the meta-tools unusable.
func (h *Handler) executeEnvironmentTool(ctx context.Context, scope Scope, r Request, fields map[string]json.RawMessage) (any, int, error) {
	if h.bridge == nil {
		return domain("environments_disabled"), 400, nil
	}
	var query, tool string
	// Each meta-tool accepts exactly the keys below; anything else is a fault the
	// model should correct rather than something to ignore.
	//
	// The string arguments are named explicitly and read by name, not positionally:
	// iterating a map to pair keys with destinations would depend on Go's randomised
	// map order and silently swap arguments.
	switch r.Tool {
	case ToolSearchName:
		if err := decodeToolArgs(fields, []string{"query"}, []string{"query"}); err != nil {
			return domain("invalid_arguments"), 400, nil
		}
		query = fieldString(fields, "query")
	case ToolDescribeName:
		if err := decodeToolArgs(fields, []string{"tool"}, []string{"tool"}); err != nil {
			return domain("invalid_arguments"), 400, nil
		}
		tool = fieldString(fields, "tool")
	case ToolCallName:
		// `arguments` is intentionally unstructured: its shape belongs to the
		// adapter's own schema, which was validated at discovery.
		if err := decodeToolArgs(fields, []string{"tool", "arguments"}, []string{"tool"}); err != nil {
			return domain("invalid_arguments"), 400, nil
		}
		tool = fieldString(fields, "tool")
	default:
		return domain("unknown_tool"), 400, nil
	}

	switch r.Tool {
	case ToolSearchName:
		text, err := SearchCatalogue(h.bridge, query)
		if err != nil {
			return domain("environments_unavailable"), 503, nil
		}
		return map[string]any{"text": text}, 200, nil
	case ToolDescribeName:
		text, err := DescribeCatalogue(h.bridge, tool)
		if err != nil {
			if errors.Is(err, ErrUnknownTool) {
				// A 200 with the explanation is deliberate: an unknown name is
				// something the model can fix itself, and a transport error reads
				// to it as "the system is broken, stop trying".
				return map[string]any{"text": err.Error()}, 200, nil
			}
			return domain("environments_unavailable"), 503, nil
		}
		return map[string]any{"text": text}, 200, nil
	}

	// ToolCall: resolve, authorize, then run.
	entry, ok := findEntry(h.bridge.Catalogue(), tool)
	if !ok {
		if err := unknownToolError(h.bridge.Catalogue(), tool); err != nil {
			return map[string]any{"text": err.Error()}, 200, nil
		}
		return domain("unknown_tool"), 400, nil
	}
	if err := h.authorizeEnvironmentTool(scope, entry); err != nil {
		// A refusal is a 200 with an explanation for the same reason: it is the
		// model's request that was wrong, not the transport.
		return map[string]any{"text": err.Error()}, 200, nil
	}
	arguments := fields["arguments"]
	if len(arguments) == 0 || string(arguments) == "null" {
		arguments = json.RawMessage(`{}`)
	}
	result, err := h.bridge.Call(ctx, scope.BotID, tool, entry, arguments)
	if err != nil {
		// The adapter failed, not the model. Reporting the reason lets the model
		// decide to retry or report, instead of guessing why nothing happened.
		return map[string]any{"text": "The environment reported an error: " + err.Error()}, 200, nil
	}
	if len(result) == 0 {
		return map[string]any{"text": "Done; the environment returned nothing."}, 200, nil
	}
	// Bounded here so an adapter returning a large payload cannot flood the
	// context. The adapter is third-party; the ceiling is Sun's.
	return map[string]any{"text": BoundToolResult(result)}, 200, nil
}

// authorizeEnvironmentTool enforces who may use an environment's tools.
//
// The rule is exactly the ingress and delegation rule: an instance's tools are
// available to the bot the instance is bound to. One concept, so an operator who
// understands contacts understands this too, and installing an environment never
// silently hands its tools to every bot in Sun.
func (h *Handler) authorizeEnvironmentTool(scope Scope, entry environments.CatalogEntry) error {
	instance, err := h.s.GetEnvironmentInstance(entry.Instance)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%s is no longer configured", entry.Instance)
		}
		return err
	}
	if !instance.Enabled {
		return fmt.Errorf("environment instance %q is disabled", entry.Instance)
	}
	if instance.BotID != scope.BotID {
		return fmt.Errorf("%q belongs to a different bot; its tools are not available here", entry.Name())
	}
	return nil
}

// decodeToolArgs enforces an exact key set.
//
// Explicit rather than reflective: the permitted keys are part of the contract
// with the model, and keeping them visible here makes a change to that contract
// obvious in review.
func decodeToolArgs(fields map[string]json.RawMessage, allowed, required []string) error {
	ok := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		ok[key] = true
	}
	for key := range fields {
		if !ok[key] {
			return fmt.Errorf("unexpected argument %q", key)
		}
	}
	for _, key := range required {
		if _, present := fields[key]; !present {
			return fmt.Errorf("missing argument %q", key)
		}
	}
	for _, key := range allowed {
		raw, present := fields[key]
		if !present || string(raw) == "null" || key == "arguments" {
			continue
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return fmt.Errorf("argument %q must be a string", key)
		}
	}
	return nil
}

// fieldString reads a validated string argument, tolerating absence and null.
func fieldString(fields map[string]json.RawMessage, key string) string {
	raw, present := fields[key]
	if !present || string(raw) == "null" {
		return ""
	}
	var text string
	_ = json.Unmarshal(raw, &text)
	return text
}
