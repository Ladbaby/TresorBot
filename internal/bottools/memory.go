package bottools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"tresorbot/internal/memory"
	"tresorbot/internal/store"
)

type Scope struct {
	RunnerID                                         string
	ExecutionID, AssignmentID, BotID, Service, Alias string
	Epoch                                            int64
	// DirectChat marks a gateway alias-resolved turn with no execution
	// authority and no durable parent task. Delegation synthesizes a parent.
	DirectChat bool
}
type Request struct {
	OperationID, Tool string
	Arguments         json.RawMessage
	Reconcile         bool
}
type Handler struct {
	s  *store.Store
	m  *memory.Store
	mu sync.Mutex
}

func New(s *store.Store, m *memory.Store) *Handler { return &Handler{s: s, m: m} }
func hash(v []byte) string                         { h := sha256.Sum256(v); return hex.EncodeToString(h[:]) }
func (h *Handler) Execute(ctx context.Context, scope Scope, r Request) (any, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 503, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var fields map[string]json.RawMessage
	if json.Unmarshal(r.Arguments, &fields) != nil || fields == nil {
		return domain("invalid_arguments"), 400, nil
	}
	allowed := map[string]bool{"scope": true}
	switch r.Tool {
	case "TresorBotMemoryList":
		allowed["prefix"] = true
	case "TresorBotMemoryRead":
		allowed["path"] = true
	case "TresorBotMemoryWrite":
		for _, key := range []string{"path", "expected_revision", "content"} {
			allowed[key] = true
		}
		for _, key := range []string{"scope", "path", "expected_revision", "content"} {
			if _, ok := fields[key]; !ok {
				return domain("invalid_arguments"), 400, nil
			}
		}
	}
	for key, raw := range fields {
		var text string
		if !allowed[key] || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &text) != nil {
			return domain("invalid_arguments"), 400, nil
		}
	}
	var args struct {
		Scope    string `json:"scope"`
		Path     string `json:"path,omitempty"`
		Prefix   string `json:"prefix,omitempty"`
		Expected string `json:"expected_revision,omitempty"`
		Content  string `json:"content,omitempty"`
	}
	d := json.NewDecoder(bytes.NewReader(r.Arguments))
	d.DisallowUnknownFields()
	if d.Decode(&args) != nil {
		return domain("invalid_arguments"), 400, nil
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return domain("invalid_arguments"), 400, nil
	}
	if args.Scope != "private" && args.Scope != "shared" {
		return domain("invalid_scope"), 400, nil
	}
	switch r.Tool {
	case "TresorBotMemoryRead":
		if args.Path == "" || args.Content != "" || args.Expected != "" || args.Prefix != "" {
			return domain("invalid_arguments"), 400, nil
		}
	case "TresorBotMemoryList":
		if args.Path != "" || args.Content != "" || args.Expected != "" {
			return domain("invalid_arguments"), 400, nil
		}
	case "TresorBotMemoryWrite":
		if args.Path == "" || args.Prefix != "" {
			return domain("invalid_arguments"), 400, nil
		}
	default:
		return domain("unknown_tool"), 400, nil
	}
	var canonical any
	dec := json.NewDecoder(bytes.NewReader(r.Arguments))
	dec.UseNumber()
	if dec.Decode(&canonical) != nil {
		return domain("invalid_arguments"), 400, nil
	}
	raw, _ := json.Marshal(canonical)
	sc, _ := json.Marshal(scope)
	req, _ := json.Marshal([]any{r.Tool, json.RawMessage(raw)})
	op, _, err := h.s.BeginToolOperation(r.OperationID, hash(sc), hash(req))
	if errors.Is(err, store.ErrToolOperationConflict) {
		return domain("operation_conflict"), 409, nil
	}
	if err != nil {
		return nil, 503, err
	}
	if op.State == "complete" {
		var payload any
		if json.Unmarshal(op.Payload, &payload) != nil {
			return nil, 503, errors.New("invalid recorded tool outcome")
		}
		return payload, op.Status, nil
	}
	bot, err := h.s.GetBot(scope.BotID)
	if err != nil || !bot.Enabled {
		return h.complete(r.OperationID, domain("bot_disabled"), 403)
	}
	// A Worker has no memory; reject every memory tool for its scope.
	if bot.IsWorker() {
		return h.complete(r.OperationID, domain("memory_denied"), 403)
	}
	scopeName := args.Scope
	if scopeName == "private" {
		scopeName = scope.BotID
	}
	var payload any
	switch r.Tool {
	case "TresorBotMemoryRead":
		payload, err = h.m.Read(scopeName, scope.BotID, args.Path)
	case "TresorBotMemoryWrite":
		payload, err = h.m.Write(scopeName, scope.BotID, args.Path, args.Expected, args.Content, r.OperationID)
	case "TresorBotMemoryList":
		var entries []memory.Entry
		entries, err = h.m.List(scopeName, scope.BotID)
		if err == nil {
			filtered := []memory.Entry{}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Path, args.Prefix) {
					if len(filtered) >= 100 {
						return h.complete(r.OperationID, domain("memory_list_too_large"), 409)
					}
					filtered = append(filtered, entry)
				}
			}
			payload = map[string]any{"entries": filtered}
		}
	}
	if err != nil {
		status := 403
		code := "memory_denied"
		switch {
		case errors.Is(err, memory.ErrNotFound):
			status = 404
			code = "memory_not_found"
		case errors.Is(err, memory.ErrConflict):
			status = 409
			code = "memory_conflict"
		case errors.Is(err, memory.ErrInvalid):
			status = 400
			code = "invalid_memory_request"
		default:
			if !errors.Is(err, store.ErrDisabled) && !errors.Is(err, store.ErrNotFound) && !strings.Contains(err.Error(), "permission") {
				return nil, 503, errors.New("memory operation unavailable")
			}
		}
		return h.complete(r.OperationID, domain(code), status)
	}
	return h.complete(r.OperationID, payload, 200)
}
func domain(code string) any {
	return map[string]any{"error": map[string]any{"code": code, "message": code, "retryable": false}}
}
func (h *Handler) complete(id string, payload any, status int) (any, int, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, 503, err
	}
	if len(b) > 2<<20 {
		return nil, 503, errors.New("tool result exceeds bound")
	}
	if err = h.s.CompleteToolOperation(id, status, b, ""); err != nil {
		return nil, 503, err
	}
	return payload, status, nil
}
func Definitions() []map[string]any {
	scope := map[string]any{"type": "string", "enum": []string{"private", "shared"}}
	tool := func(name, description string, props map[string]any, required []string) map[string]any {
		// label is what progress lines show while the bot works.
		return map[string]any{"name": name, "description": description, "parameters": map[string]any{"type": "object", "properties": props, "required": required, "additionalProperties": false}, "operation": "execute-tool", "label": name}
	}
	text := func() map[string]any { return map[string]any{"type": "string"} }
	return []map[string]any{
		tool("TresorBotMemoryList", "List Markdown memory paths and revisions in private or shared memory.", map[string]any{"scope": scope, "prefix": text()}, []string{"scope"}),
		tool("TresorBotMemoryRead", "Read one Markdown memory document and its revision.", map[string]any{"scope": scope, "path": text()}, []string{"scope", "path"}),
		tool("TresorBotMemoryWrite", "Save Markdown with compare-and-swap revision. Use an empty expected_revision for a new file.", map[string]any{"scope": scope, "path": text(), "expected_revision": text(), "content": text()}, []string{"scope", "path", "expected_revision", "content"}),
	}
}
