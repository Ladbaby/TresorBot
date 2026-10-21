package bottools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"tresorbot/internal/store"
)

// ContactView deliberately excludes workspace, runner and private instructions.
type ContactView struct {
	Target      string `json:"target"`
	Alias       string `json:"alias"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (h *Handler) Contacts(botID string) ([]ContactView, error) {
	contacts, err := h.s.ListContacts(botID)
	if err != nil {
		return nil, err
	}
	result := []ContactView{}
	for _, c := range contacts {
		b, err := h.s.GetBot(c.Target)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !b.Enabled || b.ID == botID {
			continue
		}
		if len(result) >= 100 || len(c.Description) > 4096 {
			return nil, errors.New("contacts exceed tool limit")
		}
		result = append(result, ContactView{b.ID, b.Alias, b.Name, c.Description})
	}
	return result, nil
}

func (h *Handler) DelegationDefinitions(botID string) ([]map[string]any, error) {
	contacts, err := h.Contacts(botID)
	if err != nil {
		return nil, err
	}
	definitions := []map[string]any{{"name": "TresorBotContactsList", "description": "List your currently permitted bot contacts and their descriptions.", "parameters": map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}, "operation": "execute-tool"}}
	if len(contacts) == 0 {
		return definitions, nil
	}
	targets := []string{}
	for _, c := range contacts {
		targets = append(targets, c.Target)
	}
	definitions = append(definitions, map[string]any{"name": "TresorBotSendMessage", "description": "Send a message to a permitted bot and wait for its reply. Use TresorBotContactsList to discover recipients. Include only the context you intend to share; do not copy private history.", "parameters": map[string]any{"type": "object", "properties": map[string]any{"target_bot": map[string]any{"type": "string", "enum": targets}, "task": map[string]any{"type": "string", "maxLength": 32768}, "relevant_context": map[string]any{"type": "string", "maxLength": 32768}}, "required": []string{"target_bot", "task", "relevant_context"}, "additionalProperties": false}, "operation": "execute-tool"})
	return definitions, nil
}

// PrepareDelegation persists child identity before admitting a long-lived HTTP
// subscriber. task_dependencies owns replay; no memory mutex spans this wait.
func (h *Handler) PrepareDelegation(ctx context.Context, scope Scope, r Request) (store.Task, any, int, error) {
	if err := ctx.Err(); err != nil {
		return store.Task{}, nil, 503, err
	}
	g, err := h.s.ValidateGatewayExecution(scope.ExecutionID, scope.AssignmentID, scope.BotID, scope.Alias, scope.Service, scope.Epoch)
	if err != nil {
		return store.Task{}, domain("execution_unauthorized"), 403, nil
	}
	var args struct {
		Target  string `json:"target_bot"`
		Task    string `json:"task"`
		Context string `json:"relevant_context"`
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(r.Arguments, &fields) != nil || len(fields) != 3 {
		return store.Task{}, domain("invalid_arguments"), 400, nil
	}
	for _, name := range []string{"target_bot", "task", "relevant_context"} {
		var v string
		b, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(b), []byte("null")) || json.Unmarshal(b, &v) != nil {
			return store.Task{}, domain("invalid_arguments"), 400, nil
		}
	}
	d := json.NewDecoder(bytes.NewReader(r.Arguments))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&args) != nil || d.Decode(&extra) != io.EOF || strings.TrimSpace(args.Task) == "" || len(args.Task) > 32768 || len(args.Context) > 32768 || !validOperationID(r.OperationID) {
		return store.Task{}, domain("invalid_arguments"), 400, nil
	}
	// The dependency hash includes context even when empty; replay cannot silently
	// change the specialist's input. Nothing from the parent is implicitly copied.
	body, _ := json.Marshal(map[string]string{"text": "Delegated task:\n" + args.Task + "\n\nRelevant context:\n" + args.Context, "delegated_task": args.Task, "relevant_context": args.Context})
	child, err := h.s.AdmitChild(g.TaskID, args.Target, r.OperationID, body, store.ChildAdmissionFence{AssignmentID: scope.AssignmentID, ExecutionID: scope.ExecutionID, Epoch: scope.Epoch})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrConflict):
			return store.Task{}, domain("delegation_conflict"), 409, nil
		case errors.Is(err, store.ErrInvalid), errors.Is(err, store.ErrDisabled), errors.Is(err, store.ErrNotFound):
			return store.Task{}, domain("delegation_denied"), 403, nil
		}
		return store.Task{}, nil, 503, err
	}
	return child, nil, 200, nil
}
func validOperationID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// AwaitChild observes persistence only. Disconnection never cancels the child,
// expires a slot, or treats needs_attention as a terminal outcome.
func (h *Handler) AwaitChild(ctx context.Context, scope Scope, id string) (any, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		g, err := h.s.ValidateGatewayExecution(scope.ExecutionID, scope.AssignmentID, scope.BotID, scope.Alias, scope.Service, scope.Epoch)
		if err != nil {
			return nil, errors.New("delegation execution no longer authorized")
		}
		child, err := h.s.GetTask(id)
		if err != nil {
			return nil, err
		}
		if child.ParentID != g.TaskID {
			return nil, errors.New("child does not belong to execution")
		}
		switch child.State {
		case "completed":
			var result struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(child.Result, &result) != nil || len(result.Text) > 256*1024 {
				return nil, errors.New("invalid child result")
			}
			return map[string]any{"child_task_id": child.ID, "status": child.State, "text": result.Text}, nil
		case "failed", "cancelled":
			return map[string]any{"child_task_id": child.ID, "status": child.State, "error": map[string]string{"code": "child_" + child.State}}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Handler) ListContactsTool(ctx context.Context, scope Scope, r Request) (any, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 503, err
	}
	var args map[string]json.RawMessage
	if json.Unmarshal(r.Arguments, &args) != nil || args == nil || len(args) != 0 {
		return domain("invalid_arguments"), 400, nil
	}
	contacts, err := h.Contacts(scope.BotID)
	if err != nil {
		return nil, 503, err
	}
	return map[string]any{"contacts": contacts}, 200, nil
}
