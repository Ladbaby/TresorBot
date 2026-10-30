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

// EarthView lists a runner the sender may target with TresorBotSendMessage.
type EarthView struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Earths lists non-revoked runners available as SendToEarth targets.
func (h *Handler) Earths() ([]EarthView, error) {
	runners, err := h.s.ListRunners()
	if err != nil {
		return nil, err
	}
	out := []EarthView{}
	for _, r := range runners {
		if r.Revoked {
			continue
		}
		if len(out) >= 100 {
			return nil, errors.New("earths exceed tool limit")
		}
		out = append(out, EarthView{r.ID, r.Name})
	}
	return out, nil
}

// DelegationDefinitions returns the tool surface for a bot. A Worker has no
// memory and cannot delegate, so it only gets discovery tools; ordinary brains
// also get contacts and messaging. EarthList always ships so a brain can choose
// where TresorBotSendMessage should run.
func (h *Handler) DelegationDefinitions(botID string) ([]map[string]any, error) {
	contacts, err := h.Contacts(botID)
	if err != nil {
		return nil, err
	}
	b, err := h.s.GetBot(botID)
	if err != nil {
		return nil, err
	}
	definitions := []map[string]any{
		{"name": "TresorBotContactsList", "description": "List your currently permitted bot contacts and their descriptions.", "parameters": map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}, "operation": "execute-tool", "label": "TresorBotContactsList"},
		{"name": "TresorBotEarthList", "description": "List the Earth machines available to run delegated work.", "parameters": map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}, "operation": "execute-tool", "label": "TresorBotEarthList"},
	}
	if b.IsWorker() {
		// Worker: no memory, no delegation. It returns a final message to its caller.
		return definitions, nil
	}
	if len(contacts) == 0 {
		return definitions, nil
	}
	targets := []string{}
	for _, c := range contacts {
		targets = append(targets, c.Target)
	}
	definitions = append(definitions, map[string]any{"name": "TresorBotSendMessage", "description": "Send a message to a permitted bot and wait for its reply. Use TresorBotContactsList to discover recipients and TresorBotEarthList to choose where the recipient should run. Include only the context you intend to share; do not copy private history.", "parameters": map[string]any{"type": "object", "properties": map[string]any{"target_bot": map[string]any{"type": "string", "enum": targets}, "task": map[string]any{"type": "string", "maxLength": 32768}, "relevant_context": map[string]any{"type": "string", "maxLength": 32768}, "earth": map[string]any{"type": "string", "maxLength": 256, "description": "Optional Earth id or name to run the recipient on. Omit to use the bot's default."}}, "required": []string{"target_bot", "task", "relevant_context"}, "additionalProperties": false}, "operation": "execute-tool", "label": "TresorBotSendMessage"})
	return definitions, nil
}

// resolveEarth accepts a runner ID or an exact runner name and returns its ID.
func (h *Handler) resolveEarth(earth string) (string, error) {
	runners, err := h.s.ListRunners()
	if err != nil {
		return "", err
	}
	match := ""
	for _, r := range runners {
		if r.Revoked {
			continue
		}
		if r.ID == earth {
			return r.ID, nil
		}
		if r.Name == earth {
			if match != "" && match != r.ID {
				return "", errors.New("ambiguous earth name")
			}
			match = r.ID
		}
	}
	if match == "" {
		return "", errors.New("unknown earth")
	}
	return match, nil
}

func (h *Handler) ListEarthTool(ctx context.Context, r Request) (any, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 503, err
	}
	var args map[string]json.RawMessage
	if json.Unmarshal(r.Arguments, &args) != nil || args == nil || len(args) != 0 {
		return domain("invalid_arguments"), 400, nil
	}
	earths, err := h.Earths()
	if err != nil {
		return nil, 503, err
	}
	return map[string]any{"earths": earths}, 200, nil
}

// PrepareDelegation persists child identity before admitting a long-lived HTTP
// subscriber. task_dependencies owns replay; no memory mutex spans this wait.
func (h *Handler) PrepareDelegation(ctx context.Context, scope Scope, r Request) (store.Task, any, int, error) {
	if err := ctx.Err(); err != nil {
		return store.Task{}, nil, 503, err
	}
	g, err := h.validateScope(scope)
	if err != nil {
		return store.Task{}, domain("execution_unauthorized"), 403, nil
	}
	var args struct {
		Target  string `json:"target_bot"`
		Task    string `json:"task"`
		Context string `json:"relevant_context"`
		Earth   string `json:"earth"`
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(r.Arguments, &fields) != nil || (len(fields) != 3 && len(fields) != 4) {
		return store.Task{}, domain("invalid_arguments"), 400, nil
	}
	for _, name := range []string{"target_bot", "task", "relevant_context"} {
		var v string
		b, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(b), []byte("null")) || json.Unmarshal(b, &v) != nil {
			return store.Task{}, domain("invalid_arguments"), 400, nil
		}
	}
	if raw, ok := fields["earth"]; ok {
		var v string
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &v) != nil {
			return store.Task{}, domain("invalid_arguments"), 400, nil
		}
	}
	d := json.NewDecoder(bytes.NewReader(r.Arguments))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&args) != nil || d.Decode(&extra) != io.EOF || strings.TrimSpace(args.Task) == "" || len(args.Task) > 32768 || len(args.Context) > 32768 || len(args.Earth) > 256 || !validOperationID(r.OperationID) {
		return store.Task{}, domain("invalid_arguments"), 400, nil
	}
	// A bot may never message itself.
	if args.Target == scope.BotID {
		return store.Task{}, domain("delegation_denied"), 403, nil
	}
	// A Worker has no memory and cannot delegate, so it cannot send messages.
	if sender, err := h.s.GetBot(scope.BotID); err == nil && sender.IsWorker() {
		return store.Task{}, domain("delegation_denied"), 403, nil
	}
	targetRunner := ""
	if args.Earth != "" {
		id, err := h.resolveEarth(args.Earth)
		if err != nil {
			return store.Task{}, domain("unknown_earth"), 400, nil
		}
		targetRunner = id
	}
	// The dependency hash includes context even when empty; replay cannot silently
	// change the specialist's input. Nothing from the parent is implicitly copied.
	body, _ := json.Marshal(map[string]string{"text": "Delegated task:\n" + args.Task + "\n\nRelevant context:\n" + args.Context, "delegated_task": args.Task, "relevant_context": args.Context})
	// Direct chat has no execution authority to fence against; the alias was
	// already resolved and the synthetic parent carries the identity. The
	// Earth-Pi path keeps its assignment/execution fence.
	var fences []store.ChildAdmissionFence
	if !scope.DirectChat {
		fences = append(fences, store.ChildAdmissionFence{RunnerID: scope.RunnerID, AssignmentID: scope.AssignmentID, ExecutionID: scope.ExecutionID, Epoch: scope.Epoch})
	}
	child, err := h.s.AdmitChild(g.TaskID, args.Target, r.OperationID, body, store.ChildOptions{TargetRunnerID: targetRunner}, fences...)
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
func (h *Handler) validateScope(scope Scope) (store.GatewayRegistration, error) {
	if scope.DirectChat {
		// Direct chat is authorized by alias with no execution authority and no
		// durable parent task. Synthesize a lightweight parent so delegation can
		// attach a child via AdmitChild.
		t, err := h.s.EnsureDirectParent(scope.BotID, scope.ExecutionID)
		if err != nil {
			return store.GatewayRegistration{}, err
		}
		return store.GatewayRegistration{TaskID: t.ID}, nil
	}
	if scope.RunnerID == "" {
		return h.s.ValidateGatewayExecution(scope.ExecutionID, scope.AssignmentID, scope.BotID, scope.Alias, scope.Service, scope.Epoch)
	}
	task, err := h.s.ValidateNativeAssignment(scope.AssignmentID, scope.RunnerID, scope.Epoch)
	if err != nil || task.BotID != scope.BotID {
		return store.GatewayRegistration{}, store.ErrConflict
	}
	return store.GatewayRegistration{TaskID: task.ID}, nil
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
		g, err := h.validateScope(scope)
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
