package sun

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"tresorbot/internal/bottools"
	"tresorbot/internal/bridge"
	"tresorbot/internal/chatinput"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/gateway"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

func (a *Application) integration(c config.Config) (*gateway.Registry, *bridge.Service, error) {
	if c.IntegrationToken == "" {
		return nil, nil, nil
	}
	registry, err := gateway.New(gateway.Config{URL: c.GatewayURL, BindingID: c.GatewayBindingID, ControlToken: c.GatewayControlToken, ServiceToken: c.IntegrationToken}, a.store)
	if err != nil {
		return nil, nil, err
	}
	tools := bottools.New(a.store, a.memory)
	parse := func(raw json.RawMessage, alias string) (json.RawMessage, bool, error) {
		input, e := chatinput.Parse(raw, alias)
		if e != nil {
			return nil, false, e
		}
		body, _ := json.Marshal(map[string]any{"chat": input})
		return body, input.Stream, nil
	}
	service, err := bridge.New(bridge.Options{Token: c.IntegrationToken, BindingID: c.GatewayBindingID, Store: a.store,
		ValidateChat: func(raw json.RawMessage, alias string) error { _, e := chatinput.Parse(raw, alias); return e },
		Invoke:       bridge.ChatInvoker(a.store, func() { a.dispatch.Wake() }, parse),
		Managed: func(input bridge.Input, bot store.Bot) (any, error) {
			ex := input.Execution
			_, e := a.store.ValidateGatewayExecution(ex.ID, ex.Assignment, ex.Subject, bot.Alias, input.BindingID, ex.Epoch)
			if e != nil || ex.Subject != bot.ID {
				return nil, store.ErrConflict
			}
			return map[string]any{"kind": "managedTools", "service": "local", "codec": "openai-chat.v1", "instructions": "Use tresorbot_memory_list/read/write for durable memory. Private memory belongs to this bot; shared writes require permission. Pi owns all other available tools. Read revisions before modifying existing Markdown.", "tools": bottools.Definitions()}, nil
		},
		Tools: func(w http.ResponseWriter, r *http.Request, raw json.RawMessage) {
			reply := func(status int, payload any) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"version": contracts.IntegrationVersion, "payload": payload})
			}
			failure := func(status int, code string) {
				reply(status, map[string]any{"error": map[string]any{"code": code, "message": code, "retryable": status >= 500}})
			}
			var request contracts.ManagedToolRequest
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if d.Decode(&request) != nil || request.Validate() != nil {
				failure(400, "invalid_tool_request")
				return
			}
			ex := request.Execution
			if _, e := a.store.ValidateGatewayExecution(ex.ExecutionID, ex.AssignmentID, ex.Subject, ex.AliasID, ex.Service, ex.Epoch); e != nil || ex.Service != c.GatewayBindingID {
				failure(403, "execution_unauthorized")
				return
			}
			payload, status, e := tools.Execute(r.Context(), bottools.Scope{ExecutionID: ex.ExecutionID, AssignmentID: ex.AssignmentID, BotID: ex.Subject, Alias: ex.AliasID, Service: ex.Service, Epoch: ex.Epoch}, bottools.Request{OperationID: request.OperationID, Tool: request.Tool, Arguments: request.Arguments, Reconcile: request.Reconcile})
			if e != nil {
				failure(503, "tool_operation_uncertain")
				return
			}
			reply(status, payload)
		}})
	return registry, service, err
}
func (a *Application) authorizeGateway(registry *gateway.Registry) func(context.Context, store.Assignment, store.Task) (*runnertransport.GatewayScope, error) {
	if registry == nil {
		return nil
	}
	return func(ctx context.Context, assignment store.Assignment, task store.Task) (*runnertransport.GatewayScope, error) {
		var bot store.Bot
		if json.Unmarshal(task.Snapshot, &bot) != nil {
			return nil, store.ErrInvalid
		}
		g, e := registry.Ensure(ctx, assignment, bot)
		if e != nil {
			return nil, e
		}
		return &runnertransport.GatewayScope{URL: g.URL, Capability: g.Capability, ExecutionID: g.ExecutionID, AssignmentID: g.AssignmentID, Epoch: g.Epoch, Alias: g.Alias}, nil
	}
}
