package sun

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"tresorbot/internal/bottools"
	"tresorbot/internal/bridge"
	"tresorbot/internal/chatinput"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/gateway"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

// botDecision builds the managed-tools decision for a bot: its memory index and
// guidance plus the TresorBot tool surface. The downstream model answers; the
// tools give it access to the bot's memory and contacts. Used for both direct
// chat and a Pi-driven task, so the downstream model sees the same brain.
func (a *Application) botDecision(bot store.Bot) (any, error) {
	delegation, err := a.bottools.DelegationDefinitions(bot.ID)
	if err != nil {
		return nil, err
	}
	definitions := append(bottools.Definitions(), delegation...)
	instructions := "You are the bot " + bot.Name + ". Use TresorBotMemoryList, TresorBotMemoryRead and TresorBotMemoryWrite for durable memory. Private memory belongs to this bot; shared writes require permission. Use TresorBotContactsList to discover permitted recipients and TresorBotSendMessage for explicitly requested communication; it runs the recipient on an Earth and waits for its reply. Include only the context you intend to share. Read revisions before modifying existing Markdown."
	if index, err := a.memory.Index(bot.ID, bot.ID); err == nil && strings.TrimSpace(index) != "" {
		instructions += "\n\n" + index
	}
	// The stream policy shapes streaming response content only; the client's
	// stream request always governs the transport.
	heartbeat, showIntermediate, err := a.store.StreamSettings()
	if err != nil {
		return nil, err
	}
	return map[string]any{"kind": "managedTools", "service": "local", "codec": "openai-chat.v1", "instructions": instructions, "tools": definitions,
		"stream": map[string]any{"heartbeat": heartbeat, "show_intermediate": showIntermediate}}, nil
}

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
	service, err := bridge.New(bridge.Options{Token: c.IntegrationToken, ResolveBinding: registry.ResolveBinding, Store: a.store,
		ValidateChat: func(raw json.RawMessage, alias string) error { _, e := chatinput.Parse(raw, alias); return e },
		Invoke:       bridge.ChatInvoker(a.store, func() { a.dispatch.Wake() }, parse),
		Managed: func(input bridge.Input, bot store.Bot) (any, error) {
			// No execution authority to verify: the bot is identified by the
			// requested alias resolved in the bridge. Supply its tools and memory.
			return a.botDecision(bot)
		},
		ChatDecision: func(bot store.Bot, bindingID string) (any, error) {
			// Direct chat: the resolved downstream model answers with the bot's
			// memory and tools attached. No Earth Pi session substitutes it.
			return a.botDecision(bot)
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
			// No execution authority to verify. Identify the bot from the alias the
			// gateway routed on, so tool effects land in the right bot's memory.
			bot, e := a.store.BotByAlias(ex.AliasID)
			if e != nil {
				failure(403, "bot_not_found")
				return
			}
			// Direct chat is the alias-resolved managed-tools turn: Tresor
			// synthesizes AssignmentID "chat" and there is no execution
			// authority. A gateway-registered turn carries a real assignment and
			// keeps its authority check.
			directChat := ex.AssignmentID == "chat"
			scope := bottools.Scope{ExecutionID: ex.ExecutionID, AssignmentID: ex.AssignmentID, BotID: bot.ID, Alias: ex.AliasID, Service: ex.Service, Epoch: ex.Epoch, DirectChat: directChat}
			toolRequest := bottools.Request{OperationID: request.OperationID, Tool: request.Tool, Arguments: request.Arguments, Reconcile: request.Reconcile}
			if request.Tool == "TresorBotEarthList" {
				payload, status, e := tools.ListEarthTool(r.Context(), toolRequest)
				if e != nil {
					failure(503, "tools_unavailable")
					return
				}
				reply(status, payload)
				return
			}
			if request.Tool == "TresorBotContactsList" {
				payload, status, e := tools.ListContactsTool(r.Context(), scope, toolRequest)
				if e != nil {
					failure(503, "contacts_unavailable")
					return
				}
				reply(status, payload)
				return
			}
			if request.Tool == "TresorBotSendMessage" {
				child, payload, status, e := tools.PrepareDelegation(r.Context(), scope, toolRequest)
				if e != nil {
					failure(503, "delegation_uncertain")
					return
				}
				if status != 200 {
					reply(status, payload)
					return
				}
				a.dispatch.Wake()
				// Commit only after durable admission so the service header
				// deadline stays separate from task time. No body heartbeat is
				// needed on this direct service channel; the final JSON remains bounded.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				payload, e = tools.AwaitChild(r.Context(), scope, child.ID)
				if e != nil {
					replyPayload := map[string]any{"error": map[string]any{"code": "delegation_uncertain", "message": "delegation outcome unavailable", "retryable": false}}
					_ = json.NewEncoder(w).Encode(map[string]any{"version": contracts.IntegrationVersion, "payload": replyPayload})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"version": contracts.IntegrationVersion, "payload": payload})
				return
			}
			payload, status, e := tools.Execute(r.Context(), scope, toolRequest)
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
