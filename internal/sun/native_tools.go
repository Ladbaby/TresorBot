package sun

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"tresorbot/internal/bottools"
)

func (a *Application) nativeTools() http.Handler {
	tools := bottools.New(a.store, a.memory)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reply := func(status int, payload any) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(payload)
		}
		fail := func(status int, code string) { reply(status, map[string]any{"error": map[string]string{"code": code}}) }
		runner := r.Header.Get("X-Runner-ID")
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			fail(401, "runner_unauthorized")
			return
		}
		if _, err := a.store.AuthenticateRunner(runner, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")); err != nil {
			fail(401, "runner_unauthorized")
			return
		}
		if r.Method != "POST" {
			fail(405, "method_not_allowed")
			return
		}
		var request struct {
			AssignmentID string          `json:"assignment_id"`
			Epoch        int64           `json:"epoch"`
			OperationID  string          `json:"operation_id"`
			Tool         string          `json:"tool"`
			Arguments    json.RawMessage `json:"arguments"`
			Reconcile    bool            `json:"reconcile"`
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, (1<<20)+1))
		dec.DisallowUnknownFields()
		if dec.Decode(&request) != nil {
			fail(400, "invalid_tool_request")
			return
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			fail(400, "invalid_tool_request")
			return
		}
		task, err := a.store.ValidateNativeAssignment(request.AssignmentID, runner, request.Epoch)
		if err != nil {
			fail(403, "assignment_unauthorized")
			return
		}
		if r.URL.Path == "/api/runner/tools/definitions" {
			delegation, err := tools.DelegationDefinitions(task.BotID)
			if err != nil {
				fail(503, "tools_unavailable")
				return
			}
			reply(200, map[string]any{"tools": append(bottools.Definitions(), delegation...)})
			return
		}
		if r.URL.Path != "/api/runner/tools/execute" {
			fail(404, "not_found")
			return
		}
		scope := bottools.Scope{AssignmentID: request.AssignmentID, BotID: task.BotID, Epoch: request.Epoch, RunnerID: runner}
		call := bottools.Request{OperationID: request.OperationID, Tool: request.Tool, Arguments: request.Arguments, Reconcile: request.Reconcile}
		if request.Tool == "TresorBotSendMessage" {
			child, payload, status, err := tools.PrepareDelegation(r.Context(), scope, call)
			if err != nil {
				fail(503, "delegation_unavailable")
				return
			}
			if status != 200 {
				reply(status, payload)
				return
			}
			a.dispatch.Wake()
			payload, err = tools.AwaitChild(r.Context(), scope, child.ID)
			if err != nil {
				fail(503, "delegation_uncertain")
				return
			}
			reply(200, payload)
			return
		}
		if request.Tool == "TresorBotEarthList" {
			payload, status, err := tools.ListEarthTool(r.Context(), call)
			if err != nil {
				fail(503, "tools_unavailable")
				return
			}
			reply(status, payload)
			return
		}
		var payload any
		var status int
		if request.Tool == "TresorBotContactsList" {
			payload, status, err = tools.ListContactsTool(r.Context(), scope, call)
		} else {
			payload, status, err = tools.Execute(r.Context(), scope, call)
		}
		if err != nil {
			fail(503, "tools_unavailable")
			return
		}
		reply(status, payload)
	})
}
