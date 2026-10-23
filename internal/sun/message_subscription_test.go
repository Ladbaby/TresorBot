package sun

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"tresorbot/internal/config"
	"tresorbot/internal/store"
)

func TestMessageDisconnectReconcilesSameChildWithoutBlockingMemory(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			root := t.TempDir()
			app, err := Open(config.Config{Role: "sun", DBPath: filepath.Join(root, "sun.db"), MemoryRoot: filepath.Join(root, "memory"), IntegrationToken: "service", GatewayURL: "http://127.0.0.1:1", GatewayBindingID: "binding", GatewayControlToken: "control"})
			must(err)
			defer app.Close()
			runner, err := app.store.CreateRunner("earth")
			must(err)
			parent, err := app.store.CreateBot(store.Bot{Alias: "bot/parent", Name: "Parent", Enabled: true, RunnerID: runner})
			must(err)
			target, err := app.store.CreateBot(store.Bot{Alias: "bot/target", Name: "Target", Enabled: true, RunnerID: runner})
			must(err)
			must(app.store.ReplaceContacts(parent.ID, []store.Contact{{Target: target.ID}}))
			task, _, err := app.store.AdmitTask(parent.ID, "owner", "", []byte(`{"text":"request"}`))
			must(err)
			assignment, err := app.store.ClaimTask(task.ID, runner)
			must(err)
			must(app.store.Accept(assignment))
			_, _, err = app.store.ReserveGatewayExecution(store.GatewayRegistration{ExecutionID: assignment.ExecutionID, AssignmentID: assignment.ID, TaskID: task.ID, BotID: parent.ID, Alias: parent.Alias, BindingID: "binding", URL: "http://127.0.0.1:1", Epoch: assignment.Epoch})
			must(err)
			must(app.store.ActivateGatewayExecution(assignment.ExecutionID, strings.Repeat("c", 64)))
			server := httptest.NewServer(app.Handler)
			defer server.Close()
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.ResponseHeaderTimeout = time.Second
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			execution := map[string]any{"ExecutionID": assignment.ExecutionID, "AssignmentID": assignment.ID, "Subject": parent.ID, "AliasID": parent.Alias, "Service": "binding", "Epoch": assignment.Epoch, "InferenceID": "message-inference"}
			payload := map[string]any{"execution": execution, "operation_id": strings.Repeat("d", 64), "tool": "TresorBotSendMessage", "arguments": map[string]string{"target_bot": target.ID, "task": "hello", "relevant_context": "shared explicitly"}, "reconcile": false}
			send := func(ctx context.Context, payload any) *http.Response {
				t.Helper()
				b, err := json.Marshal(map[string]any{"version": "integration.v1", "payload": payload})
				must(err)
				req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/integration/tools/execute", bytes.NewReader(b))
				must(err)
				req.Header.Set("Authorization", "Bearer service")
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				must(err)
				return resp
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := send(ctx, payload)
			if first.StatusCode != 200 {
				t.Fatal(first.StatusCode)
			}
			tasks, err := app.store.ListTasks("", 100, 0)
			must(err)
			if len(tasks) != 2 {
				t.Fatal("headers arrived before durable child", len(tasks))
			}
			var child store.Task
			for _, v := range tasks {
				if v.ParentID == task.ID {
					child = v
				}
			}
			if child.ID == "" || child.State != "queued" {
				t.Fatal(child)
			}
			cancel()
			first.Body.Close()
			// Reattach while pending. Both calls return headers independently of child
			// completion; neither connection owns the child lifetime.
			payload["reconcile"] = true
			waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			second := send(waitCtx, payload)
			defer second.Body.Close()
			concurrent := send(waitCtx, payload)
			defer concurrent.Body.Close()
			memoryPayload := map[string]any{"execution": execution, "operation_id": strings.Repeat("e", 64), "tool": "TresorBotMemoryWrite", "arguments": map[string]string{"scope": "private", "path": "index.md", "expected_revision": "", "content": "while waiting"}}
			mem := send(waitCtx, memoryPayload)
			memBody, err := io.ReadAll(mem.Body)
			mem.Body.Close()
			must(err)
			if mem.StatusCode != 200 {
				t.Fatal(string(memBody))
			}
			// A changed operation must fail before admitting another child.
			payload["arguments"] = map[string]string{"target_bot": target.ID, "task": "different", "relevant_context": "shared explicitly"}
			conflict := send(waitCtx, payload)
			conflict.Body.Close()
			if conflict.StatusCode != 409 {
				t.Fatal("changed operation accepted", conflict.StatusCode)
			}
			if outcome == "cancelled" {
				must(app.store.CancelTask(child.ID))
			} else {
				ca, err := app.store.ClaimTask(child.ID, runner)
				must(err)
				must(app.store.Accept(ca))
				result, _ := json.Marshal(map[string]any{"state": outcome, "result": map[string]string{"text": "recipient reply"}})
				must(app.store.RecordEvent(store.Report{AssignmentID: ca.ID, RunnerID: runner, Epoch: ca.Epoch, EventID: uuid.NewString(), Sequence: 1, Kind: "terminal", Payload: result}))
			}
			for _, resp := range []*http.Response{second, concurrent} {
				b, err := io.ReadAll(resp.Body)
				must(err)
				if !json.Valid(b) || !bytes.Contains(b, []byte(`"status":"`+outcome+`"`)) {
					t.Fatal("invalid replay result", string(b))
				}
			}
			tasks, err = app.store.ListTasks("", 100, 0)
			must(err)
			if len(tasks) != 2 {
				t.Fatal("duplicate child", len(tasks))
			}
			current, err := app.store.GetTask(task.ID)
			must(err)
			if current.State != "running" {
				t.Fatal("parent not resumed", current.State)
			}
		})
	}
}
