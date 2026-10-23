package sun

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"tresorbot/internal/config"
	"tresorbot/internal/store"
)

func TestScopedIntegrationRegistersAndExecutesMemoryThroughSun(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer control" {
			t.Error("wrong control credential")
		}
		var input struct {
			ExecutionID string `json:"execution_id"`
		}
		json.NewDecoder(r.Body).Decode(&input)
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(map[string]string{"execution_id": input.ExecutionID, "capability": strings.Repeat("a", 64)})
	}))
	defer gateway.Close()
	root := t.TempDir()
	app, e := Open(config.Config{Role: "sun", DBPath: filepath.Join(root, "sun.db"), MemoryRoot: filepath.Join(root, "memory"), IntegrationToken: "service", GatewayURL: gateway.URL, GatewayBindingID: "binding", GatewayControlToken: "control"})
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	runner, e := app.store.CreateRunner("earth")
	if e != nil {
		t.Fatal(e)
	}
	bot, e := app.store.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", RunnerID: runner, Workspace: root, Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	_, _, e = app.store.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"test"}`))
	if e != nil {
		t.Fatal(e)
	}
	assignment, e := app.store.ClaimNext(bot.ID, runner)
	if e != nil {
		t.Fatal(e)
	}
	registry, _, e := app.integration(config.Config{IntegrationToken: "service", GatewayURL: gateway.URL, GatewayBindingID: "binding", GatewayControlToken: "control"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = registry.Ensure(context.Background(), assignment, bot); e != nil {
		t.Fatal(e)
	}
	call := func(path string, payload any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]any{"version": "integration.v1", "payload": payload})
		r := httptest.NewRequest("POST", path, bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer service")
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		app.Handler.ServeHTTP(w, r)
		return w
	}
	prepared := call("/integration/prepare", map[string]any{"binding_id": "binding", "routing": map[string]any{"requestedModel": bot.Alias, "downstream": map[string]string{"format": "openai"}}, "execution": map[string]any{"id": assignment.ExecutionID, "subject": bot.ID, "assignment": assignment.ID, "epoch": assignment.Epoch}})
	if prepared.Code != 200 || !strings.Contains(prepared.Body.String(), "managedTools") {
		t.Fatal(prepared.Code, prepared.Body.String())
	}
	execution := map[string]any{"ExecutionID": assignment.ExecutionID, "AssignmentID": assignment.ID, "Subject": bot.ID, "AliasID": bot.Alias, "Service": "binding", "Epoch": assignment.Epoch, "InferenceID": "inference-1"}
	payload := map[string]any{"execution": execution, "operation_id": strings.Repeat("b", 64), "tool": "TresorBotMemoryWrite", "arguments": map[string]string{"scope": "private", "path": "index.md", "expected_revision": "", "content": "remembered"}, "reconcile": false}
	w := call("/integration/tools/execute", payload)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	payload["reconcile"] = true
	w = call("/integration/tools/execute", payload)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	doc, e := app.memory.Read(bot.ID, bot.ID, "index.md")
	if e != nil || doc.Content != "remembered" {
		t.Fatal(doc, e)
	}
	execution["Epoch"] = assignment.Epoch + 1
	w = call("/integration/tools/execute", payload)
	if w.Code != 403 {
		t.Fatal("stale scope accepted", w.Code, w.Body.String())
	}
}
