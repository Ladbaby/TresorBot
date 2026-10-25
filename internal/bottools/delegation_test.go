package bottools

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"tresorbot/internal/store"
)

func TestDelegationReattachesChildAndRejectsChangedContext(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sun.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	runner, err := s.CreateRunner("earth")
	must(err)
 must(s.UpdateRunnerMetadata(runner,t.TempDir(),"test"))
	parent, err := s.CreateBot(store.Bot{Alias: "bot/parent", Name: "Parent", Enabled: true, RunnerID: runner})
	must(err)
	specialist, err := s.CreateBot(store.Bot{Alias: "bot/specialist", Name: "Specialist", Enabled: true, RunnerID: runner, Instructions: "PRIVATE"})
	must(err)
	must(s.ReplaceContacts(parent.ID, []store.Contact{{Target: specialist.ID, Description: "Research"}}))
	task, _, err := s.AdmitTask(parent.ID, "owner", "", []byte(`{"text":"PRIVATE PARENT"}`))
	must(err)
	a, err := s.ClaimTask(task.ID, runner)
	must(err)
	must(s.Accept(a))
	must(s.InitGatewaySchema())
	_, _, err = s.ReserveGatewayExecution(store.GatewayRegistration{ExecutionID: a.ExecutionID, AssignmentID: a.ID, TaskID: task.ID, BotID: parent.ID, Alias: parent.Alias, BindingID: "binding", URL: "http://localhost", Epoch: a.Epoch})
	must(err)
	must(s.ActivateGatewayExecution(a.ExecutionID, strings.Repeat("c", 64)))
	h := New(s, nil)
	scope := Scope{a.ExecutionID, a.ID, parent.ID, "binding", parent.Alias, a.Epoch}
	defs, err := h.DelegationDefinitions(parent.ID)
	must(err)
	b, _ := json.Marshal(defs)
	if !strings.Contains(string(b), specialist.ID) || strings.Contains(string(b), "PRIVATE") {
		t.Fatal(string(b))
	}
	if defs[1]["name"] != "TresorBotSendMessage" {
		t.Fatal("unexpected messaging tool name", defs[1]["name"])
	}
	description, ok := defs[1]["description"].(string)
	if !ok || !strings.Contains(description, "TresorBotContactsList") || strings.Contains(description, specialist.ID) || strings.Contains(description, "Research") {
		t.Fatal("delegate description must direct discovery without embedding Contacts", description)
	}
	arguments := func(context string) json.RawMessage {
		b, _ := json.Marshal(map[string]string{"target_bot": specialist.ID, "task": "research", "relevant_context": context})
		return b
	}
	r := Request{OperationID: strings.Repeat("a", 64), Tool: "TresorBotSendMessage", Arguments: arguments("explicit")}
	child, _, status, err := h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 200 {
		t.Fatal(status)
	}
	replay, _, status, err := h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 200 || replay.ID != child.ID {
		t.Fatal(status, replay)
	}
	if strings.Contains(string(child.Input), "PRIVATE PARENT") {
		t.Fatal("parent context leaked")
	}
	r.Arguments = arguments("changed")
	_, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 409 {
		t.Fatal(status)
	}
	// Revocation prevents new work but does not erase a previously admitted
	// dependency: replay attaches to the same child, whose queued dispatch also
	// rechecks Contacts. No reverse permission is needed to receive its result.
	must(s.ReplaceContacts(parent.ID, nil))
	r.Arguments = arguments("explicit")
	replay, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 200 || replay.ID != child.ID {
		t.Fatal("lost existing child", status)
	}
	r.OperationID = strings.Repeat("b", 64)
	_, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 403 {
		t.Fatal("revoked contact admitted", status)
	}
	must(s.ReplaceContacts(parent.ID, []store.Contact{{Target: specialist.ID}}))
	selfArgs, _ := json.Marshal(map[string]string{"target_bot": parent.ID, "task": "loop", "relevant_context": ""})
	r.Arguments = selfArgs
	_, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 403 {
		t.Fatal("self accepted", status)
	}
	r.Arguments = arguments("explicit")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err = h.AwaitChild(ctx, scope, child.ID); err == nil {
		t.Fatal("pending child appeared terminal")
	}
	must(s.CancelTask(child.ID))
	out, err := h.AwaitChild(context.Background(), scope, child.ID)
	must(err)
	b, _ = json.Marshal(out)
	if !strings.Contains(string(b), `"status":"cancelled"`) {
		t.Fatal(string(b))
	}
	tasks, err := s.ListTasks("", 100, 0)
	must(err)
	if len(tasks) != 2 {
		t.Fatal(len(tasks))
	}
	must(s.CancelTask(task.ID))
	_, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 403 {
		t.Fatal("cancelled parent admitted work", status)
	}
	scope.Epoch++
	_, _, status, err = h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 403 {
		t.Fatal("stale execution accepted", status)
	}
}
