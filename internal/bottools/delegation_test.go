package bottools

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"tresorbot/internal/contracts"
	"tresorbot/internal/store"
)

func TestDelegationReattachesChildAndRejectsChangedContext(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "gateway", true: "native"}[native], func(t *testing.T) { testDelegationReattaches(t, native) })
	}
}
func testDelegationReattaches(t *testing.T, native bool) {
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
	must(s.UpdateRunnerMetadata(runner, t.TempDir(), "test"))
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
	if !native {
		must(s.InitGatewaySchema())
		_, _, err = s.ReserveGatewayExecution(store.GatewayRegistration{ExecutionID: a.ExecutionID, AssignmentID: a.ID, TaskID: task.ID, BotID: parent.ID, Alias: parent.Alias, BindingID: "binding", URL: "http://localhost", Epoch: a.Epoch})
		must(err)
		must(s.ActivateGatewayExecution(a.ExecutionID, strings.Repeat("c", 64)))
	}
	h := New(s, nil)
	scope := Scope{ExecutionID: a.ExecutionID, AssignmentID: a.ID, BotID: parent.ID, Service: "binding", Alias: parent.Alias, Epoch: a.Epoch}
	if native {
		scope.RunnerID = runner
		scope.ExecutionID = ""
	}
	defs, err := h.DelegationDefinitions(parent.ID)
	must(err)
	b, _ := json.Marshal(defs)
	if !strings.Contains(string(b), specialist.ID) || strings.Contains(string(b), "PRIVATE") {
		t.Fatal(string(b))
	}
	var sendDef map[string]any
	for _, def := range defs {
		if def["name"] == "TresorBotSendMessage" {
			sendDef = def
		}
	}
	if sendDef == nil {
		t.Fatal("missing TresorBotSendMessage tool")
	}
	description, ok := sendDef["description"].(string)
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

func TestContactsIncludesWorker(t *testing.T) {
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
	me := store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true}
	created, err := s.CreateBot(me)
	must(err)
	must(s.EnsureWorkerForAll())
	h := New(s, nil)
	contacts, err := h.Contacts(created.ID)
	must(err)
	workerID, err := s.EnsureWorker()
	must(err)
	found := false
	for _, c := range contacts {
		if c.Target == workerID {
			found = true
		}
	}
	if !found {
		t.Fatalf("worker missing from contacts: %+v", contacts)
	}
}

func TestDirectChatSendMessageAdmitsChildAndReturnsReply(t *testing.T) {
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
	must(s.UpdateRunnerMetadata(runner, t.TempDir(), "test"))
	me, err := s.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true, RunnerID: runner})
	must(err)
	worker, err := s.CreateBot(store.Bot{Alias: "bot/worker", Name: "Worker", Enabled: true, Kind: store.Worker, RunnerID: runner})
	must(err)
	must(s.ReplaceContacts(me.ID, []store.Contact{{Target: worker.ID, Description: "worker"}}))
	h := New(s, nil)
	// Direct chat: no gateway registration, no RunnerID — authorized by alias.
	scope := Scope{ExecutionID: "exec-dc-1", BotID: me.ID, Alias: me.Alias, Service: "local", Epoch: 1, DirectChat: true}
	args, _ := json.Marshal(map[string]string{"target_bot": worker.ID, "task": "list /root", "relevant_context": "read only"})
	r := Request{OperationID: strings.Repeat("a", 64), Tool: "TresorBotSendMessage", Arguments: args}
	child, payload, status, err := h.PrepareDelegation(context.Background(), scope, r)
	must(err)
	if status != 200 {
		t.Fatalf("PrepareDelegation status=%d payload=%+v", status, payload)
	}
	if child.BotID != worker.ID {
		t.Fatalf("child bot=%s want %s", child.BotID, worker.ID)
	}
	// Drive the child to completion on Earth.
	a, err := s.ClaimTask(child.ID, runner)
	must(err)
	must(s.Accept(a))
	term := contracts.Terminal{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "terminal", ID: uuid.NewString(), RunnerID: runner, AssignmentID: a.ID, Epoch: a.Epoch}, State: contracts.Completed, Result: json.RawMessage(`{"text":"listing done"}`)}
	must(s.RecordTerminal(term, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.AwaitChild(ctx, scope, child.ID)
	must(err)
	m := res.(map[string]any)
	if m["text"] != "listing done" {
		t.Fatalf("reply=%+v", res)
	}
	// The synthesized direct parent must be finalized so it does not linger.
	parent, err := s.GetTask(child.ParentID)
	must(err)
	if parent.State != "completed" {
		t.Fatalf("direct parent state=%q want completed", parent.State)
	}
}

func TestDirectChatSendMessageRejectsSelfMessage(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sun.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	me, err := s.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, nil)
	scope := Scope{ExecutionID: "exec-dc-2", BotID: me.ID, Alias: me.Alias, Service: "local", Epoch: 1, DirectChat: true}
	args, _ := json.Marshal(map[string]string{"target_bot": me.ID, "task": "x", "relevant_context": "y"})
	r := Request{OperationID: strings.Repeat("b", 64), Tool: "TresorBotSendMessage", Arguments: args}
	_, payload, status, err := h.PrepareDelegation(context.Background(), scope, r)
	if err != nil {
		t.Fatal(err)
	}
	if status != 403 {
		t.Fatalf("self-message status=%d payload=%+v", status, payload)
	}
}

func TestDirectChatSendMessageRejectsWorkerSender(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sun.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	worker, err := s.CreateBot(store.Bot{Alias: "bot/worker", Name: "Worker", Enabled: true, Kind: store.Worker})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateBot(store.Bot{Alias: "bot/other", Name: "Other", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceContacts(worker.ID, []store.Contact{{Target: other.ID, Description: "x"}}); err != nil {
		t.Fatal(err)
	}
	h := New(s, nil)
	scope := Scope{ExecutionID: "exec-dc-3", BotID: worker.ID, Alias: worker.Alias, Service: "local", Epoch: 1, DirectChat: true}
	args, _ := json.Marshal(map[string]string{"target_bot": other.ID, "task": "x", "relevant_context": "y"})
	r := Request{OperationID: strings.Repeat("c", 64), Tool: "TresorBotSendMessage", Arguments: args}
	_, payload, status, err := h.PrepareDelegation(context.Background(), scope, r)
	if err != nil {
		t.Fatal(err)
	}
	if status != 403 {
		t.Fatalf("worker sender status=%d payload=%+v", status, payload)
	}
}
