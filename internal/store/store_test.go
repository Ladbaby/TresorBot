package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"tresorbot/internal/contracts"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func botTest(t *testing.T, s *Store, alias string) Bot {
	t.Helper()
	b, err := s.CreateBot(Bot{Alias: alias, Name: alias, Enabled: true, Workspace: "workspace", Instructions: "original"})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func TestBotsContactsAndSnapshots(t *testing.T) {
	s := openTest(t)
	a := botTest(t, s, "bot/a")
	b := botTest(t, s, "bot/b")
	must(t, s.ReplaceContacts(a.ID, []Contact{{b.ID, "research"}}))
	reverse, err := s.ListContacts(b.ID)
	must(t, err)
	if len(reverse) != 0 {
		t.Fatal("contact became reciprocal")
	}
	if !errors.Is(s.ReplaceContacts(a.ID, []Contact{{b.ID, "one"}, {b.ID, "two"}}), ErrInvalid) {
		t.Fatal("duplicate accepted")
	}
	if !errors.Is(s.ReplaceContacts(a.ID, []Contact{{uuid.NewString(), "bad"}}), ErrNotFound) {
		t.Fatal("missing accepted")
	}
	cs, err := s.ListContacts(a.ID)
	must(t, err)
	if len(cs) != 1 || cs[0].Description != "research" {
		t.Fatal("replacement not atomic")
	}
	task, fresh, err := s.AdmitTask(a.ID, "owner", "key", []byte(`{"b":2,"a":1}`))
	must(t, err)
	if !fresh {
		t.Fatal("not new")
	}
	again, fresh, err := s.AdmitTask(a.ID, "owner", "key", []byte(`{ "a":1,"b":2 }`))
	must(t, err)
	if fresh || again.ID != task.ID {
		t.Fatal("not idempotent")
	}
	_, _, err = s.AdmitTask(a.ID, "owner", "key", []byte(`{"a":2}`))
	if !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	text := "changed"
	updated, err := s.UpdateBot(a.ID, a.Revision, BotPatch{Instructions: &text})
	must(t, err)
	if updated.Revision != 2 {
		t.Fatal(updated)
	}
	_, err = s.UpdateBot(a.ID, 1, BotPatch{})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("revision accepted")
	}
	var original Bot
	must(t, json.Unmarshal(task.Snapshot, &original))
	if original.Instructions != "original" {
		t.Fatal("snapshot changed")
	}
	if !errors.Is(s.DeleteBot(a.ID), ErrConflict) {
		t.Fatal("deleted queued bot")
	}
	must(t, s.CancelTask(task.ID))
	must(t, s.DeleteBot(a.ID))
	if _, err = s.GetTask(task.ID); err != nil {
		t.Fatal("lost task", err)
	}
	if _, err = s.GetBot(a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = s.CreateBot(Bot{Alias: a.Alias, Name: "reuse", Enabled: true}); err == nil {
		t.Fatal("reused retired identity")
	}
}

// fakeRunner drives the same durable assignment/report boundary future Earth will use.
type fakeRunner struct {
	s   *Store
	a   Assignment
	seq int64
}

func (f *fakeRunner) event(t *testing.T, kind, payload string) Report {
	t.Helper()
	f.seq++
	r := Report{f.a.ID, f.a.RunnerID, f.a.Epoch, uuid.NewString(), f.seq, kind, json.RawMessage(payload)}
	must(t, f.s.RecordEvent(r))
	return r
}
func TestFakeRunnerRecoveryAndDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite")
	s, err := Open(path)
	must(t, err)
	defer func() { s.Close() }()
	b := botTest(t, s, "bot/run")
	runner, err := s.CreateRunner("fake")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{"prompt":"hello"}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	duplicate, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	if duplicate.ID != a.ID {
		t.Fatal("duplicate dispatch")
	}
	must(t, s.Accept(a))
	must(t, s.Accept(a))
	fake := fakeRunner{s: s, a: a}
	progress := fake.event(t, "progress", `{"text":"started"}`)
	must(t, s.RecordEvent(progress))
	gap := progress
	gap.EventID = uuid.NewString()
	gap.Sequence = 3
	if !errors.Is(s.RecordEvent(gap), ErrConflict) {
		t.Fatal("gap accepted")
	}
	stale := a
	stale.Epoch++
	if !errors.Is(s.Accept(stale), ErrConflict) {
		t.Fatal("stale accepted")
	}
	bad := progress
	bad.Payload = json.RawMessage(`{"text":"changed"}`)
	if !errors.Is(s.RecordEvent(bad), ErrConflict) {
		t.Fatal("conflicting duplicate")
	}
	must(t, s.SetTaskState(task.ID, "running", "needs_attention"))
	must(t, s.Close())
	s, err = Open(path)
	must(t, err)
	fake.s = s
	got, err := s.GetTask(task.ID)
	must(t, err)
	if got.State != "needs_attention" {
		t.Fatal(got)
	}
	if _, err = s.ClaimTask(task.ID, runner); !errors.Is(err, ErrConflict) {
		t.Fatal("uncertain restart retried")
	}
	terminal := fake.event(t, "terminal", `{"state":"completed","result":{"text":"done"}}`)
	must(t, s.RecordEvent(terminal))
	must(t, s.Close())
	s, err = Open(path)
	must(t, err)
	must(t, s.RecordEvent(terminal))
	got, err = s.GetTask(task.ID)
	must(t, err)
	if got.State != "completed" || string(got.Result) != `{"text":"done"}` {
		t.Fatal(got)
	}
	n, err := s.EventCount(task.ID)
	must(t, err)
	if n != 2 {
		t.Fatal(n)
	}
}
func TestConcurrentClaimsAcrossConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s, err := Open(path)
	must(t, err)
	defer s.Close()
	other, err := Open(path)
	must(t, err)
	defer other.Close()
	b := botTest(t, s, "bot/concurrent")
	runner, err := s.CreateRunner("fake")
	must(t, err)
	tasks := make([]Task, 2)
	for i := range tasks {
		tasks[i], _, err = s.AdmitTask(b.ID, "", "", []byte(`{}`))
		must(t, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, db := range []*Store{s, other} {
		wg.Add(1)
		go func(i int, db *Store) { defer wg.Done(); _, err := db.ClaimTask(tasks[i].ID, runner); errs <- err }(i, db)
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("slot claims", wins)
	}
}
func TestCancellationAndSlotRetention(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/cancel")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	first, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	second, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(first.ID, runner)
	must(t, err)
	must(t, s.CancelTask(first.ID))
	must(t, s.Accept(a))
	if _, err = s.ClaimTask(second.ID, runner); !errors.Is(err, ErrConflict) {
		t.Fatal("released cancellation slot")
	}
	fake := fakeRunner{s: s, a: a}
	fake.event(t, "terminal", `{"state":"cancelled","result":{"reason":"confirmed stopped"}}`)
	if _, err = s.ClaimTask(second.ID, runner); err != nil {
		t.Fatal("slot not released", err)
	}
	no := false
	_, err = s.UpdateBot(b.ID, b.Revision, BotPatch{Enabled: &no})
	must(t, err)
	if _, _, err = s.AdmitTask(b.ID, "", "", []byte(`{}`)); !errors.Is(err, ErrDisabled) {
		t.Fatal("disabled admitted")
	}
	got, err := s.GetTask(second.ID)
	must(t, err)
	if got.State != "dispatching" {
		t.Fatal("disable cancelled task")
	}
	if err = s.SetTaskState(second.ID, "dispatching", "completed"); !errors.Is(err, ErrInvalid) {
		t.Fatal("unconfirmed completion")
	}
}
func TestTerminalRollbackAndRunnerFencing(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/fencing")
	runner, err := s.CreateRunner("owner")
	must(t, err)
	other, err := s.CreateRunner("other")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	r := Report{AssignmentID: a.ID, RunnerID: other, Epoch: a.Epoch, EventID: uuid.NewString(), Sequence: 1, Kind: "terminal", Payload: json.RawMessage(`{"state":"completed","result":"bad"}`)}
	if !errors.Is(s.RecordEvent(r), ErrConflict) {
		t.Fatal("wrong runner accepted")
	}
	r.RunnerID = runner
	if err = s.RecordEvent(r); err == nil {
		t.Fatal("unaccepted success")
	}
	must(t, s.Accept(a))
	must(t, s.SetTaskState(task.ID, "running", "waiting_child"))
	if err = s.RecordEvent(r); err == nil {
		t.Fatal("waiting child completed")
	}
	must(t, s.SetTaskState(task.ID, "waiting_child", "running"))
	// Force event persistence to fail after task/assignment updates: both must roll back.
	_, err = s.db.Exec(`CREATE TRIGGER reject_terminal BEFORE INSERT ON task_events BEGIN SELECT RAISE(ABORT,'injected failure'); END`)
	must(t, err)
	if err = s.RecordEvent(r); err == nil {
		t.Fatal("injected write succeeded")
	}
	got, err := s.GetTask(task.ID)
	must(t, err)
	if got.State != "running" || len(got.Result) != 0 {
		t.Fatal("partial terminal write", got)
	}
	_, err = s.db.Exec(`DROP TRIGGER reject_terminal`)
	must(t, err)
	r.Payload = json.RawMessage(`{"state":"failed","result":{"code":"runner_exit"}}`)
	must(t, s.RecordEvent(r))
}

func TestRevocationDoesNotRedispatchOrLoseEvidence(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/revoke")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	must(t, s.RevokeRunner(runner))
	if _, err = s.ClaimTask(task.ID, runner); !errors.Is(err, ErrDisabled) {
		t.Fatal("revoked redispatch", err)
	}
	// Persisting already-authenticated reconciliation evidence remains possible;
	// the future connection layer owns runner credential admission.
	must(t, s.CancelTask(task.ID))
	f := fakeRunner{s: s, a: a}
	f.event(t, "terminal", `{"state":"cancelled","result":"termination confirmed"}`)
}

func TestDispatchFailureAndRevokedRunner(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/start-failure")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	f := fakeRunner{s: s, a: a}
	terminal := f.event(t, "terminal", `{"state":"failed","result":{"code":"start_failed"}}`)
	events, err := s.ListEvents(task.ID)
	must(t, err)
	if len(events) != 1 || events[0].EventID != terminal.EventID {
		t.Fatal(events)
	}
	must(t, s.RevokeRunner(runner))
	next, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	if _, err = s.ClaimTask(next.ID, runner); !errors.Is(err, ErrDisabled) {
		t.Fatal("revoked runner claimed", err)
	}
}

func TestClaimUsesAdmittedRunnerSnapshot(t *testing.T) {
	s := openTest(t)
	first, err := s.CreateRunner("first")
	must(t, err)
	second, err := s.CreateRunner("second")
	must(t, err)
	b, err := s.CreateBot(Bot{Alias: "bot/pinned", Name: "Pinned", RunnerID: first, Enabled: true})
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	_, err = s.UpdateBot(b.ID, b.Revision, BotPatch{RunnerID: &second})
	must(t, err)
	if _, err = s.ClaimTask(task.ID, second); !errors.Is(err, ErrConflict) {
		t.Fatal("queued config changed", err)
	}
	if _, err = s.ClaimTask(task.ID, first); err != nil {
		t.Fatal(err)
	}
}

func TestCRUDValidationAndNoKeyAdmission(t *testing.T) {
	s := openTest(t)
	if _, err := s.CreateBot(Bot{Alias: "bad alias", Name: "bad"}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	b := botTest(t, s, "bot/crud")
	list, err := s.ListBots()
	must(t, err)
	if len(list) != 1 || list[0].ID != b.ID {
		t.Fatal(list)
	}
	clear := ""
	no := false
	b, err = s.UpdateBot(b.ID, b.Revision, BotPatch{Instructions: &clear, SharedWrite: &no})
	must(t, err)
	if b.Instructions != "" {
		t.Fatal("explicit clear failed")
	}
	if !errors.Is(s.ReplaceContacts(b.ID, []Contact{{b.ID, "self"}}), ErrInvalid) {
		t.Fatal("self contact")
	}
	a, _, err := s.AdmitTask(b.ID, "", "", []byte(`{"x":1}`))
	must(t, err)
	c, _, err := s.AdmitTask(b.ID, "", "", []byte(`{"x":1}`))
	must(t, err)
	if a.ID == c.ID {
		t.Fatal("unkeyed requests deduplicated")
	}
	if _, _, err = s.AdmitTask(b.ID, "", "", []byte(`{} trailing`)); !errors.Is(err, ErrInvalid) {
		t.Fatal("bad JSON accepted")
	}
	must(t, s.CancelTask(a.ID))
	must(t, s.CancelTask(c.ID))
	must(t, s.DeleteBot(b.ID))
	list, err = s.ListBots()
	must(t, err)
	if len(list) != 0 {
		t.Fatal(list)
	}
}

func TestTypedProgressAndAcceptance(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/progress")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	header := contracts.RunnerRecord{Version: 1, Type: "accepted", ID: uuid.NewString(), RunnerID: runner, AssignmentID: a.ID, Epoch: a.Epoch}
	must(t, s.AcceptRecord(contracts.Accepted{RunnerRecord: header, Accepted: true}))
	header.Type = "event"
	header.ID = uuid.NewString()
	event := contracts.Event{RunnerRecord: header, Sequence: 1, EventID: uuid.NewString(), Kind: "progress", Payload: json.RawMessage(`{"text":"working"}`)}
	must(t, s.AppendRunnerEvent(event))
	must(t, s.AppendRunnerEvent(event))
	event.Kind = "terminal"
	if err = s.AppendRunnerEvent(event); !errors.Is(err, ErrInvalid) {
		t.Fatal("terminal bypass", err)
	}
}

func TestTypedTerminalContractPersistsFailure(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/wire")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "", "", []byte(`{}`))
	must(t, err)
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	record := contracts.Terminal{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "terminal", ID: uuid.NewString(), RunnerID: runner, AssignmentID: a.ID, Epoch: a.Epoch}, State: contracts.Failed, Error: &contracts.Error{Code: "start_failed", Message: "process unavailable"}}
	must(t, s.RecordTerminal(record, 1))
	must(t, s.RecordTerminal(record, 1))
	got, err := s.GetTask(task.ID)
	must(t, err)
	var domain contracts.Error
	must(t, json.Unmarshal(got.Result, &domain))
	if domain.Code != "start_failed" || got.State != "failed" {
		t.Fatal(got)
	}
}

func TestInputBounds(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/bounds")
	oversized := strings.Repeat("x", 65537)
	if _, err := s.UpdateBot(b.ID, b.Revision, BotPatch{Instructions: &oversized}); !errors.Is(err, ErrInvalid) {
		t.Fatal("instructions bound", err)
	}
	if err := s.ReplaceContacts(b.ID, make([]Contact, 257)); !errors.Is(err, ErrInvalid) {
		t.Fatal("contacts bound", err)
	}
	if _, _, err := s.AdmitTask(b.ID, "", "", []byte(`"`+strings.Repeat("x", 1<<20)+`"`)); !errors.Is(err, ErrInvalid) {
		t.Fatal("input bound", err)
	}
}

func TestRestartAtAdmissionDispatchCancellationBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boundaries.db")
	s, err := Open(path)
	must(t, err)
	defer func() { s.Close() }()
	b := botTest(t, s, "bot/boundary")
	runner, err := s.CreateRunner("runner")
	must(t, err)
	task, _, err := s.AdmitTask(b.ID, "owner", "stable", []byte(`{"prompt":"work"}`))
	must(t, err)
	reopen := func() { must(t, s.Close()); s, err = Open(path); must(t, err) }
	reopen()
	got, err := s.GetTask(task.ID)
	must(t, err)
	if got.State != "queued" {
		t.Fatal(got)
	}
	again, fresh, err := s.AdmitTask(b.ID, "owner", "stable", task.Input)
	must(t, err)
	if fresh || again.ID != task.ID {
		t.Fatal("lost admission identity")
	}
	a, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	reopen()
	duplicate, err := s.ClaimTask(task.ID, runner)
	must(t, err)
	if duplicate.ID != a.ID {
		t.Fatal("lost dispatch identity")
	}
	must(t, s.CancelTask(task.ID))
	reopen()
	got, err = s.GetTask(task.ID)
	must(t, err)
	if got.State != "cancelling" || !got.CancelRequested {
		t.Fatal("lost cancellation", got)
	}
	f := fakeRunner{s: s, a: a}
	f.event(t, "terminal", `{"state":"cancelled","result":"stopped"}`)
	again, fresh, err = s.AdmitTask(b.ID, "owner", "stable", task.Input)
	must(t, err)
	if fresh || again.ID != task.ID || again.State != "cancelled" {
		t.Fatal("retry must return retained outcome, not repeat work")
	}
}

func TestConcurrentOptimisticUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "updates.db")
	s, err := Open(path)
	must(t, err)
	defer s.Close()
	other, err := Open(path)
	must(t, err)
	defer other.Close()
	b := botTest(t, s, "bot/update")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i, db := range []*Store{s, other} {
		wg.Add(1)
		go func(i int, db *Store) {
			defer wg.Done()
			name := []string{"one", "two"}[i]
			_, err := db.UpdateBot(b.ID, b.Revision, BotPatch{Name: &name})
			results <- err
		}(i, db)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("lost revision guard", wins)
	}
}

func TestMigrationAndForeignKeys(t *testing.T) {
	s := openTest(t)
	must(t, s.migrate())
	_, err := s.db.Exec(`INSERT INTO contacts VALUES('missing','other','')`)
	if err == nil {
		t.Fatal("foreign keys off")
	}
	var version int
	must(t, s.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version))
	if version != 3 {
		t.Fatal(version)
	}
	_, err = s.db.Exec(`INSERT INTO schema_migrations VALUES(99)`)
	must(t, err)
	if err = s.migrate(); err == nil {
		t.Fatal("future schema accepted")
	}
}
