package earth_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthjournal"
	"tresorbot/internal/piadapter"
	"tresorbot/internal/store"
	"tresorbot/internal/sun"
)

type uncertainExecutor struct{ calls chan string }

func (e *uncertainExecutor) Execute(ctx context.Context, a contracts.Assign, started func(int, string) error, progress func(piadapter.Progress)) (piadapter.Result, error) {
	e.calls <- a.AssignmentID
	if err := started(123, a.AssignmentID); err != nil {
		return piadapter.Result{}, err
	}
	return piadapter.Result{}, context.DeadlineExceeded
}

func TestStartedExecutorErrorJournalsNonterminalUncertainty(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "sun.db")
	app, err := sun.Open(config.Config{Role: "sun", DBPath: db, MemoryRoot: filepath.Join(dir, "memory"), BindAddr: "127.0.0.1:11520"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(app.Handler)
	defer server.Close()
	defer app.Close()
	s, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	runner, err := s.CreateRunner("Earth")
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := s.IssueRunnerCredential(runner)
	if err != nil {
		t.Fatal(err)
	}
	bot, err := s.CreateBot(store.Bot{Alias: "bot/uncertain", Name: "Uncertain", RunnerID: runner, Adapter: "pi", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	journal, err := earthjournal.Open(filepath.Join(dir, "earth.db"), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	executor := &uncertainExecutor{calls: make(chan string, 1)}
	client, err := earth.New(earth.Options{SunURL: server.URL, RunnerID: runner, Token: token, Journal: journal, Executor: executor, DefaultWorkspace: func() string { return dir }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil {
				t.Error(runErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("Earth shutdown blocked")
		}
	}()
	select {
	case <-executor.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("assignment was not executed")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := s.GetTask(task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State == "needs_attention" {
			if got.AssignmentID == "" {
				t.Fatal("uncertainty released assignment")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("uncertainty was not journaled", got.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	gotTask, err := s.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	record, err := journal.Get(gotTask.AssignmentID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != earthjournal.Running || record.LastSequence != 1 {
		t.Fatalf("unexpected journal state: %+v", record)
	}
	reports, err := s.ListEvents(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Kind != "execution_uncertain" || reports[0].Sequence != 1 || string(reports[0].Payload) != `{"reason":"executor_failed_after_start"}` {
		t.Fatalf("unexpected uncertainty reports: %+v", reports)
	}
	var event contracts.Event
	if err = json.Unmarshal(reports[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	if reports[0].AssignmentID != gotTask.AssignmentID || reports[0].RunnerID != runner || reports[0].Epoch != gotTask.Epoch {
		t.Fatalf("incorrect event identity: %+v", event.RunnerRecord)
	}
}

type completingExecutor struct{ calls chan string }

func (e *completingExecutor) Execute(ctx context.Context, a contracts.Assign, started func(int, string) error, progress func(piadapter.Progress)) (piadapter.Result, error) {
	e.calls <- a.AssignmentID
	if err := started(123, a.AssignmentID); err != nil {
		return piadapter.Result{}, err
	}
	return piadapter.Result{Text: "test result", Completed: true, ProcessExited: true}, nil
}
func TestOutboundRunnerDispatchesAndPersistsTerminalResult(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "sun.db")
	app, e := sun.Open(config.Config{Role: "sun", DBPath: db, MemoryRoot: filepath.Join(dir, "memory"), BindAddr: "127.0.0.1:11520"})
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(app.Handler)
	defer server.Close()
	defer app.Close()
	s, e := store.Open(db)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	runner, e := s.CreateRunner("Earth")
	if e != nil {
		t.Fatal(e)
	}
	token, _, e := s.IssueRunnerCredential(runner)
	if e != nil {
		t.Fatal(e)
	}
	bot, e := s.CreateBot(store.Bot{Alias: "bot/test", Name: "Test", RunnerID: runner, Adapter: "pi", Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	task, _, e := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"test"}`))
	if e != nil {
		t.Fatal(e)
	}
	journal, e := earthjournal.Open(filepath.Join(dir, "earth.db"), 100)
	if e != nil {
		t.Fatal(e)
	}
	defer journal.Close()
	executor := &completingExecutor{calls: make(chan string, 2)}
	client, e := earth.New(earth.Options{SunURL: server.URL, RunnerID: runner, Token: token, Journal: journal, Executor: executor, DefaultWorkspace: func() string { return dir }})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(5 * time.Second):
			t.Error("Earth shutdown blocked")
		}
	}()
	select {
	case <-executor.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("no assignment dispatched")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, e := s.GetTask(task.ID)
		if e != nil {
			t.Fatal(e)
		}
		if got.State == "completed" {
			var pinned store.Bot
			if json.Unmarshal(got.Snapshot, &pinned) != nil || pinned.Workspace != dir {
				t.Fatal("inherited workspace not pinned", string(got.Snapshot))
			}
			var result map[string]string
			if json.Unmarshal(got.Result, &result) != nil || result["text"] != "test result" {
				t.Fatal(string(got.Result))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal not persisted", got.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-executor.calls:
		t.Fatal("duplicate execution")
	default:
	}
}
