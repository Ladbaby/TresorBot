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
	bot, e := s.CreateBot(store.Bot{Alias: "bot/test", Name: "Test", RunnerID: runner, Workspace: dir, Adapter: "pi", Enabled: true})
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
	client, e := earth.New(earth.Options{SunURL: server.URL, RunnerID: runner, Token: token, Journal: journal, Executor: executor})
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
