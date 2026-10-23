package earth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"tresorbot/internal/config"
	"tresorbot/internal/earth"
	"tresorbot/internal/earthjournal"
	"tresorbot/internal/store"
	"tresorbot/internal/sun"
)

// Drop the terminal ack only after the real Sun transport has persisted it.
// Earth must reconnect and drain its durable outbox, not launch again.
func TestLostTerminalAcknowledgementReplaysWithoutReexecution(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "sun.db")
	app, err := sun.Open(config.Config{Role: "sun", DBPath: db, MemoryRoot: filepath.Join(dir, "memory"), BindAddr: "127.0.0.1:11520"})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	server := httptest.NewServer(app.Handler)
	defer server.Close()
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
	bot, err := s.CreateBot(store.Bot{Alias: "bot/test", Name: "Test", RunnerID: runner, Workspace: dir, Adapter: "pi", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dropped atomic.Bool
	var connections atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream, _, err := websocket.Dial(r.Context(), "ws"+strings.TrimPrefix(server.URL, "http")+r.URL.Path, &websocket.DialOptions{HTTPHeader: r.Header.Clone()})
		if err != nil {
			http.Error(w, "upstream failed", 502)
			return
		}
		defer upstream.CloseNow()
		downstream, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer downstream.CloseNow()
		connections.Add(1)
		session, stop := context.WithCancel(r.Context())
		defer stop()
		copied := make(chan struct{})
		go func() {
			defer close(copied)
			defer stop()
			for {
				typ, b, e := downstream.Read(session)
				if e != nil {
					return
				}
				if upstream.Write(session, typ, b) != nil {
					return
				}
			}
		}()
		defer func() { stop(); downstream.CloseNow(); upstream.CloseNow(); <-copied }()
		for {
			typ, b, e := upstream.Read(session)
			if e != nil {
				return
			}
			var record struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(b, &record)
			if record.Type == "terminal_ack" && dropped.CompareAndSwap(false, true) {
				return
			}
			if downstream.Write(session, typ, b) != nil {
				return
			}
		}
	}))
	defer proxy.Close()
	journal, err := earthjournal.Open(filepath.Join(dir, "earth.db"), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	executor := &completingExecutor{calls: make(chan string, 4)}
	client, err := earth.New(earth.Options{SunURL: proxy.URL, RunnerID: runner, Token: token, Journal: journal, Executor: executor})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		records, e := journal.List()
		if e != nil {
			t.Fatal(e)
		}
		if dropped.Load() && connections.Load() >= 2 && len(records) == 1 && records[0].State == earthjournal.Terminal && records[0].Acknowledged == records[0].LastSequence && records[0].LastSequence > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("terminal replay not acknowledged", records, connections.Load(), dropped.Load())
		case <-ticker.C:
		}
	}
	got, err := s.GetTask(task.ID)
	if err != nil || got.State != "completed" {
		t.Fatal(got, err)
	}
	var result struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(got.Result, &result) != nil || result.Text != "test result" {
		t.Fatal("changed terminal result", string(got.Result))
	}
	events, err := s.ListEvents(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	terminal := 0
	for _, event := range events {
		if event.Kind == "terminal" {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatal("terminal event count", terminal, events)
	}
	if len(executor.calls) != 1 {
		t.Fatal("executor calls", len(executor.calls))
	}
}
