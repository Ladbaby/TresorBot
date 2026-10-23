package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"tresorbot/internal/store"
)

func TestFailedRevocationSurvivesRestartAndRejectsChangedGateway(t *testing.T) {
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "sun.db")
	s, err := store.Open(path)
	must(err)
	defer func() { s.Close() }()
	runner, err := s.CreateRunner("earth")
	must(err)
	bot, err := s.CreateBot(store.Bot{Alias: "bot/test", Name: "Test", Enabled: true, RunnerID: runner})
	must(err)
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
	must(err)
	a, err := s.ClaimTask(task.ID, runner)
	must(err)
	var fail atomic.Bool
	fail.Store(true)
	var posts, deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			deletes.Add(1)
			if fail.Load() {
				w.WriteHeader(503)
			} else {
				w.WriteHeader(204)
			}
			return
		}
		posts.Add(1)
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(map[string]string{"execution_id": a.ExecutionID, "capability": strings.Repeat("a", 64)})
	}))
	defer server.Close()
	cfg := Config{URL: server.URL, BindingID: "binding", ControlToken: "control", ServiceToken: "service"}
	registry, err := New(cfg, s)
	must(err)
	_, err = registry.Ensure(context.Background(), a, bot)
	must(err)
	must(s.CancelTask(task.ID))
	if registry.Cleanup(context.Background()) == nil {
		t.Fatal("failed revoke accepted")
	}
	g, err := s.GatewayExecution(a.ExecutionID)
	must(err)
	if g.State != "revoking" {
		t.Fatal(g.State)
	}
	must(s.Close())
	s, err = store.Open(path)
	must(err)
	changed := cfg
	changed.BindingID = "different-binding"
	other, err := New(changed, s)
	must(err)
	if other.Cleanup(context.Background()) == nil || deletes.Load() != 1 {
		t.Fatal("changed config sent old authority")
	}
	registry, err = New(cfg, s)
	must(err)
	fail.Store(false)
	must(registry.Cleanup(context.Background()))
	g, err = s.GatewayExecution(a.ExecutionID)
	must(err)
	if g.State != "revoked" || g.Capability != "" || posts.Load() != 1 || deletes.Load() != 2 {
		t.Fatal("recovery duplicated registration or retained capability", g.State, posts.Load(), deletes.Load())
	}
	must(registry.Cleanup(context.Background()))
	if deletes.Load() != 2 {
		t.Fatal("repeated settled cleanup")
	}
}

func TestRegistrationIsDurableAndOneShot(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "lost-response"}[lost], func(t *testing.T) {
			s, e := store.Open(":memory:")
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			runner, e := s.CreateRunner("earth")
			if e != nil {
				t.Fatal(e)
			}
			bot, e := s.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", RunnerID: runner, Enabled: true})
			if e != nil {
				t.Fatal(e)
			}
			_, _, e = s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
			if e != nil {
				t.Fatal(e)
			}
			a, e := s.ClaimNext(bot.ID, runner)
			if e != nil {
				t.Fatal(e)
			}
			var posts, deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer control" || r.Header.Get("X-Tresor-Integration-Service") != "binding" {
					t.Error("wrong control identity")
				}
				if r.Method == "DELETE" {
					deletes.Add(1)
					w.WriteHeader(204)
					return
				}
				posts.Add(1)
				if lost {
					w.WriteHeader(201)
					w.Write([]byte(`{"execution_id":`))
					return
				}
				var input map[string]any
				json.NewDecoder(r.Body).Decode(&input)
				w.WriteHeader(201)
				json.NewEncoder(w).Encode(map[string]string{"execution_id": a.ExecutionID, "capability": strings.Repeat("a", 64)})
			}))
			defer server.Close()
			registry, e := New(Config{URL: server.URL, BindingID: "binding", ControlToken: "control", ServiceToken: "service"}, s)
			if e != nil {
				t.Fatal(e)
			}
			g, e := registry.Ensure(context.Background(), a, bot)
			if (e != nil) != lost {
				t.Fatal(e)
			}
			_, e = registry.Ensure(context.Background(), a, bot)
			if (e != nil) != lost || posts.Load() != 1 {
				t.Fatalf("repeated registration posts=%d err=%v", posts.Load(), e)
			}
			if lost {
				return
			}
			if e = s.CancelTask(a.TaskID); e != nil {
				t.Fatal(e)
			}
			if e = registry.Cleanup(context.Background()); e != nil {
				t.Fatal(e)
			}
			g, e = s.GatewayExecution(g.ExecutionID)
			if e != nil || g.State != "revoked" || g.Capability != "" || deletes.Load() != 1 {
				t.Fatal(g, e)
			}
		})
	}
}
