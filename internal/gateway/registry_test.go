package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		if r.URL.Path == "/api/integrations/bindings/resolve" {
			json.NewEncoder(w).Encode(map[string]string{"binding_id": strings.Repeat("b", 64)})
			return
		}
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
	changed.URL = "http://127.0.0.1:1"
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

func TestBindingDiscoveryRefusesRedirectsAndOversizedResponses(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer target.Close()
	for _, mode := range []string{"redirect", "oversized", "malformed"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch mode {
			case "redirect":
				http.Redirect(w, r, target.URL, 307)
			case "oversized":
				w.Write([]byte(strings.Repeat(" ", 4097) + `{"binding_id":"` + strings.Repeat("a", 64) + `"}`))
			default:
				w.Write([]byte(`{"binding_id":`))
			}
		}))
		registry, err := New(Config{URL: server.URL, ControlToken: "control", ServiceToken: "service"}, s)
		if err != nil {
			t.Fatal(err)
		}
		_, err = registry.ResolveBinding(context.Background())
		server.Close()
		if err == nil {
			t.Fatal("accepted", mode)
		}
	}
	if leaked.Load() != 0 {
		t.Fatal("credentials forwarded via redirect")
	}
}

func TestConcurrentBindingDiscoverySharesLookupAndAllowsCancelledWaiter(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		json.NewEncoder(w).Encode(map[string]string{"binding_id": strings.Repeat("f", 64)})
	}))
	defer server.Close()
	registry, err := New(Config{URL: server.URL, ControlToken: "control", ServiceToken: "service"}, s)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := registry.ResolveBinding(context.Background()); result <- err }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.ResolveBinding(ctx); err != context.Canceled {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if _, err := registry.ResolveBinding(ctx); err != context.DeadlineExceeded {
				t.Errorf("waiting lookup: %v", err)
			}
		}()
	}
	wg.Wait()
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("concurrent lookup duplicated", calls.Load())
	}
}

func TestBindingDiscoveryAuthenticatesAndRevalidates(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var status atomic.Int32
	status.Store(200)
	var binding atomic.Value
	binding.Store(strings.Repeat("d", 64))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/integrations/bindings/resolve" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer control" {
			t.Error("wrong discovery request")
		}
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["service_token"] != "service" {
			t.Error("missing service credential")
		}
		w.WriteHeader(int(status.Load()))
		json.NewEncoder(w).Encode(map[string]string{"binding_id": binding.Load().(string)})
	}))
	defer server.Close()
	registry, err := New(Config{URL: server.URL, BindingID: "obsolete-rule-id", ControlToken: "control", ServiceToken: "service"}, s)
	if err != nil {
		t.Fatal(err)
	}
	got, err := registry.ResolveBinding(context.Background())
	if err != nil || got != strings.Repeat("d", 64) {
		t.Fatal(got, err)
	}
	binding.Store(strings.Repeat("e", 64))
	got, err = registry.ResolveBinding(context.Background())
	if err != nil || got != strings.Repeat("e", 64) {
		t.Fatal("stale discovery", got, err)
	}
	for _, code := range []int{401, 403, 409, 404, 405, 503, 302} {
		status.Store(int32(code))
		if _, err = registry.ResolveBinding(context.Background()); err == nil {
			t.Fatalf("accepted status %d", code)
		}
	}
	status.Store(200)
	binding.Store("invalid")
	if _, err = registry.ResolveBinding(context.Background()); err == nil {
		t.Fatal("accepted invalid binding")
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
			var changed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/integrations/bindings/resolve" {
					id := strings.Repeat("b", 64)
					if changed.Load() {
						id = strings.Repeat("c", 64)
					}
					json.NewEncoder(w).Encode(map[string]string{"binding_id": id})
					return
				}
				if r.Header.Get("Authorization") != "Bearer control" || r.Header.Get("X-Tresor-Integration-Service") != strings.Repeat("b", 64) {
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
			changed.Store(true)
			if _, err := registry.Ensure(context.Background(), a, bot); err == nil || posts.Load() != 1 {
				t.Fatal("existing execution rebound after configuration change", err)
			}
			// Revocation uses the original durable binding, not the newly discovered one.
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
