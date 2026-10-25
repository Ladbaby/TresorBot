package runnertransport

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"tresorbot/internal/contracts"
	"tresorbot/internal/store"
)

func header(kind, id string) contracts.RunnerRecord {
	return contracts.RunnerRecord{Version: 1, Type: kind, ID: contracts.NewID(), RunnerID: id}
}
func writeRecord(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e = c.Write(ctx, websocket.MessageText, b); e != nil {
		t.Fatal(e)
	}
}
func TestAuthenticatedHandshakeDispatchAndDurableAcknowledgement(t *testing.T) {
	s, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	id, e := s.CreateRunner("earth")
	if e != nil {
		t.Fatal(e)
	}
	token, _, e := s.IssueRunnerCredential(id)
	if e != nil {
		t.Fatal(e)
	}
	ready := make(chan struct{}, 4)
	hub := New(Options{Store: s, AuthorizeAssignment: func(_ context.Context, a store.Assignment, _ store.Task) (*GatewayScope, error) {
		return &GatewayScope{URL: "http://127.0.0.1:11510", Capability: "assignment-only", ExecutionID: a.ExecutionID, AssignmentID: a.ID, Epoch: a.Epoch, Alias: "bot/test"}, nil
	}, Changed: func() { ready <- struct{}{} }})
	server := httptest.NewServer(hub)
	defer server.Close()
	defer hub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	c, _, e := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}, "X-Runner-ID": {id}}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	if hub.Available(id) {
		t.Fatal("available before handshake")
	}
	writeRecord(t, c, contracts.Control{RunnerRecord: header("hello", id)})
	writeRecord(t, c, contracts.Control{RunnerRecord: header("reconcile", id)})
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !hub.Available(id) {
		t.Fatal("not ready")
	}
	bot, e := s.CreateBot(store.Bot{Alias: "bot/test", Name: "Test", RunnerID: id,Workspace:t.TempDir(), Adapter: "pi", Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	task, _, e := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.ClaimNext(bot.ID, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = hub.Dispatch(ctx, a); e != nil {
		t.Fatal(e)
	}
	_, b, e := c.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := contracts.DecodeRunnerRecord(b)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e = contracts.DecodeRunnerRecord(b)
	if control, ok := decoded.(*contracts.Control); ok && control.Type == "ready" {
		_, b, e = c.Read(ctx)
		if e != nil {
			t.Fatal(e)
		}
		decoded, e = contracts.DecodeRunnerRecord(b)
	}
	if e != nil {
		t.Fatal(e)
	}
	assignment := decoded.(*contracts.Assign)
	var executionInput ExecutionInput
	if e = json.Unmarshal(assignment.Input, &executionInput); e != nil || executionInput.Gateway == nil || executionInput.Gateway.Capability != "assignment-only" || executionInput.Gateway.ExecutionID != a.ExecutionID {
		t.Fatalf("missing scoped authority: %+v %v", executionInput, e)
	}
	if assignment.AssignmentID != a.ID {
		t.Fatal(assignment)
	}
	accepted := contracts.Accepted{RunnerRecord: header("accepted", id), Accepted: true}
	accepted.AssignmentID = a.ID
	accepted.Epoch = a.Epoch
	writeRecord(t, c, accepted)
	event := contracts.Event{RunnerRecord: header("event", id), Sequence: 1, EventID: contracts.NewID(), Kind: "progress", Payload: json.RawMessage(`{"status":"started"}`)}
	event.AssignmentID = a.ID
	event.Epoch = a.Epoch
	writeRecord(t, c, event)
	_, b, e = c.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	ack, e := contracts.DecodeRunnerRecord(b)
	if e != nil {
		t.Fatal(e)
	}
	if ack.(*contracts.Control).EventID != event.EventID {
		t.Fatal(ack)
	}
	persisted, e := s.ListEvents(task.ID)
	if e != nil || len(persisted) != 1 {
		t.Fatal("ack before durable event", persisted, e)
	}
	hub.Invalidate(id)
	if hub.Available(id) {
		t.Fatal("invalidated connection ready")
	}
	current, e := s.GetTask(task.ID)
	if e != nil || current.State != "needs_attention" {
		t.Fatal("disconnect lost uncertainty", current, e)
	}
}
func readRecord(t *testing.T, ctx context.Context, c *websocket.Conn) any {
	t.Helper()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v, err := contracts.DecodeRunnerRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func connectReady(t *testing.T, ctx context.Context, url, id, token string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}, "X-Runner-ID": {id}}})
	if err != nil {
		t.Fatal(err)
	}
	writeRecord(t, c, contracts.Control{RunnerRecord: header("hello", id)})
	writeRecord(t, c, contracts.Control{RunnerRecord: header("reconcile", id)})
	if ready, ok := readRecord(t, ctx, c).(*contracts.Control); !ok || ready.Type != "ready" {
		t.Fatalf("expected ready, got %#v", ready)
	}
	return c
}

func TestTerminalReportRetryAfterReconnectIsIdempotentlyAcknowledged(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil { t.Fatal(err) }
	defer s.Close()
	id, err := s.CreateRunner("earth")
	if err != nil { t.Fatal(err) }
	token, _, err := s.IssueRunnerCredential(id)
	if err != nil { t.Fatal(err) }
	hub := New(Options{Store: s})
	server := httptest.NewServer(hub)
	defer server.Close()
	defer hub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	c := connectReady(t, ctx, url, id, token)
	bot, err := s.CreateBot(store.Bot{Alias: "bot/retry", Name: "Retry", RunnerID: id,Workspace:t.TempDir(), Adapter: "pi", Enabled: true})
	if err != nil { t.Fatal(err) }
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
	if err != nil { t.Fatal(err) }
	a, err := s.ClaimNext(bot.ID, id)
	if err != nil { t.Fatal(err) }
	if err = hub.Dispatch(ctx, a); err != nil { t.Fatal(err) }
	if _, ok := readRecord(t, ctx, c).(*contracts.Assign); !ok { t.Fatal("expected assignment") }
	accepted := contracts.Accepted{RunnerRecord: header("accepted", id), Accepted: true}
	accepted.AssignmentID, accepted.Epoch = a.ID, a.Epoch
	writeRecord(t, c, accepted)
	terminal := contracts.Terminal{RunnerRecord: header("terminal", id), Sequence: 1, State: contracts.Completed, Result: json.RawMessage(`{"result":"ok"}`)}
	terminal.AssignmentID, terminal.Epoch = a.ID, a.Epoch
	writeRecord(t, c, terminal)
	first, ok := readRecord(t, ctx, c).(*contracts.Control)
	if !ok || first.Type != "terminal_ack" || first.EventID != terminal.ID { t.Fatalf("first ack: %#v", first) }
	_ = c.Close(websocket.StatusNormalClosure, "reconnect")
	c = connectReady(t, ctx, url, id, token)
	defer c.CloseNow()
	terminal.AssignmentID, terminal.Epoch = a.ID, a.Epoch
	writeRecord(t, c, terminal)
	second, ok := readRecord(t, ctx, c).(*contracts.Control)
	if !ok || second.Type != "terminal_ack" || second.EventID != terminal.ID || second.AssignmentID != a.ID || second.Epoch != a.Epoch || second.Sequence != terminal.Sequence { t.Fatalf("retry ack: %#v", second) }
	current, err := s.GetTask(task.ID)
	if err != nil || current.State != "completed" || current.AssignmentID != a.ID { t.Fatalf("task changed: %+v %v", current, err) }
	events, err := s.ListEvents(task.ID)
	if err != nil || len(events) != 1 { t.Fatalf("events: %+v %v", events, err) }
}

func TestCredentialRotationFencesLiveAcceptedAssignment(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil { t.Fatal(err) }
	defer s.Close()
	id, err := s.CreateRunner("earth")
	if err != nil { t.Fatal(err) }
	token, _, err := s.IssueRunnerCredential(id)
	if err != nil { t.Fatal(err) }
	hub := New(Options{Store: s})
	server := httptest.NewServer(hub)
	defer server.Close()
	defer hub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	c := connectReady(t, ctx, url, id, token)
	bot, err := s.CreateBot(store.Bot{Alias: "bot/rotation", Name: "Rotation", RunnerID: id,Workspace:t.TempDir(), Adapter: "pi", Enabled: true})
	if err != nil { t.Fatal(err) }
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
	if err != nil { t.Fatal(err) }
	a, err := s.ClaimNext(bot.ID, id)
	if err != nil { t.Fatal(err) }
	if err = hub.Dispatch(ctx, a); err != nil { t.Fatal(err) }
	if _, ok := readRecord(t, ctx, c).(*contracts.Assign); !ok { t.Fatal("expected assignment") }
	accepted := contracts.Accepted{RunnerRecord: header("accepted", id), Accepted: true}
	accepted.AssignmentID, accepted.Epoch = a.ID, a.Epoch
	writeRecord(t, c, accepted)
	deadline, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	for {
		current, e := s.GetTask(task.ID)
		if e != nil { t.Fatal(e) }
		if current.State == "running" { break }
		select { case <-deadline.Done(): t.Fatal("acceptance was not persisted"); case <-time.After(5*time.Millisecond): }
	}
	if _, _, err = s.IssueRunnerCredential(id); err != nil { t.Fatal(err) }
	hub.Invalidate(id)
	terminal := contracts.Terminal{RunnerRecord: header("terminal", id), Sequence: 1, State: contracts.Completed, Result: json.RawMessage(`{"result":"stale"}`)}
	terminal.AssignmentID, terminal.Epoch = a.ID, a.Epoch
	writeRecord(t, c, terminal)
	_ = c.Close(websocket.StatusNormalClosure, "stale")
	if stale, resp, dialErr := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}, "X-Runner-ID": {id}}}); dialErr == nil {
		stale.CloseNow(); t.Fatal("rotated credential was accepted")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized { t.Fatalf("stale token response: %#v %v", resp, dialErr) }
	current, err := s.GetTask(task.ID)
	if err != nil || current.State != "needs_attention" || current.AssignmentID != a.ID || current.Epoch != a.Epoch { t.Fatalf("uncertain slot not retained: %+v %v", current, err) }
	events, err := s.ListEvents(task.ID)
	if err != nil || len(events) != 0 { t.Fatalf("false settled event: %+v %v", events, err) }
}

func TestRunnerAuthenticationAndHandshakeOrdering(t *testing.T) {
	s, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	id, e := s.CreateRunner("earth")
	if e != nil {
		t.Fatal(e)
	}
	token, _, e := s.IssueRunnerCredential(id)
	if e != nil {
		t.Fatal(e)
	}
	hub := New(Options{Store: s})
	server := httptest.NewServer(hub)
	defer server.Close()
	defer hub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	if c, resp, e := websocket.Dial(ctx, url, nil); e == nil {
		c.CloseNow()
		t.Fatal("unauthenticated connection accepted")
	} else if resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	c, _, e := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}, "X-Runner-ID": {id}}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	writeRecord(t, c, contracts.Control{RunnerRecord: header("reconcile", id)})
	if _, _, e = c.Read(ctx); e == nil {
		t.Fatal("reconcile before hello accepted")
	}
	if hub.Available(id) {
		t.Fatal("bad handshake ready")
	}
}
