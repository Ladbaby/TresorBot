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

func TestRevocationFencesLiveAssignmentWithoutCertifyingTermination(t *testing.T) {
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.CreateRunner("Earth")
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := s.IssueRunnerCredential(id)
	if err != nil {
		t.Fatal(err)
	}
	hub := New(Options{Store: s})
	defer hub.Close()
	server := httptest.NewServer(hub)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	c := connectReady(t, ctx, url, id, token)
	defer c.CloseNow()
	bot, err := s.CreateBot(store.Bot{Alias: "bot/revoke", Name: "Revoke", RunnerID: id,Workspace:t.TempDir(), Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := s.AdmitTask(bot.ID, "owner", "", []byte(`{"text":"work"}`))
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.ClaimNext(bot.ID, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = hub.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, ok := readRecord(t, ctx, c).(*contracts.Assign); !ok {
		t.Fatal("missing assignment")
	}
	accepted := contracts.Accepted{RunnerRecord: header("accepted", id), Accepted: true}
	accepted.AssignmentID = a.ID
	accepted.Epoch = a.Epoch
	writeRecord(t, c, accepted)
	progress := contracts.Event{RunnerRecord: header("event", id), Sequence: 1, EventID: contracts.NewID(), Kind: "progress", Payload: json.RawMessage(`{"status":"running"}`)}
	progress.AssignmentID = a.ID
	progress.Epoch = a.Epoch
	writeRecord(t, c, progress)
	if ack, ok := readRecord(t, ctx, c).(*contracts.Control); !ok || ack.Type != "event_ack" {
		t.Fatal("progress not acknowledged")
	}
	if err = s.RevokeRunner(id); err != nil {
		t.Fatal(err)
	}
	hub.Invalidate(id)
	terminal := contracts.Terminal{RunnerRecord: header("terminal", id), Sequence: 2, State: contracts.Completed}
	terminal.AssignmentID = a.ID
	terminal.Epoch = a.Epoch
	b, _ := json.Marshal(terminal)
	_ = c.Write(ctx, websocket.MessageText, b)
	if _, _, err = c.Read(ctx); err == nil {
		t.Fatal("revoked connection received acknowledgement")
	}
	if other, response, e := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}, "X-Runner-ID": {id}}}); e == nil {
		other.CloseNow()
		t.Fatal("revoked token accepted")
	} else if response == nil || response.StatusCode != 401 {
		t.Fatal("unexpected rejection", response, e)
	}
	got, err := s.GetTask(task.ID)
	if err != nil || got.State != "needs_attention" || got.AssignmentID != a.ID {
		t.Fatal("lost uncertainty", got, err)
	}
	events, err := s.ListEvents(task.ID)
	if err != nil || len(events) != 1 || events[0].Kind != "progress" {
		t.Fatal("false terminal", events, err)
	}
}
