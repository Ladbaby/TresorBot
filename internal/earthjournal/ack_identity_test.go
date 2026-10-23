package earthjournal

import (
	"encoding/json"
	"errors"
	"testing"
	"tresorbot/internal/contracts"
)

func TestAcknowledgementChecksExactReportIdentityAfterRestart(t *testing.T) {
	j, path := openTest(t, 8)
	a := assign()
	if _, e := j.Admit(a); e != nil {
		t.Fatal(e)
	}
	event := contracts.Event{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "event", ID: contracts.NewID(), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, Sequence: 1, EventID: contracts.NewID(), Kind: "progress", Payload: json.RawMessage(`{}`)}
	if _, e := j.Append(a.AssignmentID, a.RunnerID, a.Epoch, event); e != nil {
		t.Fatal(e)
	}
	ack := contracts.Control{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "event_ack", ID: contracts.NewID(), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, Sequence: 1, EventID: event.EventID}
	wrong := ack
	wrong.EventID = contracts.NewID()
	if e := j.AcknowledgeRecord(wrong); !errors.Is(e, ErrIdentity) {
		t.Fatal("forged ack", e)
	}
	if e := j.AcknowledgeRecord(ack); e != nil {
		t.Fatal("progress ack failed", e)
	}
	terminal := contracts.Terminal{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "terminal", ID: contracts.NewID(), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, State: contracts.Completed, Result: json.RawMessage(`{"text":"done"}`)}
	if _, e := j.Append(a.AssignmentID, a.RunnerID, a.Epoch, terminal); e != nil {
		t.Fatal(e)
	}
	last := ack
	last.Type = "terminal_ack"
	last.EventID = terminal.ID
	last.Sequence = 2
	if e := j.AcknowledgeRecord(last); e != nil {
		t.Fatal("terminal ack failed", e)
	}
	if e := j.RequestCancel(a.AssignmentID, a.RunnerID, a.Epoch); e != nil {
		t.Fatal(e)
	}
	j.Close()
	j, e := Open(path, 8)
	if e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	for _, record := range []contracts.Control{ack, last} {
		if e = j.AcknowledgeRecord(record); e != nil {
			t.Fatal("valid duplicate ack lost", e)
		}
	}
	wrong = ack
	wrong.Epoch++
	if e = j.AcknowledgeRecord(wrong); !errors.Is(e, ErrIdentity) {
		t.Fatal("stale epoch accepted", e)
	}
	r, e := j.Get(a.AssignmentID)
	if e != nil || !r.CancelRequested || r.Acknowledged != 2 {
		t.Fatal(r, e)
	}
}
