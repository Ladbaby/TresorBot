package earthjournal

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"tresorbot/internal/contracts"
)

func aid(n string) string { return "00000000-0000-4000-8000-" + n }
func assign() contracts.Assign {
	return contracts.Assign{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "assign", ID: aid("000000000001"), RunnerID: aid("000000000002"), AssignmentID: aid("000000000003"), Epoch: 4}, TaskID: aid("000000000004"), ExecutionID: aid("000000000005"), Input: json.RawMessage(`{"x":1}`)}
}
func openTest(t *testing.T, cap int) (*Journal, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "journal.db")
	j, e := Open(p, cap)
	if e != nil {
		t.Fatal(e)
	}
	return j, p
}
func TestAdmitConflictAndRestartRecovery(t *testing.T) {
	j, p := openTest(t, 4)
	a := assign()
	if ok, e := j.Admit(a); e != nil || !ok {
		t.Fatalf("admit %v %v", ok, e)
	}
	if ok, e := j.Admit(a); e != nil || ok {
		t.Fatalf("duplicate %v %v", ok, e)
	}
	a.Input = json.RawMessage(`{"x":2}`)
	if _, e := j.Admit(a); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	a = assign()
	if e := j.BeginLaunch(a.AssignmentID, a.RunnerID, a.Epoch); e != nil {
		t.Fatal(e)
	}
	j.Close()
	j, e := Open(p, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	if e = j.Recover(); e != nil {
		t.Fatal(e)
	}
	r, e := j.Get(a.AssignmentID)
	if e != nil || r.State != Uncertain {
		t.Fatalf("%+v %v", r, e)
	}
	if e = j.BeginLaunch(a.AssignmentID, a.RunnerID, a.Epoch); !errors.Is(e, ErrIneligible) {
		t.Fatal(e)
	}
}
func TestConcurrentLaunchAndIdentity(t *testing.T) {
	j, _ := openTest(t, 3)
	defer j.Close()
	a := assign()
	j.Admit(a)
	var wg sync.WaitGroup
	var successes int
	var mu sync.Mutex
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if j.BeginLaunch(a.AssignmentID, a.RunnerID, a.Epoch) == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("successful launches %d", successes)
	}
	if e := j.MarkStarted(a.AssignmentID, a.RunnerID, a.Epoch, 55, "session"); e != nil {
		t.Fatal(e)
	}
}
func TestOutboxContiguousBoundedAckAndTerminalReplay(t *testing.T) {
	j, p := openTest(t, 2)
	a := assign()
	j.Admit(a)
	j.BeginLaunch(a.AssignmentID, a.RunnerID, a.Epoch)
	for i := uint64(1); i <= 2; i++ {
		ev := contracts.Event{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "event", ID: aid("000000000010"), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, Sequence: i, EventID: aid("000000000011"), Kind: "progress", Payload: json.RawMessage(`{}`)}
		if _, e := j.Append(a.AssignmentID, a.RunnerID, a.Epoch, ev); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := j.Append(a.AssignmentID, a.RunnerID, a.Epoch, contracts.Event{}); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	if e := j.Acknowledge(a.AssignmentID, 1); e != nil {
		t.Fatal(e)
	}
	if e := j.Acknowledge(a.AssignmentID, 1); e != nil {
		t.Fatal(e)
	}
	j.Close()
	j, e := Open(p, 2)
	if e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	if _, e = j.Append(a.AssignmentID, a.RunnerID, a.Epoch, contracts.Event{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "event", ID: aid("000000000010"), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, Sequence: 3, EventID: aid("000000000012"), Kind: "progress", Payload: json.RawMessage(`{}`)}); e != nil {
		t.Fatal(e)
	}
	term := contracts.Terminal{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "terminal", ID: aid("000000000013"), RunnerID: a.RunnerID, AssignmentID: a.AssignmentID, Epoch: a.Epoch}, State: contracts.Completed, Result: json.RawMessage(`{"ok":true}`)}
	if _, e = j.Append(a.AssignmentID, a.RunnerID, a.Epoch, term); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	j.Acknowledge(a.AssignmentID, 3)
	if _, e = j.Append(a.AssignmentID, a.RunnerID, a.Epoch, term); e != nil {
		t.Fatal(e)
	}
	r, e := j.Get(a.AssignmentID)
	if e != nil || r.State != Terminal {
		t.Fatalf("%+v %v", r, e)
	}
	out, e := j.Reports(a.AssignmentID)
	if e != nil || len(out) != 1 {
		t.Fatalf("%d %v", len(out), e)
	}
	if e = j.Recover(); e != nil {
		t.Fatal(e)
	}
	out, e = j.Reports(a.AssignmentID)
	if e != nil || len(out) != 1 {
		t.Fatal(e)
	}
}
