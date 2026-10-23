package store

import (
	"errors"
	"testing"
)

func TestFIFOClaimsAndOwnerReplay(t *testing.T) {
	s := openTest(t)
	b := botTest(t, s, "bot/fifo")
	runner, e := s.CreateRunner("runner")
	must(t, e)
	first, _, e := s.AdmitTask(b.ID, "owner", "first", []byte(`{}`))
	must(t, e)
	second, _, e := s.AdmitTask(b.ID, "owner", "second", []byte(`{}`))
	must(t, e)
	if _, e = s.ClaimTask(second.ID, runner); !errors.Is(e, ErrConflict) {
		t.Fatal("FIFO bypass", e)
	}
	a, e := s.ClaimNext(b.ID, runner)
	must(t, e)
	if a.TaskID != first.ID {
		t.Fatal("wrong FIFO claim")
	}
	if _, e = s.ClaimNext(b.ID, runner); !errors.Is(e, ErrConflict) {
		t.Fatal("slot reused", e)
	}
	events, e := s.ReadOwnerEvents(0, first.ID, 100)
	must(t, e)
	if len(events) < 2 {
		t.Fatal("missing admission/dispatch events")
	}
	last := events[len(events)-1].Cursor
	must(t, s.CancelTask(first.ID))
	replay, e := s.ReadOwnerEvents(last, first.ID, 100)
	must(t, e)
	if len(replay) != 1 || replay[0].Cursor <= last {
		t.Fatal("invalid replay", replay)
	}
}

func TestDelegationRejectsCrossRootWaitCycle(t *testing.T) {
	s := openTest(t)
	a := botTest(t, s, "bot/a")
	b := botTest(t, s, "bot/b")
	must(t, s.ReplaceContacts(a.ID, []Contact{{Target: b.ID}}))
	must(t, s.ReplaceContacts(b.ID, []Contact{{Target: a.ID}}))
	runner, e := s.CreateRunner("runner")
	must(t, e)
	p, _, e := s.AdmitTask(a.ID, "owner", "", []byte(`{}`))
	must(t, e)
	q, _, e := s.AdmitTask(b.ID, "owner", "", []byte(`{}`))
	must(t, e)
	pa, e := s.ClaimTask(p.ID, runner)
	must(t, e)
	qa, e := s.ClaimTask(q.ID, runner)
	must(t, e)
	must(t, s.Accept(pa))
	must(t, s.Accept(qa))
	child, e := s.AdmitChild(p.ID, b.ID, "operation", []byte(`{"text":"child"}`))
	must(t, e)
	again, e := s.AdmitChild(p.ID, b.ID, "operation", []byte(`{"text":"child"}`))
	must(t, e)
	if child.ID != again.ID {
		t.Fatal("duplicate child")
	}
	if _, e = s.AdmitChild(q.ID, a.ID, "cycle", []byte(`{}`)); !errors.Is(e, ErrConflict) {
		t.Fatal("cross-root cycle accepted", e)
	}
	must(t, s.CancelTask(p.ID))
	child, e = s.GetTask(child.ID)
	must(t, e)
	if child.State != "cancelled" {
		t.Fatal("child cancellation did not cascade", child)
	}
	q, e = s.GetTask(q.ID)
	must(t, e)
	if q.State != "running" {
		t.Fatal("rejected delegation changed parent", q)
	}
}
