package sun

import (
	"context"
	"sync"

	"tresorbot/internal/scheduler"
	"tresorbot/internal/store"
)

// dispatchLoop wakes on committed work or runner activity, not execution timers.
// Connectivity never releases an occupied slot or replaces an assignment.
type dispatchLoop struct {
	store            *store.Store
	transport        scheduler.Transport
	cancelAssignment func(context.Context, store.Assignment) error
	wake             chan struct{}
	cancel           context.CancelFunc
	done             chan struct{}
	once             sync.Once
}

func newDispatchLoop(s *store.Store, t scheduler.Transport, cancelAssignment func(context.Context, store.Assignment) error) *dispatchLoop {
	ctx, cancel := context.WithCancel(context.Background())
	d := &dispatchLoop{store: s, transport: t, cancelAssignment: cancelAssignment, wake: make(chan struct{}, 1), cancel: cancel, done: make(chan struct{})}
	go d.run(ctx)
	return d
}
func (d *dispatchLoop) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}
func (d *dispatchLoop) Close() { d.once.Do(d.cancel); <-d.done }
func (d *dispatchLoop) run(ctx context.Context) {
	defer close(d.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.wake:
			d.pass(ctx)
		}
	}
}
func (d *dispatchLoop) pass(ctx context.Context) {
	runners, err := d.store.ListRunners()
	if err != nil {
		return
	}
	for _, r := range runners {
		if r.Revoked || !d.transport.Available(r.ID) {
			continue
		}
		assignments, err := d.store.CurrentRunnerAssignments(r.ID)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.CancelRequested && a.State != "completed" && a.State != "failed" && a.State != "cancelled" && d.cancelAssignment != nil {
				_ = d.cancelAssignment(ctx, a.Assignment)
			}
		}
	}
	bots, err := d.store.ListBots()
	if err != nil {
		return
	}
	s := scheduler.Scheduler{Store: d.store, Transport: d.transport}
	for _, b := range bots {
		if ctx.Err() != nil {
			return
		}
		if !b.Enabled || b.RunnerID == "" {
			continue
		}
		_, _ = s.DispatchNext(ctx, b.ID, b.RunnerID)
	}
}
