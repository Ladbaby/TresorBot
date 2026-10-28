// Package scheduler connects durable queue claims to a caller-provided runner transport.
// Sun does not install a production transport until Earth connections are available.
package scheduler

import (
	"context"
	"errors"

	"tresorbot/internal/store"
)

var ErrUnavailable = errors.New("runner transport unavailable")

type Transport interface {
	Available(runnerID string) bool
	Dispatch(context.Context, store.Assignment) error
}
type Scheduler struct {
	Store     *store.Store
	Transport Transport
}

func (s Scheduler) DispatchNext(ctx context.Context, botID, runnerID string) (store.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return store.Assignment{}, err
	}
	if s.Transport == nil || !s.Transport.Available(runnerID) {
		return store.Assignment{}, ErrUnavailable
	}
	assignment, err := s.Store.ClaimNext(botID, runnerID)
	if err != nil {
		return assignment, err
	}
	if err = s.Transport.Dispatch(ctx, assignment); err != nil {
		// A failed send may already have reached Earth. Never release or automatically retry.
		return assignment, errors.Join(err, s.Store.SetTaskState(assignment.TaskID, "dispatching", "needs_attention"))
	}
	return assignment, nil
}

// DispatchNextForRunner claims and dispatches the next task eligible for a
// specific runner (SendToEarth pins and unowned defaults), regardless of the
// bot's configured default runner.
func (s Scheduler) DispatchNextForRunner(ctx context.Context, runnerID string) (store.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return store.Assignment{}, err
	}
	if s.Transport == nil || !s.Transport.Available(runnerID) {
		return store.Assignment{}, ErrUnavailable
	}
	assignment, err := s.Store.ClaimNextForRunner(runnerID)
	if err != nil {
		return assignment, err
	}
	if err = s.Transport.Dispatch(ctx, assignment); err != nil {
		return assignment, errors.Join(err, s.Store.SetTaskState(assignment.TaskID, "dispatching", "needs_attention"))
	}
	return assignment, nil
}
