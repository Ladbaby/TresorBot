package environments

import (
	"context"
	"encoding/json"
	"time"

	"tresorbot/internal/store"
)

// Delivery pump tuning.
const (
	// deliveryPollInterval is how often the pump looks for unsettled work. It is a
	// reconciliation loop rather than a notification hook, so the interval bounds
	// reply latency rather than correctness.
	deliveryPollInterval = 2 * time.Second
	// deliveryBatch bounds how much one pass does, so a large backlog cannot
	// monopolise the loop.
	deliveryBatch = 50
	// deliveryAttemptTimeout bounds one adapter call.
	deliveryAttemptTimeout = 30 * time.Second
	// maxDeliveryAttempts is where a delivery becomes dead. A reply that has
	// failed five times over a growing backoff is not going to succeed, and
	// retrying forever hides it from the operator.
	maxDeliveryAttempts = 5
	// deliveryBackoff is the base step; the delay doubles per attempt.
	deliveryBackoff = 30 * time.Second
)

// DeliveryStore is the persistence the pump needs.
type DeliveryStore interface {
	SettledEnvironmentTasksAwaitingDelivery(limit int) ([]store.SettledEnvironmentTask, error)
	EnvironmentEventForTask(taskID string) (store.EnvironmentEvent, bool, error)
	CreateEnvironmentDelivery(instance, taskID, deliveryKey string, route, outcome json.RawMessage) (store.EnvironmentDelivery, bool, error)
	DueEnvironmentDeliveries(now time.Time, limit int) ([]store.EnvironmentDelivery, error)
	MarkEnvironmentDeliveryDelivered(id string) error
	MarkEnvironmentDeliveryRetry(id string, attempt int, next time.Time, reason string) error
	MarkEnvironmentDeliveryDead(id string, reason string) error
}

// Deliverer sends one delivery to its adapter.
type Deliverer interface {
	Deliver(ctx context.Context, p DeliveryParams) (DeliveryResult, error)
}

// Pump is the durable reply path: a settled task becomes a queued delivery, and
// a queued delivery is retried until it is delivered or provably impossible.
//
// Two phases, both reconciliation, run on the same tick:
//
//  1. enqueue — find settled environment tasks with no delivery row and create one.
//     This repairs anything a crash between settle and enqueue lost.
//  2. send — take due deliveries and hand them to the adapter that owns the
//     instance, retrying with backoff.
//
// The two-phase split is what makes "exactly once" reachable in practice: the
// delivery key is derived from the task id, so phase 1 is idempotent even if it
// runs twice, and phase 2's at-least-once retries are safe because the adapter
// receives a stable key and an attempt number.
type Pump struct {
	Store     DeliveryStore
	Deliverer Deliverer
	// Routes resolves the reply route for a task. Sun records it with the event,
	// so it is durable across restarts.
	Routes func(ctx context.Context, taskID string) (json.RawMessage, error)
	Logf   func(format string, args ...any)

	wake chan struct{}
	done chan struct{}
}

// NewPump builds a pump. Call Run to start it.
func NewPump(s DeliveryStore, d Deliverer) *Pump {
	return &Pump{Store: s, Deliverer: d, wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func (p *Pump) logf(format string, args ...any) {
	if p.Logf != nil {
		p.Logf(format, args...)
	}
}

// Wake asks for an immediate pass, so a settled task's reply does not wait for
// the poll interval.
func (p *Pump) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run drives the pump until ctx is cancelled.
func (p *Pump) Run(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(deliveryPollInterval)
	defer ticker.Stop()
	for {
		p.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.wake:
		}
	}
}

// Wait blocks until Run has returned.
func (p *Pump) Wait() { <-p.done }

// pass runs both phases once.
func (p *Pump) pass(ctx context.Context) {
	if p.Store == nil {
		return
	}
	// Ordering matters: enqueue before send, so a delivery created in this pass is
	// eligible immediately rather than on the next tick.
	p.enqueue(ctx)
	p.send(ctx)
}

// enqueue creates delivery rows for settled tasks that have none.
func (p *Pump) enqueue(ctx context.Context) {
	if p.Deliverer == nil {
		return
	}
	pending, err := p.Store.SettledEnvironmentTasksAwaitingDelivery(deliveryBatch)
	if err != nil {
		p.logf("environment delivery: cannot list settled tasks: %v", err)
		return
	}
	for _, t := range pending {
		if ctx.Err() != nil {
			return
		}
		// needs_attention, cancelled and unknown states have nothing worth sending
		// upward, but they must not be retried forever either; recording a delivery
		// for them keeps the reconciliation query from returning them every pass.
		outcome := OutcomeFor(t.State, t.Result)
		if outcome.State == "" {
			continue
		}
		route := t.Route
		if len(route) == 0 && p.Routes != nil {
			if resolved, err := p.Routes(ctx, t.TaskID); err == nil {
				route = resolved
			}
		}
		if len(route) == 0 {
			route = json.RawMessage(`{}`)
		}
		body, err := json.Marshal(outcome)
		if err != nil {
			continue
		}
		// The delivery key is derived from the task, not from time or a counter, so
		// a re-run of this phase cannot create a second delivery.
		key := t.Instance + ":" + t.TaskID
		if _, _, err := p.Store.CreateEnvironmentDelivery(t.Instance, t.TaskID, key, route, body); err != nil {
			p.logf("environment delivery: cannot enqueue %s: %v", key, err)
			continue
		}
		p.logf("environment delivery: queued %s (%s)", key, outcome.State)
	}
}

// send attempts every due delivery.
func (p *Pump) send(ctx context.Context) {
	if p.Deliverer == nil {
		return
	}
	due, err := p.Store.DueEnvironmentDeliveries(time.Now(), deliveryBatch)
	if err != nil {
		p.logf("environment delivery: cannot list due deliveries: %v", err)
		return
	}
	for _, d := range due {
		if ctx.Err() != nil {
			return
		}
		p.attempt(ctx, d)
	}
}

func (p *Pump) attempt(ctx context.Context, d store.EnvironmentDelivery) {
	var route Route
	if err := json.Unmarshal(d.Route, &route); err != nil {
		// A corrupted route cannot be repaired by retrying.
		_ = p.Store.MarkEnvironmentDeliveryDead(d.ID, "invalid_route")
		return
	}
	var outcome Outcome
	if err := json.Unmarshal(d.Outcome, &outcome); err != nil {
		_ = p.Store.MarkEnvironmentDeliveryDead(d.ID, "invalid_outcome")
		return
	}
	attemptCtx, cancel := context.WithTimeout(ctx, deliveryAttemptTimeout)
	defer cancel()
	result, err := p.Deliverer.Deliver(attemptCtx, DeliveryParams{
		Instance:    d.Instance,
		DeliveryKey: d.DeliveryKey,
		Route:       route,
		Outcome:     outcome,
		Attempt:     d.Attempts + 1,
		TaskID:      d.TaskID,
	})
	attempt := d.Attempts + 1
	if err == nil && result.Delivered {
		if err := p.Store.MarkEnvironmentDeliveryDelivered(d.ID); err != nil {
			p.logf("environment delivery: cannot record success for %s: %v", d.DeliveryKey, err)
		}
		p.logf("environment delivery: delivered %s on attempt %d", d.DeliveryKey, attempt)
		return
	}
	reason := result.Code
	if reason == "" && err != nil {
		reason = err.Error()
	}
	if reason == "" {
		reason = "delivery_failed"
	}
	switch {
	case err == nil && !result.Delivered && !result.Retryable:
		// The adapter says this can never succeed — an unknown conversation, a
		// revoked token. Retrying would hide the problem.
		_ = p.Store.MarkEnvironmentDeliveryDead(d.ID, reason)
		p.logf("environment delivery: %s is permanently undeliverable: %s", d.DeliveryKey, reason)
	case attempt >= maxDeliveryAttempts:
		_ = p.Store.MarkEnvironmentDeliveryDead(d.ID, "exhausted: "+reason)
		p.logf("environment delivery: %s exhausted %d attempts: %s", d.DeliveryKey, attempt, reason)
	default:
		next := time.Now().Add(deliveryBackoff << uint(attempt-1))
		if err := p.Store.MarkEnvironmentDeliveryRetry(d.ID, attempt, next, reason); err != nil {
			p.logf("environment delivery: cannot schedule retry for %s: %v", d.DeliveryKey, err)
		}
		p.logf("environment delivery: %s attempt %d failed (%s), retrying at %s", d.DeliveryKey, attempt, reason, next.Format(time.RFC3339))
	}
}
