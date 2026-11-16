package environments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"tresorbot/internal/store"
)

// DefaultMaxPendingPerBot bounds how much input work may queue behind one bot.
//
// It exists because an input is a firehose Sun does not control: a busy Slack
// workspace or a mail folder replay can produce events far faster than an Earth
// finishes turns. Without a cap the queue grows without bound and every upstream
// is told "accepted" for work that will age for hours. With it, the upstream is
// told to retry — which is the truth, and which keeps the queue honest.
const DefaultMaxPendingPerBot = 100

// Store is the persistence intake needs. It is an interface so intake can be
// tested without a database, and so the dependency is visible.
type Store interface {
	GetEnvironmentInstance(name string) (store.EnvironmentInstance, error)
	LookupEnvironmentEvent(instance, eventID string) (store.EnvironmentEvent, bool, error)
	RecordEnvironmentEvent(store.EnvironmentEvent) error
	GetBot(id string) (store.Bot, error)
	BotByAlias(alias string) (store.Bot, error)
	ListContacts(botID string) ([]store.Contact, error)
	QueuedTaskCount(botID string) (int, error)
	AdmitTask(botID, scope, key string, input []byte) (store.Task, bool, error)
}

// Intake is the single admission path for every input source.
//
// Everything inbound — an adapter's emitted event, a webhook the adapter parsed,
// a scheduled tick — goes through Admit. That is what makes dedupe, backpressure
// and provenance uniform: a new source cannot accidentally skip them, because
// there is no other door.
type Intake struct {
	Store Store
	// MaxPendingPerBot bounds queued work per bot. Zero uses the default.
	MaxPendingPerBot int
	// Wake nudges the dispatcher after admission. Nil is allowed, for tests.
	Wake func()
	// Logf records admission decisions that an operator may need to see.
	Logf func(format string, args ...any)
	// Inspect reports one configured instance and its resolved bot. It is a hook
	// rather than a store method because the host layer owns instance caching.
	Inspect func(instance string) (Instance, error)
}

// Instance is what intake needs to know about a configured input source.
type Instance struct {
	Name        string
	Environment string
	BotID       string
	Enabled     bool
}

func (i *Intake) logf(format string, args ...any) {
	if i.Logf != nil {
		i.Logf(format, args...)
	}
}

func (i *Intake) maxPending() int {
	if i.MaxPendingPerBot > 0 {
		return i.MaxPendingPerBot
	}
	return DefaultMaxPendingPerBot
}

// Admit validates, dedupes, renders and admits one event.
//
// The return value is the ack the adapter receives, and its shape is the whole
// contract: an adapter advances its cursor only on Accepted, or on !Retryable.
// The ordering below is therefore not arbitrary:
//
//  1. Validate the envelope, before touching state.
//  2. Check the event ledger, before resolving anything that can change. A bot's
//     alias is mutable and admission's dedupe hash binds the bot id, so resolving
//     first would let a retarget turn a redelivery into a conflict.
//  3. Resolve the instance and bot, so a misconfigured source is rejected
//     permanently instead of being retried forever.
//  4. Apply backpressure, which is retryable by nature.
//  5. Render and admit, then record the outcome.
func (i *Intake) Admit(ctx context.Context, e Event) (Ack, error) {
	if err := ctx.Err(); err != nil {
		return Retry("cancelled"), err
	}
	if err := e.Validate(); err != nil {
		i.logf("input %q rejected event: %v", e.Instance, err)
		return Reject("invalid_event"), err
	}
	if i.Store == nil || i.Inspect == nil {
		return Retry("intake_unavailable"), errors.New("intake is not configured")
	}
	// Step 2: the ledger is authoritative for "already handled". A duplicate of a
	// previously rejected event is acknowledged too, so an upstream that retries a
	// permanently bad event eventually stops.
	if recorded, found, err := i.Store.LookupEnvironmentEvent(e.Instance, e.EventID); err != nil {
		return Retry("ledger_unavailable"), err
	} else if found {
		if recorded.TaskID == "" {
			// Already rejected once. Report the same verdict without re-admitting.
			i.logf("input %q duplicate event %q was previously rejected: %s", e.Instance, e.EventID, recorded.RejectedReason)
			return Accept("", true), nil
		}
		return Accept(recorded.TaskID, true), nil
	}
	// Step 3: resolve the instance, then the bot.
	inst, err := i.Inspect(e.Instance)
	if err != nil {
		return i.reject(e, "unknown_instance", err)
	}
	if !inst.Enabled {
		return i.reject(e, "instance_disabled", fmt.Errorf("%w: %s", ErrUnknownInstance, e.Instance))
	}
	bot, err := i.resolveBot(inst, e.Target)
	if err != nil {
		return i.reject(e, "unknown_target", err)
	}
	if err := i.authorizeTarget(inst, bot, e.Target); err != nil {
		return i.reject(e, "unauthorized_target", err)
	}
	if !bot.Enabled {
		return i.reject(e, "bot_disabled", fmt.Errorf("%w: %s", ErrUnknownTarget, bot.ID))
	}
	// Step 4: backpressure.
	pending, err := i.Store.QueuedTaskCount(bot.ID)
	if err != nil {
		return Retry("backpressure_check_failed"), err
	}
	if pending >= i.maxPending() {
		i.logf("input %q deferred event %q: bot %s has %d queued (cap %d)", e.Instance, e.EventID, bot.Alias, pending, i.maxPending())
		return Retry("bot_pending_cap"), ErrBackpressure
	}
	// Step 5: render and admit.
	rendered, err := Render(e)
	if err != nil {
		return i.reject(e, "render_failed", err)
	}
	task, created, err := i.Store.AdmitTask(bot.ID, "input:"+e.Instance, e.EventID, rendered)
	if err != nil {
		// ErrConflict here means request_keys already holds this (scope, key) with a
		// different content hash. Treating it as retryable would loop forever, so it
		// is recorded as a rejection and acknowledged.
		if errors.Is(err, store.ErrConflict) {
			return i.reject(e, "duplicate_key_conflict", err)
		}
		if errors.Is(err, store.ErrDisabled) {
			return i.reject(e, "bot_disabled", err)
		}
		if errors.Is(err, store.ErrInvalid) {
			return i.reject(e, "invalid_task", err)
		}
		return Retry("admission_failed"), err
	}
	occurred := ""
	if !e.OccurredAt.IsZero() {
		occurred = e.OccurredAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	// Recording the outcome after admission is deliberate: a crash between the two
	// leaves a task with no ledger row, and the next delivery re-admits it as a
	// duplicate through request_keys, which is safe. The reverse order would record
	// an event as handled that no task exists for — a lost event.
	if err := i.Store.RecordEnvironmentEvent(store.EnvironmentEvent{Instance: e.Instance, EventID: e.EventID, Kind: e.Kind, OccurredAt: occurred, TaskID: task.ID}); err != nil {
		i.logf("input %q could not record event %q: %v", e.Instance, e.EventID, err)
		return Retry("ledger_write_failed"), err
	}
	if created && i.Wake != nil {
		i.Wake()
	}
	if created {
		i.logf("input %q admitted event %q as task %s for bot %s (%s)", e.Instance, e.EventID, task.ID, bot.Alias, e.Kind)
	}
	return Accept(task.ID, !created), nil
}

// authorizeTarget decides whether an instance may send work to a bot other than
// its own.
//
// An adapter writes Event.Target, so it is untrusted input. Without this check a
// single installed input could address every bot in Sun — it would know their ids
// or aliases and nothing would stop it. The gate is a real contact edge from the
// instance's configured bot, which is exactly the permission an operator already
// grants deliberately on the Bots tab; delegation uses the same edge.
//
// The instance's own bot is always allowed: that is the configured destination,
// and requiring a self-contact would be nonsense.
func (i *Intake) authorizeTarget(inst Instance, bot store.Bot, target string) error {
	if strings.TrimSpace(target) == "" || bot.ID == inst.BotID {
		return nil
	}
	contacts, err := i.Store.ListContacts(inst.BotID)
	if err != nil {
		return err
	}
	for _, contact := range contacts {
		if contact.Target == bot.ID {
			return nil
		}
	}
	return fmt.Errorf("%w: instance %q may not reach bot %s", ErrUnknownTarget, inst.Name, bot.Alias)
}

// reject records a permanent verdict and returns it.
func (i *Intake) reject(e Event, code string, cause error) (Ack, error) {
	if err := i.Store.RecordEnvironmentEvent(store.EnvironmentEvent{Instance: e.Instance, EventID: e.EventID, Kind: e.Kind, RejectedReason: code}); err != nil {
		i.logf("input %q could not record rejection of %q: %v", e.Instance, e.EventID, err)
	}
	i.logf("input %q rejected event %q: %s (%v)", e.Instance, e.EventID, code, cause)
	return Reject(code), cause
}

// resolveBot finds the recipient: the event's target if it named one, otherwise
// the instance's configured bot.
//
// A target may be an id or an alias. Resolution says only that the bot exists; it
// says nothing about whether this instance may reach it. authorizeTarget is the
// gate, and the two are separate so an error message can tell an operator which
// of the two failed.
func (i *Intake) resolveBot(inst Instance, target string) (store.Bot, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return i.Store.GetBot(inst.BotID)
	}
	if bot, err := i.Store.GetBot(target); err == nil {
		return bot, nil
	}
	if bot, err := i.Store.BotByAlias(target); err == nil {
		return bot, nil
	}
	return store.Bot{}, fmt.Errorf("%w: %s", ErrUnknownTarget, target)
}

// OutcomeFor builds the delivery outcome for a settled task.
//
// Only a completed task carries model output. Every other state reports a stable
// code, because an adapter forwards this into a system a human reads and an
// agent's partial reasoning must not leak there. needs_attention is included even
// though it is not terminal: an upstream waiting on a reply would otherwise hang
// forever, which is a worse failure than an honest "it stopped".
func OutcomeFor(state string, result json.RawMessage) Outcome {
	switch state {
	case "completed":
		var payload struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(result, &payload)
		return Outcome{State: state, Text: payload.Text}
	case "failed":
		return Outcome{State: state, Code: resultCode(result, "failed")}
	case "cancelled":
		return Outcome{State: state, Code: resultCode(result, "cancelled")}
	case "needs_attention":
		return Outcome{State: state, Code: resultCode(result, "execution_uncertain"), Uncertain: true}
	default:
		// Not settled yet; the pump must not deliver a running task.
		return Outcome{}
	}
}

// resultCode extracts a stable code from a task result body, falling back when
// the body is not the expected shape.
func resultCode(result json.RawMessage, fallback string) string {
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(result, &payload); err == nil {
		if payload.Code != "" {
			return payload.Code
		}
		if payload.Message != "" && len(payload.Message) <= 200 {
			return payload.Message
		}
	}
	return fallback
}
