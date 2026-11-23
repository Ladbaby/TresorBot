package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Environment schema is created additively, the way gateway_executions is, rather than
// through internal/store/store.go's migration log.
//
// The reason is compatibility in both directions. A migration would need the
// `version > 11` guard bumped, and an operator who downgraded Sun would then be
// refused at startup by a database that only has four extra empty tables. These
// tables are self-contained: nothing in the existing schema references them and
// they reference tasks by plain id, so an older binary simply ignores them.
const environmentSchemaV1 = `
CREATE TABLE IF NOT EXISTS environment_instances(
  name TEXT PRIMARY KEY,
  environment TEXT NOT NULL,
  bot_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  settings TEXT NOT NULL DEFAULT '{}',
  revision INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS environment_cursors(
  instance TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(instance, key)
);
CREATE TABLE IF NOT EXISTS environment_events(
  instance TEXT NOT NULL,
  event_id TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT '',
  occurred_at TEXT,
  received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  task_id TEXT,
  rejected_reason TEXT NOT NULL DEFAULT '',
  -- reply_to is the adapter's own routing handle for this event, verbatim. It is
  -- stored rather than derived because the delivery pump needs to answer in the
  -- conversation the event came from, and that place is known only to the adapter
  -- that received it.
  reply_to TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(instance, event_id)
);
CREATE INDEX IF NOT EXISTS environment_events_task ON environment_events(task_id);
CREATE TABLE IF NOT EXISTS environment_deliveries(
  id TEXT PRIMARY KEY,
  instance TEXT NOT NULL,
  task_id TEXT NOT NULL,
  delivery_key TEXT NOT NULL UNIQUE,
  route TEXT NOT NULL DEFAULT '{}',
  outcome TEXT NOT NULL DEFAULT '{}',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  delivered_at TEXT,
  last_error TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS environment_deliveries_pending ON environment_deliveries(state, next_attempt_at);
CREATE INDEX IF NOT EXISTS environment_deliveries_instance ON environment_deliveries(instance);
-- environment_conversations is the set of places an instance may SPEAK.
--
-- It exists because outbound chat is not the same authority as outbound tooling.
-- A bot with a chat environment could otherwise message any user id, and a
-- prompt injection is enough to guess one. Rows are learned automatically when a
-- conversation messages the instance, which is what lets a bot answer someone new
-- without any operator configuration; operator-added rows cover the case where the
-- bot must speak first.
CREATE TABLE IF NOT EXISTS environment_conversations(
  instance TEXT NOT NULL,
  -- conversation is the adapter's own chat identifier, normalised by the adapter.
  conversation TEXT NOT NULL,
  -- label is human-facing only: a group title or a person's name, for the dashboard.
  label TEXT NOT NULL DEFAULT '',
  -- source is 'inbound' (learned) or 'manual' (an operator added it).
  source TEXT NOT NULL DEFAULT 'inbound',
  -- allowed is 0 for a conversation an operator has blocked. Row presence is not
  -- enough, because blocking must not be undone by the next inbound message.
  allowed INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(instance, conversation)
);
CREATE INDEX IF NOT EXISTS environment_conversations_instance ON environment_conversations(instance);
`

// InitEnvironmentsSchema creates the environment-side tables. It is idempotent and safe to
// call on every start, including on a database written by an older Sun.
func (s *Store) InitEnvironmentsSchema() error {
	if _, err := s.db.Exec(environmentSchemaV1); err != nil {
		return err
	}
	// Environments shipped before reply_to existed, so an installed database has
	// the table without the column. Adding it is additive and safe to repeat;
	// failing here would strand every existing deployment on an upgrade.
	if err := s.ensureColumn("environment_events", "reply_to", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return nil
}

// ensureColumn adds a column when an older database lacks it.
func (s *Store) ensureColumn(table, column, definition string) error {
	rows, err := s.db.Query("SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition)
	return err
}

// EnvironmentInstance is one configured environment: an adapter module plus the bot
// it feeds and the settings that adapter declared.
//
// Instances live in the database rather than sun.yaml because they are plural
// per adapter, edited from the dashboard, and revised concurrently. A nested map
// in YAML cannot express a per-instance revision, which is what makes two
// browser tabs editing different instances safe.
type EnvironmentInstance struct {
	Name        string          `json:"name"`
	Environment string          `json:"environment"`
	BotID       string          `json:"bot_id"`
	Enabled     bool            `json:"enabled"`
	Settings    json.RawMessage `json:"settings"`
	Revision    int64           `json:"revision"`
	CreatedAt   string          `json:"created_at,omitempty"`
	UpdatedAt   string          `json:"updated_at,omitempty"`
}

// EnvironmentInstancePatch is a partial edit. A nil field is left unchanged.
type EnvironmentInstancePatch struct {
	BotID    *string
	Enabled  *bool
	Settings *json.RawMessage
}

var ErrEnvironmentNotFound = errors.New("environment instance not found")

const maxEnvironmentInstanceName = 64

func validEnvironmentInstanceName(name string) bool {
	if name == "" || len(name) > maxEnvironmentInstanceName {
		return false
	}
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}

// CreateEnvironmentInstance adds one instance. The name is the operator's choice and is
// also the ingress path segment, so it is validated as a slug.
func (s *Store) CreateEnvironmentInstance(in EnvironmentInstance) (EnvironmentInstance, error) {
	if !validEnvironmentInstanceName(in.Name) {
		return EnvironmentInstance{}, ErrInvalid
	}
	if in.Environment == "" || len(in.Environment) > 64 || in.BotID == "" {
		return EnvironmentInstance{}, ErrInvalid
	}
	settings := in.Settings
	if len(settings) == 0 {
		settings = json.RawMessage(`{}`)
	}
	if !validJSON(settings) {
		return EnvironmentInstance{}, ErrInvalid
	}
	tx, err := s.db.Begin()
	if err != nil {
		return EnvironmentInstance{}, err
	}
	defer tx.Rollback()
	// A bot that does not exist would admit nothing, so refuse it here where the
	// operator can see why.
	var exists int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM bots WHERE id=? AND deleted=0`, in.BotID).Scan(&exists); err != nil {
		return EnvironmentInstance{}, err
	}
	if exists != 1 {
		return EnvironmentInstance{}, ErrNotFound
	}
	if _, err = tx.Exec(`INSERT INTO environment_instances(name,environment,bot_id,enabled,settings) VALUES(?,?,?,?,?)`, in.Name, in.Environment, in.BotID, in.Enabled, string(settings)); err != nil {
		return EnvironmentInstance{}, err
	}
	out, err := getEnvironmentInstance(tx, in.Name)
	if err != nil {
		return EnvironmentInstance{}, err
	}
	return out, tx.Commit()
}

func getEnvironmentInstance(q querier, name string) (EnvironmentInstance, error) {
	var in EnvironmentInstance
	var settings string
	err := q.QueryRow(`SELECT name,environment,bot_id,enabled,settings,revision,COALESCE(created_at,''),COALESCE(updated_at,'') FROM environment_instances WHERE name=?`, name).
		Scan(&in.Name, &in.Environment, &in.BotID, &in.Enabled, &settings, &in.Revision, &in.CreatedAt, &in.UpdatedAt)
	if err == sql.ErrNoRows {
		return in, ErrEnvironmentNotFound
	}
	if err != nil {
		return in, err
	}
	in.Settings = json.RawMessage(settings)
	return in, nil
}

// GetEnvironmentInstance reads one instance by name.
func (s *Store) GetEnvironmentInstance(name string) (EnvironmentInstance, error) {
	return getEnvironmentInstance(s.db, name)
}

// ListEnvironmentInstances returns every configured instance.
func (s *Store) ListEnvironmentInstances() ([]EnvironmentInstance, error) {
	rows, err := s.db.Query(`SELECT name FROM environment_instances ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]EnvironmentInstance, 0, len(names))
	for _, name := range names {
		in, err := s.GetEnvironmentInstance(name)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, nil
}

// UpdateEnvironmentInstance applies a patch, guarded by the caller's revision so a
// stale dashboard tab cannot silently overwrite a newer edit.
func (s *Store) UpdateEnvironmentInstance(name string, revision int64, patch EnvironmentInstancePatch) (EnvironmentInstance, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return EnvironmentInstance{}, err
	}
	defer tx.Rollback()
	current, err := getEnvironmentInstance(tx, name)
	if err != nil {
		return EnvironmentInstance{}, err
	}
	if current.Revision != revision {
		return EnvironmentInstance{}, ErrConflict
	}
	botID, enabled, settings := current.BotID, current.Enabled, current.Settings
	if patch.BotID != nil {
		var exists int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM bots WHERE id=? AND deleted=0`, *patch.BotID).Scan(&exists); err != nil {
			return EnvironmentInstance{}, err
		}
		if exists != 1 {
			return EnvironmentInstance{}, ErrNotFound
		}
		botID = *patch.BotID
	}
	if patch.Enabled != nil {
		enabled = *patch.Enabled
	}
	if patch.Settings != nil {
		if !validJSON(*patch.Settings) {
			return EnvironmentInstance{}, ErrInvalid
		}
		settings = *patch.Settings
	}
	if _, err = tx.Exec(`UPDATE environment_instances SET bot_id=?,enabled=?,settings=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE name=?`, botID, enabled, string(settings), name); err != nil {
		return EnvironmentInstance{}, err
	}
	out, err := getEnvironmentInstance(tx, name)
	if err != nil {
		return EnvironmentInstance{}, err
	}
	return out, tx.Commit()
}

// DeleteEnvironmentInstance removes one instance along with its cursors. Events and
// deliveries are kept: they are the record of what happened, and losing them
// would make a duplicate upstream redelivery indistinguishable from a new one.
func (s *Store) DeleteEnvironmentInstance(name string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`DELETE FROM environment_instances WHERE name=?`, name)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrEnvironmentNotFound
	}
	if _, err = tx.Exec(`DELETE FROM environment_cursors WHERE instance=?`, name); err != nil {
		return err
	}
	return tx.Commit()
}

// EnvironmentCursor reads one adapter-owned cursor. A missing cursor is empty rather
// than an error: "never ran before" is a normal state and the adapter decides
// what that means for its upstream.
func (s *Store) EnvironmentCursor(instance, key string) (string, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM environment_cursors WHERE instance=? AND key=?`, instance, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetEnvironmentCursor stores an adapter-owned cursor.
//
// The values are opaque: Sun never interprets them, because only the adapter
// knows whether a cursor is an IMAP UID, a Slack timestamp or a custom offset.
// Persisting them is what lets an adapter resume after a restart without
// re-reading (or worse, re-admitting) everything it already handled.
func (s *Store) SetEnvironmentCursor(instance, key, value string) error {
	if instance == "" || key == "" || len(key) > 256 || len(value) > 64*1024 {
		return ErrInvalid
	}
	_, err := s.db.Exec(`INSERT INTO environment_cursors(instance,key,value) VALUES(?,?,?) ON CONFLICT(instance,key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP`, instance, key, value)
	return err
}

// EnvironmentEvent is the durable record of one inbound event and what became of it.
type EnvironmentEvent struct {
	Instance       string `json:"instance"`
	EventID        string `json:"event_id"`
	Kind           string `json:"kind"`
	OccurredAt     string `json:"occurred_at,omitempty"`
	ReceivedAt     string `json:"received_at,omitempty"`
	TaskID         string `json:"task_id,omitempty"`
	RejectedReason string `json:"rejected_reason,omitempty"`
	// ReplyTo is the adapter's routing handle, stored verbatim as JSON. Sun does not
	// interpret it: the adapter wrote it and the same adapter reads it back when a
	// result is ready to deliver.
	ReplyTo string `json:"reply_to,omitempty"`
}

// LookupEnvironmentEvent reports whether an event was already handled.
//
// This is the authoritative idempotency check for ingress, and it deliberately
// runs before admission rather than relying on request_keys alone. Admission's
// hash binds the bot id, and bot aliases are mutable, so a redelivery after a
// retarget would otherwise conflict with its own earlier admission and be
// retried forever by a well-behaved upstream.
func (s *Store) LookupEnvironmentEvent(instance, eventID string) (EnvironmentEvent, bool, error) {
	var e EnvironmentEvent
	var taskID sql.NullString
	err := s.db.QueryRow(`SELECT instance,event_id,kind,COALESCE(occurred_at,''),COALESCE(received_at,''),COALESCE(task_id,''),rejected_reason,reply_to FROM environment_events WHERE instance=? AND event_id=?`, instance, eventID).
		Scan(&e.Instance, &e.EventID, &e.Kind, &e.OccurredAt, &e.ReceivedAt, &taskID, &e.RejectedReason, &e.ReplyTo)
	if err == sql.ErrNoRows {
		return e, false, nil
	}
	if err != nil {
		return e, false, err
	}
	e.TaskID = taskID.String
	return e, true, nil
}

// RecordEnvironmentEvent persists the outcome of one event. The first write wins: a
// concurrent duplicate loses the insert and keeps the original record, so two
// racing deliveries cannot both be treated as new.
func (s *Store) RecordEnvironmentEvent(e EnvironmentEvent) error {
	if e.Instance == "" || e.EventID == "" {
		return ErrInvalid
	}
	var occurred any
	if e.OccurredAt != "" {
		occurred = e.OccurredAt
	}
	var taskID any
	if e.TaskID != "" {
		taskID = e.TaskID
	}
	_, err := s.db.Exec(`INSERT INTO environment_events(instance,event_id,kind,occurred_at,task_id,rejected_reason,reply_to) VALUES(?,?,?,?,?,?,?) ON CONFLICT(instance,event_id) DO NOTHING`, e.Instance, e.EventID, e.Kind, occurred, taskID, e.RejectedReason, e.ReplyTo)
	return err
}

// ListEnvironmentEvents returns recent events for the dashboard, newest first.
func (s *Store) ListEnvironmentEvents(instance string, limit int) ([]EnvironmentEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(`SELECT instance,event_id,kind,COALESCE(occurred_at,''),COALESCE(received_at,''),COALESCE(task_id,''),rejected_reason,reply_to FROM environment_events WHERE (?='' OR instance=?) ORDER BY received_at DESC LIMIT ?`, instance, instance, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EnvironmentEvent{}
	for rows.Next() {
		var e EnvironmentEvent
		if err = rows.Scan(&e.Instance, &e.EventID, &e.Kind, &e.OccurredAt, &e.ReceivedAt, &e.TaskID, &e.RejectedReason, &e.ReplyTo); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// QueuedTaskCount reports how much work is waiting for one bot.
//
// Intake uses it for backpressure so an upstream firehose cannot queue unbounded
// work behind a slow runner. It counts only 'queued', because dispatching and
// running work is already bounded by max_concurrency.
func (s *Store) QueuedTaskCount(botID string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE bot_id=? AND state='queued'`, botID).Scan(&n)
	return n, err
}

// EnvironmentDelivery states.
const (
	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	// DeliveryDead is a delivery the adapter reported as permanently impossible.
	// It is kept visible so a lost reply is a recorded fact, not a silent drop.
	DeliveryDead = "dead"
)

// EnvironmentDelivery is one queued reply.
type EnvironmentDelivery struct {
	ID            string          `json:"id"`
	Instance      string          `json:"instance"`
	TaskID        string          `json:"task_id"`
	DeliveryKey   string          `json:"delivery_key"`
	Route         json.RawMessage `json:"route"`
	Outcome       json.RawMessage `json:"outcome"`
	Attempts      int             `json:"attempts"`
	NextAttemptAt string          `json:"next_attempt_at,omitempty"`
	DeliveredAt   string          `json:"delivered_at,omitempty"`
	LastError     string          `json:"last_error,omitempty"`
	State         string          `json:"state"`
	CreatedAt     string          `json:"created_at,omitempty"`
	UpdatedAt     string          `json:"updated_at,omitempty"`
}

// CreateEnvironmentDelivery enqueues one reply, idempotently by delivery key.
//
// Keying by delivery key rather than by task means a task that settles once
// enqueues once, even if the pump is woken repeatedly or Sun restarts between
// the settle and the send.
func (s *Store) CreateEnvironmentDelivery(instance, taskID, deliveryKey string, route, outcome json.RawMessage) (EnvironmentDelivery, bool, error) {
	if instance == "" || taskID == "" || deliveryKey == "" || len(deliveryKey) > 512 {
		return EnvironmentDelivery{}, false, ErrInvalid
	}
	if len(route) == 0 {
		route = json.RawMessage(`{}`)
	}
	if !validJSON(route) || !validJSON(outcome) {
		return EnvironmentDelivery{}, false, ErrInvalid
	}
	tx, err := s.db.Begin()
	if err != nil {
		return EnvironmentDelivery{}, false, err
	}
	defer tx.Rollback()
	var existing string
	err = tx.QueryRow(`SELECT id FROM environment_deliveries WHERE delivery_key=?`, deliveryKey).Scan(&existing)
	if err == nil {
		d, err := getEnvironmentDelivery(tx, existing)
		return d, false, err
	}
	if err != sql.ErrNoRows {
		return EnvironmentDelivery{}, false, err
	}
	id := uuid.NewString()
	if _, err = tx.Exec(`INSERT INTO environment_deliveries(id,instance,task_id,delivery_key,route,outcome,state) VALUES(?,?,?,?,?,?,?)`, id, instance, taskID, deliveryKey, string(route), string(outcome), DeliveryPending); err != nil {
		return EnvironmentDelivery{}, false, err
	}
	d, err := getEnvironmentDelivery(tx, id)
	if err != nil {
		return EnvironmentDelivery{}, false, err
	}
	return d, true, tx.Commit()
}

func getEnvironmentDelivery(q querier, id string) (EnvironmentDelivery, error) {
	var d EnvironmentDelivery
	var route, outcome string
	err := q.QueryRow(`SELECT id,instance,task_id,delivery_key,route,outcome,attempts,COALESCE(next_attempt_at,''),COALESCE(delivered_at,''),last_error,state,COALESCE(created_at,''),COALESCE(updated_at,'') FROM environment_deliveries WHERE id=?`, id).
		Scan(&d.ID, &d.Instance, &d.TaskID, &d.DeliveryKey, &route, &outcome, &d.Attempts, &d.NextAttemptAt, &d.DeliveredAt, &d.LastError, &d.State, &d.CreatedAt, &d.UpdatedAt)
	if err == sql.ErrNoRows {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	d.Route = json.RawMessage(route)
	d.Outcome = json.RawMessage(outcome)
	return d, nil
}

// DueEnvironmentDeliveries returns pending deliveries whose retry time has arrived.
func (s *Store) DueEnvironmentDeliveries(now time.Time, limit int) ([]EnvironmentDelivery, error) {
	if limit <= 0 || limit > 200 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(`SELECT id FROM environment_deliveries WHERE state=? AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY created_at LIMIT ?`, DeliveryPending, now.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]EnvironmentDelivery, 0, len(ids))
	for _, id := range ids {
		d, err := getEnvironmentDelivery(s.db, id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// EnvironmentDeliveryByKey reads one delivery by its stable key.
func (s *Store) EnvironmentDeliveryByKey(deliveryKey string) (EnvironmentDelivery, error) {
	var id string
	err := s.db.QueryRow(`SELECT id FROM environment_deliveries WHERE delivery_key=?`, deliveryKey).Scan(&id)
	if err == sql.ErrNoRows {
		return EnvironmentDelivery{}, ErrNotFound
	}
	if err != nil {
		return EnvironmentDelivery{}, err
	}
	return getEnvironmentDelivery(s.db, id)
}

// MarkEnvironmentDeliveryDelivered records a successful send.
func (s *Store) MarkEnvironmentDeliveryDelivered(id string) error {
	_, err := s.db.Exec(`UPDATE environment_deliveries SET state=?,delivered_at=CURRENT_TIMESTAMP,attempts=attempts+1,last_error='',next_attempt_at=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?`, DeliveryDelivered, id)
	return err
}

// MarkEnvironmentDeliveryRetry records a failed attempt and schedules the next one.
func (s *Store) MarkEnvironmentDeliveryRetry(id string, attempt int, next time.Time, reason string) error {
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	_, err := s.db.Exec(`UPDATE environment_deliveries SET attempts=?,next_attempt_at=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?`, attempt, next.UTC().Format(time.RFC3339), reason, id)
	return err
}

// MarkEnvironmentDeliveryDead stops retrying and leaves the delivery visible.
func (s *Store) MarkEnvironmentDeliveryDead(id string, reason string) error {
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	_, err := s.db.Exec(`UPDATE environment_deliveries SET state=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?`, DeliveryDead, reason, id)
	return err
}

// ListInputDeliveries returns deliveries for the dashboard, newest first.
func (s *Store) ListInputDeliveries(state string, limit int) ([]EnvironmentDelivery, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(`SELECT id FROM environment_deliveries WHERE (?='' OR state=?) ORDER BY created_at DESC LIMIT ?`, state, state, limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]EnvironmentDelivery, 0, len(ids))
	for _, id := range ids {
		d, err := getEnvironmentDelivery(s.db, id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// SettledEnvironmentTask is one task admitted from an environment that has settled
// without a delivery row yet.
type SettledEnvironmentTask struct {
	Instance string
	TaskID   string
	State    string
	Result   json.RawMessage
	Route    json.RawMessage
}

// SettledEnvironmentTasksAwaitingDelivery finds tasks admitted from environments that have
// reached a state worth reporting but have no delivery queued.
//
// This is a reconciliation query rather than a hook, and that is deliberate: a
// hook can be missed by a crash or a restart between the state change and the
// enqueue, and a missed reply is silent. Asking the database what is outstanding
// is self-healing — every pass repairs whatever the previous one missed.
//
// needs_attention is included even though it is not terminal: an upstream waiting
// on a reply would otherwise wait forever.
func (s *Store) SettledEnvironmentTasksAwaitingDelivery(limit int) ([]SettledEnvironmentTask, error) {
	if limit <= 0 || limit > 200 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(`
		SELECT e.instance, t.id, t.state, COALESCE(t.result,'null')
		FROM environment_events e
		JOIN tasks t ON t.id = e.task_id
		WHERE e.task_id IS NOT NULL
		  AND t.state IN ('completed','failed','cancelled','needs_attention')
		  AND NOT EXISTS(SELECT 1 FROM environment_deliveries d WHERE d.task_id = t.id)
		ORDER BY t.updated_at
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SettledEnvironmentTask{}
	for rows.Next() {
		var t SettledEnvironmentTask
		var result string
		if err = rows.Scan(&t.Instance, &t.TaskID, &t.State, &result); err != nil {
			return nil, err
		}
		t.Result = json.RawMessage(result)
		out = append(out, t)
	}
	return out, rows.Err()
}

// EnvironmentEventRoute reads the reply route recorded for one event, so the pump can
// tell an adapter where its answer belongs without re-deriving it.
func (s *Store) EnvironmentEventForTask(taskID string) (EnvironmentEvent, bool, error) {
	var e EnvironmentEvent
	var task sql.NullString
	err := s.db.QueryRow(`SELECT instance,event_id,kind,COALESCE(occurred_at,''),COALESCE(received_at,''),COALESCE(task_id,''),rejected_reason,reply_to FROM environment_events WHERE task_id=? LIMIT 1`, taskID).
		Scan(&e.Instance, &e.EventID, &e.Kind, &e.OccurredAt, &e.ReceivedAt, &task, &e.RejectedReason, &e.ReplyTo)
	if err == sql.ErrNoRows {
		return e, false, nil
	}
	if err != nil {
		return e, false, err
	}
	e.TaskID = task.String
	return e, true, nil
}
