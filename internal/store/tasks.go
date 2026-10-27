package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"
	"tresorbot/internal/contracts"
)

type Task struct {
	ID, BotID, State        string
	Input, Snapshot, Result []byte
	AssignmentID            string
	Epoch                   int64
	CancelRequested         bool
	ParentID, RootID        string
	QueueOrder              int64
	// TargetRunnerID optionally pins where the task runs (SendToEarth). Empty
	// means the bot's configured default runner decides, as before.
	TargetRunnerID string
}
type Assignment struct {
	ID, TaskID, RunnerID, ExecutionID string
	Epoch                             int64
}
type Report struct {
	AssignmentID, RunnerID string
	Epoch                  int64
	EventID                string
	Sequence               int64
	Kind                   string
	Payload                json.RawMessage
}

func validJSON(b []byte) bool { return len(b) <= 1<<20 && json.Valid(b) }

// canonicalJSON normalizes object ordering/whitespace without rounding numbers.
func canonicalJSON(b []byte) ([]byte, error) {
	if !validJSON(b) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
func (s *Store) AdmitTask(botID, scope, key string, input []byte) (Task, bool, error) {
	canonical, err := canonicalJSON(input)
	if err != nil {
		return Task{}, false, err
	}
	if len(scope) > 256 || len(key) > 256 || (key != "" && scope == "") {
		return Task{}, false, ErrInvalid
	}
	sum := sha256.Sum256(append(append([]byte(botID), 0), canonical...))
	hash := hex.EncodeToString(sum[:])
	tx, err := s.db.Begin()
	if err != nil {
		return Task{}, false, err
	}
	defer tx.Rollback()
	if key != "" {
		var oldHash, id string
		err = tx.QueryRow(`SELECT hash,task_id FROM request_keys WHERE scope=? AND key=?`, scope, key).Scan(&oldHash, &id)
		if err == nil {
			if oldHash != hash {
				return Task{}, false, ErrConflict
			}
			t, err := getTask(tx, id)
			return t, false, err
		}
		if err != sql.ErrNoRows {
			return Task{}, false, err
		}
	}
	b, err := scanBot(tx.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, botID))
	if err != nil {
		return Task{}, false, err
	}
	if !b.Enabled {
		return Task{}, false, ErrDisabled
	}
	t := Task{ID: uuid.NewString(), BotID: botID, State: "queued", Input: canonical, Snapshot: snapshot(b)}
	if _, err = tx.Exec(`INSERT INTO tasks(id,bot_id,input,snapshot,state) VALUES(?,?,?,?,?)`, t.ID, t.BotID, t.Input, t.Snapshot, t.State); err != nil {
		return t, false, err
	}
	if key != "" {
		if _, err = tx.Exec(`INSERT INTO request_keys VALUES(?,?,?,?)`, scope, key, hash, t.ID); err != nil {
			return t, false, err
		}
	}
	t, err = getTask(tx, t.ID)
	if err != nil {
		return t, false, err
	}
	return t, true, tx.Commit()
}

type querier interface{ QueryRow(string, ...any) *sql.Row }

func getTask(q querier, id string) (Task, error) {
	var t Task
	err := q.QueryRow(`SELECT id,bot_id,state,input,snapshot,result,COALESCE(current_assignment,''),epoch,cancel_requested,COALESCE(target_runner_id,'') FROM tasks WHERE id=?`, id).Scan(&t.ID, &t.BotID, &t.State, &t.Input, &t.Snapshot, &t.Result, &t.AssignmentID, &t.Epoch, &t.CancelRequested, &t.TargetRunnerID)
	if err == sql.ErrNoRows {
		err = ErrNotFound
	}
	if err == nil {
		err = q.QueryRow(`SELECT COALESCE(parent_id,''),COALESCE(root_id,id),(SELECT position FROM task_order WHERE task_id=tasks.id) FROM tasks WHERE id=?`, id).Scan(&t.ParentID, &t.RootID, &t.QueueOrder)
	}
	return t, err
}
func (s *Store) GetTask(id string) (Task, error) { return getTask(s.db, id) }
func (s *Store) ClaimTask(id, runner string) (Assignment, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Assignment{}, err
	}
	defer tx.Rollback()
	a, err := claimTask(tx, id, runner)
	if err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func claimTask(tx *sql.Tx, id, runner string) (Assignment, error) {
	t, err := getTask(tx, id)
	if err != nil {
		return Assignment{}, err
	}
	if t.AssignmentID != "" {
		var a Assignment
		err = tx.QueryRow(`SELECT id,task_id,runner_id,execution_id,epoch FROM assignments WHERE id=?`, t.AssignmentID).Scan(&a.ID, &a.TaskID, &a.RunnerID, &a.ExecutionID, &a.Epoch)
		if err != nil {
			return a, err
		}
		if a.RunnerID == runner && (t.State == "dispatching" || t.State == "running") {
			var revoked bool
			if err = tx.QueryRow(`SELECT revoked FROM runners WHERE id=?`, runner).Scan(&revoked); err != nil {
				return a, err
			}
			if revoked {
				return a, ErrDisabled
			}
			return a, nil
		}
		return a, ErrConflict
	}
	if t.State != "queued" {
		return Assignment{}, ErrInvalid
	}
	if t.ParentID != "" {
		var allowed int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM contacts c JOIN tasks p ON p.bot_id=c.sender WHERE p.id=? AND c.target=? AND p.state='waiting_child'`, t.ParentID, t.BotID).Scan(&allowed); err != nil {
			return Assignment{}, err
		}
		if allowed != 1 {
			return Assignment{}, ErrDisabled
		}
	}
	var earlier int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM tasks t JOIN task_order o ON o.task_id=t.id WHERE t.bot_id=? AND t.state='queued' AND o.position<?`, t.BotID, t.QueueOrder).Scan(&earlier); err != nil {
		return Assignment{}, err
	}
	if earlier > 0 {
		return Assignment{}, ErrConflict
	}
	var enabled, revoked bool
	err = tx.QueryRow(`SELECT b.enabled,r.revoked FROM bots b JOIN runners r ON r.id=? WHERE b.id=? AND b.deleted=0`, runner, t.BotID).Scan(&enabled, &revoked)
	if err == sql.ErrNoRows {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, err
	}
	if !enabled || revoked {
		return Assignment{}, ErrDisabled
	}
	var admitted Bot
	if err = json.Unmarshal(t.Snapshot, &admitted); err != nil {
		return Assignment{}, ErrInvalid
	}
	// A per-task target runner (SendToEarth) overrides the bot's default; a
	// pinned target must be honored exactly, never silently rerouted.
	if t.TargetRunnerID != "" {
		if t.TargetRunnerID != runner {
			return Assignment{}, ErrConflict
		}
	} else {
		configured := admitted.RunnerID
		if configured != "" && configured != runner {
			return Assignment{}, ErrConflict
		}
	}
	var active int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE bot_id=? AND state IN('dispatching','running','waiting_child','cancelling','needs_attention')`, t.BotID).Scan(&active); err != nil {
		return Assignment{}, err
	}
	if active > 0 {
		return Assignment{}, ErrConflict
	}
	if admitted.Workspace == "" {
		if err = tx.QueryRow(`SELECT default_workspace FROM runners WHERE id=?`, runner).Scan(&admitted.Workspace); err != nil {
			return Assignment{}, err
		}
		if admitted.Workspace == "" {
			return Assignment{}, ErrWorkspaceRequired
		}
		snapshot, e := json.Marshal(admitted)
		if e != nil {
			return Assignment{}, e
		}
		if _, err = tx.Exec(`UPDATE tasks SET snapshot=? WHERE id=?`, string(snapshot), id); err != nil {
			return Assignment{}, err
		}
	}
	a := Assignment{uuid.NewString(), t.ID, runner, uuid.NewString(), t.Epoch + 1}
	if _, err = tx.Exec(`INSERT INTO assignments(id,task_id,runner_id,execution_id,epoch) VALUES(?,?,?,?,?)`, a.ID, a.TaskID, a.RunnerID, a.ExecutionID, a.Epoch); err != nil {
		return a, err
	}
	if _, err = tx.Exec(`UPDATE tasks SET state='dispatching',current_assignment=?,epoch=? WHERE id=?`, a.ID, a.Epoch, id); err != nil {
		return a, err
	}
	return a, nil
}
func reportTask(tx *sql.Tx, r Report) (Task, error) {
	var taskID string
	err := tx.QueryRow(`SELECT task_id FROM assignments WHERE id=? AND runner_id=? AND epoch=?`, r.AssignmentID, r.RunnerID, r.Epoch).Scan(&taskID)
	if err == sql.ErrNoRows {
		return Task{}, ErrConflict
	}
	if err != nil {
		return Task{}, err
	}
	t, err := getTask(tx, taskID)
	if err != nil {
		return t, err
	}
	if t.AssignmentID != r.AssignmentID || t.Epoch != r.Epoch {
		return t, ErrConflict
	}
	return t, nil
}
func (s *Store) Accept(a Assignment) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, err := reportTask(tx, Report{AssignmentID: a.ID, RunnerID: a.RunnerID, Epoch: a.Epoch})
	if err != nil {
		return err
	}
	var accepted bool
	if err = tx.QueryRow(`SELECT accepted FROM assignments WHERE id=?`, a.ID).Scan(&accepted); err != nil {
		return err
	}
	if accepted {
		return nil
	}
	if t.State != "dispatching" && t.State != "cancelling" {
		return ErrInvalid
	}
	if _, err = tx.Exec(`UPDATE assignments SET accepted=1 WHERE id=?`, a.ID); err != nil {
		return err
	}
	if t.State == "dispatching" {
		if _, err = tx.Exec(`UPDATE tasks SET state='running' WHERE id=?`, t.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AcceptRecord applies a positive runner acknowledgment without starting another assignment.
func (s *Store) AcceptRecord(record contracts.Accepted) error {
	if err := record.Validate(); err != nil || !record.Accepted {
		return ErrInvalid
	}
	return s.Accept(Assignment{ID: record.AssignmentID, RunnerID: record.RunnerID, Epoch: record.Epoch})
}

// AppendRunnerEvent validates the shared wire representation before persistence.
func (s *Store) AppendRunnerEvent(record contracts.Event) error {
	if err := record.Validate(); err != nil || record.Sequence > uint64(1<<63-1) || record.Kind == "terminal" {
		return ErrInvalid
	}
	return s.RecordEvent(Report{AssignmentID: record.AssignmentID, RunnerID: record.RunnerID, Epoch: record.Epoch, EventID: record.EventID, Sequence: int64(record.Sequence), Kind: record.Kind, Payload: record.Payload})
}

// RecordTerminal adapts a validated runner terminal record to the durable event log.
// A stable message ID is also the terminal event ID; sequence is supplied by the runner journal.
func (s *Store) RecordTerminal(record contracts.Terminal, sequence int64) error {
	if err := record.Validate(); err != nil {
		return ErrInvalid
	}
	result := record.Result
	if record.Error != nil {
		result, _ = json.Marshal(record.Error)
	}
	if len(result) == 0 {
		result = json.RawMessage(`null`)
	}
	payload, err := json.Marshal(struct {
		State  contracts.TaskState `json:"state"`
		Result json.RawMessage     `json:"result"`
	}{record.State, result})
	if err != nil {
		return ErrInvalid
	}
	return s.RecordEvent(Report{AssignmentID: record.AssignmentID, RunnerID: record.RunnerID, Epoch: record.Epoch, EventID: record.ID, Sequence: sequence, Kind: "terminal", Payload: payload})
}

// RecordEvent enforces contiguous sequence numbers and stable event identity.
// terminal kind uses payload {"state":"completed|failed|cancelled","result":...}.
func (s *Store) RecordEvent(r Report) error {
	if _, err := uuid.Parse(r.EventID); err != nil || r.Sequence < 1 || r.Kind == "" {
		return ErrInvalid
	}
	payload, err := canonicalJSON(r.Payload)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, err := reportTask(tx, r)
	if err != nil {
		return err
	}
	var oldAssignment, oldKind string
	var oldSeq int64
	var oldPayload []byte
	err = tx.QueryRow(`SELECT assignment_id,sequence,kind,payload FROM task_events WHERE id=?`, r.EventID).Scan(&oldAssignment, &oldSeq, &oldKind, &oldPayload)
	if err == nil {
		if oldAssignment == r.AssignmentID && oldSeq == r.Sequence && oldKind == r.Kind && bytes.Equal(oldPayload, payload) {
			return nil
		}
		return ErrConflict
	}
	if err != sql.ErrNoRows {
		return err
	}
	if terminal(t.State) {
		return ErrConflict
	}
	var last int64
	if err = tx.QueryRow(`SELECT COALESCE(MAX(sequence),0) FROM task_events WHERE assignment_id=?`, r.AssignmentID).Scan(&last); err != nil {
		return err
	}
	if r.Sequence != last+1 {
		return ErrConflict
	}
	if r.Kind == "execution_uncertain" {
		var uncertainty struct {
			Reason string `json:"reason"`
		}
		if json.Unmarshal(payload, &uncertainty) != nil || uncertainty.Reason != "executor_failed_after_start" || !bytes.Equal(payload, []byte(`{"reason":"executor_failed_after_start"}`)) {
			return ErrInvalid
		}
		if t.State != "running" && t.State != "waiting_child" && t.State != "cancelling" && t.State != "needs_attention" {
			return ErrInvalid
		}
		if t.State != "needs_attention" {
			if _, err = tx.Exec(`UPDATE tasks SET state='needs_attention' WHERE id=?`, t.ID); err != nil {
				return err
			}
		}
	} else if r.Kind == "terminal" {
		var children int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled')`, t.ID).Scan(&children); err != nil {
			return err
		}
		if children > 0 {
			return ErrConflict
		}
		var outcome struct {
			State  string          `json:"state"`
			Result json.RawMessage `json:"result"`
		}
		if json.Unmarshal(payload, &outcome) != nil || !terminal(outcome.State) || !validJSON(outcome.Result) {
			return ErrInvalid
		}
		if !contracts.ValidTransition(contracts.TaskState(t.State), contracts.TaskState(outcome.State)) {
			return ErrInvalid
		}
		if outcome.State == "completed" && (t.CancelRequested || (t.State != "running" && t.State != "needs_attention")) {
			return ErrConflict
		}
		if outcome.State == "cancelled" && !t.CancelRequested {
			return ErrConflict
		}
		if t.State == "cancelling" && outcome.State != "cancelled" && outcome.State != "failed" {
			return ErrConflict
		}
		if _, err = tx.Exec(`UPDATE tasks SET state=?,result=? WHERE id=?`, outcome.State, []byte(outcome.Result), t.ID); err != nil {
			return err
		}
		if _, err = tx.Exec(`UPDATE assignments SET terminal_state=?,terminal_result=? WHERE id=?`, outcome.State, []byte(outcome.Result), r.AssignmentID); err != nil {
			return err
		}
		if t.ParentID != "" {
			if _, err = tx.Exec(`UPDATE tasks SET state='running' WHERE id=? AND state='waiting_child' AND NOT EXISTS(SELECT 1 FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled'))`, t.ParentID, t.ParentID); err != nil {
				return err
			}
		}
	} else if t.State != "running" && t.State != "waiting_child" && t.State != "cancelling" && t.State != "needs_attention" {
		return ErrInvalid
	}
	if _, err = tx.Exec(`INSERT INTO task_events VALUES(?,?,?,?,?,?,?)`, r.EventID, t.ID, r.AssignmentID, r.Epoch, r.Sequence, r.Kind, payload); err != nil {
		return err
	}
	return tx.Commit()
}
func terminal(state string) bool {
	return contracts.TaskState(state).IsTerminal()
}
func (s *Store) CancelTask(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, err := getTask(tx, id)
	if err != nil {
		return err
	}
	if terminal(t.State) {
		return nil
	}
	_, err = tx.Exec(`WITH RECURSIVE owned(id) AS (SELECT ? UNION ALL SELECT t.id FROM tasks t JOIN owned o ON t.parent_id=o.id) UPDATE tasks SET state=CASE WHEN state='queued' THEN 'cancelled' WHEN state='needs_attention' THEN 'needs_attention' ELSE 'cancelling' END,cancel_requested=1 WHERE id IN(SELECT id FROM owned) AND state NOT IN('completed','failed','cancelled')`, id)
	if err != nil {
		return err
	}
	if t.ParentID != "" {
		if _, err = tx.Exec(`UPDATE tasks SET state='running' WHERE id=? AND state='waiting_child' AND NOT EXISTS(SELECT 1 FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled'))`, t.ParentID, t.ParentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetTaskState only controls waiting/reconciliation boundaries; runner terminal reports settle work.
func (s *Store) SetTaskState(id, from, to string) error {
	allowed := (from == "running" && to == "waiting_child") || (from == "waiting_child" && to == "running") || ((from == "dispatching" || from == "running" || from == "waiting_child" || from == "cancelling") && to == "needs_attention")
	if !allowed {
		return ErrInvalid
	}
	r, err := s.db.Exec(`UPDATE tasks SET state=? WHERE id=? AND state=? AND (?<>'running' OR NOT EXISTS(SELECT 1 FROM tasks children WHERE children.parent_id=tasks.id AND children.state NOT IN('completed','failed','cancelled')))`, to, id, from, to)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return ErrConflict
	}
	return nil
}

// ListEvents returns persisted reports in assignment epoch/sequence order.
// Owner subscriptions use their own durable cursor through ReadOwnerEvents.
func (s *Store) ListEvents(taskID string) ([]Report, error) {
	if _, err := s.GetTask(taskID); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT e.assignment_id,a.runner_id,e.epoch,e.id,e.sequence,e.kind,e.payload FROM task_events e JOIN assignments a ON a.id=e.assignment_id WHERE e.task_id=? ORDER BY e.epoch,e.sequence`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reports := []Report{}
	for rows.Next() {
		var r Report
		if err = rows.Scan(&r.AssignmentID, &r.RunnerID, &r.Epoch, &r.EventID, &r.Sequence, &r.Kind, &r.Payload); err != nil {
			return nil, err
		}
		reports = append(reports, r)
	}
	return reports, rows.Err()
}

func (s *Store) EventCount(task string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM task_events WHERE task_id=?`, task).Scan(&n)
	return n, err
}
