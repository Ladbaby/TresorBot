package store

import (
	"database/sql"
	"encoding/json"

	"github.com/google/uuid"
)

var schemaV2 = []string{
	`ALTER TABLE tasks ADD COLUMN parent_id TEXT REFERENCES tasks(id)`,
	`ALTER TABLE tasks ADD COLUMN root_id TEXT`,
	`UPDATE tasks SET root_id=id`,
	`CREATE TABLE task_order(position INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id))`,
	`INSERT INTO task_order(task_id) SELECT id FROM tasks ORDER BY created_at,rowid`,
	`CREATE TABLE task_dependencies(parent_id TEXT NOT NULL REFERENCES tasks(id),child_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),operation_id TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(parent_id,operation_id))`,
	`CREATE TABLE owner_events(cursor INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL REFERENCES tasks(id),kind TEXT NOT NULL,payload TEXT NOT NULL)`,
	`CREATE TRIGGER task_admitted AFTER INSERT ON tasks BEGIN INSERT INTO task_order(task_id) VALUES(NEW.id); UPDATE tasks SET root_id=COALESCE(NEW.root_id,NEW.id) WHERE id=NEW.id; INSERT INTO owner_events(task_id,kind,payload) VALUES(NEW.id,'admitted',json_object('state',NEW.state)); END`,
	`CREATE TRIGGER task_changed AFTER UPDATE OF state,result,cancel_requested ON tasks WHEN OLD.state<>NEW.state OR OLD.cancel_requested<>NEW.cancel_requested OR OLD.result IS NOT NEW.result BEGIN INSERT INTO owner_events(task_id,kind,payload) VALUES(NEW.id,'state',json_object('state',NEW.state,'cancel_requested',NEW.cancel_requested)); END`,
	`CREATE TRIGGER task_progress AFTER INSERT ON task_events BEGIN INSERT INTO owner_events(task_id,kind,payload) VALUES(NEW.task_id,'progress',json_object('epoch',NEW.epoch,'sequence',NEW.sequence)); END`,
}

type Runner struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Revoked bool   `json:"revoked"`
}

func (s *Store) ListRunners() ([]Runner, error) {
	rows, e := s.db.Query(`SELECT id,name,revoked FROM runners ORDER BY name,id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Runner{}
	for rows.Next() {
		var r Runner
		if e = rows.Scan(&r.ID, &r.Name, &r.Revoked); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) ListTasks(botID string, limit, offset int) ([]Task, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 || offset < 0 {
		return nil, ErrInvalid
	}
	rows, e := s.db.Query(`SELECT id FROM tasks WHERE (?='' OR bot_id=?) ORDER BY rowid DESC LIMIT ? OFFSET ?`, botID, botID, limit, offset)
	if e != nil {
		return nil, e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	out := []Task{}
	for _, id := range ids {
		t, e := s.GetTask(id)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, nil
}

type OwnerEvent struct {
	Cursor  int64           `json:"cursor"`
	TaskID  string          `json:"task_id"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Store) ReadOwnerEvents(after int64, taskID string, limit int) ([]OwnerEvent, error) {
	if after < 0 || limit < 1 || limit > 500 {
		return nil, ErrInvalid
	}
	rows, e := s.db.Query(`SELECT cursor,task_id,kind,payload FROM owner_events WHERE cursor>? AND (?='' OR task_id=?) ORDER BY cursor LIMIT ?`, after, taskID, taskID, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []OwnerEvent{}
	for rows.Next() {
		var v OwnerEvent
		var payload string
		if e = rows.Scan(&v.Cursor, &v.TaskID, &v.Kind, &payload); e != nil {
			return nil, e
		}
		v.Payload = json.RawMessage(payload)
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) RecoverActiveTasks() error {
	_, e := s.db.Exec(`UPDATE tasks SET state='needs_attention' WHERE state IN('dispatching','running','waiting_child','cancelling')`)
	return e
}
func (s *Store) RetryTask(id string) (Task, error) {
	t, e := s.GetTask(id)
	if e != nil {
		return Task{}, e
	}
	if !terminal(t.State) {
		return Task{}, ErrConflict
	}
	n, _, e := s.AdmitTask(t.BotID, "owner", "", t.Input)
	return n, e
}
func (s *Store) AbandonTask(id string, confirmed bool) error {
	if !confirmed {
		return ErrInvalid
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	t, e := getTask(tx, id)
	if e != nil {
		return e
	}
	if t.State != "needs_attention" {
		return ErrConflict
	}
	var children int
	if e = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled')`, id).Scan(&children); e != nil {
		return e
	}
	if children > 0 {
		return ErrConflict
	}
	_, e = tx.Exec(`UPDATE tasks SET state='failed',result=? WHERE id=?`, []byte(`{"code":"owner_abandoned","message":"Owner confirmed execution stopped; prior side effects may remain."}`), id)
	if e != nil {
		return e
	}
	if t.AssignmentID != "" {
		if _, e = tx.Exec(`UPDATE assignments SET terminal_state='failed',terminal_result=(SELECT result FROM tasks WHERE id=?) WHERE id=?`, id, t.AssignmentID); e != nil {
			return e
		}
	}
	if t.ParentID != "" {
		if _, e = tx.Exec(`UPDATE tasks SET state='running' WHERE id=? AND state='waiting_child' AND NOT EXISTS(SELECT 1 FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled'))`, t.ParentID, t.ParentID); e != nil {
			return e
		}
	}
	return tx.Commit()
}

// ClaimNext only claims durable work; a transport must actually send the assignment.
func (s *Store) ClaimNext(botID, runnerID string) (Assignment, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return Assignment{}, e
	}
	defer tx.Rollback()
	var id string
	e = tx.QueryRow(`SELECT t.id FROM tasks t JOIN task_order o ON o.task_id=t.id WHERE bot_id=? AND state='queued' ORDER BY o.position LIMIT 1`, botID).Scan(&id)
	if e == sql.ErrNoRows {
		return Assignment{}, ErrNotFound
	}
	if e != nil {
		return Assignment{}, e
	}
	a, e := claimTask(tx, id, runnerID)
	if e != nil {
		return a, e
	}
	return a, tx.Commit()
}

// AdmitChild records child identity before any dispatch and checks the complete wait graph.
func (s *Store) AdmitChild(parentID, targetID, operationID string, input []byte) (Task, error) {
	canonical, e := canonicalJSON(input)
	if e != nil || operationID == "" || len(operationID) > 256 {
		return Task{}, ErrInvalid
	}
	tx, e := s.db.Begin()
	if e != nil {
		return Task{}, e
	}
	defer tx.Rollback()
	hash := string(canonical) + "\x00" + targetID
	var oldID, oldHash string
	e = tx.QueryRow(`SELECT child_id,hash FROM task_dependencies WHERE parent_id=? AND operation_id=?`, parentID, operationID).Scan(&oldID, &oldHash)
	if e == nil {
		if hash != oldHash {
			return Task{}, ErrConflict
		}
		return getTask(tx, oldID)
	}
	if e != sql.ErrNoRows {
		return Task{}, e
	}
	parent, e := getTask(tx, parentID)
	if e != nil {
		return Task{}, e
	}
	if parent.State != "running" && parent.State != "waiting_child" {
		return Task{}, ErrConflict
	}
	var allowed int
	if e = tx.QueryRow(`SELECT COUNT(*) FROM contacts WHERE sender=? AND target=?`, parent.BotID, targetID).Scan(&allowed); e != nil {
		return Task{}, e
	}
	if allowed != 1 || parent.BotID == targetID {
		return Task{}, ErrInvalid
	}
	b, e := scanBot(tx.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, targetID))
	if e != nil {
		return Task{}, e
	}
	if !b.Enabled {
		return Task{}, ErrDisabled
	}
	child := Task{ID: uuid.NewString(), BotID: targetID, State: "queued", Input: canonical, Snapshot: snapshot(b), ParentID: parent.ID, RootID: parent.RootID}
	if _, e = tx.Exec(`INSERT INTO tasks(id,bot_id,input,snapshot,state,parent_id,root_id) VALUES(?,?,?,?,?,?,?)`, child.ID, child.BotID, child.Input, child.Snapshot, child.State, child.ParentID, child.RootID); e != nil {
		return Task{}, e
	}
	if _, e = tx.Exec(`INSERT INTO task_dependencies VALUES(?,?,?,?)`, parent.ID, child.ID, operationID, hash); e != nil {
		return Task{}, e
	}
	if _, e = tx.Exec(`UPDATE tasks SET state='waiting_child' WHERE id=?`, parent.ID); e != nil {
		return Task{}, e
	}
	cycle, e := hasWaitCycle(tx)
	if e != nil {
		return Task{}, e
	}
	if cycle {
		return Task{}, ErrConflict
	}
	if e = tx.Commit(); e != nil {
		return Task{}, e
	}
	return s.GetTask(child.ID)
}
func hasWaitCycle(tx *sql.Tx) (bool, error) {
	rows, e := tx.Query(`SELECT p.id,c.id FROM task_dependencies d JOIN tasks p ON p.id=d.parent_id JOIN tasks c ON c.id=d.child_id WHERE p.state NOT IN('completed','failed','cancelled') AND c.state NOT IN('completed','failed','cancelled') UNION SELECT q.id,b.id FROM tasks q JOIN task_order qo ON qo.task_id=q.id JOIN tasks b ON b.bot_id=q.bot_id AND b.id<>q.id JOIN task_order bo ON bo.task_id=b.id WHERE q.state='queued' AND (b.state IN('dispatching','running','waiting_child','cancelling','needs_attention') OR (b.state='queued' AND bo.position<qo.position))`)
	if e != nil {
		return false, e
	}
	defer rows.Close()
	edges := map[string][]string{}
	for rows.Next() {
		var a, b string
		if e = rows.Scan(&a, &b); e != nil {
			return false, e
		}
		edges[a] = append(edges[a], b)
	}
	if e = rows.Err(); e != nil {
		return false, e
	}
	color := map[string]int{}
	var visit func(string) bool
	visit = func(n string) bool {
		if color[n] == 1 {
			return true
		}
		if color[n] == 2 {
			return false
		}
		color[n] = 1
		for _, v := range edges[n] {
			if visit(v) {
				return true
			}
		}
		color[n] = 2
		return false
	}
	for n := range edges {
		if visit(n) {
			return true, nil
		}
	}
	return false, nil
}
