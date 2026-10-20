package earthjournal

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"

	_ "modernc.org/sqlite"
	"tresorbot/internal/contracts"
)

var (
	ErrConflict   = errors.New("assignment content conflict")
	ErrNotFound   = errors.New("assignment not found")
	ErrIneligible = errors.New("assignment is not launch eligible")
	ErrFull       = errors.New("report outbox is full")
	ErrSequence   = errors.New("event sequence is not contiguous")
	ErrIdentity   = errors.New("runner or assignment epoch mismatch")
)

type State string

const (
	Admitted     State = "admitted"
	LaunchIntent State = "launch_intent"
	Running      State = "running"
	Uncertain    State = "uncertain"
	Terminal     State = "terminal"
)

type Record struct {
	Assignment   contracts.Assign `json:"assignment"`
	Hash         string           `json:"hash"`
	State        State            `json:"state"`
	ProcessID    int64            `json:"process_id,omitempty"`
	SessionID    string           `json:"session_id,omitempty"`
	LastSequence uint64           `json:"last_sequence"`
	Acknowledged uint64           `json:"acknowledged"`
	Outcome      json.RawMessage  `json:"outcome,omitempty"`
}
type Journal struct {
	db       *sql.DB
	mu       sync.Mutex
	capacity int
}

// Open opens or creates a durable SQLite assignment journal. capacity bounds unacknowledged reports per assignment.
func Open(path string, capacity int) (*Journal, error) {
	if capacity < 1 {
		return nil, errors.New("capacity must be positive")
	}
	db, e := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	j := &Journal{db: db, capacity: capacity}
	_, e = db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS assignments(id TEXT PRIMARY KEY, runner TEXT NOT NULL, epoch INTEGER NOT NULL, hash TEXT NOT NULL, body BLOB NOT NULL, state TEXT NOT NULL, process_id INTEGER NOT NULL DEFAULT 0, session_id TEXT NOT NULL DEFAULT '', next_seq INTEGER NOT NULL DEFAULT 1, ack_seq INTEGER NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS outcomes(assignment TEXT PRIMARY KEY,body BLOB NOT NULL); CREATE TABLE IF NOT EXISTS reports(assignment TEXT NOT NULL, seq INTEGER NOT NULL, body BLOB NOT NULL, terminal INTEGER NOT NULL, PRIMARY KEY(assignment,seq));`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return j, nil
}

// Close closes the underlying database.
func (j *Journal) Close() error { return j.db.Close() }
func canonical(a contracts.Assign) ([]byte, string, error) {
	if e := a.Validate(); e != nil {
		return nil, "", e
	}
	if len(a.Input) > contracts.MaxRunnerRecordBytes {
		return nil, "", ErrConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(a.Input))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return nil, "", err
	}
	a.Input, _ = json.Marshal(input)
	b, e := json.Marshal(a)
	if e != nil {
		return nil, "", e
	}
	// Message identity changes on retransmission; execution identity does not.
	a.ID = ""
	stable, e := json.Marshal(a)
	if e != nil {
		return nil, "", e
	}
	h := sha256.Sum256(stable)
	return b, hex.EncodeToString(h[:]), nil
}

// Admit validates and durably records the canonical assignment before returning acceptance; exact replays are idempotent.
func (j *Journal) Admit(a contracts.Assign) (bool, error) {
	b, h, e := canonical(a)
	if e != nil {
		return false, e
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var old string
	e = j.db.QueryRow(`SELECT hash FROM assignments WHERE id=?`, a.AssignmentID).Scan(&old)
	if e == nil {
		if old != h {
			return false, ErrConflict
		}
		return false, nil
	}
	if e != sql.ErrNoRows {
		return false, e
	}
	_, e = j.db.Exec(`INSERT INTO assignments(id,runner,epoch,hash,body,state) VALUES(?,?,?,?,?,?)`, a.AssignmentID, a.RunnerID, a.Epoch, h, b, Admitted)
	return e == nil, e
}
func (j *Journal) get(id string) (Record, error) {
	var r Record
	var b, outcome []byte
	var st string
	e := j.db.QueryRow(`SELECT body,hash,state,process_id,session_id,next_seq-1,ack_seq,COALESCE((SELECT body FROM outcomes WHERE assignment=assignments.id),'null') FROM assignments WHERE id=?`, id).Scan(&b, &r.Hash, &st, &r.ProcessID, &r.SessionID, &r.LastSequence, &r.Acknowledged, &outcome)
	if e == sql.ErrNoRows {
		return r, ErrNotFound
	}
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(b, &r.Assignment); e != nil {
		return r, e
	}
	r.State = State(st)
	r.Outcome = json.RawMessage(outcome)
	return r, nil
}

// Get returns the durable assignment and current lifecycle evidence.
func (j *Journal) Get(id string) (Record, error) { return j.get(id) }

// List returns durable assignments ordered by assignment ID.
func (j *Journal) List() ([]Record, error) {
	rows, e := j.db.Query(`SELECT id FROM assignments ORDER BY id`)
	if e != nil {
		return nil, e
	}
	var ids []string
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
	out := make([]Record, 0, len(ids))
	for _, id := range ids {
		r, e := j.get(id)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}

// BeginLaunch atomically records intent; it succeeds only once for an admitted assignment.
func (j *Journal) BeginLaunch(id, runner string, epoch int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, e := j.get(id)
	if e != nil {
		return e
	}
	if r.Assignment.RunnerID != runner || r.Assignment.Epoch != epoch {
		return ErrIdentity
	}
	if r.State != Admitted {
		return ErrIneligible
	}
	res, e := j.db.Exec(`UPDATE assignments SET state=? WHERE id=? AND state=?`, LaunchIntent, id, Admitted)
	if e != nil {
		return e
	}
	n, e := res.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return ErrIneligible
	}
	return nil
}

// MarkStarted durably records process/session evidence and moves launch intent to running.
func (j *Journal) MarkStarted(id, runner string, epoch, processID int64, sessionID string) error {
	if processID <= 0 || sessionID == "" {
		return errors.New("process ID and session ID are required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	r, e := j.get(id)
	if e != nil {
		return e
	}
	if r.Assignment.RunnerID != runner || r.Assignment.Epoch != epoch {
		return ErrIdentity
	}
	if r.State != LaunchIntent {
		return ErrIneligible
	}
	_, e = j.db.Exec(`UPDATE assignments SET state=?,process_id=?,session_id=? WHERE id=?`, Running, processID, sessionID, id)
	return e
}
func (j *Journal) append(id, runner string, epoch int64, body []byte, terminal bool) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(body) > contracts.MaxRunnerRecordBytes {
		return 0, ErrFull
	}
	tx, e := j.db.Begin()
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	var actualRunner, state string
	var actualEpoch, next int64
	var count int
	e = tx.QueryRow(`SELECT runner,epoch,state,next_seq,(SELECT COUNT(*) FROM reports WHERE assignment=?) FROM assignments WHERE id=?`, id, id).Scan(&actualRunner, &actualEpoch, &state, &next, &count)
	if e == sql.ErrNoRows {
		return 0, ErrNotFound
	}
	if e != nil {
		return 0, e
	}
	if actualRunner != runner || actualEpoch != epoch {
		return 0, ErrIdentity
	}
	if State(state) == Terminal {
		return 0, ErrIneligible
	}
	if count >= j.capacity || next == math.MaxInt64 {
		return 0, ErrFull
	}
	var header contracts.RunnerRecord
	if terminal {
		var t contracts.Terminal
		if e = json.Unmarshal(body, &t); e != nil {
			return 0, e
		}
		if e = t.Validate(); e != nil {
			return 0, e
		}
		header = t.RunnerRecord
		if t.Sequence != 0 && t.Sequence != uint64(next) {
			return 0, ErrSequence
		}
		t.Sequence = uint64(next)
		body, e = json.Marshal(t)
	} else {
		var event contracts.Event
		if e = json.Unmarshal(body, &event); e != nil {
			return 0, e
		}
		if e = event.Validate(); e != nil {
			return 0, e
		}
		header = event.RunnerRecord
		if event.Sequence != uint64(next) {
			return 0, ErrSequence
		}
	}
	if e != nil {
		return 0, e
	}
	if header.RunnerID != runner || header.AssignmentID != id || header.Epoch != epoch {
		return 0, ErrIdentity
	}
	if _, e = tx.Exec(`INSERT INTO reports VALUES(?,?,?,?)`, id, next, body, terminal); e != nil {
		return 0, e
	}
	if terminal {
		if _, e = tx.Exec(`INSERT INTO outcomes VALUES(?,?)`, id, body); e != nil {
			return 0, e
		}
		state = string(Terminal)
	}
	if _, e = tx.Exec(`UPDATE assignments SET state=?,next_seq=next_seq+1 WHERE id=?`, state, id); e != nil {
		return 0, e
	}
	return uint64(next), tx.Commit()
}

// Append durably queues a validated Event or Terminal. Terminal state and report commit atomically.
func (j *Journal) Append(id, runner string, epoch int64, v any) (uint64, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return 0, e
	}
	switch v.(type) {
	case contracts.Event:
		return j.append(id, runner, epoch, b, false)
	case contracts.Terminal:
		return j.append(id, runner, epoch, b, true)
	default:
		return 0, fmt.Errorf("unsupported report type %T", v)
	}
}

// Reports returns retained reports after the acknowledged cursor in sequence order.
func (j *Journal) Reports(id string) ([]json.RawMessage, error) {
	rows, e := j.db.Query(`SELECT body FROM reports WHERE assignment=? AND seq>(SELECT ack_seq FROM assignments WHERE id=?) ORDER BY seq`, id, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []json.RawMessage
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Acknowledge advances the contiguous durable cursor; duplicate acknowledgements are harmless.
func (j *Journal) Acknowledge(id string, seq uint64) error {
	if seq > math.MaxInt64 {
		return ErrSequence
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	tx, e := j.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var ack, next int64
	e = tx.QueryRow(`SELECT ack_seq,next_seq FROM assignments WHERE id=?`, id).Scan(&ack, &next)
	if e == sql.ErrNoRows {
		return ErrNotFound
	}
	if e != nil {
		return e
	}
	if int64(seq) <= ack {
		return nil
	}
	if int64(seq) >= next {
		return ErrSequence
	}
	if _, e = tx.Exec(`DELETE FROM reports WHERE assignment=? AND seq<=?`, id, seq); e != nil {
		return e
	}
	if _, e = tx.Exec(`UPDATE assignments SET ack_seq=? WHERE id=?`, seq, id); e != nil {
		return e
	}
	return tx.Commit()
}

// Recover marks launch intents and running assignments uncertain; none can be relaunched automatically.
func (j *Journal) Recover() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, e := j.db.Exec(`UPDATE assignments SET state=? WHERE state IN (?,?)`, Uncertain, LaunchIntent, Running)
	return e
}
