package store

import (
	"database/sql"
	"tresorbot/internal/contracts"
)

// RunnerAssignment links current durable state to an immutable dispatch identity.
type RunnerAssignment struct {
	Assignment
	State           string
	CancelRequested bool
	Accepted        bool
	LastSequence    uint64
}

// CurrentRunnerAssignments includes terminal evidence until explicitly retained away.
// Callers must fence runner credentials separately before exposing these records.
func (s *Store) CurrentRunnerAssignments(runnerID string) ([]RunnerAssignment, error) {
	if !contracts.ValidID(runnerID) {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(`SELECT a.id,a.task_id,a.runner_id,a.execution_id,a.epoch,t.state,t.cancel_requested,a.accepted,COALESCE((SELECT MAX(sequence) FROM task_events WHERE assignment_id=a.id),0) FROM assignments a JOIN tasks t ON t.current_assignment=a.id AND t.epoch=a.epoch WHERE a.runner_id=? ORDER BY t.rowid`, runnerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunnerAssignment{}
	for rows.Next() {
		var a RunnerAssignment
		if err = rows.Scan(&a.ID, &a.TaskID, &a.RunnerID, &a.ExecutionID, &a.Epoch, &a.State, &a.CancelRequested, &a.Accepted, &a.LastSequence); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ReconcileRunnerAssignment only restores execution state on positive evidence
// from the authenticated, still-current runner. Uncertain launch never resumes.
func (s *Store) ReconcileRunnerAssignment(runnerID string, e contracts.AssignmentEvidence) error {
	if !contracts.ValidID(runnerID) || !contracts.ValidID(e.AssignmentID) || e.Epoch <= 0 || e.Acknowledged > e.LastSequence {
		return ErrInvalid
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var revoked bool
	if err = tx.QueryRow(`SELECT revoked FROM runners WHERE id=?`, runnerID).Scan(&revoked); err == sql.ErrNoRows {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if revoked {
		return ErrConflict
	}
	t, err := reportTask(tx, Report{AssignmentID: e.AssignmentID, RunnerID: runnerID, Epoch: e.Epoch})
	if err != nil {
		return err
	}
	var last uint64
	if err = tx.QueryRow(`SELECT COALESCE(MAX(sequence),0) FROM task_events WHERE assignment_id=?`, e.AssignmentID).Scan(&last); err != nil {
		return err
	}
	// A runner cannot acknowledge events that Sun has not committed, or lose
	// already committed events from its journal. Replay fills only genuine gaps.
	if e.Acknowledged > last || e.LastSequence < last {
		return ErrConflict
	}
	if terminal(t.State) {
		return tx.Commit()
	}
	if e.State != "running" {
		return tx.Commit()
	}
	if t.State != "needs_attention" && t.State != "dispatching" {
		return tx.Commit()
	}
	// Only positive live-process evidence resumes an uncertain assignment.
	// Restoring child waits must also retain dependency semantics.
	next := "running"
	var children int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE parent_id=? AND state NOT IN('completed','failed','cancelled')`, t.ID).Scan(&children); err != nil {
		return err
	}
	if children > 0 {
		next = "waiting_child"
	}
	if t.CancelRequested {
		next = "cancelling"
	}
	if _, err = tx.Exec(`UPDATE assignments SET accepted=1 WHERE id=?`, e.AssignmentID); err != nil {
		return err
	}
	if _, err = tx.Exec(`UPDATE tasks SET state=? WHERE id=?`, next, t.ID); err != nil {
		return err
	}
	return tx.Commit()
}
