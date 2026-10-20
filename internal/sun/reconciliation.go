package sun

import (
	"tresorbot/internal/contracts"
	"tresorbot/internal/store"
)

// reconcileStore treats journal records as evidence for existing assignments,
// never as authority to create replacement assignments or start processes.
type reconcileStore struct{ store *store.Store }

func (r reconcileStore) ReconcileRunner(runnerID string, evidence []contracts.AssignmentEvidence) error {
	current, err := r.store.CurrentRunnerAssignments(runnerID)
	if err != nil {
		return err
	}
	known := make(map[string]store.RunnerAssignment, len(current))
	for _, a := range current {
		known[a.ID] = a
	}
	for _, e := range evidence {
		a, ok := known[e.AssignmentID]
		if !ok || a.Epoch != e.Epoch {
			return store.ErrConflict
		}
		if err = r.store.ReconcileRunnerAssignment(runnerID, e); err != nil {
			return err
		}
	}
	// Missing evidence leaves recovered/disconnected assignments uncertain.
	return nil
}
