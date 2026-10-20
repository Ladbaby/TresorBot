package store

// MarkRunnerDisconnected retains execution identity and occupied slots while
// exposing uncertainty. A lost control channel is never proof of termination.
func (s *Store) MarkRunnerDisconnected(runnerID string) error {
	_, err := s.db.Exec(`UPDATE tasks SET state='needs_attention' WHERE state IN('dispatching','running','waiting_child','cancelling') AND current_assignment IN(SELECT id FROM assignments WHERE runner_id=?)`, runnerID)
	return err
}
