package store

// ValidateNativeAssignment checks durable task authority independently of gateway
// inference registration. The endpoint authenticates the runner credential first.
func (s *Store) ValidateNativeAssignment(assignment, runner string, epoch int64) (Task, error) {
	var taskID string
	err := s.db.QueryRow(`SELECT t.id FROM assignments a JOIN tasks t ON t.current_assignment=a.id JOIN bots b ON b.id=t.bot_id JOIN runners r ON r.id=a.runner_id WHERE a.id=? AND a.runner_id=? AND a.epoch=? AND t.epoch=a.epoch AND t.cancel_requested=0 AND t.state IN('running','waiting_child') AND b.enabled=1 AND b.deleted=0 AND r.revoked=0`, assignment, runner, epoch).Scan(&taskID)
	if err != nil {
		return Task{}, ErrConflict
	}
	return s.GetTask(taskID)
}
