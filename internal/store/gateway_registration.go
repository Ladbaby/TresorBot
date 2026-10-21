package store

import (
	"database/sql"
	"tresorbot/internal/contracts"
)

type GatewayRegistration struct {
	ExecutionID, AssignmentID, TaskID, BotID, Alias, BindingID, URL, State string
	Epoch                                                                  int64
	Capability                                                             string `json:"-"`
}

func (s *Store) InitGatewaySchema() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS gateway_executions(execution_id TEXT PRIMARY KEY,assignment_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,bot_id TEXT NOT NULL,alias TEXT NOT NULL,binding_id TEXT NOT NULL,url TEXT NOT NULL,epoch INTEGER NOT NULL,state TEXT NOT NULL,capability TEXT NOT NULL DEFAULT '')`)
	return e
}

// ReserveGatewayExecution records a one-shot registration intent before network I/O.
// Any existing intent is uncertain, not permission to repeat registration.
func (s *Store) ReserveGatewayExecution(g GatewayRegistration) (GatewayRegistration, bool, error) {
	if !contracts.ValidID(g.ExecutionID) || !contracts.ValidID(g.AssignmentID) || g.Epoch <= 0 || g.BindingID == "" || g.BotID == "" {
		return g, false, ErrInvalid
	}
	tx, e := s.db.Begin()
	if e != nil {
		return g, false, e
	}
	defer tx.Rollback()
	t, e := getTask(tx, g.TaskID)
	if e != nil {
		return g, false, e
	}
	if t.AssignmentID != g.AssignmentID || t.Epoch != g.Epoch || t.BotID != g.BotID || t.CancelRequested || terminal(t.State) {
		return g, false, ErrConflict
	}
	result, e := tx.Exec(`INSERT OR IGNORE INTO gateway_executions(execution_id,assignment_id,task_id,bot_id,alias,binding_id,url,epoch,state) VALUES(?,?,?,?,?,?,?,?,'intent')`, g.ExecutionID, g.AssignmentID, g.TaskID, g.BotID, g.Alias, g.BindingID, g.URL, g.Epoch)
	if e != nil {
		return g, false, e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return g, false, e
	}
	old, e := scanGateway(tx.QueryRow(`SELECT execution_id,assignment_id,task_id,bot_id,alias,binding_id,url,epoch,state,capability FROM gateway_executions WHERE execution_id=?`, g.ExecutionID))
	if e != nil {
		return g, false, e
	}
	if old.AssignmentID != g.AssignmentID || old.TaskID != g.TaskID || old.BotID != g.BotID || old.Alias != g.Alias || old.BindingID != g.BindingID || old.URL != g.URL || old.Epoch != g.Epoch {
		return g, false, ErrConflict
	}
	return old, n == 1, tx.Commit()
}
func scanGateway(r scanner) (GatewayRegistration, error) {
	var g GatewayRegistration
	e := r.Scan(&g.ExecutionID, &g.AssignmentID, &g.TaskID, &g.BotID, &g.Alias, &g.BindingID, &g.URL, &g.Epoch, &g.State, &g.Capability)
	if e == sql.ErrNoRows {
		e = ErrNotFound
	}
	return g, e
}
func (s *Store) GatewayExecution(id string) (GatewayRegistration, error) {
	return scanGateway(s.db.QueryRow(`SELECT execution_id,assignment_id,task_id,bot_id,alias,binding_id,url,epoch,state,capability FROM gateway_executions WHERE execution_id=?`, id))
}
func (s *Store) ActivateGatewayExecution(id, capability string) error {
	r, e := s.db.Exec(`UPDATE gateway_executions SET state='active',capability=? WHERE execution_id=? AND state='intent'`, capability, id)
	return changed(r, e)
}
func (s *Store) MarkGatewayRevoking(id string) error {
	_, e := s.db.Exec(`UPDATE gateway_executions SET state='revoking' WHERE execution_id=? AND state<>'revoked'`, id)
	return e
}
func (s *Store) MarkGatewayRevoked(id string) error {
	_, e := s.db.Exec(`UPDATE gateway_executions SET state='revoked',capability='' WHERE execution_id=?`, id)
	return e
}
func (s *Store) GatewayCleanupCandidates() ([]GatewayRegistration, error) {
	rows, e := s.db.Query(`SELECT g.execution_id,g.assignment_id,g.task_id,g.bot_id,g.alias,g.binding_id,g.url,g.epoch,g.state,g.capability FROM gateway_executions g JOIN tasks t ON t.id=g.task_id WHERE g.state<>'revoked' AND (g.state='revoking' OR t.cancel_requested=1 OR t.state IN('completed','failed','cancelled') OR t.current_assignment<>g.assignment_id OR t.epoch<>g.epoch)`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []GatewayRegistration
	for rows.Next() {
		g, e := scanGateway(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, g)
	}
	return out, rows.Err()
}
func (s *Store) ValidateGatewayExecution(execution, assignment, bot, alias, binding string, epoch int64) (GatewayRegistration, error) {
	g, e := s.GatewayExecution(execution)
	if e != nil {
		return g, e
	}
	if g.State != "active" || g.AssignmentID != assignment || g.BotID != bot || g.Alias != alias || g.BindingID != binding || g.Epoch != epoch {
		return g, ErrConflict
	}
	t, e := s.GetTask(g.TaskID)
	if e != nil {
		return g, e
	}
	if t.AssignmentID != assignment || t.Epoch != epoch || t.CancelRequested || terminal(t.State) {
		return g, ErrConflict
	}
	b, e := s.GetBot(bot)
	if e != nil {
		return g, e
	}
	if !b.Enabled {
		return g, ErrDisabled
	}
	return g, nil
}
