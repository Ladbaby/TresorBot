package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"

	"github.com/google/uuid"
)

// directParentScope keys the idempotency ledger for synthesized direct-chat
// parents. The key is botID + ":" + executionID so one parent exists per
// direct-chat execution.
const directParentScope = "direct-chat"

// EnsureDirectParent creates or reuses the synthetic parent task that lets a
// direct-chat TresorBotSendMessage attach a child via AdmitChild. Direct chat
// has no durable parent task and no execution authority, so this lightweight
// "running" parent stands in for one. It is idempotent per (botID, executionID)
// and never queued (state "running"), so the dispatch loop ignores it.
func (s *Store) EnsureDirectParent(botID, executionID string) (Task, error) {
	if botID == "" || executionID == "" {
		return Task{}, ErrInvalid
	}
	key := botID + ":" + executionID
	sum := sha256.Sum256([]byte(key))
	hash := hex.EncodeToString(sum[:])
	tx, err := s.db.Begin()
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	var oldHash, id string
	err = tx.QueryRow(`SELECT hash,task_id FROM request_keys WHERE scope=? AND key=?`, directParentScope, key).Scan(&oldHash, &id)
	if err == nil {
		// Reuse the existing parent. Revive it if a prior turn left it in
		// needs_attention, or if it was completed by an older build or by
		// startup recovery, so the next SendMessage in the same execution
		// can attach a new child.
		if _, err = tx.Exec(`UPDATE tasks SET state='running' WHERE id=? AND direct_parent=1 AND state IN('needs_attention','completed')`, id); err != nil {
			return Task{}, err
		}
		t, err := getTask(tx, id)
		if err != nil {
			return Task{}, err
		}
		return t, tx.Commit()
	}
	if err != sql.ErrNoRows {
		return Task{}, err
	}
	b, err := scanBot(tx.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, botID))
	if err != nil {
		return Task{}, err
	}
	if !b.Enabled {
		return Task{}, ErrDisabled
	}
	t := Task{ID: uuid.NewString(), BotID: botID, State: "running", Input: []byte(`{"direct_chat":true}`), Snapshot: snapshot(b)}
	if _, err = tx.Exec(`INSERT INTO tasks(id,bot_id,input,snapshot,state,direct_parent) VALUES(?,?,?,?,?,1)`, t.ID, t.BotID, t.Input, t.Snapshot, t.State); err != nil {
		return Task{}, err
	}
	if _, err = tx.Exec(`INSERT INTO request_keys VALUES(?,?,?,?)`, directParentScope, key, hash, t.ID); err != nil {
		return Task{}, err
	}
	t, err = getTask(tx, t.ID)
	if err != nil {
		return Task{}, err
	}
	return t, tx.Commit()
}

// RecoverDirectParents completes any stale synthesized direct-chat parents at
// startup (e.g. after a crash mid-AwaitChild) so they cannot block the sender
// bot's future claims. A direct parent returns to 'running' after each child so
// a single execution can delegate many times; this cleans up only parents left
// with no non-terminal children (the execution has ended).
func (s *Store) RecoverDirectParents() error {
	_, err := s.db.Exec(`UPDATE tasks SET state='completed', result=COALESCE(result,'{"text":""}') WHERE direct_parent=1 AND state IN('running','waiting_child','needs_attention') AND NOT EXISTS(SELECT 1 FROM tasks c WHERE c.parent_id=tasks.id AND c.state NOT IN('completed','failed','cancelled'))`)
	return err
}

// RecoverOrphanedChildren cancels queued children whose parent is permanently
// gone (terminal or missing). claimTask only admits a child while its parent is
// 'waiting_child', so a child whose parent reached a terminal state can never be
// claimed yet would block later siblings forever via the per-bot FIFO check.
// Cancelling them unblocks the queue. A parent in a recoverable state
// (running/waiting_child/needs_attention/cancelling) does NOT orphan its child.
// Runs at startup and is safe to repeat.
func (s *Store) RecoverOrphanedChildren() error {
	_, err := s.db.Exec(`UPDATE tasks SET state='cancelled', result=COALESCE(result,'{"text":""}') WHERE state='queued' AND parent_id IS NOT NULL AND parent_id<>'' AND NOT EXISTS(SELECT 1 FROM tasks p WHERE p.id=tasks.parent_id AND p.state NOT IN('completed','failed','cancelled'))`)
	return err
}
