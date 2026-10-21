package store

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

var stableToolOperationID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ToolOperation is the durable outcome of a bot-host tool invocation.
type ToolOperation struct {
	ScopeHash   string
	RequestHash string
	State       string
	Status      int
	Payload     []byte
	Error       string
}

// BeginToolOperation persists an intent, or returns the previously recorded operation.
// A conflicting reuse of operationID returns ErrToolOperationConflict.
var ErrToolOperationConflict = errors.New("tool operation conflict")

func (s *Store) BeginToolOperation(operationID, scopeHash, requestHash string) (ToolOperation, bool, error) {
	if !stableToolOperationID.MatchString(operationID) {
		return ToolOperation{}, false, fmt.Errorf("operation ID must be 64 lowercase hex characters")
	}
	if _, err := hex.DecodeString(scopeHash); err != nil || len(scopeHash) != 64 {
		return ToolOperation{}, false, fmt.Errorf("invalid scope hash")
	}
	if _, err := hex.DecodeString(requestHash); err != nil || len(requestHash) != 64 {
		return ToolOperation{}, false, fmt.Errorf("invalid request hash")
	}
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS bot_tool_operations (operation_id TEXT PRIMARY KEY, scope_hash TEXT NOT NULL, request_hash TEXT NOT NULL, state TEXT NOT NULL, status INTEGER NOT NULL DEFAULT 0, payload BLOB, error TEXT NOT NULL DEFAULT '', updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		return ToolOperation{}, false, err
	}
	_, err := s.db.Exec(`INSERT OR IGNORE INTO bot_tool_operations(operation_id,scope_hash,request_hash,state) VALUES(?,?,?,'intent')`, operationID, scopeHash, requestHash)
	if err != nil {
		return ToolOperation{}, false, err
	}
	op, err := s.GetToolOperation(operationID)
	if err != nil {
		return ToolOperation{}, false, err
	}
	if op.ScopeHash != scopeHash || op.RequestHash != requestHash {
		return ToolOperation{}, false, ErrToolOperationConflict
	}
	return op, op.State == "intent", nil
}

func (s *Store) GetToolOperation(operationID string) (ToolOperation, error) {
	var op ToolOperation
	err := s.db.QueryRow(`SELECT scope_hash,request_hash,state,status,payload,error FROM bot_tool_operations WHERE operation_id=?`, operationID).Scan(&op.ScopeHash, &op.RequestHash, &op.State, &op.Status, &op.Payload, &op.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return ToolOperation{}, err
	}
	return op, err
}

func (s *Store) CompleteToolOperation(operationID string, status int, payload []byte, domainError string) error {
	result, err := s.db.Exec(`UPDATE bot_tool_operations SET state='complete',status=?,payload=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND state='intent'`, status, payload, domainError, operationID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		op, err := s.GetToolOperation(operationID)
		if err != nil {
			return err
		}
		if op.State == "complete" && op.Status == status && bytes.Equal(op.Payload, payload) && op.Error == domainError {
			return nil
		}
		return fmt.Errorf("operation is not pending")
	}
	return nil
}
