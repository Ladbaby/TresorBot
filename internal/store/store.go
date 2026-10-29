package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrConflict          = errors.New("conflict")
	ErrInvalid           = errors.New("invalid input or transition")
	ErrDisabled          = errors.New("bot disabled")
	ErrWorkspaceRequired = errors.New("runner default workspace required")
)

type Store struct{ db *sql.DB }

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err = db.Ping(); err == nil {
		err = s.migrate()
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Ping() error  { return s.db.Ping() }

func (s *Store) migrate() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY)`); err != nil {
		return err
	}
	var version int
	if err = tx.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > 8 {
		return fmt.Errorf("database schema %d is newer than supported version 8", version)
	}
	if version == 0 {
		for _, q := range schemaV1 {
			if _, err = tx.Exec(q); err != nil {
				return fmt.Errorf("migration 1: %w", err)
			}
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(1)`); err != nil {
			return err
		}
	}
	if version < 2 {
		for _, q := range schemaV2 {
			if _, err = tx.Exec(q); err != nil {
				return fmt.Errorf("migration 2: %w", err)
			}
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(2)`); err != nil {
			return err
		}
	}
	if version < 3 {
		if _, err = tx.Exec(`CREATE TABLE runner_credentials(runner_id TEXT PRIMARY KEY REFERENCES runners(id) ON DELETE CASCADE,token_digest BLOB NOT NULL,generation INTEGER NOT NULL CHECK(generation>0))`); err != nil {
			return fmt.Errorf("migration 3: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(3)`); err != nil {
			return err
		}
	}
	if version < 4 {
		if _, err = tx.Exec(`ALTER TABLE runners ADD COLUMN default_workspace TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migration 4: %w", err)
		}
		if _, err = tx.Exec(`ALTER TABLE runners ADD COLUMN platform TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migration 4: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(4)`); err != nil {
			return err
		}
	}
	if version < 5 {
		if _, err = tx.Exec(`CREATE TABLE conversations(id TEXT PRIMARY KEY,bot_id TEXT NOT NULL REFERENCES bots(id),scope TEXT NOT NULL,session_id TEXT NOT NULL,last_input BLOB NOT NULL,task_id TEXT REFERENCES tasks(id),UNIQUE(bot_id,scope,session_id))`); err != nil {
			return fmt.Errorf("migration 5: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(5)`); err != nil {
			return err
		}
	}
	if version < 6 {
		if _, err = tx.Exec(`ALTER TABLE bots ADD COLUMN kind TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migration 6: %w", err)
		}
		if _, err = tx.Exec(`ALTER TABLE tasks ADD COLUMN target_runner_id TEXT`); err != nil {
			return fmt.Errorf("migration 6: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(6)`); err != nil {
			return err
		}
	}
	if version < 7 {
		if _, err = tx.Exec(`ALTER TABLE tasks ADD COLUMN direct_parent INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("migration 7: %w", err)
		}
		// Synthesized direct-chat parents must not occupy the bot's single
		// active-task slot, so exclude them from the one_bot_slot index.
		if _, err = tx.Exec(`DROP INDEX one_bot_slot`); err != nil {
			return fmt.Errorf("migration 7: %w", err)
		}
		if _, err = tx.Exec(`CREATE UNIQUE INDEX one_bot_slot ON tasks(bot_id) WHERE direct_parent=0 AND state IN('dispatching','running','waiting_child','cancelling','needs_attention')`); err != nil {
			return fmt.Errorf("migration 7: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(7)`); err != nil {
			return err
		}
	}
	if version < 8 {
		if _, err = tx.Exec(`CREATE TABLE settings(key TEXT PRIMARY KEY,value INTEGER NOT NULL)`); err != nil {
			return fmt.Errorf("migration 8: %w", err)
		}
		if _, err = tx.Exec(`INSERT INTO schema_migrations VALUES(8)`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var schemaV1 = []string{
	`CREATE TABLE runners(id TEXT PRIMARY KEY,name TEXT NOT NULL,revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN(0,1)))`,
	`CREATE TABLE bots(id TEXT PRIMARY KEY,alias TEXT NOT NULL UNIQUE,name TEXT NOT NULL,instructions TEXT NOT NULL,runner_id TEXT REFERENCES runners(id),workspace TEXT NOT NULL,adapter TEXT NOT NULL,shared_write INTEGER NOT NULL,enabled INTEGER NOT NULL,revision INTEGER NOT NULL DEFAULT 1,deleted INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
	`CREATE TABLE contacts(sender TEXT NOT NULL REFERENCES bots(id),target TEXT NOT NULL REFERENCES bots(id),description TEXT NOT NULL,PRIMARY KEY(sender,target),CHECK(sender<>target))`,
	`CREATE TABLE tasks(id TEXT PRIMARY KEY,bot_id TEXT NOT NULL REFERENCES bots(id),input BLOB NOT NULL,snapshot BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN('queued','dispatching','running','waiting_child','cancelling','needs_attention','completed','failed','cancelled')),result BLOB,current_assignment TEXT,epoch INTEGER NOT NULL DEFAULT 0,cancel_requested INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
	`CREATE UNIQUE INDEX one_bot_slot ON tasks(bot_id) WHERE state IN('dispatching','running','waiting_child','cancelling','needs_attention')`,
	`CREATE TABLE assignments(id TEXT PRIMARY KEY,task_id TEXT NOT NULL REFERENCES tasks(id),runner_id TEXT NOT NULL REFERENCES runners(id),execution_id TEXT NOT NULL UNIQUE,epoch INTEGER NOT NULL CHECK(epoch>0),accepted INTEGER NOT NULL DEFAULT 0,terminal_state TEXT,terminal_result BLOB,UNIQUE(task_id,epoch))`,
	`CREATE TABLE task_events(id TEXT PRIMARY KEY,task_id TEXT NOT NULL REFERENCES tasks(id),assignment_id TEXT NOT NULL REFERENCES assignments(id),epoch INTEGER NOT NULL,sequence INTEGER NOT NULL CHECK(sequence>0),kind TEXT NOT NULL,payload BLOB NOT NULL,UNIQUE(assignment_id,sequence))`,
	`CREATE TABLE request_keys(scope TEXT NOT NULL,key TEXT NOT NULL,hash TEXT NOT NULL,task_id TEXT NOT NULL REFERENCES tasks(id),PRIMARY KEY(scope,key))`,
	`CREATE TRIGGER bots_updated AFTER UPDATE ON bots BEGIN UPDATE bots SET updated_at=CURRENT_TIMESTAMP WHERE id=NEW.id; END`,
	`CREATE TRIGGER tasks_updated AFTER UPDATE ON tasks BEGIN UPDATE tasks SET updated_at=CURRENT_TIMESTAMP WHERE id=NEW.id; END`,
}
