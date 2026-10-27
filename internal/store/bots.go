package store

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

type Bot struct {
	ID           string `json:"id"`
	Alias        string `json:"alias"`
	Name         string `json:"name"`
	Instructions string `json:"instructions"`
	RunnerID     string `json:"runner_id"`
	Workspace    string `json:"workspace"`
	Adapter      string `json:"adapter"`
	SharedWrite  bool   `json:"shared_write"`
	Enabled      bool   `json:"enabled"`
	Revision     int64  `json:"revision"`
	// Kind distinguishes ordinary brains from the built-in Worker: a Worker has
	// no memory and cannot delegate, so its tool surface is reduced accordingly.
	Kind string `json:"kind"`
}
type BotPatch struct {
	Alias, Name, Instructions, RunnerID, Workspace, Adapter *string
	SharedWrite, Enabled                                    *bool
}
type Contact struct {
	Target      string `json:"target"`
	Description string `json:"description"`
}

// Worker marks the built-in stateless recipient kind: no memory, no delegation.
const Worker = "worker"

func (b Bot) IsWorker() bool { return b.Kind == Worker }

func validBot(b Bot) bool {
	return len(b.Alias) <= 256 && len(b.Name) <= 256 &&
		len(b.Instructions) <= 65536 && len(b.Workspace) <= 4096 &&
		strings.TrimSpace(b.Alias) == b.Alias && b.Alias != "" &&
		!strings.ContainsAny(b.Alias, "\r\n\t ") && strings.TrimSpace(b.Name) != "" && b.Adapter == "pi" &&
		(b.Kind == "" || b.Kind == Worker)
}
func (s *Store) CreateRunner(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", ErrInvalid
	}
	id := uuid.NewString()
	_, err := s.db.Exec(`INSERT INTO runners(id,name) VALUES(?,?)`, id, name)
	return id, err
}
func (s *Store) UpdateRunnerMetadata(id, workspace, platform string) error {
	if len(workspace) > 4096 || len(platform) > 256 || strings.ContainsRune(workspace, 0) || strings.ContainsRune(platform, 0) {
		return ErrInvalid
	}
	r, err := s.db.Exec(`UPDATE runners SET default_workspace=?,platform=? WHERE id=? AND revoked=0`, workspace, platform, id)
	return changed(r, err)
}

func (s *Store) RevokeRunner(id string) error {
	r, err := s.db.Exec(`UPDATE runners SET revoked=1 WHERE id=?`, id)
	return changed(r, err)
}
func changed(r sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n == 0 {
		return ErrNotFound
	}
	return err
}
func (s *Store) CreateBot(b Bot) (Bot, error) {
	b.ID = uuid.NewString()
	b.Revision = 1
	if b.Adapter == "" {
		b.Adapter = "pi"
	}
	if !validBot(b) {
		return Bot{}, ErrInvalid
	}
	_, err := s.db.Exec(`INSERT INTO bots(id,alias,name,instructions,runner_id,workspace,adapter,shared_write,enabled,kind) VALUES(?,?,?,?,NULLIF(?,''),?,?,?,?,?)`, b.ID, b.Alias, b.Name, b.Instructions, b.RunnerID, b.Workspace, b.Adapter, b.SharedWrite, b.Enabled, b.Kind)
	return b, err
}

type scanner interface{ Scan(...any) error }

const botColumns = `id,alias,name,instructions,COALESCE(runner_id,''),workspace,adapter,shared_write,enabled,revision,COALESCE(kind,'')`

func scanBot(row scanner) (Bot, error) {
	var b Bot
	err := row.Scan(&b.ID, &b.Alias, &b.Name, &b.Instructions, &b.RunnerID, &b.Workspace, &b.Adapter, &b.SharedWrite, &b.Enabled, &b.Revision, &b.Kind)
	if err == sql.ErrNoRows {
		err = ErrNotFound
	}
	return b, err
}
func (s *Store) GetBot(id string) (Bot, error) {
	return scanBot(s.db.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, id))
}

// BotByAlias resolves a bot from its alias (the model name a client requests).
// This is how a tool call, which only carries the alias, maps back to a bot.
func (s *Store) BotByAlias(alias string) (Bot, error) {
	return scanBot(s.db.QueryRow(`SELECT `+botColumns+` FROM bots WHERE alias=? AND deleted=0`, alias))
}
func (s *Store) ListBots() ([]Bot, error) {
	rows, err := s.db.Query(`SELECT ` + botColumns + ` FROM bots WHERE deleted=0 ORDER BY alias`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Bot{}
	for rows.Next() {
		b, err := scanBot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UpdateBot uses optimistic revision checks; nil fields are unchanged, pointers allow explicit clearing.
func (s *Store) UpdateBot(id string, revision int64, p BotPatch) (Bot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Bot{}, err
	}
	defer tx.Rollback()
	b, err := scanBot(tx.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, id))
	if err != nil {
		return b, err
	}
	if b.Revision != revision {
		return b, ErrConflict
	}
	for _, v := range []struct {
		dst *string
		src *string
	}{{&b.Alias, p.Alias}, {&b.Name, p.Name}, {&b.Instructions, p.Instructions}, {&b.RunnerID, p.RunnerID}, {&b.Workspace, p.Workspace}, {&b.Adapter, p.Adapter}} {
		if v.src != nil {
			*v.dst = *v.src
		}
	}
	if p.Enabled != nil {
		b.Enabled = *p.Enabled
	}
	if p.SharedWrite != nil {
		b.SharedWrite = *p.SharedWrite
	}
	if !validBot(b) {
		return b, ErrInvalid
	}
	_, err = tx.Exec(`UPDATE bots SET alias=?,name=?,instructions=?,runner_id=NULLIF(?,''),workspace=?,adapter=?,shared_write=?,enabled=?,revision=revision+1 WHERE id=?`, b.Alias, b.Name, b.Instructions, b.RunnerID, b.Workspace, b.Adapter, b.SharedWrite, b.Enabled, id)
	if err != nil {
		return b, err
	}
	b.Revision++
	return b, tx.Commit()
}
func (s *Store) DeleteBot(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE bot_id=? AND state NOT IN('completed','failed','cancelled')`, id).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return ErrConflict
	}
	r, err := tx.Exec(`UPDATE bots SET deleted=1,enabled=0,revision=revision+1 WHERE id=? AND deleted=0`, id)
	if err = changed(r, err); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM contacts WHERE sender=? OR target=?`, id, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ReplaceContacts(id string, contacts []Contact) error {
	if len(contacts) > 256 {
		return ErrInvalid
	}
	for _, c := range contacts {
		if len(c.Description) > 8192 {
			return ErrInvalid
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM bots WHERE id=? AND deleted=0`, id).Scan(&exists); err != nil {
		return err
	}
	if exists != 1 {
		return ErrNotFound
	}
	seen := map[string]bool{}
	for _, c := range contacts {
		if c.Target == id || seen[c.Target] {
			return ErrInvalid
		}
		seen[c.Target] = true
		if err = tx.QueryRow(`SELECT COUNT(*) FROM bots WHERE id=? AND deleted=0`, c.Target).Scan(&exists); err != nil {
			return err
		}
		if exists != 1 {
			return ErrNotFound
		}
	}
	if _, err = tx.Exec(`DELETE FROM contacts WHERE sender=?`, id); err != nil {
		return err
	}
	for _, c := range contacts {
		if _, err = tx.Exec(`INSERT INTO contacts VALUES(?,?,?)`, id, c.Target, c.Description); err != nil {
			return err
		}
	}
	// The built-in Worker is always a permitted recipient. Re-insert it so a
	// user contact edit never silently drops it (INSERT OR IGNORE keeps this
	// idempotent when the user already listed the Worker explicitly).
	if err = ensureWorkerContactTx(tx, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ListContacts(id string) ([]Contact, error) {
	if _, err := s.GetBot(id); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT target,description FROM contacts WHERE sender=? ORDER BY target`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Contact{}
	for rows.Next() {
		var c Contact
		if err = rows.Scan(&c.Target, &c.Description); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func snapshot(b Bot) []byte { data, _ := json.Marshal(b); return data }

// WorkerAlias is the fixed alias of the built-in Worker bot. It is always
// present and always a permitted recipient for every enabled bot.
const WorkerAlias = "bot/worker"

// EnsureWorker idempotently creates the built-in Worker bot and returns its
// ID. The Worker is a stateless recipient (no memory, no delegation).
func (s *Store) EnsureWorker() (string, error) {
	if b, err := s.BotByAlias(WorkerAlias); err == nil {
		return b.ID, nil
	} else if err != ErrNotFound {
		return "", err
	}
	b, err := s.CreateBot(Bot{Alias: WorkerAlias, Name: "Worker", Enabled: true, Kind: Worker})
	if err != nil {
		return "", err
	}
	return b.ID, nil
}

// EnsureWorkerContact idempotently adds the built-in Worker as a contact of
// the given bot. It is a no-op for the Worker itself.
func (s *Store) EnsureWorkerContact(botID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = ensureWorkerContactTx(tx, botID); err != nil {
		return err
	}
	return tx.Commit()
}

// ensureWorkerContactTx adds the Worker contact for botID inside an existing
// transaction. Skips when botID is the Worker or the bot is not an enabled,
// non-worker brain. INSERT OR IGNORE keeps it idempotent on the (sender,target)
// primary key.
func ensureWorkerContactTx(tx *sql.Tx, botID string) error {
	var kind string
	var enabled bool
	err := tx.QueryRow(`SELECT COALESCE(kind,''),enabled FROM bots WHERE id=? AND deleted=0`, botID).Scan(&kind, &enabled)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if !enabled || kind == Worker {
		return nil
	}
	workerID, err := workerIDTx(tx)
	if err != nil {
		return err
	}
	if workerID == "" || workerID == botID {
		return nil
	}
	_, err = tx.Exec(`INSERT OR IGNORE INTO contacts(sender,target,description) VALUES(?,?,?)`, botID, workerID, "built-in worker")
	return err
}

// workerIDTx resolves the Worker's ID inside a transaction, creating it if
// absent. Returns "" when it cannot be resolved.
func workerIDTx(tx *sql.Tx) (string, error) {
	var id string
	err := tx.QueryRow(`SELECT id FROM bots WHERE alias=? AND deleted=0`, WorkerAlias).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	id = uuid.NewString()
	if _, err = tx.Exec(`INSERT INTO bots(id,alias,name,instructions,workspace,adapter,shared_write,enabled,kind) VALUES(?,?,?,?,?,?,?,?,?)`,
		id, WorkerAlias, "Worker", "", "", "pi", false, true, Worker); err != nil {
		return "", err
	}
	return id, nil
}

// EnsureWorkerForAll seeds the Worker bot and guarantees it is a contact of
// every enabled, non-worker bot. Idempotent; called at Sun startup.
func (s *Store) EnsureWorkerForAll() error {
	if _, err := s.EnsureWorker(); err != nil {
		return err
	}
	bots, err := s.ListBots()
	if err != nil {
		return err
	}
	for _, b := range bots {
		if !b.Enabled || b.IsWorker() {
			continue
		}
		if err := s.EnsureWorkerContact(b.ID); err != nil {
			return err
		}
	}
	return nil
}
