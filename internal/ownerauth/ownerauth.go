package ownerauth

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	_ "modernc.org/sqlite"
)

const SessionLifetime = 365 * 24 * time.Hour

var ErrClosed = errors.New("owner auth store closed")

type Store struct {
	db *sql.DB
}

// Open opens an owner-session database independent from the application database.
func Open(path, password string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS owner_sessions (token_hash TEXT PRIMARY KEY, password_fingerprint TEXT NOT NULL, expires_at INTEGER NOT NULL)`); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err = s.SetPassword(password); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func fingerprint(password string) string {
	h := sha256.Sum256([]byte(password))
	return hex.EncodeToString(h[:])
}

// SetPassword changes the active password fingerprint and invalidates old sessions.
func (s *Store) SetPassword(password string) error {
	if s == nil || s.db == nil {
		return ErrClosed
	}
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS owner_auth_state (id INTEGER PRIMARY KEY CHECK(id=1), password_fingerprint TEXT NOT NULL)`)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`DELETE FROM owner_sessions WHERE password_fingerprint<>?`, fingerprint(password)); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO owner_auth_state(id,password_fingerprint) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET password_fingerprint=excluded.password_fingerprint`, fingerprint(password))
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Issue returns a cryptographically random bearer token. Only its hash is persisted.
func (s *Store) Issue() (string, error) {
	if s == nil || s.db == nil {
		return "", ErrClosed
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)
	var fp string
	if err := s.db.QueryRow(`SELECT password_fingerprint FROM owner_auth_state WHERE id=1`).Scan(&fp); err != nil {
		return "", err
	}
	_, err := s.db.Exec(`INSERT INTO owner_sessions(token_hash,password_fingerprint,expires_at) VALUES(?,?,?)`, tokenHash(token), fp, time.Now().Add(SessionLifetime).Unix())
	if err != nil {
		return "", err
	}
	return token, nil
}

func tokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// Valid checks the token, expiry and current password fingerprint.
func (s *Store) Valid(token string) (bool, error) {
	if s == nil || s.db == nil {
		return false, ErrClosed
	}
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM owner_sessions s JOIN owner_auth_state a ON a.id=1 AND s.password_fingerprint=a.password_fingerprint WHERE s.token_hash=? AND s.expires_at>?`, tokenHash(token), time.Now().Unix()).Scan(&n)
	return n == 1, err
}

// Revoke invalidates only the supplied session token.
func (s *Store) Revoke(token string) error {
	if s == nil || s.db == nil {
		return ErrClosed
	}
	_, err := s.db.Exec(`DELETE FROM owner_sessions WHERE token_hash=?`, tokenHash(token))
	return err
}

func (s *Store) Renew(token string) error {
	if s == nil || s.db == nil {
		return ErrClosed
	}
	_, err := s.db.Exec(`UPDATE owner_sessions SET expires_at=? WHERE token_hash=? AND expires_at>? AND password_fingerprint=(SELECT password_fingerprint FROM owner_auth_state WHERE id=1)`, time.Now().Add(SessionLifetime).Unix(), tokenHash(token), time.Now().Unix())
	return err
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}
