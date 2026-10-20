package store

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
)

// IssueRunnerCredential replaces the runner's credential and returns its opaque token and generation.
func (s *Store) IssueRunnerCredential(id string) (string, int64, error) {
	if id == "" {
		return "", 0, ErrInvalid
	}
	var tokenBytes [32]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return "", 0, fmt.Errorf("generate runner credential: %w", err)
	}
	token := fmt.Sprintf("%x", tokenBytes[:])
	digest := sha256.Sum256([]byte(token))
	tx, err := s.db.Begin()
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback()
	var revoked int
	if err = tx.QueryRow(`SELECT revoked FROM runners WHERE id=?`, id).Scan(&revoked); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, ErrNotFound
		}
		return "", 0, err
	}
	if revoked != 0 {
		return "", 0, ErrDisabled
	}
	var generation int64
	err = tx.QueryRow(`SELECT generation FROM runner_credentials WHERE runner_id=?`, id).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		generation = 1
		_, err = tx.Exec(`INSERT INTO runner_credentials(runner_id,token_digest,generation) VALUES(?,?,?)`, id, digest[:], generation)
	} else if err == nil {
		generation++
		_, err = tx.Exec(`UPDATE runner_credentials SET token_digest=?,generation=? WHERE runner_id=?`, digest[:], generation, id)
	}
	if err != nil {
		return "", 0, err
	}
	if err = tx.Commit(); err != nil {
		return "", 0, err
	}
	return token, generation, nil
}

// AuthenticateRunner validates only the currently issued credential for an unrevoked runner.
func (s *Store) AuthenticateRunner(id, token string) (int64, error) {
	if id == "" || token == "" {
		return 0, ErrInvalid
	}
	digest := sha256.Sum256([]byte(token))
	var stored []byte
	var generation int64
	var revoked int
	err := s.db.QueryRow(`SELECT c.token_digest,c.generation,r.revoked FROM runner_credentials c JOIN runners r ON r.id=c.runner_id WHERE c.runner_id=?`, id).Scan(&stored, &generation, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	match := 0
	if len(stored) == len(digest) {
		match = subtle.ConstantTimeCompare(stored, digest[:])
	}
	if revoked != 0 || match != 1 {
		return 0, ErrDisabled
	}
	return generation, nil
}

// CurrentRunnerGeneration returns the credential generation for connection fencing.
func (s *Store) CurrentRunnerGeneration(id string) (int64, error) {
	if id == "" {
		return 0, ErrInvalid
	}
	var generation int64
	var revoked int
	err := s.db.QueryRow(`SELECT c.generation,r.revoked FROM runners r JOIN runner_credentials c ON c.runner_id=r.id WHERE r.id=?`, id).Scan(&generation, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		var exists int
		if e := s.db.QueryRow(`SELECT 1 FROM runners WHERE id=?`, id).Scan(&exists); errors.Is(e, sql.ErrNoRows) {
			return 0, ErrNotFound
		} else if e != nil {
			return 0, e
		}
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if revoked != 0 {
		return 0, ErrDisabled
	}
	return generation, nil
}
