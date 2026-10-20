package memory

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

const MaxDocumentBytes = 1 << 20

var (
	ErrNotFound   = errors.New("memory document not found")
	ErrConflict   = errors.New("memory revision conflict")
	ErrInvalid    = errors.New("invalid memory path or request")
	uuidPattern   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Document struct {
	Path     string `json:"path"`
	Content  string `json:"content"`
	Revision string `json:"revision"`
}
type Entry struct {
	Path     string `json:"path"`
	Revision string `json:"revision"`
	Size     int64  `json:"size"`
}
type Store struct {
	root      *os.Root
	db        *sql.DB
	authorize func(string) (bool, error)
	mu        sync.Mutex
}

func Open(root string, authorize func(botID string) (sharedWrite bool, err error)) (*Store, error) {
	if root == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = rejectLinks(abs); err != nil {
		return nil, err
	}
	r, err := os.OpenRoot(abs)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(abs, "memory-operations.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		r.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{root: r, db: db, authorize: authorize}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, scope TEXT NOT NULL, bot_id TEXT NOT NULL, path TEXT NOT NULL, expected TEXT NOT NULL, new_revision TEXT NOT NULL, content BLOB NOT NULL, state TEXT NOT NULL)`)
	if err == nil {
		err = s.recover()
	}
	if err != nil {
		db.Close()
		r.Close()
		return nil, err
	}
	return s, nil
}
func rejectLinks(root string) error {
	cur := filepath.VolumeName(root) + string(filepath.Separator)
	rest := strings.TrimPrefix(root, cur)
	for _, p := range strings.Split(rest, string(filepath.Separator)) {
		if p == "" {
			continue
		}
		cur = filepath.Join(cur, p)
		i, e := os.Lstat(cur)
		if e != nil {
			return e
		}
		if i.Mode()&os.ModeSymlink != 0 || i.IsDir() && i.Mode()&os.ModeIrregular != 0 {
			return ErrInvalid
		}
	}
	return nil
}
func (s *Store) Close() error {
	e := s.db.Close()
	if x := s.root.Close(); e == nil {
		e = x
	}
	return e
}
func hash(b []byte) string { x := sha256.Sum256(b); return hex.EncodeToString(x[:]) }
func validUUID(v string) bool {
	if !uuidPattern.MatchString(v) {
		return false
	}
	_, e := uuid.Parse(v)
	return e == nil
}
func validOp(v string) bool { return validUUID(v) || digestPattern.MatchString(v) }
func (s *Store) authorizeBot(bot string) (bool, error) {
	if !validUUID(bot) || s.authorize == nil {
		return false, ErrInvalid
	}
	return s.authorize(bot)
}
func (s *Store) key(scope, bot, p string) (string, error) {
	if scope == "shared" {
		if bot != "" && !validUUID(bot) {
			return "", ErrInvalid
		}
	} else if !validUUID(scope) || (bot != "" && scope != bot) {
		return "", ErrInvalid
	}
	if p == "" || len(p) > 240 || strings.ContainsAny(p, "\\:") || strings.TrimSpace(p) != p || strings.HasSuffix(p, ".") || path.IsAbs(p) || path.Clean(p) != p || p == "." || strings.HasPrefix(p, "../") || !strings.HasSuffix(p, ".md") {
		return "", ErrInvalid
	}
	for _, part := range strings.Split(p, "/") {
		stem := strings.TrimSuffix(part, ".md")
		u := strings.ToUpper(strings.TrimSuffix(part, "."))
		if strings.TrimRight(stem, " .") != stem || u == "" || strings.HasSuffix(part, " ") || strings.HasSuffix(part, ".") || strings.TrimSuffix(strings.ToUpper(part), ".MD") == "CON" || strings.TrimSuffix(strings.ToUpper(part), ".MD") == "PRN" || strings.TrimSuffix(strings.ToUpper(part), ".MD") == "AUX" || strings.TrimSuffix(strings.ToUpper(part), ".MD") == "NUL" || regexp.MustCompile(`^(COM|LPT)[1-9](\\..*)?$`).MatchString(strings.ToUpper(part)) {
			return "", ErrInvalid
		}
	}
	base := "shared"
	if scope != "shared" {
		base = path.Join("bots", scope)
	}
	return path.Join(base, p), nil
}
func (s *Store) noLinks(k string) error {
	parts := strings.Split(k, "/")
	for i := range parts {
		info, err := s.root.Lstat(strings.Join(parts[:i+1], "/"))
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
			return ErrInvalid
		}
	}
	return nil
}
func (s *Store) actual(k string) ([]byte, string, error) {
	if err := s.noLinks(k); err != nil {
		return nil, "", err
	}
	f, e := s.root.Open(k)
	if errors.Is(e, fs.ErrNotExist) {
		return nil, "", nil
	}
	if e != nil {
		return nil, "", e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, MaxDocumentBytes+1))
	if e != nil {
		return nil, "", e
	}
	if len(b) > MaxDocumentBytes {
		return nil, "", ErrInvalid
	}
	return b, hash(b), nil
}
func (s *Store) List(scope, bot string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, e := s.key(scope, bot, "index.md")
	if e != nil {
		return nil, e
	}
	if bot != "" {
		if _, e = s.authorizeBot(bot); e != nil {
			return nil, e
		}
	}
	base := path.Dir(k)
	out := []Entry{}
	e = s.walk(base, func(k string, b []byte) {
		out = append(out, Entry{strings.TrimPrefix(k, base+"/"), hash(b), int64(len(b))})
	})
	return out, e
}
func (s *Store) walk(base string, fn func(string, []byte)) error {
	if e := s.noLinks(base); e != nil {
		return e
	}
	dir, e := s.root.Open(base)
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	entries, e := dir.ReadDir(-1)
	dir.Close()
	if errors.Is(e, fs.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	for _, x := range entries {
		k := path.Join(base, x.Name())
		if x.Type()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		if x.IsDir() {
			if e = s.walk(k, fn); e != nil {
				return e
			}
		} else if strings.HasSuffix(x.Name(), ".md") {
			b, _, e := s.actual(k)
			if e != nil {
				return e
			}
			fn(k, b)
		}
	}
	return nil
}
func (s *Store) Read(scope, bot, p string) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bot != "" {
		if _, e := s.authorizeBot(bot); e != nil {
			return Document{}, e
		}
	}
	k, e := s.key(scope, bot, p)
	if e != nil {
		return Document{}, e
	}
	b, rev, e := s.actual(k)
	if e != nil {
		return Document{}, e
	}
	if rev == "" {
		return Document{}, ErrNotFound
	}
	return Document{p, string(b), rev}, nil
}
func (s *Store) Write(scope, bot, p, expected, content, op string) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validOp(op) || len(content) > MaxDocumentBytes {
		return Document{}, ErrInvalid
	}
	if bot != "" {
		allowed, e := s.authorizeBot(bot)
		if e != nil {
			return Document{}, e
		}
		if scope == "shared" && !allowed {
			return Document{}, ErrInvalid
		}
	}
	k, e := s.key(scope, bot, p)
	if e != nil {
		return Document{}, e
	}
	nr := hash([]byte(content))
	rh := hash([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", scope, bot, p, expected, nr)))
	var old, oldhash string
	err := s.db.QueryRow(`SELECT state,request_hash FROM operations WHERE id=?`, op).Scan(&old, &oldhash)
	if err == nil {
		if oldhash != rh || old == "conflict" {
			return Document{}, ErrConflict
		}
		if old != "done" {
			if e = s.recover(); e != nil {
				return Document{}, e
			}
			if e = s.db.QueryRow(`SELECT state FROM operations WHERE id=?`, op).Scan(&old); e != nil {
				return Document{}, e
			}
			if old != "done" {
				return Document{}, ErrConflict
			}
		}
		return Document{p, content, nr}, nil
	}
	if err != sql.ErrNoRows {
		return Document{}, err
	}
	_, cur, e := s.actual(k)
	if e != nil {
		return Document{}, e
	}
	if cur != expected {
		return Document{}, ErrConflict
	}
	if e = s.root.MkdirAll(path.Dir(k), 0755); e != nil {
		return Document{}, e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return Document{}, e
	}
	_, e = tx.Exec(`INSERT INTO operations VALUES(?,?,?,?,?,?,?,?,?)`, op, rh, scope, bot, p, expected, nr, []byte(content), "prepared")
	if e != nil {
		tx.Rollback()
		return Document{}, e
	}
	if e = tx.Commit(); e != nil {
		return Document{}, e
	}
	return s.apply(op, k, p, expected, nr, []byte(content))
}
func (s *Store) mark(op, state string) error {
	_, e := s.db.Exec(`UPDATE operations SET state=? WHERE id=?`, state, op)
	return e
}
func (s *Store) apply(op, k, p, expected, nr string, b []byte) (Document, error) {
	_, cur, e := s.actual(k)
	if e != nil {
		return Document{}, e
	}
	if cur == nr {
		e = s.mark(op, "done")
		return Document{p, string(b), nr}, e
	}
	if cur != expected {
		e = s.mark(op, "conflict")
		if e != nil {
			return Document{}, e
		}
		return Document{}, ErrConflict
	}
	stage := k + ".stage-" + hash([]byte(op))
	f, e := s.root.OpenFile(stage, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		return Document{}, e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return Document{}, e
	}
	_, cur, e = s.actual(k)
	if e != nil {
		return Document{}, e
	}
	if cur != expected && cur != nr {
		_ = s.mark(op, "conflict")
		return Document{}, ErrConflict
	}
	if cur != nr {
		if e = s.root.Rename(stage, k); e != nil {
			return Document{}, e
		}
	}
	e = s.mark(op, "done")
	return Document{p, string(b), nr}, e
}
func (s *Store) recover() error {
	rows, e := s.db.Query(`SELECT id,scope,bot_id,path,expected,new_revision,content FROM operations WHERE state='prepared'`)
	if e != nil {
		return e
	}
	type rec struct {
		id, scope, bot, p, expected, nr string
		b                               []byte
	}
	var all []rec
	for rows.Next() {
		var x rec
		if e = rows.Scan(&x.id, &x.scope, &x.bot, &x.p, &x.expected, &x.nr, &x.b); e != nil {
			rows.Close()
			return e
		}
		all = append(all, x)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, x := range all {
		if !validOp(x.id) {
			_ = s.mark(x.id, "conflict")
			continue
		}
		k, e := s.key(x.scope, x.bot, x.p)
		if e != nil {
			_ = s.mark(x.id, "conflict")
			continue
		}
		_, e = s.apply(x.id, k, x.p, x.expected, x.nr, x.b)
		if e != nil && !errors.Is(e, ErrConflict) {
			return fmt.Errorf("recover operation %s: %w", x.id, e)
		}
	}
	return nil
}
