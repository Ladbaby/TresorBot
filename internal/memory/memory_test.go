package memory

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func openTest(t *testing.T, auth func(string) (bool, error)) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, e := Open(filepath.Join(dir, "mem"), auth)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, filepath.Join(dir, "mem")
}
func TestPrivateAndSharedScopesAndAuthorization(t *testing.T) {
	actor := uuid.NewString()
	allowed := true
	calls := 0
	s, _ := openTest(t, func(id string) (bool, error) {
		calls++
		if id != actor {
			return false, ErrInvalid
		}
		return allowed, nil
	})
	d, e := s.Write(actor, actor, "private.md", "", "private", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Read(actor, "", "private.md"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Write("shared", actor, "shared.md", "", "shared", uuid.NewString()); e != nil {
		t.Fatal(e)
	}
	allowed = false
	if _, e = s.Read("shared", actor, "shared.md"); e != nil {
		t.Fatal("shared-write revocation must not revoke reading", e)
	}
	if _, e = s.List("shared", actor); e != nil {
		t.Fatal("shared-write revocation must not revoke listing", e)
	}
	if _, e = s.Write("shared", actor, "shared.md", hash([]byte("shared")), "x", uuid.NewString()); e == nil {
		t.Fatal("revoked shared write permitted")
	}
	if calls != 5 {
		t.Fatalf("authorization not checked for every bot operation: %d", calls)
	}
	if _, e = s.Write(actor, "", "private.md", d.Revision, "owner edit", uuid.NewString()); e != nil {
		t.Fatal(e)
	}
}
func TestIdempotencyBeforeRevisionAndChangedArgsConflict(t *testing.T) {
	s, _ := openTest(t, nil)
	op := uuid.NewString()
	d, e := s.Write("shared", "", "a.md", "", "one", op)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Write("shared", "", "a.md", d.Revision, "two", uuid.NewString()); e != nil {
		t.Fatal(e)
	}
	replay, e := s.Write("shared", "", "a.md", "", "one", op)
	if e != nil || replay.Revision != d.Revision {
		t.Fatalf("done replay: %#v %v", replay, e)
	}
	if _, e = s.Write("shared", "", "a.md", "", "changed", op); !errors.Is(e, ErrConflict) {
		t.Fatalf("expected conflict, got %v", e)
	}
}
func TestExternalEditRecoveryMarksConflictAndKeepsOpenUsable(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "mem")
	s, e := Open(root, nil)
	if e != nil {
		t.Fatal(e)
	}
	op := uuid.NewString()
	_, e = s.Write("shared", "", "a.md", "", "first", op)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "shared", "a.md"), []byte("external"), 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec(`UPDATE operations SET state='prepared' WHERE id=?`, op); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(root, nil)
	if e != nil {
		t.Fatalf("open on external edit conflict: %v", e)
	}
	defer s.Close()
	if _, e = s.Write("shared", "", "a.md", "", "first", op); !errors.Is(e, ErrConflict) {
		t.Fatalf("conflict replay returned %v", e)
	}
}
func TestInvalidPathsAndOperationIDs(t *testing.T) {
	s, _ := openTest(t, nil)
	for _, p := range []string{"../x.md", "a\\b.md", "a:b.md", "CON.md", "trail. .md", "x. ", "dir/../x.md"} {
		if _, e := s.Read("shared", "", p); !errors.Is(e, ErrInvalid) {
			t.Errorf("%q: %v", p, e)
		}
	}
	if _, e := s.Write("shared", "", "x.md", "", "a", "../../bad"); !errors.Is(e, ErrInvalid) {
		t.Fatalf("unsafe operation id: %v", e)
	}
	if _, e := s.Read("not-uuid", "not-uuid", "x.md"); !errors.Is(e, ErrInvalid) {
		t.Fatalf("bad scope: %v", e)
	}
}
func TestSymlinkRejectedBestEffort(t *testing.T) {
	s, root := openTest(t, nil)
	if e := os.MkdirAll(filepath.Join(root, "shared"), 0755); e != nil {
		t.Fatal(e)
	}
	e := os.Symlink(filepath.Join(t.TempDir(), "elsewhere"), filepath.Join(root, "shared", "link.md"))
	if e != nil {
		t.Skipf("symlink unavailable: %v", e)
	}
	if _, e = s.List("shared", ""); !errors.Is(e, ErrInvalid) {
		t.Fatalf("symlink listing: %v", e)
	}
}
