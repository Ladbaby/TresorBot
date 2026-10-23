package memory

import (
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedWriteRecoveryBeforeAndAfterReplacement(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[replaced], func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "memory")
			s, e := Open(root, nil)
			if e != nil {
				t.Fatal(e)
			}
			op := uuid.NewString()
			content := "recovered"
			nr := hash([]byte(content))
			rh := hash([]byte("shared\x00\x00recover.md\x00\x00" + nr))
			if e = s.root.MkdirAll("shared", 0755); e != nil {
				t.Fatal(e)
			}
			if replaced {
				if e = s.root.WriteFile("shared/recover.md", []byte(content), 0644); e != nil {
					t.Fatal(e)
				}
			}
			_, e = s.db.Exec(`INSERT INTO operations VALUES(?,?,?,?,?,?,?,?,?)`, op, rh, "shared", "", "recover.md", "", nr, []byte(content), "prepared")
			if e != nil {
				t.Fatal(e)
			}
			s.Close()
			s, e = Open(root, nil)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			doc, e := s.Read("shared", "", "recover.md")
			if e != nil || doc.Content != content {
				t.Fatal(doc, e)
			}
			if _, e = s.Write("shared", "", "recover.md", "", content, op); e != nil {
				t.Fatal("recovered operation not idempotent", e)
			}
		})
	}
}
func TestOverwriteAndPrivateIsolation(t *testing.T) {
	s, _ := openTest(t, func(string) (bool, error) { return true, nil })
	a, b := uuid.NewString(), uuid.NewString()
	first, e := s.Write(a, a, "note.md", "", "one", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Read(a, b, "note.md"); !errors.Is(e, ErrInvalid) {
		t.Fatal("private scope crossed", e)
	}
	second, e := s.Write(a, "", "note.md", first.Revision, "two", uuid.NewString())
	if e != nil {
		t.Fatal("existing-file replacement failed", e)
	}
	doc, e := s.Read(a, "", "note.md")
	if e != nil || doc.Revision != second.Revision || doc.Content != "two" {
		t.Fatal(doc, e)
	}
}
func TestSymlinkReadAndWriteRejected(t *testing.T) {
	s, root := openTest(t, nil)
	outside := filepath.Join(t.TempDir(), "target.md")
	if e := os.WriteFile(outside, []byte("outside"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := os.MkdirAll(filepath.Join(root, "shared"), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(outside, filepath.Join(root, "shared", "link.md")); e != nil {
		t.Skipf("symlink unavailable: %v", e)
	}
	if _, e := s.Read("shared", "", "link.md"); !errors.Is(e, ErrInvalid) {
		t.Fatal("link read", e)
	}
	if _, e := s.Write("shared", "", "link.md", "", "bad", uuid.NewString()); !errors.Is(e, ErrInvalid) {
		t.Fatal("link write", e)
	}
}
