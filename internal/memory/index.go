package memory

import (
	"fmt"
	"strings"
)

// Index builds a small scoped discovery document from authoritative Markdown.
// An owner's index.md is never overwritten; summaries are derived on demand.
func (s *Store) Index(scope, actor string) (string, error) {
	entries, err := s.List(scope, actor)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString("# Memory index\n\n")
	for i, e := range entries {
		if i >= 100 {
			out.WriteString("\nAdditional documents are available through the file list.\n")
			break
		}
		description := "Markdown document"
		doc, err := s.Read(scope, actor, e.Path)
		if err != nil {
			return "", err
		}
		for _, line := range strings.Split(doc.Content, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#"))
			if line != "" {
				r := []rune(line)
				if len(r) > 120 {
					line = string(r[:120]) + "…"
				}
				description = line
				break
			}
		}
		fmt.Fprintf(&out, "- %s: %s\n", e.Path, description)
	}
	return out.String(), nil
}
