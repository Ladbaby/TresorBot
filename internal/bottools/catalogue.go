package bottools

import (
	"sort"
	"strconv"
	"strings"

	"tresorbot/internal/environments"
)

// CatalogueBudget bounds the bytes the tool catalogue may occupy in a prompt.
//
// The number is a prompt-budget decision, not a memory one. Openclaw derives a
// comparable figure as min(18000, max(768, context/10)); 8 KiB is deliberately
// smaller because this catalogue is a fallback for a handful of adapters, not a
// directory of hundreds of MCP tools. A model that needs more should refine its
// query rather than read everything.
const CatalogueBudget = 8 * 1024

// Description ladder. When the catalogue overflows, descriptions shrink in these
// steps before any entry is dropped, because a tool the model can see but not
// understand is worse than a shorter explanation of a tool it can call.
var descriptionLadder = []int{180, 64, 0}

// CatalogueRow is one tool as the model sees it in a search result.
type CatalogueRow struct {
	Name        string
	Environment string
	Description string
	Signature   string
}

// RenderCatalogue renders rows within the budget, degrading in fixed steps.
//
// Degradation is ordered and reported, never silent: an omitted tool the model
// knows about is recoverable by refining the query, whereas one dropped without
// a trace looks like the environment does not exist. The footer is always
// present when anything was dropped, so "no results" and "results withheld" can
// never be confused.
func RenderCatalogue(entries []environments.CatalogEntry) (string, int) {
	rows := make([]CatalogueRow, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, CatalogueRow{
			Name:        entry.Name(),
			Environment: entry.Environment,
			Description: entry.Description,
			Signature:   entry.Signature(),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	for _, width := range descriptionLadder {
		if text := renderRows(rows, width, 0); len(text) <= CatalogueBudget {
			return text, 0
		}
	}
	// Narrowest descriptions still overflow: drop whole rows from the end, since
	// the list is sorted and a prefix is at least stable across identical catalogs.
	text := renderRows(rows, 0, 0)
	kept := len(rows)
	for kept > 0 && len(text) > CatalogueBudget {
		kept--
		text = renderRows(rows, 0, kept)
	}
	omitted := len(rows) - kept
	if kept == 0 {
		// Even the footer alone may exceed nothing, so an empty catalogue with a
		// count is the floor: the model is told tools exist and must be searched
		// for individually rather than being shown a misleading empty result.
		return "", len(rows)
	}
	return renderRows(rows, 0, kept), omitted
}

// renderRows formats up to limit rows (all when limit is 0) with descriptions
// truncated to width characters (0 means names and signatures only).
func renderRows(rows []CatalogueRow, width, limit int) string {
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	var b strings.Builder
	for _, row := range rows {
		b.WriteString("- ")
		b.WriteString(row.Name)
		b.WriteString(row.Signature)
		if width > 0 && row.Description != "" {
			b.WriteString("\n    ")
			b.WriteString(truncateRunes(row.Description, width))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// WithOmittedFooter appends the explicit recovery hint.
//
// Kept separate from rendering so the caller decides the wording: a search result
// and a describe listing may want to say different things about the same omission.
func WithOmittedFooter(text string, omitted int) string {
	if omitted <= 0 {
		return text
	}
	var b strings.Builder
	b.WriteString(strings.TrimRight(text, "\n"))
	if b.Len() > 0 {
		b.WriteString("\n")
	}
	b.WriteString("(")
	b.WriteString(strconv.Itoa(omitted))
	b.WriteString(" more tool(s) omitted for space. Use TresorBotToolDescribe with an exact name, or refine the query.)\n")
	return b.String()
}

// truncateRunes cuts a string on a rune boundary, appending an ellipsis so a
// truncated description does not read as a complete one.
func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	// Collapse whitespace first: a description that is mostly newlines wastes the
	// budget without informing the model.
	collapsed := strings.Join(strings.Fields(s), " ")
	runes := []rune(collapsed)
	if len(runes) <= max {
		return collapsed
	}
	if max == 1 {
		return "…"
	}
	return string(runes[:max-1]) + "…"
}
