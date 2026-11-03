package store

import (
	"database/sql"
	"encoding/json"
	"strings"
)

// AttentionStates are the durable states an owner may need to act on: work that
// stopped before finishing, or whose stopping has not been confirmed yet.
// Terminal success (completed) and deliberate cancellation are not attention.
var AttentionStates = []string{"failed", "needs_attention", "cancelling"}

// TaskFilter narrows a task listing. The zero value lists recent real work.
type TaskFilter struct {
	BotID  string
	States []string
	Limit  int
	Offset int
}

// ListTasksFiltered returns real work items (never the synthetic direct-chat
// parents) matching the filter. Attention listings are ordered by most recently
// touched so the freshest problem is first; plain listings keep creation order.
func (s *Store) ListTasksFiltered(f TaskFilter) ([]Task, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 || f.Offset < 0 {
		return nil, ErrInvalid
	}
	// direct_parent rows are bookkeeping for a conversation, not owner-visible
	// work; they carry no request text and no reply.
	query := `SELECT id FROM tasks WHERE direct_parent=0`
	// Dismissed work is deliberately hidden; acknowledged work still shows
	// because an interruption may have left changes behind.
	if len(f.States) > 0 {
		query += ` AND dismissed_at IS NULL`
	}
	args := []any{}
	if f.BotID != "" {
		query += ` AND bot_id=?`
		args = append(args, f.BotID)
	}
	if len(f.States) > 0 {
		query += ` AND state IN(?` + strings.Repeat(`,?`, len(f.States)-1) + `)`
		for _, st := range f.States {
			args = append(args, st)
		}
	}
	if len(f.States) > 0 {
		query += ` ORDER BY updated_at DESC, rowid DESC`
	} else {
		query += ` ORDER BY rowid DESC`
	}
	query += ` LIMIT ? OFFSET ?`
	args = append(args, limit, f.Offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []Task{}
	for _, id := range ids {
		t, err := s.GetTask(id)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// AttentionTasks lists the work an owner may need to act on, newest first.
func (s *Store) AttentionTasks(botID string, limit, offset int) ([]Task, error) {
	return s.ListTasksFiltered(TaskFilter{BotID: botID, States: AttentionStates, Limit: limit, Offset: offset})
}

// describe derives the owner-facing context for a task: the request in one line
// and the bot that ran it. Both come from the database so the dashboard never
// reimplements JSON-shape sniffing, and so a deleted bot still reads sensibly.
func (s *Store) describe(t *Task) error {
	t.Summary = taskSummary(t.Input)
	t.BotName = t.BotID
	// A missing timestamp or bot row must not fail the read: the dashboard
	// degrades to the bot id and no time rather than an empty tab.
	_ = s.db.QueryRow(`SELECT COALESCE(updated_at,''),COALESCE(acknowledged_at,''),COALESCE(dismissed_at,'') FROM tasks WHERE id=?`, t.ID).Scan(&t.UpdatedAt, &t.AcknowledgedAt, &t.DismissedAt)
	var alias, name string
	err := s.db.QueryRow(`SELECT alias,COALESCE(name,'') FROM bots WHERE id=?`, t.BotID).Scan(&alias, &name)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}
	if name != "" {
		t.BotName = name
	} else if alias != "" {
		t.BotName = alias
	}
	return nil
}

// taskSummary picks the most specific request text a task carries. Input shapes
// differ by origin: owner and delegation tasks carry "text" or "delegated_task",
// conversations carry a chat transcript.
func taskSummary(input []byte) string {
	var payload struct {
		Text          string `json:"text"`
		DelegatedTask string `json:"delegated_task"`
		Chat          struct {
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		} `json:"chat"`
	}
	if json.Unmarshal(input, &payload) != nil {
		return ""
	}
	if t := strings.TrimSpace(payload.Text); t != "" {
		return oneLine(t)
	}
	// The last user message is the request that produced this work.
	for i := len(payload.Chat.Messages) - 1; i >= 0; i-- {
		m := payload.Chat.Messages[i]
		if m.Role != "user" {
			continue
		}
		if text := messageText(m.Content); text != "" {
			return oneLine(text)
		}
	}
	return oneLine(strings.TrimSpace(payload.DelegatedTask))
}

// messageText reads a chat message body, which is either a string or a list of
// content blocks.
func messageText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return strings.TrimSpace(s)
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	parts := []string{}
	for _, b := range blocks {
		if (b.Type == "text" || b.Type == "input_text" || b.Type == "") && strings.TrimSpace(b.Text) != "" {
			parts = append(parts, strings.TrimSpace(b.Text))
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// oneLine collapses whitespace so a multi-line request fits a single line, and
// bounds the result to something a card can show.
func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	const max = 240
	if len(s) > max {
		// Trim on a rune boundary so the summary is always valid UTF-8.
		cut := max
		for cut > 0 && !isRuneStart(s[cut]) {
			cut--
		}
		return strings.TrimSpace(s[:cut]) + "…"
	}
	return s
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
