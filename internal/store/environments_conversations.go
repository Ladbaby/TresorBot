package store

import (
	"database/sql"
	"strings"
)

// Conversation is one place an environment instance may speak.
//
// Learning is deliberately separate from allowing: a chat that messages an
// instance becomes known, but an operator can block it afterwards and have that
// stick. If row presence alone meant "allowed", the next inbound message from a
// blocked group would silently restore it.
type Conversation struct {
	Instance     string `json:"instance"`
	Conversation string `json:"conversation"`
	Label        string `json:"label,omitempty"`
	// Source is "inbound" for learned conversations and "manual" for operator-added.
	Source    string `json:"source"`
	Allowed   bool   `json:"allowed"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// RecordInboundConversation notes that a conversation has messaged this instance.
//
// A brand-new conversation is allowed, because being spoken to is the whole point
// of a chat environment. An existing row keeps its Allowed value: that is what
// makes a block durable.
func (s *Store) RecordInboundConversation(instance, conversation, label string) error {
	if instance == "" || conversation == "" || len(conversation) > 256 {
		return ErrInvalid
	}
	if len(label) > 256 {
		label = label[:256]
	}
	// ON CONFLICT deliberately updates only the label: the allowed flag and the
	// source belong to whoever created the row, and inbound traffic may not
	// change either.
	_, err := s.db.Exec(`INSERT INTO environment_conversations(instance,conversation,label,source,allowed)
		VALUES(?,?,?,'inbound',1)
		ON CONFLICT(instance,conversation) DO UPDATE SET
			label=CASE WHEN excluded.label<>'' THEN excluded.label ELSE environment_conversations.label END,
			updated_at=CURRENT_TIMESTAMP`,
		instance, conversation, label)
	return err
}

// AddConversation lets an operator allow a conversation the bot has never heard
// from, which is the only way a bot can open a conversation itself.
func (s *Store) AddConversation(instance, conversation, label string) error {
	if instance == "" || conversation == "" || len(conversation) > 256 {
		return ErrInvalid
	}
	if len(label) > 256 {
		label = label[:256]
	}
	_, err := s.db.Exec(`INSERT INTO environment_conversations(instance,conversation,label,source,allowed)
		VALUES(?,?,?,'manual',1)
		ON CONFLICT(instance,conversation) DO UPDATE SET
			allowed=1,
			label=CASE WHEN excluded.label<>'' THEN excluded.label ELSE environment_conversations.label END,
			updated_at=CURRENT_TIMESTAMP`,
		instance, conversation, label)
	return err
}

// SetConversationAllowed blocks or unblocks a conversation.
func (s *Store) SetConversationAllowed(instance, conversation string, allowed bool) error {
	if instance == "" || conversation == "" {
		return ErrInvalid
	}
	flag := 0
	if allowed {
		flag = 1
	}
	res, err := s.db.Exec(`UPDATE environment_conversations SET allowed=?,updated_at=CURRENT_TIMESTAMP
		WHERE instance=? AND conversation=?`, flag, instance, conversation)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		return ErrNotFound
	}
	return nil
}

// RemoveConversation forgets a conversation entirely, which un-learns it.
func (s *Store) RemoveConversation(instance, conversation string) error {
	_, err := s.db.Exec(`DELETE FROM environment_conversations WHERE instance=? AND conversation=?`, instance, conversation)
	return err
}

// ConversationAllowed reports whether an instance may speak in a conversation.
//
// This is the gate every outbound send passes through. It errs toward refusal: an
// unknown conversation is not allowed, because the caller is a tool call whose
// arguments a model chose.
func (s *Store) ConversationAllowed(instance, conversation string) (bool, error) {
	if instance == "" || conversation == "" {
		return false, nil
	}
	var allowed int
	err := s.db.QueryRow(`SELECT allowed FROM environment_conversations WHERE instance=? AND conversation=?`,
		instance, conversation).Scan(&allowed)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return allowed != 0, nil
}

// ListConversations returns the known conversations for one instance, or for all
// instances when instance is empty.
func (s *Store) ListConversations(instance string) ([]Conversation, error) {
	rows, err := s.db.Query(`SELECT instance,conversation,label,source,allowed,
		COALESCE(created_at,''),COALESCE(updated_at,'')
		FROM environment_conversations
		WHERE (?='' OR instance=?)
		ORDER BY instance, allowed DESC, conversation`, instance, instance)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Conversation{}
	for rows.Next() {
		var c Conversation
		var allowed int
		if err := rows.Scan(&c.Instance, &c.Conversation, &c.Label, &c.Source, &allowed, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Allowed = allowed != 0
		out = append(out, c)
	}
	return out, rows.Err()
}

// normalizeConversation is the shared spelling rule for chat identifiers.
//
// Adapters call it so that a learned id and an operator-typed id cannot differ by
// whitespace alone, which would mean an allowed conversation reported as refused.
func normalizeConversation(value string) string {
	return strings.TrimSpace(value)
}
