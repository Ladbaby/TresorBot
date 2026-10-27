package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"tresorbot/internal/chatinput"
)

const maxConversationSessionID = 256

// AdmitConversationTask preserves a structured, bot-and-binding-scoped transcript.
// Continuations must supply the full completed transcript plus one new user turn.
func (s *Store) AdmitConversationTask(botID, scope, key, sessionID string, input []byte) (Task, bool, error) {
	if botID == "" || scope == "" || len(scope) > 256 || len(key) > 256 || len(sessionID) > maxConversationSessionID || !utf8.ValidString(sessionID) || strings.TrimSpace(sessionID) != sessionID || strings.ContainsAny(sessionID, "\r\n\x00") {
		return Task{}, false, ErrInvalid
	}
	canonical, err := canonicalJSON(input)
	if err != nil {
		return Task{}, false, err
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(canonical, &payload) != nil || payload == nil {
		return Task{}, false, ErrInvalid
	}
	// Generated authority is never accepted from callers or included in request hashes.
	delete(payload, "session_id")
	delete(payload, "conversation_id")
	canonical, _ = json.Marshal(payload)
	hashInput, _ := json.Marshal(struct {
		Input   json.RawMessage
		Session string
	}{canonical, sessionID})
	hash := requestHash(botID, hashInput)
	tx, err := s.db.Begin()
	if err != nil {
		return Task{}, false, err
	}
	defer tx.Rollback()
	if key != "" {
		var oldHash, id string
		err = tx.QueryRow(`SELECT hash,task_id FROM request_keys WHERE scope=? AND key=?`, scope, key).Scan(&oldHash, &id)
		if err == nil {
			if oldHash != hash {
				return Task{}, false, ErrConflict
			}
			t, e := getTask(tx, id)
			return t, false, e
		}
		if err != sql.ErrNoRows {
			return Task{}, false, err
		}
	}
	bot, err := scanBot(tx.QueryRow(`SELECT `+botColumns+` FROM bots WHERE id=? AND deleted=0`, botID))
	if err != nil {
		return Task{}, false, err
	}
	if !bot.Enabled {
		return Task{}, false, ErrDisabled
	}
	chat, err := chatinput.Parse(payload["chat"], bot.Alias)
	if err != nil {
		return Task{}, false, ErrInvalid
	}
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	var conversationID, previousTask string
	var previousInput []byte
	err = tx.QueryRow(`SELECT id,last_input,COALESCE(task_id,'') FROM conversations WHERE bot_id=? AND scope=? AND session_id=?`, botID, scope, sessionID).Scan(&conversationID, &previousInput, &previousTask)
	if err == nil {
		previous, err := getTask(tx, previousTask)
		if err != nil {
			return Task{}, false, err
		}
		if previous.State != "completed" || previous.CancelRequested {
			return Task{}, false, ErrConflict
		}
		// A changed bot/workspace must not silently reuse another execution context.
		if string(previous.Snapshot) != string(snapshot(bot)) {
			return Task{}, false, ErrConflict
		}
		var old struct {
			Chat chatinput.Input `json:"chat"`
		}
		var result struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(previousInput, &old) != nil || json.Unmarshal(previous.Result, &result) != nil {
			return Task{}, false, ErrConflict
		}
		expected := append(append([]chatinput.Message(nil), old.Chat.Messages...), chatinput.Message{Role: "assistant", Content: result.Text})
		if len(chat.Messages) != len(expected)+1 || !reflect.DeepEqual(chat.Messages[:len(expected)], expected) || chat.Messages[len(expected)].Role != "user" {
			return Task{}, false, ErrConflict
		}
	} else if err == sql.ErrNoRows {
		conversationID = uuid.NewString()
		if _, err = tx.Exec(`INSERT INTO conversations(id,bot_id,scope,session_id,last_input,task_id) VALUES(?,?,?,?,?,NULL)`, conversationID, botID, scope, sessionID, canonical); err != nil {
			return Task{}, false, err
		}
	} else {
		return Task{}, false, err
	}
	payload["session_id"], _ = json.Marshal(sessionID)
	payload["conversation_id"], _ = json.Marshal(conversationID)
	canonical, _ = json.Marshal(payload)
	if !validJSON(canonical) {
		return Task{}, false, ErrInvalid
	}
	task := Task{ID: uuid.NewString(), BotID: botID, State: "queued", Input: canonical, Snapshot: snapshot(bot)}
	if _, err = tx.Exec(`INSERT INTO tasks(id,bot_id,input,snapshot,state) VALUES(?,?,?,?,?)`, task.ID, task.BotID, task.Input, task.Snapshot, task.State); err != nil {
		return Task{}, false, err
	}
	if _, err = tx.Exec(`UPDATE conversations SET last_input=?,task_id=? WHERE id=?`, canonical, task.ID, conversationID); err != nil {
		return Task{}, false, err
	}
	if key != "" {
		if _, err = tx.Exec(`INSERT INTO request_keys VALUES(?,?,?,?)`, scope, key, hash, task.ID); err != nil {
			return Task{}, false, err
		}
	}
	task, err = getTask(tx, task.ID)
	if err != nil {
		return Task{}, false, err
	}
	return task, true, tx.Commit()
}

func requestHash(botID string, canonical []byte) string {
	sum := sha256.Sum256(append(append([]byte(botID), 0), canonical...))
	return hex.EncodeToString(sum[:])
}
