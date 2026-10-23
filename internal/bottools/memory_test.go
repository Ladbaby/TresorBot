package bottools

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"path/filepath"
	"strings"
	"testing"
	"tresorbot/internal/memory"
	"tresorbot/internal/store"
)

func TestMemoryWriteReplaysAndRejectsChangedArguments(t *testing.T) {
	s, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	bot, e := s.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true, SharedWrite: true})
	if e != nil {
		t.Fatal(e)
	}
	m, e := memory.Open(filepath.Join(t.TempDir(), "memory"), func(id string) (bool, error) { b, e := s.GetBot(id); return b.SharedWrite, e })
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	h := New(s, m)
	scope := Scope{ExecutionID: uuid.NewString(), AssignmentID: uuid.NewString(), BotID: bot.ID, Service: "binding", Alias: bot.Alias, Epoch: 1}
	r := Request{OperationID: strings.Repeat("a", 64), Tool: "TresorBotMemoryWrite", Arguments: json.RawMessage(`{"scope":"private","path":"index.md","expected_revision":"","content":"remembered"}`)}
	first, status, e := h.Execute(context.Background(), scope, r)
	if e != nil || status != 200 {
		t.Fatal(first, status, e)
	}
	r.Reconcile = true
	second, status, e := h.Execute(context.Background(), scope, r)
	if e != nil || status != 200 {
		t.Fatal(second, status, e)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	var firstJSON any
	_ = json.Unmarshal(a, &firstJSON)
	a, _ = json.Marshal(firstJSON)
	if string(a) != string(b) {
		t.Fatal("outcome changed")
	}
	r.Arguments = json.RawMessage(`{"scope":"private","path":"index.md","expected_revision":"","content":"different"}`)
	_, status, e = h.Execute(context.Background(), scope, r)
	if e != nil || status != 409 {
		t.Fatal(status, e)
	}
	doc, e := m.Read(bot.ID, bot.ID, "index.md")
	if e != nil || doc.Content != "remembered" {
		t.Fatal(doc, e)
	}
}
