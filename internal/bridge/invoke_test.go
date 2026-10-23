package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"tresorbot/internal/store"
)

func TestDisconnectedInvocationRetainsOneIdempotentTask(t *testing.T) {
	st, e := store.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	bot, e := st.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	invoke := ChatInvoker(st, nil, func(raw json.RawMessage, _ string) (json.RawMessage, bool, error) { return raw, false, nil })
	var input Input
	input.BindingID = "binding"
	input.Body = json.RawMessage(`{"text":"hello"}`)
	input.Request = json.RawMessage(`{"headers":{"idempotency-key":["same-request"]}}`)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := httptest.NewRequest("POST", "/integration/invoke", bytes.NewReader(nil)).WithContext(ctx)
		w := httptest.NewRecorder()
		invoke(w, r, input, bot)
		if w.Header().Get("X-Tresor-Task-ID") == "" {
			t.Fatal("task not persisted before subscriber detached")
		}
	}
	task, created, e := st.AdmitTask(bot.ID, "bridge:binding", "same-request", []byte(`{"binding_id":"binding","text":"hello"}`))
	if e != nil || created || task.State != "queued" {
		t.Fatalf("task replay: created=%v state=%s err=%v", created, task.State, e)
	}
}
