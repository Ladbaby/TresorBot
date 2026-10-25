package bridge

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"net/http/httptest"
	"strings"
	"testing"
	"tresorbot/internal/store"
)

func TestFinalChatJSONAndSSEHideTaskInternals(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			st, e := store.Open(":memory:")
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			bot, e := st.CreateBot(store.Bot{Alias: "bot/personal", Name: "Personal", Enabled: true,Workspace:t.TempDir()})
			if e != nil {
				t.Fatal(e)
			}
			runner, e := st.CreateRunner("test")
			if e != nil {
				t.Fatal(e)
			}
			body := json.RawMessage(`{"text":"question"}`)
			task, _, e := st.AdmitTask(bot.ID, "bridge:binding", "request", []byte(`{"binding_id":"binding","text":"question"}`))
			if e != nil {
				t.Fatal(e)
			}
			a, e := st.ClaimTask(task.ID, runner)
			if e != nil {
				t.Fatal(e)
			}
			if e = st.Accept(a); e != nil {
				t.Fatal(e)
			}
			if e = st.RecordEvent(store.Report{AssignmentID: a.ID, RunnerID: runner, Epoch: a.Epoch, EventID: uuid.NewString(), Sequence: 1, Kind: "terminal", Payload: json.RawMessage(`{"state":"completed","result":{"text":"Final answer"}}`)}); e != nil {
				t.Fatal(e)
			}
			invoke := ChatInvoker(st, nil, func(json.RawMessage, string) (json.RawMessage, bool, error) { return body, stream, nil })
			var input Input
			input.BindingID = "binding"
			input.Body = body
			input.Request = json.RawMessage(`{"headers":{"idempotency-key":["request"]}}`)
			w := httptest.NewRecorder()
			invoke(w, httptest.NewRequest("POST", "/integration/invoke", bytes.NewReader(nil)), input, bot)
			if w.Code != 200 || !strings.Contains(w.Body.String(), "Final answer") || strings.Contains(w.Body.String(), "binding_id") || strings.Contains(w.Body.String(), "tool_calls") {
				t.Fatal(w.Code, w.Body.String())
			}
			if stream && strings.Count(w.Body.String(), `"finish_reason":"stop"`) != 1 {
				t.Fatal("wrong successful finish count", w.Body.String())
			}
			if stream && !strings.Contains(w.Body.String(), "data: [DONE]") {
				t.Fatal(w.Body.String())
			}
		})
	}
}
