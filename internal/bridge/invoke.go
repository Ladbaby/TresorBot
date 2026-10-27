package bridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"tresorbot/internal/store"
)

func chatFailure(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": code, "type": "tresorbot_error"}})
}

// ChatInvoker waits only as a subscriber; the durable task outlives its request.
func ChatInvoker(st *store.Store, wake func(), parse func(json.RawMessage, string) (json.RawMessage, bool, error)) func(http.ResponseWriter, *http.Request, Input, store.Bot) {
	return func(w http.ResponseWriter, r *http.Request, input Input, bot store.Bot) {
		taskBody, stream, err := parse(input.Body, bot.Alias)
		if err != nil {
			chatFailure(w, 400, "unsupported_chat_request")
			return
		}
		// Persist binding with task, not caller-supplied bot authority.
		var payload map[string]any
		if json.Unmarshal(taskBody, &payload) != nil {
			chatFailure(w, 400, "invalid_chat_request")
			return
		}
		payload["binding_id"] = input.BindingID
		taskBody, _ = json.Marshal(payload)
		var request struct {
			Headers map[string][]string `json:"headers"`
		}
		if json.Unmarshal(input.Request, &request) != nil {
			chatFailure(w, 400, "invalid_request_metadata")
			return
		}
		keySeen := false
		key := ""
		sessionID := ""
		sessionSeen := false
		for k, v := range request.Headers {
			if http.CanonicalHeaderKey(k) == "X-Tresorbot-Session-Id" {
				if len(v) != 1 || sessionSeen || v[0] == "" {
					chatFailure(w, 400, "invalid_session_id")
					return
				}
				sessionID, sessionSeen = v[0], true
			}
			if http.CanonicalHeaderKey(k) == "Idempotency-Key" {
				if len(v) != 1 || keySeen {
					chatFailure(w, 400, "invalid_idempotency_key")
					return
				}
				keySeen = true
				key = v[0]
			}
		}
		var task store.Task
		if _, chat := payload["chat"]; chat {
			task, _, err = st.AdmitConversationTask(bot.ID, "bridge:"+input.BindingID, key, sessionID, taskBody)
		} else {
			task, _, err = st.AdmitTask(bot.ID, "bridge:"+input.BindingID, key, taskBody)
		}
		if err != nil {
			code := 503
			if err == store.ErrConflict {
				code = 409
			}
			if err == store.ErrInvalid {
				code = 400
			}
			chatFailure(w, code, "task_admission_failed")
			return
		}
		w.Header().Set("X-Tresor-Task-ID", task.ID)
		var admitted struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal(task.Input, &admitted) == nil && admitted.SessionID != "" {
			sessionID = admitted.SessionID
		}
		if sessionID != "" {
			w.Header().Set("X-TresorBot-Session-ID", sessionID)
		}
		if wake != nil {
			wake()
		}
		// Commit SSE headers only after durable admission. This is a subscriber,
		// not an execution deadline; long tasks must not wait for HTTP headers.
		committed := false
		if stream && task.State != "failed" && task.State != "cancelled" {
			committed = true
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		failure := func(status int, code string) {
			if !committed {
				chatFailure(w, status, code)
				return
			}
			b, _ := json.Marshal(map[string]any{"error": map[string]string{"code": code, "message": code}})
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", b)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		// Polling observes persistence, never controls execution termination or slots.
		heartbeat := time.NewTicker(10 * time.Second)
		defer heartbeat.Stop()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			switch task.State {
			case "completed", "failed", "cancelled", "needs_attention":
				if task.State != "completed" {
					failure(502, "task_"+task.State)
					return
				}
				var result struct {
					Text     string          `json:"text"`
					Usage    json.RawMessage `json:"usage"`
					Model    string          `json:"model"`
					Provider string          `json:"provider"`
				}
				if json.Unmarshal(task.Result, &result) != nil {
					failure(502, "invalid_task_result")
					return
				}
				usage := taskUsage(result.Usage)
				base := map[string]any{"id": "chatcmpl-" + task.ID, "created": time.Now().Unix(), "model": bot.Alias}
				base["tresorbot"] = map[string]any{"task_id": task.ID, "session_id": sessionID, "usage_scope": "task", "usage": usage, "pi_usage": result.Usage, "model": result.Model, "provider": result.Provider}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					base["object"] = "chat.completion.chunk"
					base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": result.Text}, "finish_reason": nil}}
					b, _ := json.Marshal(base)
					fmt.Fprintf(w, "data: %s\n\n", b)
					base["choices"] = []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}
					b, _ = json.Marshal(base)
					fmt.Fprintf(w, "data: %s\n\n", b)
					if usage != nil && includeUsage(input.Body) {
						base["choices"] = []any{}
						base["usage"] = usage
						b, _ = json.Marshal(base)
						fmt.Fprintf(w, "data: %s\n\n", b)
					}
					fmt.Fprint(w, "data: [DONE]\n\n")
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				} else {
					w.Header().Set("Content-Type", "application/json")
					base["object"] = "chat.completion"
					if usage != nil {
						base["usage"] = usage
					}
					base["choices"] = []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": result.Text}, "finish_reason": "stop"}}
					_ = json.NewEncoder(w).Encode(base)
				}
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-heartbeat.C:
				if committed {
					if _, err := fmt.Fprint(w, ": waiting\n\n"); err != nil {
						return
					}
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
				continue
			case <-ticker.C:
			}
			task, err = st.GetTask(task.ID)
			if err != nil {
				failure(503, "task_lookup_unavailable")
				return
			}
		}
	}
}
