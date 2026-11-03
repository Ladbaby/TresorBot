package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"tresorbot/internal/store"
)

func TestOwnerMutationsRequireCSRFAndRevision(t *testing.T) {
	s, e := store.Open(filepath.Join(t.TempDir(), "tasks.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := New(s, nil, Options{BindAddr: "127.0.0.1:11520"})
	status := httptest.NewRecorder()
	h.ServeHTTP(status, httptest.NewRequest("GET", "http://127.0.0.1:11520/api/auth/status", nil))
	var auth struct {
		CSRF string `json:"csrf_token"`
	}
	if e = json.Unmarshal(status.Body.Bytes(), &auth); e != nil {
		t.Fatal(e)
	}
	send := func(method, path, body string, csrf bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1:11520"+path, bytes.NewBufferString(body))
		req.Header.Set("Origin", "http://127.0.0.1:11520")
		req.Header.Set("Content-Type", "application/json")
		if csrf {
			req.Header.Set("X-CSRF-Token", auth.CSRF)
			for _, c := range status.Result().Cookies() {
				req.AddCookie(c)
			}
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := send("POST", "/api/bots", `{"alias":"bot/test","name":"Test","enabled":true}`, false); rec.Code != 403 {
		t.Fatalf("missing csrf: %d", rec.Code)
	}
	rec := send("POST", "/api/bots", `{"alias":"bot/test","name":"Test","enabled":true}`, true)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	var bot store.Bot
	if e = json.Unmarshal(rec.Body.Bytes(), &bot); e != nil {
		t.Fatal(e)
	}
	rec = send("PUT", "/api/bots/"+bot.ID, `{"revision":1,"shared_write":true,"runner_id":"","instructions":"Remember"}`, true)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if e = json.Unmarshal(rec.Body.Bytes(), &bot); e != nil {
		t.Fatal(e)
	}
	if !bot.SharedWrite || bot.Instructions != "Remember" || bot.Revision != 2 {
		t.Fatal("snake case partial edit failed", bot)
	}
	rec = send("PUT", "/api/bots/"+bot.ID, `{"revision":1,"name":"stale"}`, true)
	if rec.Code != http.StatusConflict {
		t.Fatal("stale revision accepted", rec.Code)
	}
	// Work starts through conversation, so the owner API no longer admits a
	// queued task body.
	rec = send("POST", "/api/tasks", `{"bot_id":"`+bot.ID+`","text":"do it"}`, true)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatal("queued work accepted", rec.Code)
	}
}

func TestSettingsStreamingOptionsRoundTrip(t *testing.T) {
	s, e := store.Open(filepath.Join(t.TempDir(), "settings.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := New(s, nil, Options{BindAddr: "127.0.0.1:11521"})
	status := httptest.NewRecorder()
	h.ServeHTTP(status, httptest.NewRequest("GET", "http://127.0.0.1:11521/api/auth/status", nil))
	var auth struct {
		CSRF string `json:"csrf_token"`
	}
	if e = json.Unmarshal(status.Body.Bytes(), &auth); e != nil {
		t.Fatal(e)
	}
	send := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1:11521"+path, bytes.NewBufferString(body))
		req.Header.Set("Origin", "http://127.0.0.1:11521")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-CSRF-Token", auth.CSRF)
		for _, c := range status.Result().Cookies() {
			req.AddCookie(c)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	get := func() map[string]any {
		rec := send("GET", "/api/settings", "")
		if rec.Code != 200 {
			t.Fatal(rec.Code, rec.Body.String())
		}
		var out map[string]any
		if e := json.Unmarshal(rec.Body.Bytes(), &out); e != nil {
			t.Fatal(e)
		}
		return out
	}
	out := get()
	if out["stream_heartbeat"] != true || out["stream_show_intermediate"] != false {
		t.Fatalf("defaults: %v", out)
	}
	if rec := send("PUT", "/api/settings", `{"stream_heartbeat":false,"stream_show_intermediate":true}`); rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	out = get()
	if out["stream_heartbeat"] != false || out["stream_show_intermediate"] != true {
		t.Fatalf("after put: %v", out)
	}
	if rec := send("PUT", "/api/settings", `{"stream_show_intermediate":false}`); rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	out = get()
	if out["stream_heartbeat"] != false || out["stream_show_intermediate"] != false {
		t.Fatalf("partial put: %v", out)
	}
	if rec := send("PUT", "/api/settings", `{"unknown":1}`); rec.Code != 400 {
		t.Fatal("unknown field accepted", rec.Code)
	}
}
