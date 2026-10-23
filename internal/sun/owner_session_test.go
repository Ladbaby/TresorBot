package sun

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"tresorbot/internal/config"
)

func TestOwnerSessionProvidesUsableCSRFAndRejectsForeignOrigin(t *testing.T) {
	dir := t.TempDir()
	app, err := Open(config.Config{BindAddr: "127.0.0.1:11520", DBPath: filepath.Join(dir, "tasks.db"), MemoryRoot: filepath.Join(dir, "memory"), AdminPassword: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	const origin = "http://127.0.0.1:11520"
	req := httptest.NewRequest("GET", origin+"/api/auth/status", nil)
	rec := httptest.NewRecorder()
	app.Handler.ServeHTTP(rec, req)
	var state struct {
		CSRF string `json:"csrf_token"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	var cookies []*http.Cookie
	cookies = append(cookies, rec.Result().Cookies()...)
	if state.CSRF == "" || len(cookies) == 0 {
		t.Fatal("auth status must set CSRF cookie before committing response")
	}
	req = httptest.NewRequest("POST", origin+"/api/auth/login", bytes.NewBufferString(`{"password":"owner"}`))
	req.Header.Set("Origin", origin)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", state.CSRF)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec = httptest.NewRecorder()
	app.Handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("login: %d %s", rec.Code, rec.Body.String())
	}
	cookies = append(cookies, rec.Result().Cookies()...)
	for _, tc := range []struct {
		origin string
		valid  bool
	}{{origin, true}, {"http://127.0.0.1.attacker.example", false}, {"https://127.0.0.1:11520", false}} {
		req = httptest.NewRequest("POST", origin+"/api/runners", bytes.NewBufferString(`{"name":"inventory"}`))
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-CSRF-Token", state.CSRF)
		for _, c := range cookies {
			req.AddCookie(c)
		}
		rec = httptest.NewRecorder()
		app.Handler.ServeHTTP(rec, req)
		if tc.valid && rec.Code != 200 && rec.Code != 201 {
			t.Fatalf("legitimate mutation: %d %s", rec.Code, rec.Body.String())
		}
		if !tc.valid && rec.Code != 403 {
			t.Fatalf("foreign origin %s accepted: %d", tc.origin, rec.Code)
		}
	}
}
