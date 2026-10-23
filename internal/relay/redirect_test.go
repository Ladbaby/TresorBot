package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestScopedCredentialNeverFollowsRedirect(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer target.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer up.Close()
	relay, local, e := New(Options{Model: "bot/test", Authorize: func(context.Context, string) (Authorization, error) {
		return Authorization{URL: up.URL, Headers: http.Header{"Authorization": {"Bearer scoped-secret"}}}, nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer relay.Close()
	req, _ := http.NewRequest("POST", local.BaseURL+endpoint, strings.NewReader(`{"model":"bot/test"}`))
	req.Header.Set("Authorization", "Bearer "+local.APIKey)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 307 || calls.Load() != 0 {
		t.Fatal("redirect replayed scoped inference", resp.StatusCode, calls.Load())
	}
}
