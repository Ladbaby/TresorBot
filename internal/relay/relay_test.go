package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalValidationAndNoAuthorizer(t *testing.T) {
	r, c, err := New(Options{Model: "alias", Authorize: nil})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cases := []struct {
		method, path, token, body string
		want                      int
	}{
		{"POST", endpoint, c.APIKey, `{"model":"alias"}`, 502},
		{"POST", endpoint, "wrong", `{"model":"alias"}`, 401},
		{"GET", endpoint, c.APIKey, `{"model":"alias"}`, 405},
		{"POST", "/other", c.APIKey, `{"model":"alias"}`, 404},
		{"POST", endpoint, c.APIKey, `{"model":"other"}`, 403},
		{"POST", endpoint, c.APIKey, `bad`, 400},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, c.BaseURL+tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s: got %d want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
}
func TestHeadersInferenceAndStreamingCancel(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	canceled := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer authoritative" || r.Header.Get("X-Api-Key") != "scoped-key" || r.Header.Get("X-Tresor-Control") != "scoped-control" || r.Header.Get("X-Test") != "ok" {
			t.Errorf("unexpected headers: %#v", r.Header)
		}
		fl := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: first\n\n")
		fl.Flush()
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer up.Close()
	ids := make(chan string, 2)
	r, c, err := New(Options{Model: "alias", Authorize: func(ctx context.Context, id string) (Authorization, error) {
		ids <- id
		return Authorization{URL: up.URL, Headers: http.Header{"Authorization": {"Bearer authoritative"}, "X-Api-Key": {"scoped-key"}, "X-Tresor-Control": {"scoped-control"}, "X-Test": {"ok"}}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", c.BaseURL+endpoint, strings.NewReader(`{"model":"alias","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("X-Test", "evil")
	req.Header.Set("X-Tresor-Control", "evil")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	b := make([]byte, 12)
	_, err = resp.Body.Read(b)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "data:") {
		t.Fatalf("not streamed: %q", b)
	}
	resp.Body.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second * 3):
		t.Fatal("upstream not canceled")
	}
	id := <-ids
	if id == "" {
		t.Fatal("missing inference id")
	}
	if calls.Load() != 1 {
		t.Fatalf("automatic retry: %d", calls.Load())
	}
	_ = r.Close()
}
func TestCloseRevokesAndCancels(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer up.Close()
	r, c, _ := New(Options{Model: "alias", Authorize: func(context.Context, string) (Authorization, error) { return Authorization{URL: up.URL}, nil }})
	req, _ := http.NewRequest("POST", c.BaseURL+endpoint, strings.NewReader(`{"model":"alias"}`))
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	done := make(chan struct{})
	go func() {
		resp, e := http.DefaultClient.Do(req)
		if e == nil {
			resp.Body.Close()
		}
		close(done)
	}()
	<-started
	_ = r.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel")
	}
	<-done
	req2, _ := http.NewRequest("POST", c.BaseURL+endpoint, strings.NewReader(`{"model":"alias"}`))
	req2.Header.Set("Authorization", "Bearer "+c.APIKey)
	_, err := http.DefaultClient.Do(req2)
	if err == nil {
		t.Fatal("request accepted after Close")
	}
}
