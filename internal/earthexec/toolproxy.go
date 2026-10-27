package earthexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"tresorbot/internal/contracts"
)

// assignmentTools exposes only assignment-scoped bot tools to Pi. Runner
// credentials never enter prompts, extension source, or the child environment.
func assignmentTools(ctx context.Context, c Config, a contracts.Assign) (string, string, func(), error) {
	if c.SunURL == "" {
		return "", "", func() {}, nil
	}
	parsed, err := url.Parse(c.SunURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || c.RunnerID != a.RunnerID || c.RunnerToken == "" {
		return "", "", nil, errors.New("invalid native tools configuration")
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return "", "", nil, err
	}
	token := hex.EncodeToString(secret)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.Method != "POST" || (r.URL.Path != "/definitions" && r.URL.Path != "/execute") {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 {
			http.Error(w, "invalid tool request", 400)
			return
		}
		payload := map[string]any{}
		if len(raw) > 0 && json.Unmarshal(raw, &payload) != nil {
			http.Error(w, "invalid tool request", 400)
			return
		}
		payload["assignment_id"], payload["epoch"] = a.AssignmentID, a.Epoch
		data, _ := json.Marshal(payload)
		req, err := http.NewRequestWithContext(r.Context(), "POST", strings.TrimRight(c.SunURL, "/")+"/api/runner/tools"+r.URL.Path, bytes.NewReader(data))
		if err != nil {
			http.Error(w, "tools unavailable", 502)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.RunnerToken)
		req.Header.Set("X-Runner-ID", c.RunnerID)
		res, err := client.Do(req)
		if err != nil {
			http.Error(w, "tools unavailable", 502)
			return
		}
		defer res.Body.Close()
		body, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			http.Error(w, "invalid tool response", 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(body)
	})
	server := &http.Server{Handler: handler, BaseContext: func(net.Listener) context.Context { return ctx }}
	go server.Serve(listener)
	return "http://" + listener.Addr().String(), token, func() { server.Close(); transport.CloseIdleConnections() }, nil
}
