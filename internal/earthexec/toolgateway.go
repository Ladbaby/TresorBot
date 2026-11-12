package earthexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"tresorbot/internal/contracts"
)

// ToolGateway exposes assignment-scoped bot tools to agents over loopback.
//
// One server serves every assignment on this Earth. A server per assignment
// would mean 500 listening sockets at Worker scale, and each assignment would
// pay a bind plus a TLS-less handshake for no benefit. Path scoping gives the
// same isolation with one listener:
//
//	/a/{assignmentID}/{epoch}/definitions
//	/a/{assignmentID}/{epoch}/execute
//
// The runner credential never leaves Earth: the agent holds only the per-
// assignment token, and Earth performs the upstream call.
type ToolGateway struct {
	sunURL     string
	runnerID   string
	runnerToken string
	client     *http.Client

	mu       sync.Mutex
	routes   map[string]*toolRoute
	listener net.Listener
	server   *http.Server
}

type toolRoute struct {
	token  string
	assign contracts.Assign
}

// NewToolGateway starts the loopback server. It is safe to create once per Earth
// process; an Earth with no Sun URL gets a gateway that hands out no URLs.
func NewToolGateway(ctx context.Context, sunURL, runnerID, runnerToken string) (*ToolGateway, error) {
	g := &ToolGateway{
		sunURL:      sunURL,
		runnerID:    runnerID,
		runnerToken: runnerToken,
		routes:      map[string]*toolRoute{},
	}
	if sunURL == "" {
		return g, nil
	}
	parsed, err := url.Parse(sunURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || runnerToken == "" {
		return nil, errors.New("invalid native tools configuration")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	g.client = &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	g.listener = listener
	g.server = &http.Server{
		Handler:     http.HandlerFunc(g.handle),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go g.server.Serve(listener)
	return g, nil
}

// Register adds one assignment's route and returns the base URL and token an
// adapter should use. The returned function unregisters it.
func (g *ToolGateway) Register(a contracts.Assign) (string, string, func(), error) {
	if g == nil || g.listener == nil {
		return "", "", func() {}, nil
	}
	if g.runnerID != a.RunnerID {
		return "", "", nil, errors.New("invalid native tools configuration")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", nil, err
	}
	token := hex.EncodeToString(secret)
	prefix := fmt.Sprintf("/a/%s/%d", a.AssignmentID, a.Epoch)
	g.mu.Lock()
	g.routes[prefix] = &toolRoute{token: token, assign: a}
	g.mu.Unlock()
	base := "http://" + g.listener.Addr().String() + prefix
	return base, token, func() {
		g.mu.Lock()
		delete(g.routes, prefix)
		g.mu.Unlock()
	}, nil
}

// route resolves a request path to its assignment, returning the suffix.
func (g *ToolGateway) route(path string) (*toolRoute, string, bool) {
	for _, suffix := range []string{"/definitions", "/execute"} {
		if !strings.HasSuffix(path, suffix) {
			continue
		}
		prefix := strings.TrimSuffix(path, suffix)
		g.mu.Lock()
		route := g.routes[prefix]
		g.mu.Unlock()
		if route == nil {
			return nil, "", false
		}
		return route, suffix, true
	}
	return nil, "", false
}

func (g *ToolGateway) handle(w http.ResponseWriter, r *http.Request) {
	route, suffix, ok := g.route(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	// Authenticate before revealing whether the assignment exists, so a wrong
	// token cannot be distinguished from a wrong path.
	header := r.Header.Get("Authorization")
	supplied := strings.TrimPrefix(header, "Bearer ")
	if !strings.HasPrefix(header, "Bearer ") || subtle.ConstantTimeCompare([]byte(supplied), []byte(route.token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	if r.Method != "POST" {
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
	payload["assignment_id"], payload["epoch"] = route.assign.AssignmentID, route.assign.Epoch
	data, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(r.Context(), "POST", strings.TrimRight(g.sunURL, "/")+"/api/runner/tools"+suffix, bytes.NewReader(data))
	if err != nil {
		http.Error(w, "tools unavailable", 502)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.runnerToken)
	req.Header.Set("X-Runner-ID", g.runnerID)
	res, err := g.client.Do(req)
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
}

// Close stops the listener and drops every route.
func (g *ToolGateway) Close() {
	if g == nil || g.server == nil {
		return
	}
	_ = g.server.Close()
	if g.client != nil {
		g.client.Transport.(*http.Transport).CloseIdleConnections()
	}
	g.mu.Lock()
	g.routes = map[string]*toolRoute{}
	g.mu.Unlock()
}
