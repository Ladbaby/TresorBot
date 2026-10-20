package relay

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
	"sync"

	"github.com/google/uuid"
)

const MaxBodyBytes int64 = 4 << 20
const endpoint = "/v1/chat/completions"

// Authorization is scoped to one inference and never stored by Relay.
type Authorization struct {
	URL     string
	Headers http.Header
}

// Authorizer provides per-inference upstream authorization and destination.
type Authorizer func(context.Context, string) (Authorization, error)

type Options struct {
	Model     string
	Authorize Authorizer
	Client    *http.Client
}
type Relay struct {
	mu         sync.Mutex
	model, key string
	authorize  Authorizer
	client     *http.Client
	server     *http.Server
	listener   net.Listener
	closed     bool
	active     map[string]context.CancelFunc
}

// LocalConfig is the only connection material intended for the local client.
type LocalConfig struct {
	BaseURL string
	APIKey  string
}

func New(opts Options) (*Relay, LocalConfig, error) {
	if opts.Model == "" {
		return nil, LocalConfig{}, errors.New("relay model is required")
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, LocalConfig{}, errors.New("could not initialize relay")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, LocalConfig{}, errors.New("could not start relay")
	}
	r := &Relay{model: opts.Model, key: hex.EncodeToString(raw[:]), authorize: opts.Authorize, client: opts.Client, listener: ln, active: make(map[string]context.CancelFunc)}
	if r.client == nil {
		r.client = &http.Client{}
	}
	client := *r.client
	// Never follow redirects with scoped credentials or replay a POST elsewhere.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	r.client = &client
	r.server = &http.Server{Handler: http.HandlerFunc(r.serve)}
	go func() { _ = r.server.Serve(ln) }()
	return r, LocalConfig{BaseURL: "http://" + ln.Addr().String(), APIKey: r.key}, nil
}
func (r *Relay) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for _, cancel := range r.active {
		cancel()
	}
	r.mu.Unlock()
	return r.server.Close()
}
func (r *Relay) serve(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if req.URL.Path != endpoint {
		http.NotFound(w, req)
		return
	}
	if !authorized(req.Header.Get("Authorization"), r.key) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, MaxBodyBytes+1))
	if err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if int64(len(body)) > MaxBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil || obj == nil {
		http.Error(w, "invalid JSON request", 400)
		return
	}
	var model string
	if json.Unmarshal(obj["model"], &model) != nil || model != r.model {
		http.Error(w, "model not allowed", http.StatusForbidden)
		return
	}
	if r.authorize == nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	ctx, cancel := context.WithCancel(req.Context())
	id := uuid.NewString()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		http.Error(w, "relay closed", http.StatusServiceUnavailable)
		return
	}
	r.active[id] = cancel
	r.mu.Unlock()
	defer func() { cancel(); r.mu.Lock(); delete(r.active, id); r.mu.Unlock() }()
	auth, err := r.authorize(ctx, id)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	u, err := url.Parse(auth.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	// No client headers are forwarded. Only the trusted scoped authorizer
	// supplies credentials and gateway control headers.
	for k, vs := range auth.Headers {
		for _, v := range vs {
			out.Header.Add(k, v)
		}
	}
	out.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(out)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if strings.EqualFold(k, "Connection") || strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	f, flush := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, e := resp.Body.Read(buf)
		if n > 0 {
			if _, we := w.Write(buf[:n]); we != nil {
				return
			}
			if flush {
				f.Flush()
			}
		}
		if e != nil {
			if e != io.EOF {
				return
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}
func authorized(got, key string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(got, prefix) {
		return false
	}
	candidate := strings.TrimPrefix(got, prefix)
	return len(candidate) == len(key) && subtle.ConstantTimeCompare([]byte(candidate), []byte(key)) == 1
}
func reserved(k string) bool {
	k = strings.ToLower(k)
	return k == "authorization" || k == "x-api-key" || strings.HasPrefix(k, "x-tresor")
}
