package environments

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"tresorbot/internal/store"
)

// Ingress tuning.
const (
	// MaxWebhookBody bounds a forwarded body. Sun does not know what an upstream
	// sends, so the cap is generous but finite: a limit that a legitimate payload
	// can reach is a denial of service waiting to happen.
	MaxWebhookBody = 256 * 1024
	// webhookTimeout bounds one adapter's handling of a forwarded request, so a
	// hung adapter cannot hold an HTTP connection open indefinitely.
	webhookTimeout = 30 * time.Second
)

// WebhookForwarder forwards a raw request to an adapter instance.
type WebhookForwarder interface {
	Webhook(ctx context.Context, p WebhookParams) (WebhookResult, error)
}

// Ingress serves POST /ingress/{instance}: one route for every webhook-capable
// input, so adding a source does not add a Sun route.
//
// Authentication is per-instance and happens here; the upstream's own signature
// is verified by the adapter, because only the adapter knows the scheme. That
// split matters: Sun cannot validate a GitHub HMAC or a Stripe signature, and an
// adapter must not be trusted to decide whether a request reached it at all.
type Ingress struct {
	Forwarder WebhookForwarder
	// Instances reports one configured instance and its bearer token.
	Instances func(ctx context.Context, name string) (IngressInstance, error)
	// Logf records rejected requests.
	Logf func(format string, args ...any)
}

// IngressInstance is what the ingress route needs to authorize and route a call.
type IngressInstance struct {
	Name        string
	Environment string
	Enabled     bool
	// Webhook reports whether the adapter declared the capability. It is resolved
	// from the loaded manifest, not guessed from the input's name.
	Webhook bool
	// Token is the per-instance bearer credential. It is compared in constant
	// time; comparing it at all is what stops any caller reaching any adapter.
	Token string
}

func (i *Ingress) logf(format string, args ...any) {
	if i.Logf != nil {
		i.Logf(format, args...)
	}
}

// NewIngressToken returns a fresh per-instance credential.
func NewIngressToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (i *Ingress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeIngressError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/ingress/")
	// A nested path would let a crafted request probe for another instance; the
	// instance name is exactly one segment.
	if name == "" || strings.Contains(name, "/") {
		writeIngressError(w, http.StatusNotFound, "not_found")
		return
	}
	if i.Forwarder == nil || i.Instances == nil {
		writeIngressError(w, http.StatusServiceUnavailable, "ingress_unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), webhookTimeout)
	defer cancel()
	inst, err := i.Instances(ctx, name)
	if err != nil {
		// An unknown instance is a 404 rather than a 401, so a misconfigured
		// upstream URL is obvious in its own logs. It reveals only that the name
		// is not configured, which is already public in a URL.
		writeIngressError(w, http.StatusNotFound, "unknown_instance")
		return
	}
	if !inst.Enabled {
		writeIngressError(w, http.StatusConflict, "instance_disabled")
		return
	}
	if !inst.Webhook {
		// The adapter never declared the capability, so there is nothing to
		// forward to. Answering 404 keeps a non-webhook instance indistinguishable
		// from an unconfigured one, which is what an operator expects from a route
		// that was never meant to exist.
		writeIngressError(w, http.StatusNotFound, "unknown_instance")
		return
	}
	if !authorized(r, inst.Token) {
		i.logf("ingress %q rejected an unauthorized request", name)
		writeIngressError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxWebhookBody+1))
	if err != nil {
		writeIngressError(w, http.StatusBadRequest, "body_unreadable")
		return
	}
	if len(body) > MaxWebhookBody {
		writeIngressError(w, http.StatusRequestEntityTooLarge, "body_too_large")
		return
	}
	headers := forwardedHeaders(r.Header)
	result, err := i.Forwarder.Webhook(ctx, WebhookParams{
		Instance:   name,
		Method:     r.Method,
		Path:       r.URL.Path,
		Query:      r.URL.RawQuery,
		Headers:    headers,
		BodyBase64: base64.StdEncoding.EncodeToString(body),
	})
	if err != nil {
		i.logf("ingress %q forwarding failed: %v", name, err)
		writeIngressError(w, http.StatusBadGateway, "forward_failed")
		return
	}
	if !result.Handled {
		// The adapter refused the payload — typically its own signature check.
		status := result.Status
		if status < 400 || status > 599 {
			status = http.StatusBadRequest
		}
		code := result.Code
		if code == "" {
			code = "rejected"
		}
		writeIngressError(w, status, code)
		return
	}
	status := result.Status
	if status < 200 || status > 299 {
		// 202 is the honest answer: the adapter has taken ownership, and the work
		// happens asynchronously as an event Sun has admitted or will admit.
		status = http.StatusAccepted
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// forwardedHeaders copies the caller's headers, dropping Sun's own credential.
//
// Dropping Authorization is essential rather than tidy: the ingress bearer token
// is Sun's, and an adapter verifying an upstream HMAC over the Authorization
// header would verify against a credential the upstream never sent. Everything
// else is forwarded verbatim, because a signature scheme may cover any header and
// Sun has no way to know which.
func forwardedHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for name, values := range h {
		switch http.CanonicalHeaderKey(name) {
		case "Authorization", "X-Api-Key":
			continue
		case "Content-Length", "Host", "Connection", "Transfer-Encoding":
			continue
		}
		copied := make([]string, len(values))
		copy(copied, values)
		out[http.CanonicalHeaderKey(name)] = copied
	}
	return out
}

// authorized compares the bearer credential in constant time.
func authorized(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(token)) == 1
}

// writeIngressError answers with a stable code and no detail. An ingress response
// is read by an external service, so it must not leak whether a token was wrong
// because the instance is missing or because the value differed.
func writeIngressError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(map[string]any{"ok": false, "error": code})
	_, _ = w.Write(body)
}

// IngressHandler adapts a manager to the ingress interface, resolving an instance
// from the database and the loaded catalogue.
func (m *Manager) IngressInstance(ctx context.Context, name string) (IngressInstance, error) {
	inst, err := m.Store.GetEnvironmentInstance(name)
	if err != nil {
		return IngressInstance{}, err
	}
	inputs, err := m.Host.Environments(ctx)
	if err != nil {
		return IngressInstance{}, err
	}
	var capability bool
	for _, in := range inputs {
		if in.Name == inst.Environment {
			for _, c := range in.Capabilities {
				if c == CapabilityWebhook {
					capability = true
				}
			}
			break
		}
	}
	return IngressInstance{
		Name:        inst.Name,
		Environment: inst.Environment,
		Enabled:     inst.Enabled,
		Webhook:     capability,
		Token:       m.instanceToken(inst),
	}, nil
}

// IngressToken reveals an instance's ingress credential so the owner can paste it
// into the upstream's configuration.
//
// It is derived on demand rather than stored, so revealing it costs nothing but a
// hash and leaves no secret at rest.
func (m *Manager) IngressToken(ctx context.Context, name string) (string, error) {
	if m == nil {
		return "", errors.New("inputs are disabled")
	}
	if m.TokenSecret == "" {
		return "", errors.New("no ingress credential: set admin_password or integration_token")
	}
	inst, err := m.Store.GetEnvironmentInstance(name)
	if err != nil {
		return "", err
	}
	return m.instanceToken(inst), nil
}

// instanceToken derives an instance's ingress credential from Sun's own secret
// material and the instance name.
//
// Derivation rather than storage means a token never sits in the database, cannot
// be leaked by a database dump, and cannot drift from the instance it belongs to.
// It is stable across restarts as long as the deployment secret is.
func (m *Manager) instanceToken(inst store.EnvironmentInstance) string {
	if m.TokenSecret == "" {
		return ""
	}
	var ok bool
	token, ok := m.tokens.Load(inst.Name)
	if ok {
		return token.(string)
	}
	derived := deriveToken(m.TokenSecret, inst.Name)
	m.tokens.Store(inst.Name, derived)
	return derived
}
