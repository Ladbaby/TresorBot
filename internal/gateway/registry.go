package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/store"
)

var ErrUncertain = errors.New("gateway registration uncertain; explicit recovery required")

type Config struct{ URL, BindingID, ControlToken, ServiceToken string }
type Registry struct {
	c         Config
	s         *store.Store
	client    *http.Client
	mu        sync.Mutex
	resolving *bindingResolution
}
type bindingResolution struct {
	done    chan struct{}
	binding string
	err     error
}

// ResolveBinding verifies current configuration rather than trusting a configured or caller-supplied ID.
func (r *Registry) ResolveBinding(ctx context.Context) (string, error) {
	r.mu.Lock()
	if pending := r.resolving; pending != nil {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-pending.done:
			return pending.binding, pending.err
		}
	}
	pending := &bindingResolution{done: make(chan struct{})}
	r.resolving = pending
	r.mu.Unlock()
	pending.binding, pending.err = r.resolveBinding(ctx)
	if pending.err != nil {
		log.Printf("gateway binding discovery: %v", pending.err)
	}
	r.mu.Lock()
	r.resolving = nil
	close(pending.done)
	r.mu.Unlock()
	return pending.binding, pending.err
}
func (r *Registry) resolveBinding(ctx context.Context) (string, error) {
	body, _ := json.Marshal(map[string]string{"service_token": r.c.ServiceToken})
	req, err := r.request(ctx, "POST", "/api/integrations/bindings/resolve", body, "")
	if err != nil {
		return "", err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "", errors.New("cannot reach Tresor for binding discovery; check gateway_url and service availability")
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
	case 404, 405:
		return "", errors.New("Tresor does not support binding discovery; update Tresor")
	case 401, 403:
		return "", errors.New("no authorized bridge binding; check both service and control tokens and enable the Tresor rule")
	case 409:
		return "", errors.New("ambiguous bridge bindings; use a separate service/control credential pair for each bridge")
	default:
		return "", fmt.Errorf("Tresor binding discovery unavailable (HTTP %d)", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	var result struct {
		BindingID string `json:"binding_id"`
	}
	if err != nil || len(raw) > 4096 || json.Unmarshal(raw, &result) != nil {
		return "", errors.New("invalid Tresor binding discovery response")
	}
	decoded, err := hex.DecodeString(result.BindingID)
	if err != nil || len(decoded) != 32 {
		return "", errors.New("invalid Tresor binding identity")
	}
	return result.BindingID, nil
}

func New(c Config, s *store.Store) (*Registry, error) {
	if s == nil {
		return nil, errors.New("missing execution store")
	}
	if e := (config.Config{GatewayURL: c.URL, GatewayBindingID: c.BindingID, GatewayControlToken: c.ControlToken, IntegrationToken: c.ServiceToken}).ValidateIntegration(); e != nil {
		return nil, e
	}
	if c.URL == "" {
		return nil, errors.New("gateway configuration required")
	}
	if e := s.InitGatewaySchema(); e != nil {
		return nil, e
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	return &Registry{c: c, s: s, client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (r *Registry) Ensure(ctx context.Context, a store.Assignment, b store.Bot) (store.GatewayRegistration, error) {
	binding, err := r.ResolveBinding(ctx)
	if err != nil {
		return store.GatewayRegistration{}, err
	}
	task, err := r.s.GetTask(a.TaskID)
	if err != nil {
		return store.GatewayRegistration{}, err
	}
	var admitted struct {
		BindingID string `json:"binding_id"`
	}
	if json.Unmarshal(task.Input, &admitted) != nil || (admitted.BindingID != "" && admitted.BindingID != binding) {
		return store.GatewayRegistration{}, errors.New("admitted task binding no longer matches Tresor configuration")
	}
	g, fresh, e := r.s.ReserveGatewayExecution(store.GatewayRegistration{ExecutionID: a.ExecutionID, AssignmentID: a.ID, TaskID: a.TaskID, BotID: b.ID, Alias: b.Alias, BindingID: binding, URL: strings.TrimRight(r.c.URL, "/"), Epoch: a.Epoch})
	if e != nil {
		return g, e
	}
	if !fresh {
		if g.State == "active" {
			return r.s.ValidateGatewayExecution(g.ExecutionID, a.ID, b.ID, b.Alias, binding, a.Epoch)
		}
		return g, ErrUncertain
	}
	body, _ := json.Marshal(contracts.ExecutionRegistrationRequest{ExecutionID: a.ExecutionID, Subject: b.ID, Alias: b.Alias, AssignmentID: a.ID, Epoch: a.Epoch})
	req, e := r.request(ctx, "POST", "/api/integrations/executions", body, binding)
	if e != nil {
		return g, e
	}
	resp, e := r.client.Do(req)
	if e != nil {
		return g, ErrUncertain
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if e != nil || len(raw) > 4096 || resp.StatusCode != 201 {
		return g, ErrUncertain
	}
	var result contracts.ExecutionRegistrationResponse
	if json.Unmarshal(raw, &result) != nil || result.ExecutionID != a.ExecutionID || len(result.Capability) != 64 || strings.ContainsAny(result.Capability, "\r\n\x00") {
		return g, ErrUncertain
	}
	if e = r.s.ActivateGatewayExecution(g.ExecutionID, result.Capability); e != nil {
		return g, ErrUncertain
	}
	return r.s.ValidateGatewayExecution(g.ExecutionID, a.ID, b.ID, b.Alias, binding, a.Epoch)
}
func (r *Registry) request(ctx context.Context, method, path string, body []byte, binding string) (*http.Request, error) {
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.c.URL, "/")+path, bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Bearer "+r.c.ControlToken)
	req.Header.Set("X-Tresor-Integration-Service", binding)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
func (r *Registry) Revoke(ctx context.Context, g store.GatewayRegistration) error {
	if g.BindingID == "" || g.URL != strings.TrimRight(r.c.URL, "/") {
		return errors.New("gateway registration configuration changed")
	}
	if e := r.s.MarkGatewayRevoking(g.ExecutionID); e != nil {
		return e
	}
	req, e := r.request(ctx, "DELETE", "/api/integrations/executions/"+g.ExecutionID, nil, g.BindingID)
	if e != nil {
		return e
	}
	resp, e := r.client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		return errors.New("gateway revocation unavailable")
	}
	return r.s.MarkGatewayRevoked(g.ExecutionID)
}
func (r *Registry) Cleanup(ctx context.Context) error {
	gs, e := r.s.GatewayCleanupCandidates()
	if e != nil {
		return e
	}
	var errs []error
	for _, g := range gs {
		if e := r.Revoke(ctx, g); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}
