package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"tresorbot/internal/config"
	"tresorbot/internal/contracts"
	"tresorbot/internal/store"
)

var ErrUncertain = errors.New("gateway registration uncertain; explicit recovery required")

type Config struct{ URL, BindingID, ControlToken, ServiceToken string }
type Registry struct {
	c      Config
	s      *store.Store
	client *http.Client
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
	return &Registry{c, s, &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (r *Registry) Ensure(ctx context.Context, a store.Assignment, b store.Bot) (store.GatewayRegistration, error) {
	g, fresh, e := r.s.ReserveGatewayExecution(store.GatewayRegistration{ExecutionID: a.ExecutionID, AssignmentID: a.ID, TaskID: a.TaskID, BotID: b.ID, Alias: b.Alias, BindingID: r.c.BindingID, URL: strings.TrimRight(r.c.URL, "/"), Epoch: a.Epoch})
	if e != nil {
		return g, e
	}
	if !fresh {
		if g.State == "active" {
			return r.s.ValidateGatewayExecution(g.ExecutionID, a.ID, b.ID, b.Alias, r.c.BindingID, a.Epoch)
		}
		return g, ErrUncertain
	}
	body, _ := json.Marshal(contracts.ExecutionRegistrationRequest{ExecutionID: a.ExecutionID, Subject: b.ID, Alias: b.Alias, AssignmentID: a.ID, Epoch: a.Epoch})
	req, e := r.request(ctx, "POST", "/api/integrations/executions", body)
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
	return r.s.ValidateGatewayExecution(g.ExecutionID, a.ID, b.ID, b.Alias, r.c.BindingID, a.Epoch)
}
func (r *Registry) request(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.c.URL, "/")+path, bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Authorization", "Bearer "+r.c.ControlToken)
	req.Header.Set("X-Tresor-Integration-Service", r.c.BindingID)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
func (r *Registry) Revoke(ctx context.Context, g store.GatewayRegistration) error {
	if g.BindingID != r.c.BindingID || g.URL != strings.TrimRight(r.c.URL, "/") {
		return errors.New("gateway registration configuration changed")
	}
	if e := r.s.MarkGatewayRevoking(g.ExecutionID); e != nil {
		return e
	}
	req, e := r.request(ctx, "DELETE", "/api/integrations/executions/"+g.ExecutionID, nil)
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
