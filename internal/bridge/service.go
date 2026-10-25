// Package bridge serves Tresor's authenticated integration protocol, independently
// of owner sessions. Preparation is deliberately read-only.
package bridge

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"tresorbot/internal/store"
)

const Version = "integration.v1"

type HookExecution struct {
	ID         string `json:"id"`
	Subject    string `json:"subject"`
	Assignment string `json:"assignment"`
	Epoch      int64  `json:"epoch"`
}
type Input struct {
	Routing struct {
		RequestedModel string          `json:"requestedModel"`
		ResolvedModel  string          `json:"resolvedModel"`
		Alias          json.RawMessage `json:"alias"`
		Downstream     struct {
			ID     string `json:"id"`
			Format string `json:"format"`
		} `json:"downstream"`
	} `json:"routing"`
	Context   json.RawMessage `json:"context,omitempty"`
	Execution *HookExecution  `json:"execution"`
	BindingID string          `json:"binding_id"`
	Request   json.RawMessage `json:"request"`
	Body      json.RawMessage `json:"body"`
}
type Options struct {
	Token, BindingID string
	ResolveBinding   func(context.Context) (string, error)
	Store            *store.Store
	// Managed validates registered authority and returns the managed-tools decision.
	Managed      func(Input, store.Bot) (any, error)
	Invoke       func(http.ResponseWriter, *http.Request, Input, store.Bot)
	ValidateChat func(json.RawMessage, string) error
	Tools        func(http.ResponseWriter, *http.Request, json.RawMessage)
}
type Service struct{ o Options }

func New(o Options) (*Service, error) {
	if o.Token == "" || (o.BindingID == "" && o.ResolveBinding == nil) || o.Store == nil {
		return nil, errors.New("integration service requires credential, binding and store")
	}
	if len(o.Token) > 4096 || len(o.BindingID) > 256 || strings.TrimSpace(o.Token) != o.Token || strings.ContainsAny(o.Token+o.BindingID, "\r\n\x00") {
		return nil, errors.New("invalid integration credential or binding")
	}
	return &Service{o}, nil
}
func output(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"version": Version, "payload": payload})
}
func fail(w http.ResponseWriter, status int, code string) {
	output(w, status, map[string]any{"error": map[string]any{"code": code, "message": code, "retryable": status >= 500}})
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fail := fail
	if r.URL.Path == "/integration/invoke" {
		fail = chatFailure
	}
	w.Header().Set("Cache-Control", "no-store")
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare([]byte(token), []byte(s.o.Token)) != 1 {
		fail(w, 401, "integration_unauthorized")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		fail(w, 405, "method_not_allowed")
		return
	}
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		fail(w, 415, "json_required")
		return
	}
	var envelope struct {
		Version string          `json:"version"`
		Payload json.RawMessage `json:"payload"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if d.Decode(&envelope) != nil || envelope.Version != Version || len(envelope.Payload) == 0 || bytes.Equal(envelope.Payload, []byte("null")) {
		fail(w, 400, "invalid_envelope")
		return
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		fail(w, 400, "invalid_envelope")
		return
	}
	if r.URL.Path == "/integration/tools/execute" {
		if s.o.Tools == nil {
			fail(w, 503, "tools_unavailable")
			return
		}
		s.o.Tools(w, r, envelope.Payload)
		return
	}
	if r.URL.Path != "/integration/prepare" && r.URL.Path != "/integration/invoke" {
		fail(w, 404, "not_found")
		return
	}
	var input Input
	payloadDecoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	payloadDecoder.DisallowUnknownFields()
	if payloadDecoder.Decode(&input) != nil || input.BindingID == "" || input.Routing.RequestedModel == "" {
		fail(w, 400, "invalid_integration_input")
		return
	}
	binding := s.o.BindingID
	if s.o.ResolveBinding != nil {
		binding, err = s.o.ResolveBinding(r.Context())
		if err != nil {
			fail(w, 503, "gateway_binding_unavailable")
			return
		}
	}
	if input.BindingID != binding {
		fail(w, 400, "invalid_integration_input")
		return
	}
	bots, err := s.o.Store.ListBots()
	if err != nil {
		fail(w, 503, "lookup_unavailable")
		return
	}
	var bot *store.Bot
	for i := range bots {
		if bots[i].Alias == input.Routing.RequestedModel {
			if bot != nil {
				fail(w, 409, "ambiguous_bot_alias")
				return
			}
			bot = &bots[i]
		}
	}
	if bot == nil {
		if input.Execution != nil || r.URL.Path != "/integration/prepare" {
			fail(w, 404, "bot_not_found")
			return
		}
		output(w, 200, map[string]any{"decision": map[string]string{"kind": "pass"}})
		return
	}
	if input.Routing.Downstream.Format != "openai" {
		fail(w, 409, "unsupported_bot_downstream")
		return
	}
	if !bot.Enabled {
		fail(w, 409, "bot_disabled")
		return
	}
	if input.Execution != nil {
		if r.URL.Path != "/integration/prepare" || s.o.Managed == nil {
			fail(w, 403, "execution_unauthorized")
			return
		}
		decision, err := s.o.Managed(input, *bot)
		if err != nil {
			fail(w, 403, "execution_unauthorized")
			return
		}
		output(w, 200, map[string]any{"decision": decision})
		return
	}
	decodedBody, err := DecodeBody(input.Body)
	if err != nil {
		fail(w, 400, "invalid_chat_body")
		return
	}
	input.Body = decodedBody
	var metadata struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Query  string `json:"query"`
	}
	if json.Unmarshal(input.Request, &metadata) != nil || metadata.Method != "POST" || (metadata.Path != "/v1/chat/completions" && metadata.Path != "/chat/completions") || metadata.Query != "" {
		fail(w, 400, "unsupported_chat_route")
		return
	}
	if s.o.ValidateChat != nil {
		if err := s.o.ValidateChat(input.Body, bot.Alias); err != nil {
			fail(w, 400, "unsupported_chat_request")
			return
		}
	}
	if r.URL.Path == "/integration/prepare" {
		output(w, 200, map[string]any{"decision": map[string]any{"kind": "intercept", "service": "local", "operation": "invoke", "payload": input}})
		return
	}
	if s.o.Invoke == nil {
		fail(w, 503, "invocation_unavailable")
		return
	}
	s.o.Invoke(w, r, input, *bot)
}
