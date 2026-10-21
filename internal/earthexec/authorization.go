package earthexec

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"tresorbot/internal/contracts"
	"tresorbot/internal/relay"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

// AssignmentAuthorization uses only Sun-delivered, assignment-scoped authority.
// It never consumes a broad proxy key or the gateway's control credential.
func AssignmentAuthorization(_ context.Context, a contracts.Assign, inferenceID string) (relay.Authorization, error) {
	var input runnertransport.ExecutionInput
	if json.Unmarshal(a.Input, &input) != nil || input.Gateway == nil {
		return relay.Authorization{}, ErrAuthorizationUnavailable
	}
	g := input.Gateway
	var bot store.Bot
	if json.Unmarshal(input.Snapshot, &bot) != nil || g.AssignmentID != a.AssignmentID || g.ExecutionID != a.ExecutionID || g.Epoch != a.Epoch || g.Alias != bot.Alias || g.Capability == "" || strings.ContainsAny(g.Capability, "\r\n\x00") {
		return relay.Authorization{}, ErrInvalidInput
	}
	if !contracts.ValidID(g.ExecutionID) || !contracts.ValidID(g.AssignmentID) || g.Epoch <= 0 {
		return relay.Authorization{}, ErrInvalidInput
	}
	u, err := url.Parse(g.URL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return relay.Authorization{}, ErrInvalidInput
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return relay.Authorization{}, ErrInvalidInput
		}
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/chat/completions"
	return relay.Authorization{URL: u.String(), Headers: http.Header{"X-Tresor-Execution-Token": {g.Capability}, "X-Tresor-Execution-Epoch": {strconv.FormatInt(a.Epoch, 10)}, "X-Tresor-Inference-ID": {inferenceID}}}, nil
}
