package earthexec

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"testing"
	"tresorbot/internal/contracts"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

func TestAssignmentAuthorizationFencesScope(t *testing.T) {
	id := uuid.NewString()
	execution := uuid.NewString()
	a := contracts.Assign{RunnerRecord: contracts.RunnerRecord{Version: 1, Type: "assign", ID: uuid.NewString(), RunnerID: uuid.NewString(), AssignmentID: id, Epoch: 3}, TaskID: uuid.NewString(), ExecutionID: execution}
	snapshot, _ := json.Marshal(store.Bot{Alias: "bot/personal"})
	g := runnertransport.GatewayScope{URL: "http://127.0.0.1:11510", Capability: "scoped-only", ExecutionID: execution, AssignmentID: id, Epoch: 3, Alias: "bot/personal"}
	input := runnertransport.ExecutionInput{Snapshot: snapshot, Gateway: &g}
	a.Input, _ = json.Marshal(input)
	auth, err := AssignmentAuthorization(context.Background(), a, "infer-1")
	if err != nil {
		t.Fatal(err)
	}
	if auth.URL != "http://127.0.0.1:11510/v1/chat/completions" || auth.Headers.Get("X-Tresor-Execution-Token") != "scoped-only" || auth.Headers.Get("Authorization") != "" {
		t.Fatalf("unexpected auth: %+v", auth)
	}
	for _, mutate := range []func(){func() { g.Epoch++ }, func() { g.Epoch = 3; g.Alias = "wrong" }, func() { g.Alias = "bot/personal"; g.URL = "http://remote.example" }, func() { g.URL = "http://127.0.0.1:11510"; g.ExecutionID = uuid.NewString() }, func() { g.ExecutionID = execution; g.AssignmentID = uuid.NewString() }, func() { g.AssignmentID = id; g.Capability = "bad\r\nheader" }} {
		mutate()
		a.Input, _ = json.Marshal(input)
		if _, err := AssignmentAuthorization(context.Background(), a, "infer-1"); err == nil {
			t.Fatal("accepted invalid scope")
		}
	}
}
