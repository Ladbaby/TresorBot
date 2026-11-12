package contracts

import (
	"encoding/json"
	"errors"
	"strings"
)

// MaxRunnerRecordBytes bounds both control frames and durable outbox records.
const MaxRunnerRecordBytes = 2 << 20

// AssignmentEvidence is journal evidence, not a request to restart execution.
type AssignmentEvidence struct {
	AssignmentID string `json:"assignment_id"`
	Epoch        int64  `json:"epoch"`
	State        string `json:"state"`
	LastSequence uint64 `json:"last_sequence"`
	Acknowledged uint64 `json:"acknowledged"`
}

// Control carries connection-level records and assignment acknowledgements.
// Assignment payloads and reports retain their separately validated types.
type Control struct {
	RunnerRecord
	Assignments      []AssignmentEvidence `json:"assignments,omitempty"`
	Sequence         uint64               `json:"sequence,omitempty"`
	EventID          string               `json:"event_id,omitempty"`
	DefaultWorkspace string               `json:"default_workspace,omitempty"`
	Platform         string               `json:"platform,omitempty"`
	// Adapters lists the agent adapters this Earth can run, so Sun can offer
	// only adapters that will actually work on the runner a bot is placed on.
	Adapters []RunnerAdapter `json:"adapters,omitempty"`
}

// RunnerAdapter is one adapter as advertised by a runner at hello.
type RunnerAdapter struct {
	Name        string `json:"name"`
	Version     string `json:"version,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Healthy     bool   `json:"healthy"`
}

func (c Control) Validate() error {
	if err := c.RunnerRecord.Validate(); err != nil {
		return err
	}
	switch c.Type {
	case "hello":
		if c.AssignmentID != "" || c.Epoch != 0 || len(c.Assignments) != 0 || c.Sequence != 0 || c.EventID != "" || len(c.DefaultWorkspace) > 4096 || len(c.Platform) > 256 || strings.ContainsRune(c.DefaultWorkspace, 0) || strings.ContainsRune(c.Platform, 0) {
			return errors.New("invalid hello metadata")
		}
		if len(c.Adapters) > 64 {
			return errors.New("too many adapters advertised")
		}
		seen := map[string]bool{}
		for _, a := range c.Adapters {
			if !validAdapterName(a.Name) || seen[a.Name] || len(a.Version) > 64 || len(a.DisplayName) > 128 {
				return errors.New("invalid adapter advertisement")
			}
			seen[a.Name] = true
		}
	case "heartbeat", "ready":
		if c.AssignmentID != "" || c.Epoch != 0 || len(c.Assignments) != 0 || c.Sequence != 0 || c.EventID != "" || c.DefaultWorkspace != "" || c.Platform != "" || len(c.Adapters) != 0 {
			return errors.New("invalid connection record")
		}
	case "reconcile":
		if c.AssignmentID != "" || c.Epoch != 0 || c.Sequence != 0 || c.EventID != "" || len(c.Assignments) > 10000 || c.DefaultWorkspace != "" || c.Platform != "" || len(c.Adapters) != 0 {
			return errors.New("invalid reconciliation")
		}
		seen := map[string]bool{}
		for _, a := range c.Assignments {
			if !ValidID(a.AssignmentID) || a.Epoch <= 0 || a.Acknowledged > a.LastSequence || seen[a.AssignmentID] {
				return errors.New("invalid assignment evidence")
			}
			seen[a.AssignmentID] = true
			switch a.State {
			case "accepted", "launching", "running", "uncertain", "terminal":
			default:
				return errors.New("invalid journal state")
			}
		}
	case "cancel", "event_ack", "terminal_ack", "retire":
		if c.Type == "retire" {
			if c.AssignmentID == "" || c.Sequence != 0 || c.EventID != "" || len(c.Assignments) != 0 || c.DefaultWorkspace != "" || c.Platform != "" {
				return errors.New("invalid retirement record")
			}
			break
		}
		if c.AssignmentID == "" || len(c.Assignments) != 0 || c.DefaultWorkspace != "" || c.Platform != "" {
			return errors.New("missing assignment identity")
		}
		if c.Type == "cancel" && (c.Sequence != 0 || c.EventID != "") {
			return errors.New("invalid cancellation record")
		}
		if c.Type != "cancel" && (c.Sequence == 0 || !ValidID(c.EventID)) {
			return errors.New("invalid report acknowledgement")
		}
	default:
		return errors.New("unsupported control record")
	}
	return nil
}

// DecodeRunnerRecord bounds and validates a single WebSocket JSON record.
func DecodeRunnerRecord(data []byte) (any, error) {
	if len(data) == 0 || len(data) > MaxRunnerRecordBytes {
		return nil, errors.New("runner record exceeds bounds")
	}
	var h RunnerRecord
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	var v interface{ Validate() error }
	switch h.Type {
	case "assign":
		v = &Assign{}
	case "accepted":
		v = &Accepted{}
	case "event":
		v = &Event{}
	case "terminal":
		v = &Terminal{}
	default:
		v = &Control{}
	}
	if err := json.Unmarshal(data, v); err != nil {
		return nil, err
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if t, ok := v.(*Terminal); ok && t.Sequence == 0 {
		return nil, errors.New("terminal requires durable report sequence")
	}
	return v, nil
}

// validAdapterName mirrors the store's slug rule. It is duplicated here so the
// wire contract can reject a malformed advertisement without importing storage.
func validAdapterName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case (r == '/' || r == '-' || r == '_') && i > 0 && i < len(name)-1:
		default:
			return false
		}
	}
	return true
}
