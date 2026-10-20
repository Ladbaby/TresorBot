package contracts

import (
	"encoding/json"
	"errors"
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
	Assignments []AssignmentEvidence `json:"assignments,omitempty"`
	Sequence    uint64               `json:"sequence,omitempty"`
	EventID     string               `json:"event_id,omitempty"`
}

func (c Control) Validate() error {
	if err := c.RunnerRecord.Validate(); err != nil {
		return err
	}
	switch c.Type {
	case "hello", "heartbeat":
		if c.AssignmentID != "" || c.Epoch != 0 || len(c.Assignments) != 0 || c.Sequence != 0 || c.EventID != "" {
			return errors.New("invalid connection record")
		}
	case "reconcile":
		if c.AssignmentID != "" || c.Epoch != 0 || c.Sequence != 0 || c.EventID != "" || len(c.Assignments) > 1000 {
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
	case "cancel", "event_ack", "terminal_ack":
		if c.AssignmentID == "" || len(c.Assignments) != 0 {
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
