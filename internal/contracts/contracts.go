package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// TaskState is the durable state of an admitted outer task.
type TaskState string

const (
	Queued         TaskState = "queued"
	Dispatching    TaskState = "dispatching"
	Running        TaskState = "running"
	WaitingChild   TaskState = "waiting_child"
	Cancelling     TaskState = "cancelling"
	NeedsAttention TaskState = "needs_attention"
	Completed      TaskState = "completed"
	Failed         TaskState = "failed"
	Cancelled      TaskState = "cancelled"
)

func (s TaskState) Valid() bool {
	switch s {
	case Queued, Dispatching, Running, WaitingChild, Cancelling, NeedsAttention, Completed, Failed, Cancelled:
		return true
	default:
		return false
	}
}

func (s TaskState) IsTerminal() bool { return s == Completed || s == Failed || s == Cancelled }

// ValidTransition reports whether a durable task-state transition is permitted.
func ValidTransition(from, to TaskState) bool {
	if !from.Valid() || !to.Valid() || from == to || from.IsTerminal() {
		return false
	}
	switch from {
	case Queued:
		return to == Dispatching || to == Cancelling || to == Cancelled
	case Dispatching:
		return to == Running || to == Failed || to == Cancelling || to == NeedsAttention
	case Running:
		return to == WaitingChild || to == Completed || to == Failed || to == Cancelling || to == NeedsAttention
	case WaitingChild:
		return to == Running || to == Cancelling || to == NeedsAttention
	case Cancelling:
		return to == Cancelled || to == NeedsAttention || to == Completed || to == Failed
	case NeedsAttention:
		return to == Completed || to == Failed || to == Cancelled
	default:
		return false
	}
}

func NewID() string { return uuid.NewString() }
func ValidID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id.String() == strings.ToLower(s)
}

const RunnerVersion = 1
const IntegrationVersion = "integration.v1"

// Error is the stable domain-error payload shared by the wire protocols.
type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e Error) Validate() error {
	if strings.TrimSpace(e.Code) == "" || strings.TrimSpace(e.Message) == "" {
		return errors.New("error code and message are required")
	}
	return nil
}

// RunnerRecord is the common identity and version header for Earth/Sun records.
type RunnerRecord struct {
	Version      int    `json:"version"`
	Type         string `json:"type"`
	ID           string `json:"id"`
	RunnerID     string `json:"runner_id"`
	AssignmentID string `json:"assignment_id,omitempty"`
	Epoch        int64  `json:"epoch,omitempty"`
}

func (r RunnerRecord) Validate() error {
	if r.Version != RunnerVersion {
		return fmt.Errorf("unsupported runner version %d", r.Version)
	}
	if r.Type == "" || !ValidID(r.ID) || !ValidID(r.RunnerID) {
		return errors.New("runner record requires type and UUID message/runner IDs")
	}
	if r.AssignmentID != "" && !ValidID(r.AssignmentID) {
		return errors.New("invalid assignment ID")
	}
	if r.AssignmentID != "" && r.Epoch <= 0 {
		return errors.New("assignment epoch must be positive")
	}
	return nil
}

type Assign struct {
	RunnerRecord
	TaskID      string          `json:"task_id"`
	ExecutionID string          `json:"execution_id"`
	Input       json.RawMessage `json:"input"`
}

func (m Assign) Validate() error {
	if err := m.RunnerRecord.Validate(); err != nil {
		return err
	}
	if m.Type != "assign" || !ValidID(m.AssignmentID) || !ValidID(m.TaskID) || !ValidID(m.ExecutionID) || m.Epoch <= 0 || len(m.Input) == 0 || !json.Valid(m.Input) {
		return errors.New("invalid assign record")
	}
	return nil
}

type Accepted struct {
	RunnerRecord
	Accepted bool `json:"accepted"`
}

func (m Accepted) Validate() error {
	if err := m.RunnerRecord.Validate(); err != nil {
		return err
	}
	if m.Type != "accepted" || m.AssignmentID == "" {
		return errors.New("invalid accepted record")
	}
	return nil
}

type Event struct {
	RunnerRecord
	Sequence uint64          `json:"sequence"`
	EventID  string          `json:"event_id"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
}

func (m Event) Validate() error {
	if err := m.RunnerRecord.Validate(); err != nil {
		return err
	}
	if m.Type != "event" || m.AssignmentID == "" || m.Sequence == 0 || !ValidID(m.EventID) || m.Kind == "" || len(m.Payload) == 0 || !json.Valid(m.Payload) {
		return errors.New("invalid event record")
	}
	return nil
}

type Terminal struct {
	RunnerRecord
	Sequence uint64          `json:"sequence,omitempty"`
	State    TaskState       `json:"state"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    *Error          `json:"error,omitempty"`
}

func (m Terminal) Validate() error {
	if err := m.RunnerRecord.Validate(); err != nil {
		return err
	}
	if m.Type != "terminal" || m.AssignmentID == "" || !(m.State == Completed || m.State == Failed || m.State == Cancelled) {
		return errors.New("invalid terminal record")
	}
	if m.State == Failed {
		if m.Error == nil {
			return errors.New("failed terminal requires error")
		}
		return m.Error.Validate()
	}
	if m.Error != nil {
		return errors.New("successful terminal cannot contain error")
	}
	if len(m.Result) > 0 && !json.Valid(m.Result) {
		return errors.New("invalid terminal result")
	}
	return nil
}

// IntegrationEnvelope wraps only service prepare/tool JSON; invoke is raw chat JSON/SSE.
type IntegrationEnvelope struct {
	Version string          `json:"version"`
	Payload json.RawMessage `json:"payload"`
}

func (e IntegrationEnvelope) Validate() error {
	if e.Version != IntegrationVersion || len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return errors.New("invalid integration envelope")
	}
	return nil
}

// ExecutionHook is the lower-case representation supplied to V3 hooks.
type ExecutionHook struct {
	ID         string `json:"id"`
	Subject    string `json:"subject"`
	Assignment string `json:"assignment"`
	Epoch      int64  `json:"epoch"`
}

// ManagedExecution preserves Tresor's capitalized Go-JSON execution object.
type ManagedExecution struct {
	ExecutionID  string `json:"ExecutionID"`
	Service      string `json:"Service"`
	Subject      string `json:"Subject"`
	AliasID      string `json:"AliasID"`
	AssignmentID string `json:"AssignmentID"`
	Epoch        int64  `json:"Epoch"`
	InferenceID  string `json:"InferenceID"`
}
type ManagedToolRequest struct {
	Execution   ManagedExecution `json:"execution"`
	OperationID string           `json:"operation_id"`
	Tool        string           `json:"tool"`
	Arguments   json.RawMessage  `json:"arguments"`
	Reconcile   bool             `json:"reconcile"`
}

func validOperationID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (r ManagedToolRequest) Validate() error {
	// The execution carries identity and a continuation key only; there is no
	// capability to verify. The alias identifies the bot and the tool call is
	// otherwise fully specified by its name, arguments and operation id.
	if r.Execution.AliasID == "" || r.Execution.ExecutionID == "" || !validOperationID(r.OperationID) || r.Tool == "" || len(r.Arguments) == 0 || !json.Valid(r.Arguments) {
		return errors.New("invalid managed tool request")
	}
	return nil
}

type ExecutionRegistrationRequest struct {
	ExecutionID  string `json:"execution_id,omitempty"`
	Subject      string `json:"subject"`
	Alias        string `json:"alias"`
	AssignmentID string `json:"assignment_id"`
	Epoch        int64  `json:"epoch"`
}
type ExecutionRegistrationResponse struct {
	ExecutionID string `json:"execution_id"`
	Capability  string `json:"capability"`
}
