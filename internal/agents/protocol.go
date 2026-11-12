// Package agents defines the wire protocol between Earth and an agent host.
//
// Earth never links an agent. It runs a small resident host program that
// discovers adapters from a directory and multiplexes many agent sessions in a
// single process, because one process per turn costs tens of megabytes each and
// cannot reach a wide Worker fleet.
//
// The protocol is line-delimited JSON-RPC 2.0 over the host's stdin/stdout. It
// is deliberately bidirectional: the host asks Earth for things it must not do
// itself, so adapters never receive runner credentials.
package agents

import (
	"encoding/json"
	"fmt"
)

// ProtocolVersion is the contract revision. It is exchanged in the hello
// handshake so a stale host fails loudly instead of behaving oddly.
const ProtocolVersion = 1

// MaxRecordBytes bounds a single protocol line in both directions. It matches
// the historical adapter record cap so an oversized agent record is rejected at
// the same point it always was.
const MaxRecordBytes = 8 << 20

// Methods Earth sends to the host.
const (
	MethodHello        = "host/hello"
	MethodReload       = "host/reload"
	MethodSessionOpen  = "session/open"
	MethodPrompt       = "session/prompt"
	MethodCancel       = "session/cancel"
	MethodSessionClose = "session/close"
	MethodShutdown     = "host/shutdown"
)

// Notifications the host sends to Earth.
const (
	NotifyEvent           = "session/event"
	NotifySettled         = "session/settled"
	NotifyLog             = "session/log"
	NotifyAdaptersChanged = "host/adapters_changed"
)

// Error codes Earth distinguishes. Anything else is a plain failure.
const (
	CodeOverloaded     = "overloaded"
	CodeUnknownAdapter = "unknown_adapter"
	CodeUnknownSession = "unknown_session"
	CodeBadRequest     = "bad_request"
	CodeProtocol       = "protocol_mismatch"
)

// Error is the JSON-RPC error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// Reason carries the short machine-readable class so Earth can react to
	// overload differently from a bad request without parsing prose.
	Reason string `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

// AdapterInfo is what a host reports about one discovered adapter.
type AdapterInfo struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	DisplayName  string   `json:"display_name,omitempty"`
	Description  string   `json:"description,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	// Healthy is false when the manifest or entry module failed to load. A bad
	// adapter must never take the host down with it.
	Healthy bool `json:"healthy"`
	// Error explains an unhealthy adapter.
	Error string `json:"error,omitempty"`
	// Settings lists the adapter's declared configuration fields.
	Settings []SettingDescriptor `json:"settings,omitempty"`
}

// SettingType enumerates the value shapes Earth can validate and render.
type SettingType string

const (
	SettingString     SettingType = "string"
	SettingBool       SettingType = "bool"
	SettingInt        SettingType = "int"
	SettingPath       SettingType = "path"
	SettingStringList SettingType = "string_list"
	SettingEnum       SettingType = "enum"
)

// SettingDescriptor declares one adapter setting. Earth validates against this
// without knowing what the adapter does with the value.
type SettingDescriptor struct {
	Key         string      `json:"key"`
	Type        SettingType `json:"type"`
	Required    bool        `json:"required,omitempty"`
	Description string      `json:"description,omitempty"`
	Default     any         `json:"default,omitempty"`
	Values      []string    `json:"values,omitempty"`
}

// HelloResult is the handshake reply. Earth refuses to dispatch to a host that
// reports a different protocol version.
type HelloResult struct {
	Protocol    int           `json:"protocol"`
	HostVersion string        `json:"host_version"`
	Adapters    []AdapterInfo `json:"adapters"`
	// MaxSessions is the host's own ceiling; Earth never exceeds it.
	MaxSessions int `json:"max_sessions"`
}

// ReloadParams asks the host to re-read one adapter, or all when Name is empty.
type ReloadParams struct {
	Name string `json:"name,omitempty"`
}

// ReloadResult reports the adapters after reloading.
type ReloadResult struct {
	Adapters []AdapterInfo `json:"adapters"`
}

// HistoryMessage is one prior conversation turn handed to a fresh session.
type HistoryMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// OpenParams describes one assignment's agent session. Earth resolves every
// value before calling, so the adapter never reads environment or config.
type OpenParams struct {
	SessionID      string         `json:"session_id"`
	Adapter        string         `json:"adapter"`
	AssignmentID   string         `json:"assignment_id"`
	Epoch          int64          `json:"epoch"`
	ConversationID string         `json:"conversation_id"`
	TaskID         string         `json:"task_id"`
	Workspace      string         `json:"workspace"`
	StateDir       string         `json:"state_dir"`
	Settings       map[string]any `json:"settings,omitempty"`
	Instructions   string         `json:"instructions,omitempty"`
	History        []HistoryMessage `json:"history,omitempty"`
	ModelHint      string         `json:"model_hint,omitempty"`
	// ToolURL and ToolToken address Earth's loopback tool proxy for this
	// assignment. The runner credential never appears here.
	ToolURL   string `json:"tool_url,omitempty"`
	ToolToken string `json:"tool_token,omitempty"`
}

// OpenResult reports what the adapter found. SessionRef is the adapter's own
// resume handle, opaque to Earth and persisted so a later turn can continue.
type OpenResult struct {
	SessionID   string   `json:"session_id"`
	SessionRef  string   `json:"session_ref,omitempty"`
	Diagnostics []string `json:"diagnostics,omitempty"`
}

// PromptParams delivers one user turn.
type PromptParams struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}

// PromptResult reports whether the agent accepted the turn. Acceptance is not
// completion: the turn ends with a settled notification.
type PromptResult struct {
	Accepted bool `json:"accepted"`
}

// CancelParams asks one session to stop. Earth escalates to killing the host if
// the host does not acknowledge within its cancel timeout.
type CancelParams struct {
	SessionID string `json:"session_id"`
	Reason    string `json:"reason,omitempty"`
}

// CancelResult reports whether the cancel was delivered to a live turn.
type CancelResult struct {
	Cancelled bool `json:"cancelled"`
}

// CloseParams releases a session after its turn settled.
type CloseParams struct {
	SessionID string `json:"session_id"`
}

// Event is a canonical agent event. Type is an open string so an adapter can
// emit a new category without an Earth release; Earth only journals the
// lifecycle types it knows.
type Event struct {
	Type    string          `json:"type"`
	Text    string          `json:"text,omitempty"`
	CallID  string          `json:"call_id,omitempty"`
	Name    string          `json:"name,omitempty"`
	Args    json.RawMessage `json:"args,omitempty"`
	OK      *bool           `json:"ok,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Usage   *Usage          `json:"usage,omitempty"`
	Message string          `json:"message,omitempty"`
}

// Canonical event types. These mirror agent.Progress plus richer detail.
const (
	EventTurnStarted      = "turn.started"
	EventAssistantDelta   = "assistant.delta"
	EventAssistantMessage = "assistant.message"
	EventThinkingDelta    = "thinking.delta"
	EventToolCall         = "tool.call"
	EventToolResult       = "tool.result"
	EventUsage            = "usage"
	EventStatus           = "status"
	EventError            = "error"
)

// Usage is canonical token accounting, identical in shape to agent.Usage.
type Usage struct {
	Partial    bool                  `json:"partial,omitempty"`
	Input      int64                 `json:"input"`
	Output     int64                 `json:"output"`
	CacheRead  int64                 `json:"cache_read"`
	CacheWrite int64                 `json:"cache_write"`
	Total      int64                 `json:"total"`
	Models     map[string]ModelUsage `json:"models,omitempty"`
}

// ModelUsage is per-model accounting.
type ModelUsage struct {
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	Input      int64  `json:"input"`
	Output     int64  `json:"output"`
	CacheRead  int64  `json:"cache_read"`
	CacheWrite int64  `json:"cache_write"`
	Total      int64  `json:"total"`
}

// EventParams wraps one event for a session.
type EventParams struct {
	SessionID string `json:"session_id"`
	Event     Event  `json:"event"`
}

// SettledParams ends a turn. StopReason uses the canonical vocabulary; an
// adapter that cannot classify a failure reports "error" and fills Error.
type SettledParams struct {
	SessionID  string   `json:"session_id"`
	StopReason string   `json:"stop_reason"`
	Text       string   `json:"text,omitempty"`
	Usage      *Usage   `json:"usage,omitempty"`
	Model      string   `json:"model,omitempty"`
	Provider   string   `json:"provider,omitempty"`
	SessionRef string   `json:"session_ref,omitempty"`
	Error      string   `json:"error,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
	// ResetAt is an RFC3339 instant when StopReason is usage_limit.
	ResetAt string `json:"reset_at,omitempty"`
}

// LogParams is adapter diagnostics for Earth's log.
type LogParams struct {
	SessionID string `json:"session_id,omitempty"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

// AdaptersChangedParams announces a hot reload result.
type AdaptersChangedParams struct {
	Adapters []AdapterInfo `json:"adapters"`
}

// Stop reasons, matching agent.Stop*.
const (
	StopStop       = "stop"
	StopAborted    = "aborted"
	StopError      = "error"
	StopUsageLimit = "usage_limit"
	StopMaxTurns   = "max_turns"
)
