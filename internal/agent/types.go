// Package agent defines agent-neutral execution types shared by the Earth
// runtime, the adapter registry and the agent host protocol.
//
// Nothing in this package refers to a specific agent. A concrete adapter (pi,
// ACP, app-server, …) translates its native notation into these types, and the
// rest of Earth speaks only this vocabulary.
package agent

// Progress is a low-volume lifecycle observation about one assignment. It
// deliberately carries no transcript: Earth persists only categories, never
// native text or tool arguments.
type Progress struct {
	Type string
	Text string
	Tool string
}

// Progress event types. A host maps its native events onto these; unknown
// values are ignored by Earth rather than rejected, so an adapter can emit a
// new category without a TresorBot release.
const (
	ProgressTurnStarted = "turn.started"
	ProgressToolCall    = "tool.call"
	ProgressToolResult  = "tool.result"
	ProgressTurnSettled = "turn.settled"
)

// Usage is token accounting for one turn. Pointers are not used: Partial marks
// a report that could not be read exactly, so zero values are never silently
// mistaken for a real measurement.
type Usage struct {
	Partial    bool                  `json:"partial,omitempty"`
	Input      int64                 `json:"input"`
	Output     int64                 `json:"output"`
	CacheRead  int64                 `json:"cache_read"`
	CacheWrite int64                 `json:"cache_write"`
	Total      int64                 `json:"total"`
	Models     map[string]ModelUsage `json:"models,omitempty"`
}

// ModelUsage is per-model accounting within a turn.
type ModelUsage struct {
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	Input      int64  `json:"input"`
	Output     int64  `json:"output"`
	CacheRead  int64  `json:"cache_read"`
	CacheWrite int64  `json:"cache_write"`
	Total      int64  `json:"total"`
}

// Stop reasons. Stop is the only value that counts as a successful turn.
const (
	StopStop       = "stop"
	StopAborted    = "aborted"
	StopError      = "error"
	StopUsageLimit = "usage_limit"
	StopMaxTurns   = "max_turns"
)

// Result is the outcome of one assignment's agent turn.
//
// Completed is set only when the agent reported a clean stop and the session
// state was verified. Cancelled is set only when termination of descendants was
// proven; an unproven cancellation stays false so the caller can treat the
// outcome as uncertain instead of claiming success.
type Result struct {
	Text          string   `json:"text"`
	SessionRef    string   `json:"session_ref,omitempty"`
	StopReason    string   `json:"stop_reason,omitempty"`
	Error         string   `json:"error,omitempty"`
	Completed     bool     `json:"completed"`
	Cancelled     bool     `json:"cancelled"`
	ProcessExited bool     `json:"process_exited"`
	Evidence      []string `json:"evidence,omitempty"`
	Usage         *Usage   `json:"usage,omitempty"`
	Model         string   `json:"model,omitempty"`
	Provider      string   `json:"provider,omitempty"`
}
