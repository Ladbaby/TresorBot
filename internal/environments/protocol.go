package environments

import (
	"encoding/json"
	"errors"
	"fmt"

	"tresorbot/internal/settingspec"
)

// ProtocolVersion is the contract revision exchanged in the hello handshake, so
// a stale host fails loudly instead of behaving oddly. It follows
// agents.ProtocolVersion in shape; the two numbers are independent because the
// two protocols evolve independently.
const ProtocolVersion = 1

// MaxRecordBytes bounds a single protocol line in both directions. It matches
// the agent protocol's cap, and it is deliberately far above any legitimate
// event: an event is a few kilobytes, so a record near this limit is a bug.
const MaxRecordBytes = 8 << 20

// Methods Sun sends to the input host.
const (
	MethodHello   = "environment/hello"
	MethodReload  = "environment/reload"
	MethodConfig  = "environment/config"
	MethodStart   = "environment/start"
	MethodStop    = "environment/stop"
	MethodWebhook = "environment/webhook"
	MethodDeliver = "environment/deliver"
	// MethodToolsList asks an adapter which tools it exposes. It is separate from
	// definitions because the tool catalogue is fetched when the model searches,
	// not when the session starts: the MCP bridge caches /definitions for the whole
	// process, so a tool enabled mid-session would otherwise never appear.
	MethodToolsList = "environment/tools_list"
	// MethodToolsCall runs one tool. Sun sends its own id so a retried call can be
	// recognised by the adapter rather than run twice.
	MethodToolsCall = "environment/tools_call"
	MethodShutdown  = "environment/shutdown"
)

// Notifications the host sends to Sun.
const (
	NotifyEvent         = "environment/event"
	NotifyHealth        = "environment/health"
	NotifyLog           = "environment/log"
	NotifyDeliveryReset = "environment/delivery_result"
	NotifyChanged       = "environment/environments_changed"
)

// Error reasons Sun distinguishes. Anything else is a plain failure.
const (
	CodeUnknownEnvironment = "unknown_environment"
	CodeUnknownInst        = "unknown_instance"
	CodeBadRequest         = "bad_request"
	CodeProtocol           = "protocol_mismatch"
	CodeOverloaded         = "overloaded"
	CodeNotCapable         = "not_capable"
	CodeUnknownTool        = "unknown_tool"
	CodeStartFailed        = "start_failed"
)

// Errors intake classifies its own failures with. Everything else is treated as
// transient, because silently dropping a real event is worse than retrying one.
var (
	// ErrInvalidEvent marks an envelope Sun will never accept, however often it
	// is retried. The adapter is told to acknowledge and move on.
	ErrInvalidEvent = errors.New("invalid ingress event")
	// ErrUnknownTarget marks a bot that does not exist or is not enabled.
	ErrUnknownTarget = errors.New("unknown or disabled target bot")
	// ErrUnknownInstance marks an event from an instance that is not configured.
	ErrUnknownInstance = errors.New("unknown input instance")
	// ErrBackpressure marks a bot whose queue is deeper than the configured cap.
	// It is retryable: the queue drains on its own.
	ErrBackpressure = errors.New("target bot is at its pending cap")
)

// Ack is Sun's answer to `environment/event`, delivered as the JSON-RPC response.
//
// This is the load-bearing part of the contract: an adapter may advance its
// cursor only after ack.Accepted is true, or when Retryable is false (a
// permanent rejection it should skip past). A retryable rejection means "the
// event is valid, admission is temporarily impossible" and must be retried,
// because the alternative is losing work the upstream will not resend.
type Ack struct {
	Accepted bool `json:"accepted"`
	// TaskID is set when the event was admitted to an existing or new task.
	TaskID string `json:"task_id,omitempty"`
	// Duplicate reports that this event was already admitted. The adapter sees
	// Accepted: true so it advances, which is what makes redelivery safe.
	Duplicate bool `json:"duplicate,omitempty"`
	// Retryable distinguishes "try again shortly" from "never".
	Retryable bool `json:"retryable,omitempty"`
	// Reason is a stable machine-readable code, never prose for a human to parse.
	Reason string `json:"reason,omitempty"`
}

// Error implements error so intake can return an Ack and an error that agree.
func (a Ack) Error() string {
	if a.Accepted {
		return ""
	}
	return nonEmpty(a.Reason, "rejected")
}

func nonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// Reject builds a permanent rejection ack from a code.
func Reject(code string) Ack { return Ack{Reason: code} }

// Retry builds a retryable rejection ack from a code.
func Retry(code string) Ack { return Ack{Retryable: true, Reason: code} }

// Accept builds an acceptance ack, optionally for an existing duplicate task.
func Accept(taskID string, duplicate bool) Ack {
	return Ack{Accepted: true, TaskID: taskID, Duplicate: duplicate}
}

// Error is the JSON-RPC error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// Reason carries the short machine-readable class so Sun can react to a
	// specific failure without parsing prose.
	Reason string `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

// EnvironmentInfo is what the host reports about one discovered input adapter.
type EnvironmentInfo struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	DisplayName  string   `json:"display_name,omitempty"`
	Description  string   `json:"description,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	// Healthy is false when the manifest or entry module failed to load. One
	// broken adapter must never take the host down with it.
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
	// Settings lists the adapter's declared configuration fields. It is the same
	// descriptor type agent adapters use, so a setting cannot mean two things
	// depending on which extension kind declares it.
	Settings []settingspec.Descriptor `json:"settings,omitempty"`
}

// HelloResult is the handshake reply. Sun refuses a host that reports a
// different protocol version.
type HelloResult struct {
	Protocol     int               `json:"protocol"`
	HostVersion  string            `json:"host_version"`
	Environments []EnvironmentInfo `json:"environments"`
	MaxInstances int               `json:"max_instances"`
}

// ReloadParams asks the host to re-read one adapter, or all when Name is empty.
type ReloadParams struct {
	Name string `json:"name,omitempty"`
}

// ReloadResult reports the adapters after reloading.
type ReloadResult struct {
	Environments []EnvironmentInfo `json:"environments"`
}

// ConfigParams hands one instance's resolved configuration to the host.
//
// Settings have already been validated against the declared descriptors and had
// environment references resolved by Sun, so the host never reads the process
// environment and a secret exists only in this frame.
type ConfigParams struct {
	Instance    string         `json:"instance"`
	Environment string         `json:"environment"`
	Settings    map[string]any `json:"settings"`
	// Revision lets a stale configuration frame be ignored after the owner edits
	// an instance mid-restart.
	Revision int64 `json:"revision"`
}

// InstanceRef identifies one configured instance for start/stop.
type InstanceRef struct {
	Instance string `json:"instance"`
}

// WebhookParams carries a raw inbound HTTP request to the adapter that owns the
// route. Sun authenticates the caller first and forwards the body untouched, so
// an upstream signature stays verifiable.
type WebhookParams struct {
	Instance string              `json:"instance"`
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Query    string              `json:"query,omitempty"`
	Headers  map[string][]string `json:"headers,omitempty"`
	// BodyBase64 carries the raw bytes, because a signature is computed over
	// bytes and not over a decoded string.
	BodyBase64 string `json:"body_base64"`
}

// ToolsListParams asks one instance which tools it exposes.
//
// Discovery is per instance rather than per adapter because two instances of the
// same adapter are two independent surfaces: a cron instance switched off must
// not contribute tools.
type ToolsListParams struct {
	Instance string `json:"instance"`
}

// ToolDefinition is one tool an adapter offers.
//
// Parameters is carried as raw JSON on purpose: Sun validates it against a
// restricted schema subset (see validateToolSchema) before it can reach a
// provider. Decoding it into a typed struct here would let an unsupported
// construct through unexamined.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	// SideEffect declares the consequence of a call, so Sun can apply the right
	// authorization. "message" marks a tool that speaks into a conversation, which
	// is gated; anything else is ordinary work. An unlisted label is refused at
	// discovery, so a misspelling cannot silently skip a gate.
	SideEffect string `json:"side_effect,omitempty"`
}

// ToolsListResult is the adapter's catalogue for one instance.
type ToolsListResult struct {
	Tools []ToolDefinition `json:"tools"`
}

// ToolsCallParams runs one tool on one instance.
//
// CallID is Sun's, not the model's: the model never sees it. It exists so a
// retried call can be recognised as the same call rather than run twice, which
// matters for tools that act on the world.
type ToolsCallParams struct {
	Instance  string          `json:"instance"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	CallID    string          `json:"call_id"`
}

// ToolsCallResult is a tool's answer. Result is deliberately untyped: a tool may
// return a string or a structure, and Sun bounds its size rather than its shape.
type ToolsCallResult struct {
	Result json.RawMessage `json:"result,omitempty"`
}

// WebhookResult reports whether the adapter took ownership of the request. A
// false Handled means Sun answers 4xx with Code, which is what lets an adapter
// reject its own bad signatures without Sun knowing the scheme.
type WebhookResult struct {
	Handled bool   `json:"handled"`
	Status  int    `json:"status,omitempty"`
	Code    string `json:"code,omitempty"`
}

// DeliveryParams asks an adapter to send one result upstream.
//
// DeliveryKey is stable per task, and Attempt is monotonic, so an adapter that
// posted successfully and then crashed before acknowledging can recognise its
// own work instead of posting twice. Sun is at-least-once by design.
type DeliveryParams struct {
	Instance    string  `json:"instance"`
	DeliveryKey string  `json:"delivery_key"`
	Route       Route   `json:"route"`
	Outcome     Outcome `json:"outcome"`
	Attempt     int     `json:"attempt"`
	TaskID      string  `json:"task_id"`
}

// Outcome is what became of the work. Only Completed carries model output; a
// failed or uncertain task reports a stable code so an adapter cannot leak an
// agent's reasoning into an upstream system.
type Outcome struct {
	State string `json:"state"`
	// Text is the agent's final answer, present only for Completed.
	Text string `json:"text,omitempty"`
	// Code is a stable failure reason for every non-completed state.
	Code string `json:"code,omitempty"`
	// Uncertain reports that side effects may have happened. An adapter should
	// phrase its upstream note so a human checks, rather than claiming failure.
	Uncertain bool `json:"uncertain,omitempty"`
}

// DeliveryResult is the adapter's answer to input/deliver. Delivery is
// at-least-once, so a permanent failure is reported rather than thrown: Sun
// records it and stops retrying instead of looping forever.
type DeliveryResult struct {
	Delivered bool   `json:"delivered"`
	Retryable bool   `json:"retryable,omitempty"`
	Code      string `json:"code,omitempty"`
}

// HealthParams reports one adapter's runtime state, so the dashboard can show a
// socket that dropped without the owner reading a log.
type HealthParams struct {
	Instance string `json:"instance"`
	Healthy  bool   `json:"healthy"`
	Message  string `json:"message,omitempty"`
}

// LogParams is adapter diagnostics for Sun's log.
type LogParams struct {
	Instance string `json:"instance,omitempty"`
	Level    string `json:"level"`
	Message  string `json:"message"`
}

// EventParams is one emitted event, wrapped so the host can attribute it without
// trusting the payload's own instance field.
type EventParams struct {
	Instance string          `json:"instance"`
	Event    json.RawMessage `json:"event"`
}
