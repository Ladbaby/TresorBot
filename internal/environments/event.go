// Package environments is Sun's bidirectional extension system: one versioned
// event envelope, one intake pipeline, and adapters loaded by a resident host.
//
// The output side (internal/agents) answers "how does work get done". This side
// answers "how does work arrive" — an IM message, a PR event, an email, a
// scheduled tick, a webhook. Both are extension systems with the same shape: a
// JSON manifest, an ESM module, digest-pinned installation, and a host process
// that ships inside the binary rather than in the catalogue repository.
//
// The split of responsibility is deliberate and load-bearing:
//
//   - An environment adapter knows its upstream protocol — sockets, OAuth, HMAC
//     verification, cursors in whatever units the upstream uses. It emits
//     normalized events and nothing else.
//   - Sun knows bots, dedupe, admission, queueing and delivery. It never learns
//     what IMAP or Slack are.
//
// Adapters therefore never perceive the supervision topology. They see
// start/reconfigure/stop, handleWebhook, deliver, and two context helpers. Moving
// from one shared host to one process per instance is a Sun-side change only.
package environments

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Version pins the envelope's wire shape, the way contracts.IntegrationVersion
// pins the gateway's. An adapter reporting any other version is refused rather
// than allowed to emit a shape Sun would half-understand.
const Version = "ingress.v1"

// Render budgets. A rendered task must stay well inside store.validJSON's 1 MiB
// cap, and must never be empty: the Earth executor rejects a whitespace-only
// task, so an attachment-only event needs synthesized text rather than blank.
const (
	MaxTitleBytes  = 512
	MaxTextBytes   = 16 * 1024
	MaxOriginBytes = 4 * 1024
	// MaxOriginFieldBytes bounds each Origin field on its own. MaxOriginBytes caps
	// the footer rendered into the text, but the raw Origin struct is embedded in
	// the rendered JSON too, so an unbounded field bypasses that cap entirely.
	MaxOriginFieldBytes = 1024
	// MaxAttachmentBytes bounds the string fields of one attachment. Attachments
	// are locators, so a very long one is a misbehaving adapter, not a real payload.
	MaxAttachmentBytes = 2048
	// MaxRenderedBytes is the hard ceiling on a rendered task, kept under
	// store.validJSON's 1 MiB limit with room for the JSON envelope. Render clamps
	// to it unconditionally, so no future field can violate the invariant.
	MaxRenderedBytes = 256 * 1024
	// MaxAttachments bounds the reference list; attachments carry locators, never
	// payloads, so a large count is a sign of a misbehaving adapter.
	MaxAttachments = 32
)

// Event is one normalized inbound occurrence. Field names are the wire contract
// with adapters; changing them needs a Version bump.
//
// Kind is an open string on purpose: a new source must not require a Sun
// release, exactly as agent.Event.Type is open ("unknown values are ignored
// rather than rejected"). Sun reads Kind only for logging and for dashboard
// grouping — behaviour comes from the instance's declared bot, not from Kind.
type Event struct {
	Version    string    `json:"version"`
	Instance   string    `json:"instance"`
	Kind       string    `json:"kind"`
	EventID    string    `json:"event_id"`
	OccurredAt time.Time `json:"occurred_at"`
	// Target names the recipient bot, by id or by alias. Empty means "whatever bot
	// this instance is configured for", which is the common case.
	//
	// It is a CONVENIENCE, not an authorization. An adapter is untrusted code
	// writing a field, so a value here may never widen what the instance can reach:
	// intake resolves the target and then requires a real contact edge from the
	// instance's configured bot, which is the same gate delegation uses. Without
	// that, one installed environment could address every bot in Sun.
	Target  string  `json:"target,omitempty"`
	Content Content `json:"content"`
	Origin  Origin  `json:"origin"`
	ReplyTo *Route  `json:"reply_to,omitempty"`
}

// Content is what the bot will read. Text is the message body; Title is a
// one-line summary the renderer puts first and the dashboard uses for nothing
// else (taskSummary reads the rendered text).
type Content struct {
	Title       string       `json:"title,omitempty"`
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

// Attachment is a reference to something outside the envelope. It is a locator,
// never a payload: a 1 MiB task body cannot carry a file, and an adapter that
// needs the bot to see bytes should put a path or URL here and let the agent
// fetch it in its workspace.
type Attachment struct {
	Name        string `json:"name,omitempty"`
	URL         string `json:"url,omitempty"`
	MediaType   string `json:"media_type,omitempty"`
	Description string `json:"description,omitempty"`
}

// Origin says where the event came from, for the bot's benefit. Every field is
// display-only: Sun never routes or authorizes on it, because a value an adapter
// supplies must not be able to select a bot.
type Origin struct {
	Source  string `json:"source,omitempty"`
	Sender  string `json:"sender,omitempty"`
	Channel string `json:"channel,omitempty"`
	Thread  string `json:"thread,omitempty"`
	URL     string `json:"url,omitempty"`
}

// Route tells the reply pump where a result belongs. It is opaque to Sun beyond
// being handed back to the same instance that produced the event.
type Route struct {
	Channel      string `json:"channel,omitempty"`
	Conversation string `json:"conversation,omitempty"`
	Thread       string `json:"thread,omitempty"`
	// ReplyKey is upstream's own identifier for the thing being replied to (a
	// message id, a PR number, a mail Message-ID). The adapter needs it; Sun
	// only stores and returns it.
	ReplyKey string `json:"reply_key,omitempty"`
}

// Validate checks the envelope before any work happens. Errors are wrapped with
// ErrInvalidEvent so intake can classify them as permanently unrecoverable.
func (e Event) Validate() error {
	if e.Version != Version {
		return fmt.Errorf("%w: version %q, want %q", ErrInvalidEvent, e.Version, Version)
	}
	if strings.TrimSpace(e.Instance) == "" || len(e.Instance) > 64 {
		return fmt.Errorf("%w: instance is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.EventID) == "" || len(e.EventID) > 512 || strings.ContainsAny(e.EventID, "\r\n\x00") {
		return fmt.Errorf("%w: event_id is required and must be a single bounded line", ErrInvalidEvent)
	}
	if strings.TrimSpace(e.Kind) == "" || len(e.Kind) > 64 {
		return fmt.Errorf("%w: kind is required", ErrInvalidEvent)
	}
	if len(e.Target) > 256 || strings.ContainsAny(e.Target, "\r\n\x00") {
		return fmt.Errorf("%w: target must be a single bounded line", ErrInvalidEvent)
	}
	if len(e.Content.Attachments) > MaxAttachments {
		return fmt.Errorf("%w: at most %d attachments", ErrInvalidEvent, MaxAttachments)
	}
	// Attachments and Origin are embedded in the rendered task verbatim, so their
	// size has to be bounded here as well as in the text footer. An adapter that
	// sends a multi-megabyte locator would otherwise produce a task the store
	// rejects, which reads downstream as a Sun bug rather than a bad event.
	for _, a := range e.Content.Attachments {
		if len(a.Name) > MaxAttachmentBytes || len(a.URL) > MaxAttachmentBytes ||
			len(a.MediaType) > MaxAttachmentBytes || len(a.Description) > MaxAttachmentBytes {
			return fmt.Errorf("%w: an attachment field exceeds %d bytes", ErrInvalidEvent, MaxAttachmentBytes)
		}
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"source", e.Origin.Source},
		{"sender", e.Origin.Sender},
		{"channel", e.Origin.Channel},
		{"thread", e.Origin.Thread},
		{"url", e.Origin.URL},
	} {
		if len(field.value) > MaxOriginFieldBytes {
			return fmt.Errorf("%w: origin %s exceeds %d bytes", ErrInvalidEvent, field.name, MaxOriginFieldBytes)
		}
	}
	if !utf8.ValidString(e.Content.Text) || !utf8.ValidString(e.Content.Title) {
		return fmt.Errorf("%w: content must be valid UTF-8", ErrInvalidEvent)
	}
	return nil
}

// Rendered is the task input Sun stores. It is the SAME shape Earth already
// reads — {"text": ...} — plus provenance the executor ignores, so the input
// system needs no Earth release and no change to earthexec.
type Rendered struct {
	Text        string          `json:"text"`
	Origin      Origin          `json:"origin,omitempty"`
	Environment string          `json:"environment,omitempty"`
	Kind        string          `json:"kind,omitempty"`
	Event       string          `json:"event_id,omitempty"`
	Detail      json.RawMessage `json:"attachments,omitempty"`
}

// Render turns an event into a bounded, deterministic task input.
//
// Determinism is a correctness requirement, not tidiness. Admission dedupes on a
// content hash, so a re-delivered event that rendered even one byte differently
// would be refused as ErrConflict and retried forever by a well-behaved
// upstream. Everything used here therefore comes from the envelope alone —
// never time.Now(), never map iteration order.
func Render(e Event) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	var b strings.Builder
	if title := strings.TrimSpace(e.Content.Title); title != "" {
		b.WriteString(truncate(title, MaxTitleBytes))
		b.WriteString("\n\n")
	}
	body := strings.TrimSpace(e.Content.Text)
	if body == "" {
		// An attachment-only or reaction event still has to produce something a
		// bot can act on; an empty task is rejected downstream.
		body = synthesizedText(e)
	}
	b.WriteString(truncate(body, MaxTextBytes))
	if footer := originFooter(e); footer != "" {
		b.WriteString("\n\n---\n")
		b.WriteString(footer)
	}
	out := Rendered{Text: b.String(), Origin: e.Origin, Environment: e.Instance, Kind: e.Kind, Event: e.EventID}
	if len(e.Content.Attachments) > 0 {
		// An error here cannot happen: Attachment is a plain struct of strings.
		if raw, err := json.Marshal(e.Content.Attachments); err == nil {
			out.Detail = raw
		}
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	// The clamp is unconditional rather than derived from the budgets above, so a
	// field added later cannot quietly break the store's size limit. Falling back to
	// the text alone keeps the task admissible: losing provenance is far better than
	// rejecting the event after the adapter was told it was accepted.
	if len(encoded) > MaxRenderedBytes {
		out.Origin = Origin{}
		out.Detail = nil
		out.Text = truncate(out.Text, MaxRenderedBytes/2)
		if encoded, err = json.Marshal(out); err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

// synthesizedText describes an event with no body. It must not consult the clock
// or any non-deterministic source.
func synthesizedText(e Event) string {
	var parts []string
	verb := strings.ReplaceAll(e.Kind, ".", " ")
	verb = strings.TrimSpace(verb)
	if verb == "" {
		verb = "event"
	}
	if e.Origin.Source != "" {
		parts = append(parts, "A "+verb+" arrived from "+e.Origin.Source+".")
	} else {
		parts = append(parts, "A "+verb+" arrived.")
	}
	if len(e.Content.Attachments) > 0 {
		names := make([]string, 0, len(e.Content.Attachments))
		for _, a := range e.Content.Attachments {
			name := a.Name
			if name == "" {
				name = a.URL
			}
			if name == "" {
				name = a.Description
			}
			if name != "" {
				names = append(names, name)
			}
		}
		switch {
		case len(names) == 1:
			parts = append(parts, "It carries an attachment: "+names[0])
		case len(names) > 1:
			parts = append(parts, "It carries "+strconv.Itoa(len(names))+" attachments: "+strings.Join(names, ", "))
		}
	}
	return strings.Join(parts, " ")
}

// originFooter appends the provenance lines a bot needs to act sensibly: who
// sent it, where it came from, and a link if one exists.
func originFooter(e Event) string {
	var lines []string
	add := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		value = strings.Join(strings.Fields(value), " ")
		lines = append(lines, label+": "+value)
	}
	add("From", e.Origin.Sender)
	add("Source", e.Origin.Source)
	add("Where", e.Origin.Channel)
	add("Thread", e.Origin.Thread)
	add("Link", e.Origin.URL)
	if e.OccurredAt.IsZero() {
		add("Kind", e.Kind)
	} else {
		add("When", e.OccurredAt.UTC().Format(time.RFC3339))
		add("Kind", e.Kind)
	}
	if len(lines) == 0 {
		return ""
	}
	return truncate(strings.Join(lines, "\n"), MaxOriginBytes)
}

// truncate cuts s to at most max bytes without splitting a rune, and says so
// when it had to cut. The marker is deterministic, so a retried event still
// hashes identically.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := s[:max]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut + "\n[truncated]"
}
