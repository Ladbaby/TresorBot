package bottools

import (
	"encoding/json"
	"strings"
)

// MaxToolResultBytes bounds one tool's result before it enters a model's context.
//
// Nothing in the path bounded this previously: the MCP bridge's resultText passed
// whatever an adapter returned straight through, so a single large listing could
// consume a turn's whole budget. The cap lives in Go rather than in the bridge
// because the bridge is JavaScript running on Earth and an environment adapter is
// third-party code — the ceiling must not depend on either behaving.
//
// 16 KiB is chosen to be large enough for a useful listing and small enough that
// several tool calls cannot exhaust a context on their own.
const MaxToolResultBytes = 16 * 1024

// truncationMarker describes an omission in terms the model can act on.
//
// The byte count is included on purpose: a model that knows 400 KB were dropped
// will narrow its request, whereas one shown a bare ellipsis usually assumes it
// has seen everything.
const truncationMarker = "\n\n[truncated: %d more bytes were omitted. Request a narrower range or fewer fields.]"

// BoundToolResult truncates a tool result to MaxToolResultBytes, appending an
// explicit marker naming how much was dropped.
//
// Truncation is on a rune boundary so a cut cannot produce invalid UTF-8, which
// some providers reject outright.
func BoundToolResult(raw []byte) string {
	if len(raw) <= MaxToolResultBytes {
		// A JSON string result is unwrapped so the model reads text, not an escaped
		// literal. Objects stay encoded: their structure may be the point.
		return unwrapStringResult(raw)
	}
	keep := MaxToolResultBytes
	text := unwrapStringResult(raw)
	if len(text) <= MaxToolResultBytes {
		return text
	}
	cut := keep
	for cut > 0 && !utf8Start(text[cut]) {
		cut--
	}
	if cut == 0 {
		// Pathological input with no rune boundary inside the budget: keep nothing
		// rather than emit invalid UTF-8.
		return marker(len(text))
	}
	return strings.TrimRight(text[:cut], "\n") + marker(len(text)-cut)
}

// marker renders the omission notice.
func marker(omitted int) string {
	digits := "0"
	if omitted > 0 {
		digits = itoa64(int64(omitted))
	}
	return strings.Replace(truncationMarker, "%d", digits, 1)
}

// unwrapStringResult returns a JSON string's contents, or the raw bytes when the
// result is not a JSON string.
func unwrapStringResult(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if raw[0] == '"' && json.Unmarshal(raw, &text) == nil {
		return text
	}
	return string(raw)
}

// utf8Start reports whether b begins a rune (i.e. is not a continuation byte).
func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

// itoa64 formats a non-negative int64 without importing strconv for one call.
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
