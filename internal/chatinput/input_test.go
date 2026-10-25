package chatinput

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePreservesTextRolesAndStream(t *testing.T) {
	raw := json.RawMessage(`{"model":"alias-x","stream":true,"messages":[{"role":"system","content":"rules"},{"role":"user","content":"hello"},{"role":"assistant","content":"answer"}]}`)
	got, err := Parse(raw, "alias-x")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Stream || len(got.Messages) != 3 {
		t.Fatalf("unexpected parsed input: %#v", got)
	}
	want := "<|system|>\nrules\n<|user|>\nhello\n<|assistant|>\nanswer\n"
	if got.Prompt() != want {
		t.Fatalf("Prompt() = %q, want %q", got.Prompt(), want)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Input
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || roundTrip.Prompt() != want {
		t.Fatalf("serialization failed: %v", err)
	}
}

func TestParseAcceptsStandardStreamUsagePreference(t *testing.T) {
	for _, options := range []string{`{"include_usage":true}`, `{"include_usage":false}`, `{}`, `null`} {
		raw := json.RawMessage(`{"model":"a","stream":true,"stream_options":` + options + `,"messages":[{"role":"user","content":"test message"}]}`)
		got, err := Parse(raw, "a")
		if err != nil {
			t.Fatal(options, err)
		}
		if !got.Stream || len(got.Messages) != 1 || got.Messages[0].Content != "test message" {
			t.Fatal(got)
		}
		serialized, _ := json.Marshal(got)
		if strings.Contains(string(serialized), "stream_options") {
			t.Fatal("client usage preference forwarded into task")
		}
	}
	for _, options := range []string{`{"include_usage":"true"}`, `{"include_usage":null}`, `{"include_usage":1}`, `{"unexpected":true}`, `[]`, `true`} {
		raw := json.RawMessage(`{"model":"a","stream_options":` + options + `,"messages":[{"role":"user","content":"x"}]}`)
		if _, err := Parse(raw, "a"); err == nil {
			t.Fatal("accepted invalid options", options)
		}
	}
}

func TestParseRejectsUnsupportedSemantics(t *testing.T) {
	cases := []struct{ name, body string }{
		{"null stream", `{"model":"a","stream":null,"messages":[{"role":"user","content":"x"}]}`},
		{"null content", `{"model":"a","messages":[{"role":"user","content":null}]}`},
		{"malformed trailing", `{"model":"a","messages":[{"role":"user","content":"x"}]} garbage`},
		{"wrong model", `{"model":"other","messages":[{"role":"user","content":"x"}]}`},
		{"tools", `{"model":"a","tools":[],"messages":[{"role":"user","content":"x"}]}`},
		{"unknown message field", `{"model":"a","messages":[{"role":"assistant","tool_calls":[],"content":"x"}]}`},
		{"image content", `{"model":"a","messages":[{"role":"user","content":[{"type":"image_url"}]}]}`},
		{"unsupported role", `{"model":"a","messages":[{"role":"tool","content":"x"}]}`},
		{"bad stream", `{"model":"a","stream":"yes","messages":[{"role":"user","content":"x"}]}`},
		{"trailing value", `{"model":"a","messages":[{"role":"user","content":"x"}]} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse(json.RawMessage(tc.body), "a"); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestParseBoundsMessagesAndText(t *testing.T) {
	tooLong := strings.Repeat("x", MaxTextBytes+1)
	body, _ := json.Marshal(map[string]any{"model": "a", "messages": []any{map[string]string{"role": "user", "content": tooLong}}})
	if _, err := Parse(body, "a"); err == nil {
		t.Fatal("expected text bound rejection")
	}
	many := make([]any, MaxMessages+1)
	for i := range many {
		many[i] = map[string]string{"role": "user", "content": "x"}
	}
	body, _ = json.Marshal(map[string]any{"model": "a", "messages": many})
	if _, err := Parse(body, "a"); err == nil {
		t.Fatal("expected message bound rejection")
	}
}
