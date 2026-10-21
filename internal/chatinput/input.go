package chatinput

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	MaxMessages       = 100
	MaxTextBytes      = 32 * 1024
	MaxTotalTextBytes = 256 * 1024
	MaxBodyBytes      = 512 * 1024
)

var ErrInvalidInput = errors.New("invalid chat input")

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Input struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

// Parse accepts only bounded, native OpenAI Chat requests containing text messages.
func Parse(raw json.RawMessage, requestedAlias string) (Input, error) {
	var in Input
	if len(raw) == 0 || len(raw) > MaxBodyBytes {
		return in, fmt.Errorf("%w: body size outside limits", ErrInvalidInput)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var wire struct {
		Model    json.RawMessage `json:"model"`
		Messages json.RawMessage `json:"messages"`
		Stream   json.RawMessage `json:"stream"`
	}
	if err := dec.Decode(&wire); err != nil {
		return in, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if err := ensureEOF(dec); err != nil {
		return in, err
	}
	if len(wire.Model) == 0 || len(wire.Messages) == 0 {
		return in, fmt.Errorf("%w: model and messages are required", ErrInvalidInput)
	}
	if err := json.Unmarshal(wire.Model, &in.Model); err != nil || in.Model == "" || in.Model != requestedAlias {
		return Input{}, fmt.Errorf("%w: model must exactly match requested alias", ErrInvalidInput)
	}
	if len(wire.Stream) != 0 {
		if bytes.Equal(wire.Stream, []byte("null")) {
			return Input{}, fmt.Errorf("%w: stream must be boolean", ErrInvalidInput)
		}
		if err := json.Unmarshal(wire.Stream, &in.Stream); err != nil {
			return Input{}, fmt.Errorf("%w: stream must be boolean", ErrInvalidInput)
		}
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(wire.Messages, &messages); err != nil || len(messages) == 0 || len(messages) > MaxMessages {
		return Input{}, fmt.Errorf("%w: messages must contain 1..%d entries", ErrInvalidInput, MaxMessages)
	}
	total := 0
	for i, rawMessage := range messages {
		var m struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		md := json.NewDecoder(bytes.NewReader(rawMessage))
		md.DisallowUnknownFields()
		if err := md.Decode(&m); err != nil {
			return Input{}, fmt.Errorf("%w: message %d: %v", ErrInvalidInput, i, err)
		}
		if err := ensureEOF(md); err != nil {
			return Input{}, err
		}
		if m.Role != "system" && m.Role != "developer" && m.Role != "user" && m.Role != "assistant" {
			return Input{}, fmt.Errorf("%w: unsupported role %q", ErrInvalidInput, m.Role)
		}
		var text string
		if len(m.Content) == 0 || bytes.Equal(m.Content, []byte("null")) || json.Unmarshal(m.Content, &text) != nil {
			return Input{}, fmt.Errorf("%w: message %d content must be plain text", ErrInvalidInput, i)
		}
		if !utf8.ValidString(text) || len(text) > MaxTextBytes {
			return Input{}, fmt.Errorf("%w: message %d text exceeds limits or is invalid UTF-8", ErrInvalidInput, i)
		}
		total += len(text)
		if total > MaxTotalTextBytes {
			return Input{}, fmt.Errorf("%w: total message text exceeds limit", ErrInvalidInput)
		}
		in.Messages = append(in.Messages, Message{Role: m.Role, Content: text})
	}
	return in, nil
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: trailing or invalid JSON", ErrInvalidInput)
	}
	return nil
}

// Prompt renders each message separately, preserving role and boundaries.
func (in Input) Prompt() string {
	var b strings.Builder
	for _, m := range in.Messages {
		fmt.Fprintf(&b, "<|%s|>\n%s\n", m.Role, m.Content)
	}
	return b.String()
}
