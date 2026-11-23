package environments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"tresorbot/internal/store"
)

// Outbound chat authorization.
//
// A chat environment gives a model the ability to send messages to real people
// from the operator's own account. That is a different authority from every other
// tool here: a bad memory write is recoverable and visible, a message to a
// stranger is neither. So outbound sends pass a single gate, and the gate is
// Sun's, not the adapter's — an adapter is untrusted code and could otherwise
// decide for itself who it speaks to.

// ErrConversationNotAllowed marks a send to a place this instance may not speak.
var ErrConversationNotAllowed = errors.New("conversation not allowed")

// ConversationGate decides whether an instance may send into a conversation.
//
// Kept as an interface so the rule lives in one place and can be exercised
// without a database.
type ConversationGate interface {
	ConversationAllowed(instance, conversation string) (bool, error)
}

// CheckOutbound reports whether an outbound tool call may proceed.
//
// The conversation is read from the arguments the model supplied, not from the
// tool's own bookkeeping, because the arguments are the untrusted part: whatever
// the adapter would do with them, the id checked here is the id it would use.
func CheckOutbound(gate ConversationGate, instance string, args map[string]any) error {
	if gate == nil {
		// No gate configured means no environment system; refusing is correct, since
		// there is nothing to authorize against.
		return fmt.Errorf("%w: no outbound authorization is configured", ErrConversationNotAllowed)
	}
	conversation := outboundConversation(args)
	if conversation == "" {
		return fmt.Errorf("%w: the call names no conversation", ErrConversationNotAllowed)
	}
	allowed, err := gate.ConversationAllowed(instance, conversation)
	if err != nil {
		return fmt.Errorf("cannot check conversation %q: %w", conversation, err)
	}
	if !allowed {
		return fmt.Errorf("%w: %q has never messaged this bot and no operator has allowed it, so it cannot be messaged. Ask the owner to allow that conversation from the dashboard", ErrConversationNotAllowed, conversation)
	}
	return nil
}

// outboundConversation finds the conversation id in a tool's arguments.
//
// Several spellings are accepted because adapters name their parameters after
// their own platform's vocabulary, and a gate that silently read "" would refuse
// every send, or worse, be bypassed by an adapter that renamed a field.
func outboundConversation(args map[string]any) string {
	for _, key := range []string{"conversation", "chat_id", "chat", "to", "channel"} {
		if value, ok := args[key]; ok {
			switch typed := value.(type) {
			case string:
				if trimmed := strings.TrimSpace(typed); trimmed != "" {
					return trimmed
				}
			case json.Number:
				if typed.String() != "" {
					return typed.String()
				}
			case float64:
				return formatNumber(typed)
			}
		}
	}
	return ""
}

// formatNumber renders a JSON number without a decimal point, so a chat id that
// arrived as a number matches one stored as a string.
func formatNumber(value float64) string {
	if value == float64(int64(value)) {
		return fmt.Sprintf("%d", int64(value))
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

// RecordConversationForDelivery allows the conversation a delivery is going to.
//
// A result cannot be refused on the way out: the work was already done because
// somebody asked for it, and dropping the answer would leave a person waiting
// forever with no explanation. Admitting the event already recorded its route in
// the normal case; this covers a delivery whose route came from somewhere the
// intake path did not see.
func RecordConversationForDelivery(ctx context.Context, s *store.Store, instance string, route Route, label string) error {
	if s == nil || route.Conversation == "" {
		return nil
	}
	return s.RecordInboundConversation(instance, route.Conversation, label)
}
