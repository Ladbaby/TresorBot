package earthexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"tresorbot/internal/agent"
	"tresorbot/internal/agents"
	"tresorbot/internal/chatinput"
	"tresorbot/internal/contracts"
	"tresorbot/internal/relay"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

var (
	ErrAuthorizationUnavailable = errors.New("scoped authorization unavailable")
	ErrInvalidInput             = errors.New("invalid assignment execution input")
)

// Config describes one Earth's execution environment.
type Config struct {
	SunURL, RunnerID, RunnerToken string
	StateDir                      string
	RequireGatewayScope           bool
	Authorize                     func(context.Context, contracts.Assign, string) (relay.Authorization, error)
	// Adapters runs the agent host. Nil reports a configuration error at
	// execution time rather than panicking.
	Adapters *agents.Pool
	// Tools is the shared loopback gateway. Nil means agents get no bot tools.
	Tools *ToolGateway
}

// Executor turns one assignment into one agent turn.
type Executor struct {
	config Config
	run    adapterRunner
}

type adapterRunner func(context.Context, agents.ExecuteRequest, func(agent.Progress), func(int, string) error) (agent.Result, error)

// New builds an executor backed by the shared adapter pool.
func New(c Config) *Executor {
	return newExecutor(c, func(ctx context.Context, req agents.ExecuteRequest, progress func(agent.Progress), started func(int, string) error) (agent.Result, error) {
		if c.Adapters == nil {
			return agent.Result{}, errors.New("agent adapters are not configured")
		}
		return c.Adapters.Execute(ctx, req, progress, started)
	})
}

func newExecutor(c Config, run adapterRunner) *Executor {
	return &Executor{config: c, run: run}
}

func (e *Executor) Execute(ctx context.Context, a contracts.Assign, started func(pid int, session string) error, progress func(agent.Progress)) (agent.Result, error) {
	var result agent.Result
	if e == nil {
		return result, errors.New("nil executor")
	}
	if ctx == nil {
		return result, errors.New("nil context")
	}
	if err := a.Validate(); err != nil {
		return result, err
	}
	var input runnertransport.ExecutionInput
	if json.Unmarshal(a.Input, &input) != nil || len(input.Task) == 0 || len(input.Snapshot) == 0 {
		return result, ErrInvalidInput
	}
	var task struct {
		Text           string          `json:"text"`
		Chat           json.RawMessage `json:"chat,omitempty"`
		ConversationID string          `json:"conversation_id,omitempty"`
	}
	if json.Unmarshal(input.Task, &task) != nil || (strings.TrimSpace(task.Text) == "" && len(task.Chat) == 0) {
		return result, ErrInvalidInput
	}
	var bot store.Bot
	if json.Unmarshal(input.Snapshot, &bot) != nil || bot.ID == "" || bot.Alias == "" || strings.TrimSpace(bot.Adapter) == "" || bot.Workspace == "" {
		return result, ErrInvalidInput
	}
	var messages []chatinput.Message
	if len(task.Chat) > 0 {
		chat, err := chatinput.Parse(task.Chat, bot.Alias)
		if err != nil {
			return result, ErrInvalidInput
		}
		messages = chat.Messages
		task.Text = messages[len(messages)-1].Content
	}
	if !filepath.IsAbs(bot.Workspace) {
		return result, fmt.Errorf("%w: workspace must be absolute", ErrInvalidInput)
	}
	if bot.RunnerID != "" && bot.RunnerID != a.RunnerID {
		return result, fmt.Errorf("%w: runner mismatch", ErrInvalidInput)
	}
	if e.config.StateDir == "" || !filepath.IsAbs(e.config.StateDir) {
		return result, fmt.Errorf("%w: state directory must be absolute", ErrInvalidInput)
	}
	conversation := task.ConversationID
	if conversation == "" {
		conversation = a.AssignmentID
	}
	if !contracts.ValidID(conversation) {
		return result, ErrInvalidInput
	}
	state := filepath.Join(e.config.StateDir, "conversations", conversation)
	if err := os.MkdirAll(state, 0700); err != nil {
		return result, err
	}
	if e.run == nil {
		return result, errors.New("agent adapter unavailable")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	toolURL, toolToken, closeTools, err := e.assignmentTools(a)
	if err != nil {
		return result, err
	}
	defer closeTools()

	history := make([]agents.HistoryMessage, 0, len(messages))
	for _, m := range messages {
		history = append(history, agents.HistoryMessage{Role: m.Role, Content: m.Content})
	}
	return e.run(ctx, agents.ExecuteRequest{
		AssignmentID:   a.AssignmentID,
		Epoch:          a.Epoch,
		ConversationID: conversation,
		TaskID:         a.TaskID,
		BotID:          bot.ID,
		Adapter:        bot.Adapter,
		Workspace:      bot.Workspace,
		StateDir:       state,
		Instructions:   bot.Instructions,
		Text:           task.Text,
		History:        history,
		ToolURL:        toolURL,
		ToolToken:      toolToken,
	}, progress, started)
}

// assignmentTools registers this assignment with the shared gateway. A nil
// gateway is not an error: an Earth without a Sun URL simply grants no tools.
func (e *Executor) assignmentTools(a contracts.Assign) (string, string, func(), error) {
	if e.config.Tools == nil {
		return "", "", func() {}, nil
	}
	return e.config.Tools.Register(a)
}
