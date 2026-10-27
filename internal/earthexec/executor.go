package earthexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"tresorbot/internal/chatinput"
	"tresorbot/internal/contracts"
	"tresorbot/internal/piadapter"
	"tresorbot/internal/relay"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/runtimepath"
	"tresorbot/internal/store"
)

var (
	ErrAuthorizationUnavailable = errors.New("scoped authorization unavailable")
	ErrInvalidInput             = errors.New("invalid assignment execution input")
)

type Config struct {
	NodePath, CLIPath, PiPath     string
	Tools                         []string
	SunURL, RunnerID, RunnerToken string
	StateDir                      string
	RequireGatewayScope           bool
	Authorize                     func(context.Context, contracts.Assign, string) (relay.Authorization, error)
}

type adapterRunner func(context.Context, piadapter.Request, func(piadapter.Progress), func(int, string) error) (piadapter.Result, error)

type Executor struct {
	config Config
	run    adapterRunner
}

func New(c Config) *Executor {
	var warnings sync.Map
	diagnostic := func(message string) {
		if _, loaded := warnings.LoadOrStore(message, struct{}{}); !loaded {
			log.Printf("Pi compatibility warning: %s", message)
		}
	}
	return newExecutor(c, func(ctx context.Context, req piadapter.Request, progress func(piadapter.Progress), started func(int, string) error) (piadapter.Result, error) {
		resolved, err := runtimepath.Resolve(runtimepath.Options{NodePath: c.NodePath, PiPath: c.PiPath, PiCLIPath: c.CLIPath})
		if err != nil {
			return piadapter.Result{}, err
		}
		a := piadapter.New(piadapter.Config{NodePath: resolved.NodePath, CLIPath: resolved.CLIPath, Tools: c.Tools, OnStarted: started, Diagnostic: diagnostic})
		return a.Run(ctx, req, progress)
	})
}

func newExecutor(c Config, run adapterRunner) *Executor {
	return &Executor{config: c, run: run}
}

func (e *Executor) Execute(ctx context.Context, a contracts.Assign, started func(pid int, session string) error, progress func(piadapter.Progress)) (piadapter.Result, error) {
	var result piadapter.Result
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
	if json.Unmarshal(input.Snapshot, &bot) != nil || bot.ID == "" || bot.Alias == "" || bot.Adapter != "pi" || bot.Workspace == "" {
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
		return result, errors.New("Pi adapter unavailable")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	toolURL, toolToken, closeTools, err := assignmentTools(ctx, e.config, a)
	if err != nil {
		return result, err
	}
	defer closeTools()
	return e.run(ctx, piadapter.Request{ToolURL: toolURL, ToolToken: toolToken, AssignmentID: a.AssignmentID, ConversationID: conversation, Workspace: bot.Workspace, StateDir: state, Instructions: bot.Instructions, Text: task.Text, Messages: messages}, progress, started)
}
