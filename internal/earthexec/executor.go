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
	NodePath, CLIPath, PiPath string
	Tools                     []string
	StateDir                  string
	RequireGatewayScope       bool
	Authorize                 func(context.Context, contracts.Assign, string) (relay.Authorization, error)
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
	if e.config.Authorize == nil {
		return result, ErrAuthorizationUnavailable
	}
	var input runnertransport.ExecutionInput
	if json.Unmarshal(a.Input, &input) != nil || len(input.Task) == 0 || len(input.Snapshot) == 0 {
		return result, ErrInvalidInput
	}
	if e.config.RequireGatewayScope {
		if _, err := AssignmentAuthorization(ctx, a, "preflight"); err != nil {
			return result, err
		}
	}
	var task struct {
		Text string          `json:"text"`
		Chat json.RawMessage `json:"chat,omitempty"`
	}
	if json.Unmarshal(input.Task, &task) != nil || (strings.TrimSpace(task.Text) == "" && len(task.Chat) == 0) {
		return result, ErrInvalidInput
	}
	var bot store.Bot
	if json.Unmarshal(input.Snapshot, &bot) != nil || bot.ID == "" || bot.Alias == "" || bot.Adapter != "pi" || bot.Workspace == "" {
		return result, ErrInvalidInput
	}
	if len(task.Chat) > 0 {
		chat, err := chatinput.Parse(task.Chat, bot.Alias)
		if err != nil {
			return result, ErrInvalidInput
		}
		task.Text = chat.Prompt()
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
	state := filepath.Join(e.config.StateDir, a.AssignmentID)
	if err := os.MkdirAll(state, 0700); err != nil {
		return result, err
	}
	onStarted := started
	r, local, err := relay.New(relay.Options{Model: bot.Alias, Authorize: func(c context.Context, inferenceID string) (relay.Authorization, error) {
		return e.config.Authorize(c, a, inferenceID)
	}})
	if err != nil {
		return result, err
	}
	watchDone := make(chan struct{})
	watchExited := make(chan struct{})
	go func() {
		defer close(watchExited)
		select {
		case <-ctx.Done():
			_ = r.Close()
		case <-watchDone:
		}
	}()
	defer func() { close(watchDone); <-watchExited; _ = r.Close() }()
	if e.run == nil {
		return result, errors.New("Pi adapter unavailable")
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	return e.run(ctx, piadapter.Request{AssignmentID: a.AssignmentID, Workspace: bot.Workspace, StateDir: state, Model: bot.Alias, RelayURL: local.BaseURL + "/v1", RelayKey: local.APIKey, Instructions: bot.Instructions, Text: task.Text}, progress, onStarted)
}
