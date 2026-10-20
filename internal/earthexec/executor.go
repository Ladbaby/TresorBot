package earthexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"tresorbot/internal/contracts"
	"tresorbot/internal/piadapter"
	"tresorbot/internal/relay"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

var (
	ErrAuthorizationUnavailable = errors.New("scoped authorization unavailable")
	ErrInvalidInput             = errors.New("invalid assignment execution input")
)

type Config struct {
	NodePath, CLIPath string
	Tools             []string
	StateDir          string
	Authorize         func(context.Context, contracts.Assign, string) (relay.Authorization, error)
}

type adapterRunner func(context.Context, piadapter.Request, func(piadapter.Progress), func(int, string) error) (piadapter.Result, error)

type Executor struct {
	config Config
	run    adapterRunner
}

func New(c Config) *Executor {
	return newExecutor(c, func(ctx context.Context, req piadapter.Request, progress func(piadapter.Progress), started func(int, string) error) (piadapter.Result, error) {
		a := piadapter.New(piadapter.Config{NodePath: c.NodePath, CLIPath: c.CLIPath, Tools: c.Tools, OnStarted: started})
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
	var task struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(input.Task, &task) != nil || strings.TrimSpace(task.Text) == "" {
		return result, ErrInvalidInput
	}
	var bot store.Bot
	if json.Unmarshal(input.Snapshot, &bot) != nil || bot.ID == "" || bot.Alias == "" || bot.Adapter != "pi" || bot.Workspace == "" {
		return result, ErrInvalidInput
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
