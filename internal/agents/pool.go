package agents

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"tresorbot/internal/agent"
)

// ExecuteRequest is one assignment's turn, already resolved by the caller.
type ExecuteRequest struct {
	AssignmentID   string
	Epoch          int64
	ConversationID string
	TaskID         string
	BotID          string
	Adapter        string
	Workspace      string
	StateDir       string
	Instructions   string
	Text           string
	History        []HistoryMessage
	ToolURL        string
	ToolToken      string
	// Settings is the adapter's validated configuration taken from the
	// per-assignment snapshot, so a mid-flight settings edit cannot change the
	// behaviour of a turn already running.
	Settings map[string]any
}

// PoolConfig describes the whole adapter runtime for one Earth.
type PoolConfig struct {
	NodePath      string
	Entry         string
	AgentsDir     string
	StateDir      string
	Hosts         int
	MaxSessions   int
	CancelTimeout time.Duration
	OpenTimeout   time.Duration
	NodeArgs      []string
	Logf          func(format string, args ...any)
}

// Pool owns the host processes and turns a request into a settled turn.
//
// One pool serves the whole Earth process. A host per assignment would
// reintroduce the per-turn process cost that pooling exists to avoid.
type Pool struct {
	cfg    PoolConfig
	shards []*Host

	mu       sync.Mutex
	settles  map[string]chan SettledParams
	progress map[string]func(agent.Progress)
	serial   uint64
}

// NewPool builds the shards. Hosts start lazily on first use, so an idle Earth
// costs nothing.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if cfg.Entry == "" {
		return nil, errors.New("agent host entry is required")
	}
	if cfg.Hosts < 1 {
		cfg.Hosts = 1
	}
	if cfg.MaxSessions < 1 {
		cfg.MaxSessions = 1
	}
	perHost := cfg.MaxSessions / cfg.Hosts
	if perHost < 1 {
		perHost = 1
	}
	p := &Pool{
		cfg:      cfg,
		settles:  map[string]chan SettledParams{},
		progress: map[string]func(agent.Progress){},
	}
	for i := 0; i < cfg.Hosts; i++ {
		host, err := NewHost(HostConfig{
			NodePath:      cfg.NodePath,
			Entry:         cfg.Entry,
			AgentsDir:     cfg.AgentsDir,
			StateDir:      cfg.StateDir,
			MaxSessions:   perHost,
			NodeArgs:      cfg.NodeArgs,
			CancelTimeout: cfg.CancelTimeout,
			OpenTimeout:   cfg.OpenTimeout,
			Logf:          cfg.Logf,
			OnEvent:       p.onEvent,
			OnSettled:     p.onSettled,
		})
		if err != nil {
			return nil, err
		}
		p.shards = append(p.shards, host)
	}
	return p, nil
}

// shard keeps a bot's sessions on one host so a crash costs that bot's work
// rather than a slice of the whole fleet, and so per-bot ordering is stable.
func (p *Pool) shard(botID string) *Host {
	if len(p.shards) == 1 {
		return p.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(botID))
	return p.shards[int(h.Sum32())%len(p.shards)]
}

// Adapters reports the catalogue. Every shard reads the same directory, so the
// first shard speaks for all of them.
func (p *Pool) Adapters(ctx context.Context) ([]AdapterInfo, error) {
	if len(p.shards) == 0 {
		return nil, errors.New("no agent hosts configured")
	}
	return p.shards[0].Adapters(ctx)
}

// Reload re-reads adapters on every shard, so a newly written adapter is usable
// everywhere and running turns keep their pinned version.
func (p *Pool) Reload(ctx context.Context, name string) ([]AdapterInfo, error) {
	if len(p.shards) == 0 {
		return nil, errors.New("no agent hosts configured")
	}
	var out []AdapterInfo
	for _, host := range p.shards {
		adapters, err := host.Reload(ctx, name)
		if err != nil {
			return out, err
		}
		out = adapters
	}
	return out, nil
}

// onEvent forwards a canonical event to its turn's sink. Only the lifecycle
// categories reach Earth's journal; assistant text stops here.
func (p *Pool) onEvent(sessionID string, e Event) {
	p.mu.Lock()
	sink := p.progress[sessionID]
	p.mu.Unlock()
	if sink == nil {
		return
	}
	switch e.Type {
	case EventTurnStarted:
		sink(agent.Progress{Type: agent.ProgressTurnStarted})
	case EventToolCall:
		sink(agent.Progress{Type: agent.ProgressToolCall, Tool: e.Name})
	case EventToolResult:
		sink(agent.Progress{Type: agent.ProgressToolResult, Tool: e.Name})
	}
}

func (p *Pool) onSettled(settled SettledParams) {
	p.mu.Lock()
	ch := p.settles[settled.SessionID]
	delete(p.settles, settled.SessionID)
	delete(p.progress, settled.SessionID)
	p.mu.Unlock()
	if ch != nil {
		select {
		case ch <- settled:
		default:
		}
	}
}

// nextSessionID allocates a session identity. It is a host-side handle, not the
// assignment: the adapter's own resume pointer is what survives a restart.
func (p *Pool) nextSessionID() string {
	p.mu.Lock()
	p.serial++
	n := p.serial
	p.mu.Unlock()
	return fmt.Sprintf("%s-%d", time.Now().UTC().Format("20060102T150405"), n)
}

// Execute runs one assignment's turn and returns its settled result.
//
// It preserves the historical contract: Completed is set only on a clean stop,
// and Cancelled only when terminating the host provably stopped descendants. An
// unproven cancellation therefore stays false, which is what lets the caller
// record the outcome as uncertain instead of claiming success.
func (p *Pool) Execute(ctx context.Context, req ExecuteRequest, progress func(agent.Progress), started func(pid int, session string) error) (agent.Result, error) {
	var result agent.Result
	if ctx == nil {
		return result, errors.New("nil context")
	}
	if req.Text == "" && len(req.History) == 0 {
		return result, errors.New("text or history is required")
	}
	target := p.shard(req.BotID)
	sessionID := p.nextSessionID()

	settles := make(chan SettledParams, 1)
	p.mu.Lock()
	p.settles[sessionID] = settles
	if progress != nil {
		p.progress[sessionID] = progress
	}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.settles, sessionID)
		delete(p.progress, sessionID)
		p.mu.Unlock()
	}()

	opened, err := target.Open(ctx, OpenParams{
		SessionID:      sessionID,
		Adapter:        req.Adapter,
		AssignmentID:   req.AssignmentID,
		Epoch:          req.Epoch,
		ConversationID: req.ConversationID,
		TaskID:         req.TaskID,
		Workspace:      req.Workspace,
		StateDir:       req.StateDir,
		Settings:       req.Settings,
		Instructions:   req.Instructions,
		History:        req.History,
		ToolURL:        req.ToolURL,
		ToolToken:      req.ToolToken,
	})
	if err != nil {
		return result, err
	}
	result.SessionRef = opened.SessionRef
	result.Evidence = append(result.Evidence, opened.Diagnostics...)

	// Announce the session before prompting, so a crash between open and prompt
	// is still attributable to this assignment.
	if started != nil {
		if err = started(0, opened.SessionRef); err != nil {
			target.Close(context.Background(), sessionID)
			return result, err
		}
	}

	if err = target.Prompt(ctx, sessionID, promptText(req)); err != nil {
		target.Close(context.Background(), sessionID)
		if ctx.Err() != nil {
			return cancelledResult(result, target, fmt.Errorf("prompt failed: %w", ctx.Err()))
		}
		return result, err
	}

	select {
	case <-ctx.Done():
		cancelled, killed := target.Cancel(context.Background(), sessionID, "assignment cancelled")
		if killed || !cancelled {
			return cancelledResult(result, target, ctx.Err())
		}
		target.Close(context.Background(), sessionID)
		result.Cancelled = true
		result.ProcessExited = true
		return result, ctx.Err()
	case settled := <-settles:
		target.Close(context.Background(), sessionID)
		return settledToResult(settled, result), nil
	}
}

// cancelledResult reports a cancellation only when descendant termination is
// provable. Otherwise it says so, so the caller mirrors the historic
// execution_uncertain path rather than claiming the work stopped.
func cancelledResult(result agent.Result, host *Host, cause error) (agent.Result, error) {
	host.kill()
	host.wg.Wait()
	result.ProcessExited = true
	result.Cancelled = host.Certified()
	if !result.Cancelled {
		return result, fmt.Errorf("%w; descendant ownership unproven; cancellation not certified", cause)
	}
	return result, cause
}

// promptText picks the newest user message. The adapter imports prior history
// into its own session, so the prompt is only the live turn.
func promptText(req ExecuteRequest) string {
	for i := len(req.History) - 1; i >= 0; i-- {
		if req.History[i].Role == "user" {
			return req.History[i].Content
		}
	}
	return req.Text
}

func settledToResult(s SettledParams, base agent.Result) agent.Result {
	out := base
	out.Text = s.Text
	out.StopReason = s.StopReason
	out.Error = s.Error
	out.Model = s.Model
	out.Provider = s.Provider
	if s.SessionRef != "" {
		out.SessionRef = s.SessionRef
	}
	if len(s.Evidence) > 0 {
		out.Evidence = append(out.Evidence, s.Evidence...)
	}
	switch s.StopReason {
	case StopStop:
		out.Completed = true
	case StopAborted:
		out.Cancelled = true
	}
	if s.Usage != nil {
		usage := agent.Usage{
			Partial:    s.Usage.Partial,
			Input:      s.Usage.Input,
			Output:     s.Usage.Output,
			CacheRead:  s.Usage.CacheRead,
			CacheWrite: s.Usage.CacheWrite,
			Total:      s.Usage.Total,
		}
		if len(s.Usage.Models) > 0 {
			usage.Models = make(map[string]agent.ModelUsage, len(s.Usage.Models))
			for k, m := range s.Usage.Models {
				usage.Models[k] = agent.ModelUsage{
					Provider: m.Provider, Model: m.Model,
					Input: m.Input, Output: m.Output,
					CacheRead: m.CacheRead, CacheWrite: m.CacheWrite,
					Total: m.Total,
				}
			}
		}
		out.Usage = &usage
	}
	out.ProcessExited = true
	return out
}

// Close stops every host.
func (p *Pool) Close(ctx context.Context) {
	for _, host := range p.shards {
		host.Shutdown(ctx)
	}
}
