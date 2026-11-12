package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// ErrOverloaded is returned when the host refuses a session because it is at its
// concurrency ceiling. Earth surfaces it as a failed assignment rather than
// risking memory exhaustion.
var ErrOverloaded = errors.New("agent host is at its session ceiling")

// HostConfig describes one resident host process.
type HostConfig struct {
	// NodePath runs the entry module. Empty means "node" from PATH.
	NodePath string
	// Entry is the absolute path to the host's entry module.
	Entry string
	// AgentsDir is the adapter directory handed to the host.
	AgentsDir string
	// StateDir is where the host may keep its own bookkeeping.
	StateDir string
	// MaxSessions caps concurrent sessions in this host.
	MaxSessions int
	// NodeArgs are extra Node flags, e.g. --max-old-space-size.
	NodeArgs []string
	// CancelTimeout bounds a cooperative cancel before the process is killed.
	CancelTimeout time.Duration
	// OpenTimeout bounds the hello handshake.
	OpenTimeout time.Duration
	// Logf records host diagnostics. Nil discards them.
	Logf func(format string, args ...any)
	// OnSettled is called for each completed turn.
	OnSettled func(SettledParams)
	// OnEvent is called for each canonical event.
	OnEvent func(sessionID string, e Event)
}

// Host supervises one long-lived agent host process and the sessions in it.
type Host struct {
	cfg HostConfig

	mu       sync.Mutex
	cmd      *exec.Cmd
	client   *client
	owner    *processOwner
	ready    bool
	info     HelloResult
	sessions map[string]struct{}
	started  bool
	failures int

	runCtx  context.Context
	runStop context.CancelFunc
	wg      sync.WaitGroup
}

// NewHost prepares but does not start a supervisor.
func NewHost(cfg HostConfig) (*Host, error) {
	if strings.TrimSpace(cfg.Entry) == "" {
		return nil, errors.New("agent host entry module is required")
	}
	if cfg.MaxSessions < 1 {
		cfg.MaxSessions = 1
	}
	if cfg.CancelTimeout <= 0 {
		cfg.CancelTimeout = 30 * time.Second
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 60 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Host{cfg: cfg, sessions: map[string]struct{}{}}, nil
}

// Pid reports the host process id, which is the process evidence recorded for
// an assignment. Sessions are multiplexed inside this one process, so the pid
// identifies the host rather than a per-assignment child.
func (h *Host) Pid() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd == nil || h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

// Adapters reports the host's catalogue. It performs the handshake if needed.
func (h *Host) Adapters(ctx context.Context) ([]AdapterInfo, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]AdapterInfo(nil), h.info.Adapters...), nil
}

// Reload asks the host to re-read adapters from disk and swap them in. Sessions
// already running keep the module version they started with.
func (h *Host) Reload(ctx context.Context, name string) ([]AdapterInfo, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	raw, err := h.current().call(ctx, MethodReload, ReloadParams{Name: name})
	if err != nil {
		return nil, err
	}
	var out ReloadResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.info.Adapters = out.Adapters
	h.mu.Unlock()
	return out.Adapters, nil
}

// ensure starts the process once and completes the handshake.
func (h *Host) ensure(ctx context.Context) error {
	h.mu.Lock()
	if h.ready {
		h.mu.Unlock()
		return nil
	}
	if h.started {
		h.mu.Unlock()
		return errors.New("agent host is restarting")
	}
	h.started = true
	h.mu.Unlock()

	cmd := exec.Command(h.nodePath(), h.args()...)
	cmd.Dir = h.cfg.StateDir
	cmd.Env = append(os.Environ(), "TRESORBOT_AGENTS_DIR="+h.cfg.AgentsDir, "TRESORBOT_HOST_STATE_DIR="+h.cfg.StateDir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		h.reset()
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		h.reset()
		return err
	}
	var stderr ringBuffer
	stderr.limit = 32 << 10
	cmd.Stderr = &stderr

	owner, err := startOwned(cmd)
	if err != nil {
		h.reset()
		return fmt.Errorf("start agent host: %w", err)
	}

	c := newClient(stdin, func() error { return stdin.Close() })
	c.onNotify = h.notify

	h.mu.Lock()
	h.cmd, h.client, h.owner = cmd, c, owner
	h.runCtx, h.runStop = context.WithCancel(context.Background())
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		c.readLoop(stdout)
	}()

	// A host that dies before the handshake must fail loudly, not hang.
	helloCtx, cancel := context.WithTimeout(ctx, h.cfg.OpenTimeout)
	defer cancel()
	raw, err := c.call(helloCtx, MethodHello, map[string]any{"protocol": ProtocolVersion, "max_sessions": h.cfg.MaxSessions})
	if err != nil {
		h.kill()
		h.reset()
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return fmt.Errorf("agent host handshake failed: %w (%s)", err, detail)
		}
		return fmt.Errorf("agent host handshake failed: %w", err)
	}
	var hello HelloResult
	if err = json.Unmarshal(raw, &hello); err != nil {
		h.kill()
		h.reset()
		return fmt.Errorf("decode agent host handshake: %w", err)
	}
	if hello.Protocol != ProtocolVersion {
		h.kill()
		h.reset()
		return fmt.Errorf("agent host protocol %d does not match %d", hello.Protocol, ProtocolVersion)
	}
	h.mu.Lock()
	h.info = hello
	h.ready = true
	h.failures = 0
	h.mu.Unlock()
	h.cfg.Logf("agent host ready %s with %d adapter(s)", hello.HostVersion, len(hello.Adapters))
	return nil
}

func (h *Host) nodePath() string {
	if h.cfg.NodePath != "" {
		return h.cfg.NodePath
	}
	return "node"
}

func (h *Host) args() []string {
	out := make([]string, 0, len(h.cfg.NodeArgs)+1)
	out = append(out, h.cfg.NodeArgs...)
	return append(out, h.cfg.Entry)
}

func (h *Host) current() *client {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.client
}

func (h *Host) reset() {
	h.mu.Lock()
	h.ready = false
	h.started = false
	h.client = nil
	h.cmd = nil
	h.owner = nil
	h.sessions = map[string]struct{}{}
	h.mu.Unlock()
}

// notify routes host-initiated notifications to the configured handlers.
func (h *Host) notify(method string, params json.RawMessage) {
	switch method {
	case NotifyEvent:
		var p EventParams
		if json.Unmarshal(params, &p) != nil {
			return
		}
		if h.cfg.OnEvent != nil {
			h.cfg.OnEvent(p.SessionID, p.Event)
		}
	case NotifySettled:
		var p SettledParams
		if json.Unmarshal(params, &p) != nil {
			return
		}
		h.mu.Lock()
		delete(h.sessions, p.SessionID)
		h.mu.Unlock()
		if h.cfg.OnSettled != nil {
			h.cfg.OnSettled(p)
		}
	case NotifyLog:
		var p LogParams
		if json.Unmarshal(params, &p) != nil {
			return
		}
		h.cfg.Logf("agent %s: %s", p.Level, p.Message)
	case NotifyAdaptersChanged:
		var p AdaptersChangedParams
		if json.Unmarshal(params, &p) != nil {
			return
		}
		h.mu.Lock()
		h.info.Adapters = p.Adapters
		h.mu.Unlock()
	}
}

// Open creates one session. The host is the only place that knows how.
func (h *Host) Open(ctx context.Context, p OpenParams) (OpenResult, error) {
	if err := h.ensure(ctx); err != nil {
		return OpenResult{}, err
	}
	c := h.current()
	if c == nil {
		return OpenResult{}, ErrClosed
	}
	raw, err := c.call(ctx, MethodSessionOpen, p)
	if err != nil {
		var rpcErr *Error
		if errors.As(err, &rpcErr) && rpcErr.Reason == CodeOverloaded {
			return OpenResult{}, ErrOverloaded
		}
		return OpenResult{}, err
	}
	var out OpenResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return OpenResult{}, err
	}
	h.mu.Lock()
	h.sessions[p.SessionID] = struct{}{}
	h.mu.Unlock()
	return out, nil
}

// Prompt delivers a turn and reports whether it was accepted.
func (h *Host) Prompt(ctx context.Context, sessionID, text string) error {
	c := h.current()
	if c == nil {
		return ErrClosed
	}
	raw, err := c.call(ctx, MethodPrompt, PromptParams{SessionID: sessionID, Text: text})
	if err != nil {
		return err
	}
	var out PromptResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return err
	}
	if !out.Accepted {
		return errors.New("agent did not accept the prompt")
	}
	return nil
}

// Cancel asks one session to stop. If the host does not acknowledge within the
// cancel timeout the whole process is killed, because a wedged session cannot be
// isolated from its siblings.
func (h *Host) Cancel(ctx context.Context, sessionID, reason string) (cancelled bool, killed bool) {
	c := h.current()
	if c == nil {
		return false, false
	}
	cancelCtx, cancel := context.WithTimeout(ctx, h.cfg.CancelTimeout)
	defer cancel()
	raw, err := c.call(cancelCtx, MethodCancel, CancelParams{SessionID: sessionID, Reason: reason})
	if err != nil {
		h.cfg.Logf("agent session %s did not cancel cooperatively: %v", sessionID, err)
		h.kill()
		return false, true
	}
	var out CancelResult
	if json.Unmarshal(raw, &out) == nil {
		cancelled = out.Cancelled
	}
	if !cancelled {
		// A host that answered "not cancelled" has already lost the turn.
		h.kill()
		return false, true
	}
	h.sessionsDrop(sessionID)
	return true, false
}

func (h *Host) sessionsDrop(id string) {
	h.mu.Lock()
	delete(h.sessions, id)
	h.mu.Unlock()
}

// Close releases one session after its turn settled. Failure is not fatal: the
// host exits with the process anyway.
func (h *Host) Close(ctx context.Context, sessionID string) {
	c := h.current()
	if c == nil {
		return
	}
	_, _ = c.call(ctx, MethodSessionClose, CloseParams{SessionID: sessionID})
	h.sessionsDrop(sessionID)
}

// Certified reports whether terminating this host provably stopped descendants.
// It is the property the caller needs before claiming a cancellation happened.
func (h *Host) Certified() bool {
	h.mu.Lock()
	owner := h.owner
	h.mu.Unlock()
	return owner != nil && owner.Certified()
}

// kill terminates the host and every session in it.
func (h *Host) kill() {
	h.mu.Lock()
	owner := h.owner
	stop := h.runStop
	h.mu.Unlock()
	if stop != nil {
		stop()
	}
	if owner != nil {
		_ = owner.Terminate()
	}
}

// Shutdown stops the host cleanly, then force-kills it if it lingers.
func (h *Host) Shutdown(ctx context.Context) {
	c := h.current()
	if c != nil {
		short, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = c.call(short, MethodShutdown, struct{}{})
		cancel()
	}
	h.kill()
	h.closeOwner()
	h.wg.Wait()
	h.mu.Lock()
	h.ready = false
	h.started = false
	h.mu.Unlock()
}

func (h *Host) closeOwner() {
	h.mu.Lock()
	owner := h.owner
	h.mu.Unlock()
	if owner != nil {
		_ = owner.Close()
	}
}

// Sessions lists live session IDs, for reconciliation after a host restart.
func (h *Host) Sessions() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.sessions))
	for id := range h.sessions {
		out = append(out, id)
	}
	return out
}

// ringBuffer keeps the last N bytes of a stream, for crash diagnostics.
type ringBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if len(r.buf) > r.limit {
		r.buf = r.buf[len(r.buf)-r.limit:]
	}
	return len(p), nil
}

func (r *ringBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.buf)
}

// discard satisfies io.Writer when a stream is not wanted.
var _ io.Writer = (*ringBuffer)(nil)
