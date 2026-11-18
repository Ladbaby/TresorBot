package environments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"tresorbot/internal/environments/hostjs"
)

// HostConfig describes the one resident host process Sun runs for inputs.
type HostConfig struct {
	// NodePath runs the entry module. Empty means "node" from PATH.
	NodePath string
	// Entry is the absolute path to the host's entry module.
	Entry string
	// EnvironmentsDir is the input catalogue handed to the host.
	EnvironmentsDir string
	// StateDir is where the host's own bookkeeping lives.
	StateDir string
	// NodeArgs are extra Node flags.
	NodeArgs []string
	// MaxInstances caps concurrent instances inside this host.
	MaxInstances int
	// OpenTimeout bounds the hello handshake.
	OpenTimeout time.Duration
	// RestartDelay is the first backoff step after a crash.
	RestartDelay time.Duration
	// Logf records host diagnostics. Nil discards them.
	Logf func(format string, args ...any)
	// OnEvent answers one emitted event. It is called from the read loop and must
	// not block on anything slower than admission; Sun runs it on a goroutine per
	// event so a slow database does not stall other instances.
	OnEvent func(ctx context.Context, instance string, event json.RawMessage) (Ack, error)
	// OnCursor persists adapter-owned cursors.
	OnCursorGet func(ctx context.Context, instance, key string) (string, error)
	OnCursorSet func(ctx context.Context, instance, key, value string) error
	// OnHealth, OnLog and OnChanged report host state upward.
	OnHealth  func(instance string, healthy bool, message string)
	OnLog     func(instance, level, message string)
	OnChanged func(inputs []EnvironmentInfo)
	// OnCrash reports that the host process died, so Sun can re-apply
	// configuration once it is back.
	OnCrash func(err error)
}

// Host supervises the input host process and the instances inside it.
//
// It restarts the process with backoff rather than failing Sun: an input is a
// convenience, and a crashed adapter must not take the owner's dashboard with it.
// Everything the host holds that matters is already durable — cursors in Sun's
// database, events in the ledger — so a restart resumes rather than loses.
type Host struct {
	cfg HostConfig

	mu       sync.Mutex
	cmd      *exec.Cmd
	peer     *peer
	ready    bool
	started  bool
	failures int
	info     HelloResult
	// reconcile is called after every successful start to re-apply the instances
	// Sun believes should be running.
	reconcile func(ctx context.Context) error

	runCtx  context.Context
	runStop context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// NewHost prepares but does not start a supervisor.
func NewHost(cfg HostConfig) (*Host, error) {
	if strings.TrimSpace(cfg.Entry) == "" {
		return nil, errors.New("input host entry module is required")
	}
	if cfg.MaxInstances < 1 {
		cfg.MaxInstances = 200
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 60 * time.Second
	}
	if cfg.RestartDelay <= 0 {
		cfg.RestartDelay = 2 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Host{cfg: cfg}, nil
}

// SetReconcile installs the callback that re-applies instance configuration
// after a (re)start. It is separate from the constructor because the reconciler
// needs the host itself.
func (h *Host) SetReconcile(fn func(ctx context.Context) error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reconcile = fn
}

// logf reports a diagnostic.
//
// A nil Logf is normal — Open fills one in, but a Host built directly (a test, or a
// caller that does not want output) leaves it empty. Calling through unconditionally
// panicked on the first restart attempt, which is precisely the moment a host is
// already in trouble, so it discards instead.
func (h *Host) logf(format string, args ...any) {
	if h.cfg.Logf == nil {
		return
	}
	h.cfg.Logf(format, args...)
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
		return errors.New("input host is restarting")
	}
	if h.closed {
		h.mu.Unlock()
		return ErrClosed
	}
	h.started = true
	h.mu.Unlock()

	if err := h.start(ctx); err != nil {
		h.mu.Lock()
		h.started = false
		h.failures++
		h.mu.Unlock()
		return err
	}
	return nil
}

func (h *Host) start(ctx context.Context) error {
	nodePath := h.cfg.NodePath
	if nodePath == "" {
		nodePath = "node"
	}
	args := append(append([]string{}, h.cfg.NodeArgs...), h.cfg.Entry)
	cmd := exec.Command(nodePath, args...)
	cmd.Dir = h.cfg.StateDir
	// The host inherits Sun's environment, which is how it finds node's own
	// configuration. Secrets are not put here: they travel in a config frame, so
	// they never appear in the process table or in /proc/<pid>/environ.
	cmd.Env = append(os.Environ(), hostjs.EnvironmentsDirEnv+"="+h.cfg.EnvironmentsDir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr ringBuffer
	stderr.limit = 32 << 10
	cmd.Stderr = &stderr

	if err = cmd.Start(); err != nil {
		return fmt.Errorf("start input host: %w", err)
	}

	p := newPeer(stdin, func() error { return stdin.Close() })
	p.replyContext = context.Background()
	p.onRequest = h.handleRequest
	p.onNotify = h.handleNotify

	h.mu.Lock()
	h.cmd = cmd
	h.peer = p
	h.runCtx, h.runStop = context.WithCancel(context.Background())
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		p.readLoop(stdout)
		h.crashed(cmd, stderr.String())
	}()

	helloCtx, cancel := context.WithTimeout(ctx, h.cfg.OpenTimeout)
	defer cancel()
	raw, err := p.call(helloCtx, MethodHello, map[string]any{"protocol": ProtocolVersion, "max_instances": h.cfg.MaxInstances})
	if err != nil {
		h.kill()
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return fmt.Errorf("input host handshake failed: %w (%s)", err, detail)
		}
		return fmt.Errorf("input host handshake failed: %w", err)
	}
	var hello HelloResult
	if err = json.Unmarshal(raw, &hello); err != nil {
		h.kill()
		return fmt.Errorf("decode input host handshake: %w", err)
	}
	if hello.Protocol != ProtocolVersion {
		h.kill()
		return fmt.Errorf("input host protocol %d does not match %d", hello.Protocol, ProtocolVersion)
	}
	h.mu.Lock()
	h.info = hello
	h.ready = true
	h.failures = 0
	reconcile := h.reconcile
	h.mu.Unlock()
	h.logf("input host ready %s with %d input(s)", hello.HostVersion, len(hello.Environments))
	// Re-applying configuration after a restart is what makes recovery
	// meaningful: cursors survive, so instances resume rather than restart their
	// upstream reads from scratch.
	if reconcile != nil {
		go func() {
			if err := reconcile(context.Background()); err != nil {
				h.logf("input host could not re-apply configuration: %v", err)
			}
		}()
	}
	return nil
}

// handleRequest answers a request the host initiated.
func (h *Host) handleRequest(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case NotifyEvent:
		var p EventParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &Error{Message: "invalid event params", Reason: CodeBadRequest}
		}
		if h.cfg.OnEvent == nil {
			return nil, &Error{Message: "no intake configured", Reason: CodeBadRequest}
		}
		return h.cfg.OnEvent(ctx, p.Instance, p.Event)
	case "environment/cursor_get":
		var p struct {
			Instance string `json:"instance"`
			Key      string `json:"key"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &Error{Message: "invalid cursor params", Reason: CodeBadRequest}
		}
		if h.cfg.OnCursorGet == nil {
			return map[string]string{"value": ""}, nil
		}
		value, err := h.cfg.OnCursorGet(ctx, p.Instance, p.Key)
		if err != nil {
			return nil, &Error{Message: err.Error(), Reason: "cursor_unavailable"}
		}
		return map[string]string{"value": value}, nil
	case "environment/cursor_set":
		var p struct {
			Instance string `json:"instance"`
			Key      string `json:"key"`
			Value    string `json:"value"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &Error{Message: "invalid cursor params", Reason: CodeBadRequest}
		}
		if h.cfg.OnCursorSet == nil {
			return map[string]bool{"ok": true}, nil
		}
		if err := h.cfg.OnCursorSet(ctx, p.Instance, p.Key, p.Value); err != nil {
			return nil, &Error{Message: err.Error(), Reason: "cursor_unavailable"}
		}
		return map[string]bool{"ok": true}, nil
	default:
		return nil, &Error{Message: "unknown request " + method, Reason: CodeBadRequest}
	}
}

// handleNotify routes a notification to its callback.
func (h *Host) handleNotify(method string, params json.RawMessage) {
	switch method {
	case NotifyHealth:
		if h.cfg.OnHealth == nil {
			return
		}
		var p HealthParams
		if json.Unmarshal(params, &p) == nil {
			h.cfg.OnHealth(p.Instance, p.Healthy, p.Message)
		}
	case NotifyLog:
		if h.cfg.OnLog == nil {
			return
		}
		var p LogParams
		if json.Unmarshal(params, &p) == nil {
			h.cfg.OnLog(p.Instance, p.Level, p.Message)
		}
	case NotifyChanged:
		if h.cfg.OnChanged == nil {
			return
		}
		var p ReloadResult
		if json.Unmarshal(params, &p) == nil {
			h.cfg.OnChanged(p.Environments)
		}
	}
}

// crashed handles an unexpected end of the host stream, restarting with backoff.
func (h *Host) crashed(cmd *exec.Cmd, stderr string) {
	h.mu.Lock()
	if h.closed || h.cmd != cmd {
		h.mu.Unlock()
		return
	}
	h.ready = false
	h.started = false
	h.cmd = nil
	h.peer = nil
	h.failures++
	failures := h.failures
	h.mu.Unlock()

	_ = cmd.Wait()
	detail := strings.TrimSpace(stderr)
	if detail != "" {
		h.logf("input host exited: %s", detail)
	} else {
		h.logf("input host exited")
	}
	if h.cfg.OnCrash != nil {
		h.cfg.OnCrash(errors.New("input host exited"))
	}
	// Backoff is bounded so a permanently broken adapter does not turn into a
	// restart storm, but a transient crash recovers quickly.
	delay := h.cfg.RestartDelay << uint(max(0, min(failures-1, 5)))
	h.logf("restarting input host in %s", delay)
	time.AfterFunc(delay, func() {
		h.mu.Lock()
		closed := h.closed
		h.mu.Unlock()
		if closed {
			return
		}
		if err := h.ensure(context.Background()); err != nil {
			h.logf("input host restart failed: %v", err)
		}
	})
}

// kill terminates the process without waiting for a graceful shutdown.
func (h *Host) kill() {
	h.mu.Lock()
	cmd := h.cmd
	h.cmd = nil
	h.peer = nil
	h.ready = false
	h.started = false
	h.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

// Inputs reports the host's catalogue, performing the handshake if needed.
func (h *Host) Environments(ctx context.Context) ([]EnvironmentInfo, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]EnvironmentInfo(nil), h.info.Environments...), nil
}

// Reload asks the host to re-read the catalogue.
func (h *Host) Reload(ctx context.Context, name string) ([]EnvironmentInfo, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	h.mu.Lock()
	p := h.peer
	h.mu.Unlock()
	if p == nil {
		return nil, ErrClosed
	}
	raw, err := p.call(ctx, MethodReload, ReloadParams{Name: name})
	if err != nil {
		return nil, err
	}
	var out ReloadResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.info.Environments = out.Environments
	h.mu.Unlock()
	return out.Environments, nil
}

// Configure hands one instance's resolved settings to the host.
func (h *Host) Configure(ctx context.Context, p ConfigParams) error {
	if err := h.ensure(ctx); err != nil {
		return err
	}
	conn := h.conn()
	if conn == nil {
		return ErrClosed
	}
	_, err := conn.call(ctx, MethodConfig, p)
	return err
}

// StartInstance asks the host to connect one instance upstream.
func (h *Host) StartInstance(ctx context.Context, instance string) error {
	if err := h.ensure(ctx); err != nil {
		return err
	}
	conn := h.conn()
	if conn == nil {
		return ErrClosed
	}
	_, err := conn.call(ctx, MethodStart, InstanceRef{Instance: instance})
	return err
}

// StopInstance asks the host to release one instance.
func (h *Host) StopInstance(ctx context.Context, instance string) error {
	conn := h.conn()
	if conn == nil {
		return nil
	}
	_, err := conn.call(ctx, MethodStop, InstanceRef{Instance: instance})
	return err
}

// Webhook forwards one raw HTTP request to the adapter that owns the instance.
func (h *Host) Webhook(ctx context.Context, p WebhookParams) (WebhookResult, error) {
	if err := h.ensure(ctx); err != nil {
		return WebhookResult{}, err
	}
	conn := h.conn()
	if conn == nil {
		return WebhookResult{}, ErrClosed
	}
	raw, err := conn.call(ctx, MethodWebhook, p)
	if err != nil {
		return WebhookResult{}, err
	}
	var out WebhookResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return WebhookResult{}, err
	}
	return out, nil
}

// Tools lists the tools one instance exposes.
//
// A plain forwarding call rather than a cached one: the caller decides when to
// ask, because the catalogue has to be current at the moment the model searches
// and must not be stale just because the session started earlier.
func (h *Host) Tools(ctx context.Context, instance string) ([]ToolDefinition, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	conn := h.conn()
	if conn == nil {
		return nil, ErrClosed
	}
	raw, err := conn.call(ctx, MethodToolsList, ToolsListParams{Instance: instance})
	if err != nil {
		return nil, err
	}
	var out ToolsListResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Tools, nil
}

// CallTool runs one tool on one instance.
//
// The result is returned as raw JSON. Sun bounds its size but not its shape: an
// adapter may answer with a string or a structure, and narrowing that here would
// force every adapter into one representation for no benefit.
func (h *Host) CallTool(ctx context.Context, p ToolsCallParams) (json.RawMessage, error) {
	if err := h.ensure(ctx); err != nil {
		return nil, err
	}
	conn := h.conn()
	if conn == nil {
		return nil, ErrClosed
	}
	raw, err := conn.call(ctx, MethodToolsCall, p)
	if err != nil {
		return nil, err
	}
	var out ToolsCallResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Result, nil
}

// Deliver asks an adapter to send one result upstream.
func (h *Host) Deliver(ctx context.Context, p DeliveryParams) (DeliveryResult, error) {
	if err := h.ensure(ctx); err != nil {
		return DeliveryResult{}, err
	}
	conn := h.conn()
	if conn == nil {
		return DeliveryResult{}, ErrClosed
	}
	raw, err := conn.call(ctx, MethodDeliver, p)
	if err != nil {
		return DeliveryResult{}, err
	}
	var out DeliveryResult
	if err = json.Unmarshal(raw, &out); err != nil {
		return DeliveryResult{}, err
	}
	return out, nil
}

// Ready reports whether a handshake has completed.
func (h *Host) Ready() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

func (h *Host) conn() *peer {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peer
}

// Close stops the host. It is bounded, because Sun's own shutdown must not hang
// on an adapter that ignores its stop notification.
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	conn := h.peer
	cmd := h.cmd
	h.mu.Unlock()

	if conn != nil {
		// A failure here is expected and not worth a warning: the host answers the
		// reply and exits, so the pipe can close before the reply is fully read.
		// Only an unanswered shutdown that leaves a live process matters, and the
		// wait below is what detects that.
		if _, err := conn.call(ctx, MethodShutdown, struct{}{}); err != nil && ctx.Err() == nil {
			h.logf("input host did not acknowledge shutdown: %v", err)
		}
	}
	if cmd != nil && cmd.Process != nil {
		// A host that ignores shutdown is killed rather than waited on: its state is
		// already durable, so nothing is lost.
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			<-done
		}
	}
	h.mu.Lock()
	if h.runStop != nil {
		h.runStop()
	}
	h.mu.Unlock()
	h.wg.Wait()
	return nil
}

// ringBuffer keeps the tail of the host's stderr so a startup failure can explain
// itself without unbounded memory.
type ringBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if r.limit > 0 && len(r.buf) > r.limit {
		r.buf = r.buf[len(r.buf)-r.limit:]
	}
	return len(p), nil
}

func (r *ringBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.buf)
}
