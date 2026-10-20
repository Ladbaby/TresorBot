package piadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const SupportedVersion = "1.0.2"
const maxRecordBytes = 8 << 20

type Config struct {
	NodePath  string
	CLIPath   string
	Tools     []string
	OnStarted func(pid int, sessionID string) error
}
type Request struct {
	AssignmentID, Workspace, StateDir, Model, RelayURL, RelayKey, Instructions, Text string
}
type Progress struct{ Type, Text, Tool string }
type Result struct {
	Text, StateDir, SessionID, SessionFile, StopReason, Error string
	Completed, Cancelled, ProcessExited                       bool
	Evidence                                                  []string
}
type Adapter struct{ Config Config }
type recordResult struct {
	rec map[string]any
	err error
}

func New(c Config) *Adapter { return &Adapter{Config: c} }

func (a *Adapter) Run(ctx context.Context, req Request, onProgress func(Progress)) (Result, error) {
	result := Result{StateDir: req.StateDir, SessionID: req.AssignmentID}
	if ctx == nil {
		return result, errors.New("nil context")
	}
	if err := validate(req); err != nil {
		return result, err
	}
	node := a.Config.NodePath
	if node == "" {
		node = "node"
	}
	cli := a.Config.CLIPath
	if cli == "" {
		return result, errors.New("Pi CLI JS path is required")
	}
	cli, err := filepath.Abs(cli)
	if err != nil {
		return result, err
	}
	work, err := filepath.Abs(req.Workspace)
	if err != nil {
		return result, err
	}
	state, err := filepath.Abs(req.StateDir)
	if err != nil {
		return result, err
	}
	ver := exec.CommandContext(ctx, node, cli, "--version")
	ver.Env = envSlice(safeEnvironment(filepath.Join(state, "agent"), filepath.Join(state, "sessions")))
	output, err := ver.Output()
	if err != nil {
		return result, fmt.Errorf("verify Pi version: %w", err)
	}
	if strings.TrimSpace(string(output)) != "Pi Coding Agent v"+SupportedVersion && strings.TrimSpace(string(output)) != "v"+SupportedVersion && strings.TrimSpace(string(output)) != SupportedVersion {
		return result, fmt.Errorf("unsupported Pi version: require exactly %s, got %q", SupportedVersion, strings.TrimSpace(string(output)))
	}
	if err = os.MkdirAll(state, 0700); err != nil {
		return result, fmt.Errorf("create isolated state dir: %w", err)
	}
	agentDir, sessions := filepath.Join(state, "agent"), filepath.Join(state, "sessions")
	for _, d := range []string{agentDir, sessions} {
		if err = os.MkdirAll(d, 0700); err != nil {
			return result, err
		}
	}
	tools := a.tools()
	settings := map[string]any{"defaultTools": tools, "defaultProjectTrust": "never", "enableInstallTelemetry": false, "retry": map[string]any{"enabled": false}, "compaction": map[string]any{"enabled": false}}
	if err = writeJSON(filepath.Join(agentDir, "settings.json"), settings); err != nil {
		return result, err
	}
	models := map[string]any{"providers": map[string]any{"tresor-relay": map[string]any{"baseUrl": strings.TrimRight(req.RelayURL, "/"), "api": "openai-completions", "apiKey": req.RelayKey, "models": []any{map[string]any{"id": req.Model, "name": req.Model, "contextWindow": 128000, "maxTokens": 8192, "reasoning": false}}}}}
	if err = writeJSON(filepath.Join(agentDir, "models.json"), models); err != nil {
		return result, err
	}
	env := safeEnvironment(agentDir, sessions)
	env["PI_OFFLINE"] = "1"
	env["PI_SKIP_VERSION_CHECK"] = "1"
	env["PI_TELEMETRY"] = "0"
	cmd := exec.Command(node, cli, "--mode", "rpc", "--session-dir", sessions, "--session-id", req.AssignmentID, "--model", "tresor-relay/"+req.Model, "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-approve", "--offline", "--tools", strings.Join(tools, ","))
	cmd.Dir = work
	cmd.Env = envSlice(env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return result, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return result, err
	}
	var stderr strings.Builder
	cmd.Stderr = &limitedWriter{w: &stderr, max: 32 << 10}
	if err = cmd.Start(); err != nil {
		return result, fmt.Errorf("start Pi: %w", err)
	}
	result.Evidence = append(result.Evidence, "started Node Pi process with exact-version RPC interface", "isolated agent settings, models, and assignment session", "allowlisted environment excludes ambient provider credentials")
	if a.Config.OnStarted != nil {
		if err = a.Config.OnStarted(cmd.Process.Pid, req.AssignmentID); err != nil {
			_ = stdin.Close()
			waitErr := cmd.Wait()
			result.ProcessExited = true
			return result, fmt.Errorf("record Pi process before prompt (exit %v): %w", waitErr, err)
		}
	}
	readerCtx, stopReader := context.WithCancel(context.Background())
	defer stopReader()
	records := make(chan recordResult)
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); readRecordsContext(readerCtx, stdout, records) }()
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	var writeMu sync.Mutex
	write := func(v map[string]any) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, e = stdin.Write(append(b, '\n'))
		return e
	}
	serial := 0
	nextID := func() string { serial++; return fmt.Sprintf("tresor-%d", serial) }
	// All RPC responses and unsolicited events are consumed by this single dispatcher.
	responses := map[string]map[string]any{}

	read := func(id string) (map[string]any, error) {
		for {
			if r, ok := responses[id]; ok {
				delete(responses, id)
				if r["success"] != true {
					return nil, fmt.Errorf("Pi RPC %s failed: %v", id, r["error"])
				}
				return r, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case w := <-waitCh:
				result.ProcessExited = true
				return nil, fmt.Errorf("Pi process exited before response: %v (%s)", w, strings.TrimSpace(stderr.String()))
			case rr, ok := <-records:
				if !ok {
					return nil, errors.New("Pi RPC stream closed")
				}
				if rr.err != nil {
					return nil, rr.err
				}
				r := rr.rec
				if r["type"] == "response" {
					if k, ok := r["id"].(string); ok {
						responses[k] = r
					}
					continue
				}
				if r["type"] == "extension_ui_request" {
					return nil, fmt.Errorf("unsupported extension UI request: %v", r["method"])
				}

				if onProgress != nil {
					onProgress(progressOf(r))
				}
			}
		}
	}
	command := func(typ string, fields map[string]any) (map[string]any, error) {
		id := nextID()
		fields["id"], fields["type"] = id, typ
		if e := write(fields); e != nil {
			return nil, e
		}
		return read(id)
	}
	finish := func(cause error) (Result, error) {
		stopReader()
		_ = stdout.Close()
		_ = stdin.Close()
		if !result.ProcessExited {
			w := <-waitCh
			result.ProcessExited = true
			if w != nil && cause == nil {
				cause = fmt.Errorf("Pi process failed: %w (%s)", w, strings.TrimSpace(stderr.String()))
			}
		}
		if cause != nil && ctx.Err() != nil {
			cause = fmt.Errorf("%w; Pi process exit observed, descendant ownership is unproven; cancellation not certified", cause)
		}
		<-readerDone
		return result, cause
	}
	for _, c := range []string{"set_auto_retry", "set_auto_compaction"} {
		if _, err = command(c, map[string]any{"enabled": false}); err != nil {
			return finish(err)
		}
	}
	prompt := strings.Join([]string{"Assignment: " + req.AssignmentID, "Workspace: " + work, "State directory: " + state, "Instructions:", req.Instructions, "Task:", req.Text}, "\n\n")
	promptID := nextID()
	if err = write(map[string]any{"id": promptID, "type": "prompt", "message": prompt}); err != nil {
		return finish(err)
	}
	var accepted, settled bool
	for !settled {
		select {
		case <-ctx.Done():
			_ = write(map[string]any{"id": nextID(), "type": "abort"})
			_ = stdin.Close()
			return finish(ctx.Err())
		case w := <-waitCh:
			result.ProcessExited = true
			return result, fmt.Errorf("Pi exited before agent_settled: %v", w)
		case rr, ok := <-records:
			if !ok {
				return finish(errors.New("Pi RPC stream closed before agent_settled"))
			}
			if rr.err != nil {
				return finish(rr.err)
			}
			r := rr.rec
			if r["type"] == "extension_ui_request" {
				return finish(errors.New("unsupported extension UI request"))
			}
			if r["type"] == "response" {
				if r["id"] == promptID {
					if r["success"] != true {
						return finish(fmt.Errorf("Pi prompt rejected: %v", r["error"]))
					}
					d, _ := r["data"].(map[string]any)
					if d["disposition"] == "handled" {
						return finish(errors.New("Pi prompt was handled without an agent run"))
					}
					accepted = true
				}
				continue
			}

			if onProgress != nil {
				onProgress(progressOf(r))
			}
			if r["type"] == "message_end" {
				if m, ok := r["message"].(map[string]any); ok && m["role"] == "assistant" {
					result.StopReason, _ = m["stopReason"].(string)
					result.Error, _ = m["errorMessage"].(string)
					result.Text = assistantText(m)
				}
			}
			if r["type"] == "agent_settled" {
				settled = true
			}
		}
	}
	if !accepted {
		return finish(errors.New("Pi settled without accepting prompt"))
	}
	if result.StopReason != "stop" {
		return finish(fmt.Errorf("Pi assistant stopped with %q: %s", result.StopReason, result.Error))
	}
	// Verify the session's authoritative state and last assistant text after settlement.
	stateReply, e := command("get_state", map[string]any{})
	if e != nil {
		return finish(e)
	}
	if d, ok := stateReply["data"].(map[string]any); ok {
		result.SessionFile, _ = d["sessionFile"].(string)
	}
	last, e := command("get_last_assistant_text", map[string]any{})
	if e != nil {
		return finish(e)
	}
	if d, ok := last["data"].(map[string]any); ok {
		verified, _ := d["text"].(string)
		if verified == "" || verified != result.Text {
			return finish(errors.New("Pi last assistant text did not match observed final message"))
		}
	} else {
		return finish(errors.New("Pi returned no last assistant text"))
	}
	result.Completed = true
	result.Evidence = append(result.Evidence, "agent_settled observed", "assistant stopReason=stop", "get_state and get_last_assistant_text verified")
	// Reader exits with stdout closure once the process exits; this join prevents a leaked reader.
	_ = readerDone
	return finish(nil)
}
func validate(r Request) error {
	for n, v := range map[string]string{"assignment id": r.AssignmentID, "workspace": r.Workspace, "state dir": r.StateDir, "model": r.Model, "relay URL": r.RelayURL, "relay key": r.RelayKey, "text": r.Text} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s is required", n)
		}
	}
	if !filepath.IsAbs(r.Workspace) || !filepath.IsAbs(r.StateDir) {
		return errors.New("workspace and state dir must be absolute paths")
	}
	if _, e := uuid.Parse(r.AssignmentID); e != nil {
		return errors.New("assignment id must be a UUID")
	}
	return nil
}
func (a *Adapter) tools() []string {
	if len(a.Config.Tools) > 0 {
		return append([]string(nil), a.Config.Tools...)
	}
	if runtime.GOOS == "windows" {
		return []string{"read", "grep", "find", "ls", "edit", "write", "powershell"}
	}
	return []string{"read", "grep", "find", "ls", "edit", "write", "bash"}
}
func assistantText(m map[string]any) string {
	var b strings.Builder
	if blocks, ok := m["content"].([]any); ok {
		for _, v := range blocks {
			if x, ok := v.(map[string]any); ok && x["type"] == "text" {
				if t, ok := x["text"].(string); ok {
					b.WriteString(t)
				}
			}
		}
	}
	return b.String()
}
func progressOf(m map[string]any) Progress {
	p := Progress{}
	p.Type, _ = m["type"].(string)
	if e, ok := m["assistantMessageEvent"].(map[string]any); ok {
		p.Text, _ = e["delta"].(string)
	}
	p.Tool, _ = m["toolName"].(string)
	return p
}
func writeJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return os.WriteFile(path, b, 0600)
}
func safeEnvironment(agent, sessions string) map[string]string {
	keys := []string{"PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "SYSTEMDRIVE", "PATHEXT"}
	out := map[string]string{}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			out[k] = v
		}
	}
	out["PI_CODING_AGENT_DIR"] = agent
	out["PI_CODING_AGENT_SESSION_DIR"] = sessions
	out["LANG"] = "C.UTF-8"
	return out
}
func envSlice(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

type limitedWriter struct {
	w   io.Writer
	max int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if n > l.max {
		p = p[:l.max]
		l.max = 0
	} else {
		l.max -= n
	}
	_, e := l.w.Write(p)
	return n, e
}
func readRecords(r io.Reader, out chan recordResult) {
	readRecordsContext(context.Background(), r, out)
}
func readRecordsContext(ctx context.Context, r io.Reader, out chan recordResult) {
	defer close(out)
	br := bufio.NewReaderSize(r, 64*1024)
	var pending []byte
	send := func(rr recordResult) bool {
		select {
		case out <- rr:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		chunk, e := br.ReadSlice('\n')
		pending = append(pending, chunk...)
		if len(pending) > maxRecordBytes {
			send(recordResult{err: errors.New("Pi RPC record exceeds limit")})
			return
		}
		if e == bufio.ErrBufferFull {
			continue
		}
		if len(pending) > 0 {
			line := strings.TrimSuffix(strings.TrimSuffix(string(pending), "\n"), "\r")
			pending = nil
			var v map[string]any
			if er := json.Unmarshal([]byte(line), &v); er != nil {
				send(recordResult{err: fmt.Errorf("invalid Pi RPC record: %w", er)})
				return
			}
			if !send(recordResult{rec: v}) {
				return
			}
		}
		if e != nil {
			if e != io.EOF {
				send(recordResult{err: e})
			}
			return
		}
	}
}
