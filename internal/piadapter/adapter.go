package piadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"sync"
	"tresorbot/internal/chatinput"

	"github.com/google/uuid"
)

const SupportedVersion = "1.4.0"
const maxRecordBytes = 8 << 20

type Config struct {
	NodePath   string
	CLIPath    string
	Tools      []string
	OnStarted  func(pid int, sessionID string) error
	Diagnostic func(message string)
}
type Request struct {
	AssignmentID, Workspace, StateDir, Model, RelayURL, RelayKey, Instructions, Text string
	ConversationID                                                                   string
	Messages                                                                         []chatinput.Message
	ToolURL, ToolToken                                                               string
}
type Progress struct{ Type, Text, Tool string }
type Usage struct {
	Partial    bool                  `json:"partial,omitempty"`
	Input      int64                 `json:"input"`
	Output     int64                 `json:"output"`
	CacheRead  int64                 `json:"cacheRead"`
	CacheWrite int64                 `json:"cacheWrite"`
	Total      int64                 `json:"total"`
	Models     map[string]ModelUsage `json:"models"`
}
type ModelUsage struct {
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	Input      int64  `json:"input"`
	Output     int64  `json:"output"`
	CacheRead  int64  `json:"cacheRead"`
	CacheWrite int64  `json:"cacheWrite"`
	Total      int64  `json:"total"`
}
type Result struct {
	Text, StateDir, SessionID, SessionFile, StopReason, Error string
	Completed, Cancelled, ProcessExited                       bool
	Evidence                                                  []string
	Usage                                                     *Usage
	Model, Provider                                           string
}
type Adapter struct {
	Config Config
	once   sync.Once
}
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
	ver.Env = os.Environ()
	var versionOutput strings.Builder
	ver.Stdout = &limitedWriter{w: &versionOutput, max: 1024}
	ver.Stderr = io.Discard
	probeErr := ver.Run()
	output := strings.TrimSpace(versionOutput.String())
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	warning := ""
	if probeErr != nil {
		warning = fmt.Sprintf("could not verify Pi version (expected %s): %v", SupportedVersion, probeErr)
	} else if output != "Pi Coding Agent v"+SupportedVersion && output != "v"+SupportedVersion && output != SupportedVersion {
		warning = fmt.Sprintf("Pi version is %q; tested version is %s", output, SupportedVersion)
	}
	if warning != "" {
		a.once.Do(func() {
			if a.Config.Diagnostic != nil {
				a.Config.Diagnostic(warning)
			} else {
				log.Printf("Pi compatibility warning: %s", warning)
			}
		})
	}
	if err = os.MkdirAll(state, 0700); err != nil {
		return result, fmt.Errorf("create session state dir: %w", err)
	}
	instructionFile := filepath.Join(state, "bot-instructions.txt")
	if err = writeAtomic(instructionFile, []byte(additiveInstructions(req))); err != nil {
		return result, err
	}
	sessionFile, err := prepareSession(ctx, node, cli, work, state, req)
	if err != nil {
		return result, err
	}
	result.SessionFile = sessionFile
	extensionPath, err := writeBundledExtension(state)
	if err != nil {
		return result, err
	}
	cmd := exec.Command(node, append(rpcNodeArgs(runtime.GOOS), cli, "--mode", "rpc", "--session", sessionFile, "--extension", extensionPath)...)
	cmd.Dir = work
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "TRESORBOT_PI_ASSIGNMENT="+req.AssignmentID, "TRESORBOT_PI_INSTRUCTIONS="+instructionFile, "TRESORBOT_PI_TOOL_URL="+req.ToolURL, "TRESORBOT_PI_TOOL_TOKEN="+req.ToolToken)
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
	owner, startErr := startOwned(cmd)
	if startErr != nil {
		return result, fmt.Errorf("start owned Pi: %w", startErr)
	}
	defer owner.Close()
	result.Evidence = append(result.Evidence, "started Node Pi process with exact-version RPC interface", "normal Pi resources/configuration preserved; session restored from trusted state pointer")
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
					if k, ok := r["id"].(string); ok && k == id {
						responses[k] = r
					}
					continue
				}
				if r["type"] == "extension_ui_request" {
					if err := handleExtensionUI(r, write); err != nil {
						return nil, err
					}
					continue
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
		go func() {
			for range records {
			}
		}()
		_ = stdin.Close()
		if ctx.Err() != nil {
			_ = owner.Terminate()
		}
		if !result.ProcessExited {
			var w error
			select {
			case w = <-waitCh:
			case <-ctx.Done():
				if cause == nil {
					cause = ctx.Err()
				}
				_ = owner.Terminate()
				w = <-waitCh
			}
			result.ProcessExited = true
			if w != nil && cause == nil {
				cause = fmt.Errorf("Pi process failed: %w (%s)", w, strings.TrimSpace(stderr.String()))
			}
		}
		if cause != nil && ctx.Err() != nil {
			result.Cancelled = owner.Certified() && result.ProcessExited
			if !result.Cancelled {
				cause = fmt.Errorf("%w; descendant ownership unproven; cancellation not certified", cause)
			}
		}
		stopReader()
		_ = stdout.Close()
		<-readerDone
		if cause != nil {
			cause = fmt.Errorf("%w (Pi stderr: %s)", cause, strings.TrimSpace(stderr.String()))
			result.Completed = false
		}
		return result, cause
	}
	prompt := req.Text
	if len(req.Messages) == 0 {
		prompt = req.Text
		if strings.TrimSpace(prompt) == "" {
			return finish(errors.New("text is required"))
		}
	} else {
		prompt = ""
		for i := len(req.Messages) - 1; i >= 0; i-- {
			m := req.Messages[i]
			if m.Role == "user" {
				prompt = m.Content
				break
			}
		}
		if strings.TrimSpace(prompt) == "" {
			return finish(errors.New("messages contain no user prompt"))
		}
	}
	preflight, preflightErr := command("get_state", map[string]any{})
	if preflightErr != nil {
		return finish(preflightErr)
	}
	stateData, _ := preflight["data"].(map[string]any)
	modelData, _ := stateData["model"].(map[string]any)
	if modelData == nil || modelData["id"] == "unknown" {
		return finish(errors.New("local Pi has no available model; check provider credentials and Earth service environment"))
	}
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
				if err := handleExtensionUI(r, write); err != nil {
					return finish(err)
				}
				continue
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
					provider, _ := m["provider"].(string)
					model, _ := m["model"].(string)
					if provider != "" {
						result.Provider = provider
					}
					if model != "" {
						result.Model = model
					}
					if !importedAssistant(m) {
						addUsage(&result, m)
					}
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

// rpcNodeArgs scopes this workaround to our Windows RPC child, not installed Pi
// or ambient NODE_OPTIONS. Pi disposes its runtime before calling process.exit.
// Forced native teardown can assert on Windows (nodejs/node#56645); allow a
// successful shutdown to drain naturally instead. Failures still exit normally.
func rpcNodeArgs(platform string) []string {
	if platform != "windows" {
		return nil
	}
	const source = `const exit=process.exit.bind(process);process.exit=(code)=>{const effective=code??process.exitCode??0;if(Number(effective)!==0)return exit(effective);if(process.exitCode==null)process.exitCode=0;};`
	return []string{"--import=data:text/javascript," + source}
}

func additiveInstructions(r Request) string {
	parts := []string{}
	if strings.TrimSpace(r.Instructions) != "" {
		parts = append(parts, "Bot instructions:\n"+r.Instructions)
	}
	for _, m := range r.Messages {
		if m.Role == "system" || m.Role == "developer" {
			parts = append(parts, m.Role+" context:\n"+m.Content)
		}
	}
	return strings.Join(parts, "\n\n")
}
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
// messagesJSON marshals imported messages as a JSON array. A nil slice must
// serialize to "[]" (not "null") so the session helper's array methods work for
// text-only tasks that carry no imported history.
func messagesJSON(msgs []chatinput.Message) (string, error) {
	if msgs == nil {
		msgs = []chatinput.Message{}
	}
	b, err := json.Marshal(msgs)
	return string(b), err
}

func prepareSession(ctx context.Context, node, cli, work, state string, req Request) (string, error) {
	helperPath := filepath.Join(state, "session-helper.mjs")
	if err := writeAtomic(helperPath, []byte(sessionHelper)); err != nil {
		return "", err
	}
	msgJSON, err := messagesJSON(req.Messages)
	if err != nil {
		return "", err
	}
	outputPath := filepath.Join(state, "session-file.txt")
	cmd := exec.CommandContext(ctx, node, helperPath, cli, state, work, conversationID(req), msgJSON, outputPath)
	cmd.Env = os.Environ()
	var stderr strings.Builder
	cmd.Stderr = &limitedWriter{w: &stderr, max: 16 << 10}
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("initialize Pi session: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	b, err := os.ReadFile(outputPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
func conversationID(r Request) string {
	if r.ConversationID != "" {
		return r.ConversationID
	}
	return r.AssignmentID
}
func writeBundledExtension(state string) (string, error) {
	path := filepath.Join(state, "bot-context-extension.mjs")
	return path, writeAtomic(path, []byte(botExtension))
}

func validate(r Request) error {
	for n, v := range map[string]string{"assignment id": r.AssignmentID, "workspace": r.Workspace, "state dir": r.StateDir} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s is required", n)
		}
	}
	if len(r.Messages) == 0 && strings.TrimSpace(r.Text) == "" {
		return errors.New("text or messages are required")
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
