// Package earth owns runner connection and execution lifetimes independently.
package earth

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/coder/websocket"
	"tresorbot/internal/contracts"
	"tresorbot/internal/earthjournal"
	"tresorbot/internal/piadapter"
	"tresorbot/internal/runnertransport"
	"tresorbot/internal/store"
)

type Executor interface {
	Execute(context.Context, contracts.Assign, func(int, string) error, func(piadapter.Progress)) (piadapter.Result, error)
}
type Options struct {
	SunURL, RunnerID, Token string
	Journal                 *earthjournal.Journal
	Executor                Executor
	DefaultWorkspace        func() string
	ConnectionChanged       func(bool, string)
	ValidateRuntime         func() error
}
type worker struct {
	cancel context.CancelFunc
	bot    string
	live   bool
}
type Client struct {
	o       Options
	mu      sync.Mutex
	workers map[string]*worker
	wake    chan struct{}
	refresh chan struct{}
	wg      sync.WaitGroup
}

func New(o Options) (*Client, error) {
	u, e := url.Parse(o.SunURL)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !contracts.ValidID(o.RunnerID) || o.Token == "" || o.Journal == nil || o.Executor == nil {
		return nil, errors.New("invalid Earth runtime options")
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("remote Sun requires TLS")
		}
	}
	return &Client{o: o, workers: map[string]*worker{}, wake: make(chan struct{}, 1), refresh: make(chan struct{}, 1)}, nil
}

// RefreshSettings reconnects transport only; accepted workers retain their lifetimes.
func (c *Client) RefreshSettings() {
	select {
	case c.refresh <- struct{}{}:
	default:
	}
}
func (c *Client) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
func (c *Client) Run(ctx context.Context) error {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	ctx = runCtx
	defer func() {
		stop()
		c.mu.Lock()
		for _, w := range c.workers {
			w.cancel()
		}
		c.mu.Unlock()
		c.wg.Wait()
	}()
	if e := c.o.Journal.Recover(); e != nil {
		return e
	}
	delay := 100 * time.Millisecond
	for {
		if c.o.ValidateRuntime != nil {
			if err := c.o.ValidateRuntime(); err != nil {
				if c.o.ConnectionChanged != nil {
					c.o.ConnectionChanged(false, err.Error())
				}
				select {
				case <-ctx.Done():
					return nil
				case <-c.refresh:
					continue
				}
			}
		}
		err := c.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var auth *authenticationError
		if errors.As(err, &auth) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if delay < 3*time.Second {
			delay *= 2
		}
	}
}

type authenticationError struct{}

func (*authenticationError) Error() string {
	return "Sun rejected Earth credentials; issue or rotate the Earth credential"
}
func (c *Client) record(kind string) contracts.RunnerRecord {
	return contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: kind, ID: contracts.NewID(), RunnerID: c.o.RunnerID}
}
func (c *Client) session(ctx context.Context) error {
	u, _ := url.Parse(c.o.SunURL)
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = "/api/runner/connect"
	ws, resp, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + c.o.Token}, "X-Runner-ID": {c.o.RunnerID}}})
	if err != nil {
		if resp != nil && (resp.StatusCode == 401 || resp.StatusCode == 403) {
			return &authenticationError{}
		}
		return errors.New("runner connection unavailable")
	}
	defer ws.CloseNow()
	ws.SetReadLimit(contracts.MaxRunnerRecordBytes)
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	send := func(v any) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		if len(b) > contracts.MaxRunnerRecordBytes {
			return errors.New("runner record exceeds bounds")
		}
		return ws.Write(sessionCtx, websocket.MessageText, b)
	}
	hello := contracts.Control{RunnerRecord: c.record("hello"), Platform: runtime.GOOS}
	if c.o.DefaultWorkspace != nil {
		hello.DefaultWorkspace = c.o.DefaultWorkspace()
	}
	if c.o.ConnectionChanged != nil {
		c.o.ConnectionChanged(false, "connecting")
		defer c.o.ConnectionChanged(false, "disconnected")
	}
	if err = send(hello); err != nil {
		return err
	}
	evidence, err := c.o.Journal.Evidence(c.o.RunnerID)
	if err != nil {
		return err
	}
	// Only a process retained by this Earth instance provides positive live evidence.
	c.mu.Lock()
	for i := range evidence {
		if w := c.workers[evidence[i].AssignmentID]; w != nil && w.live {
			evidence[i].State = "running"
		} else if evidence[i].State == "running" || evidence[i].State == "launching" {
			evidence[i].State = "uncertain"
		}
	}
	c.mu.Unlock()
	if err = send(contracts.Control{RunnerRecord: c.record("reconcile"), Assignments: evidence}); err != nil {
		return err
	}
	type incoming struct {
		v   any
		err error
	}
	in := make(chan incoming)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			typ, b, e := ws.Read(sessionCtx)
			var v any
			if e == nil && typ != websocket.MessageText {
				e = errors.New("non-text runner record")
			}
			if e == nil {
				v, e = contracts.DecodeRunnerRecord(b)
			}
			select {
			case in <- incoming{v, e}:
			case <-sessionCtx.Done():
				return
			}
			if e != nil {
				return
			}
		}
	}()
	defer func() { cancel(); ws.CloseNow(); <-readerDone }()
	ready := false
	replay := func() error {
		records, e := c.o.Journal.List()
		if e != nil {
			return e
		}
		for _, r := range records {
			if r.Assignment.RunnerID != c.o.RunnerID {
				continue
			}
			reports, e := c.o.Journal.Reports(r.Assignment.AssignmentID)
			if e != nil {
				return e
			}
			for _, report := range reports {
				if e = ws.Write(sessionCtx, websocket.MessageText, report); e != nil {
					return e
				}
			}
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.refresh:
			return errors.New("Earth settings updated")
		case <-c.wake:
			if ready {
				if err = replay(); err != nil {
					return err
				}
			}
		case item := <-in:
			if item.err != nil {
				return item.err
			}
			switch m := item.v.(type) {
			case *contracts.Control:
				if m.RunnerID != c.o.RunnerID {
					return errors.New("runner identity mismatch")
				}
				switch m.Type {
				case "ready":
					if ready {
						return errors.New("duplicate readiness")
					}
					ready = true
					if c.o.ConnectionChanged != nil {
						c.o.ConnectionChanged(true, "connected")
					}
					records, e := c.o.Journal.List()
					if e != nil {
						return e
					}
					for _, record := range records {
						if record.Assignment.RunnerID == c.o.RunnerID && record.State == earthjournal.Admitted {
							if e = c.launch(ctx, record.Assignment); e != nil {
								return e
							}
						}
					}
					if err = replay(); err != nil {
						return err
					}
				case "event_ack", "terminal_ack":
					if !ready {
						return errors.New("ack before readiness")
					}
					if err = c.o.Journal.AcknowledgeRecord(*m); err != nil {
						return err
					}
				case "cancel":
					if !ready {
						return errors.New("cancel before readiness")
					}
					if err = c.o.Journal.RequestCancel(m.AssignmentID, m.RunnerID, m.Epoch); err != nil {
						return err
					}
					c.mu.Lock()
					if w := c.workers[m.AssignmentID]; w != nil {
						w.cancel()
					}
					c.mu.Unlock()
				default:
					return errors.New("unexpected control record")
				}
			case *contracts.Assign:
				if !ready || m.RunnerID != c.o.RunnerID {
					return errors.New("assignment before readiness or identity mismatch")
				}
				_, err = c.o.Journal.Admit(*m)
				if err != nil {
					return err
				}
				accepted := contracts.Accepted{RunnerRecord: c.record("accepted"), Accepted: true}
				accepted.AssignmentID = m.AssignmentID
				accepted.Epoch = m.Epoch
				if err = send(accepted); err != nil {
					return err
				}
				if err = c.launch(ctx, *m); err != nil {
					return err
				}
			default:
				return errors.New("unexpected Sun record")
			}
		}
	}
}
func (c *Client) launch(ctx context.Context, a contracts.Assign) error {
	r, err := c.o.Journal.Get(a.AssignmentID)
	if err != nil {
		return err
	}
	if r.State != earthjournal.Admitted {
		return nil
	}
	var input runnertransport.ExecutionInput
	var bot store.Bot
	if json.Unmarshal(a.Input, &input) != nil || json.Unmarshal(input.Snapshot, &bot) != nil || bot.ID == "" {
		return errors.New("invalid assignment snapshot")
	}
	c.mu.Lock()
	if _, ok := c.workers[a.AssignmentID]; ok {
		c.mu.Unlock()
		return nil
	}
	for _, w := range c.workers {
		if w.bot == bot.ID {
			c.mu.Unlock()
			return errors.New("bot execution slot already occupied")
		}
	}
	if err = c.o.Journal.BeginLaunch(a.AssignmentID, a.RunnerID, a.Epoch); err != nil {
		c.mu.Unlock()
		return err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	w := &worker{cancel: cancel, bot: bot.ID}
	c.workers[a.AssignmentID] = w
	c.wg.Add(1)
	c.mu.Unlock()
	executor := c.o.Executor
	if pin, ok := executor.(interface{ SnapshotExecutor() Executor }); ok {
		executor = pin.SnapshotExecutor()
	}
	go func() {
		defer c.wg.Done()
		defer cancel()
		started := false
		result, e := executor.Execute(workerCtx, a, func(pid int, session string) error {
			err := c.o.Journal.MarkStarted(a.AssignmentID, a.RunnerID, a.Epoch, int64(pid), session)
			if err == nil {
				started = true
				c.mu.Lock()
				w.live = true
				c.mu.Unlock()
			}
			return err
		}, func(p piadapter.Progress) {
			// Persist only lifecycle categories, never native transcripts/tool args.
			if p.Type != "agent_start" && p.Type != "agent_settled" && p.Type != "tool_execution_start" && p.Type != "tool_execution_end" {
				return
			}
			record, err := c.o.Journal.Get(a.AssignmentID)
			if err != nil {
				cancel()
				return
			}
			event := contracts.Event{RunnerRecord: c.record("event"), Sequence: record.LastSequence + 1, EventID: contracts.NewID(), Kind: "progress"}
			event.AssignmentID = a.AssignmentID
			event.Epoch = a.Epoch
			event.Payload, _ = json.Marshal(map[string]string{"stage": p.Type})
			if _, err = c.o.Journal.Append(a.AssignmentID, a.RunnerID, a.Epoch, event); err != nil {
				cancel()
				return
			}
			c.notify()
		})
		c.mu.Lock()
		w.live = false
		c.mu.Unlock()
		// An executor error after start is not proof descendant processes stopped.
		safe := !started || result.Completed || result.Cancelled
		if !safe {
			return
		}
		terminal := contracts.Terminal{RunnerRecord: c.record("terminal"), State: contracts.Completed}
		terminal.AssignmentID = a.AssignmentID
		terminal.Epoch = a.Epoch
		if result.Cancelled {
			terminal.State = contracts.Cancelled
		} else if e != nil || !result.Completed {
			terminal.State = contracts.Failed
			terminal.Error = &contracts.Error{Code: "execution_unavailable", Message: "Execution could not complete; inspect Earth configuration and authorization"}
		} else {
			terminal.Result, _ = json.Marshal(map[string]string{"text": result.Text})
		}
		if _, err := c.o.Journal.Append(a.AssignmentID, a.RunnerID, a.Epoch, terminal); err != nil {
			return
		}
		c.mu.Lock()
		delete(c.workers, a.AssignmentID)
		c.mu.Unlock()
		c.notify()
	}()
	return nil
}
