package runnertransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"tresorbot/internal/contracts"
	"tresorbot/internal/scheduler"
	"tresorbot/internal/store"
)

// Store is the persistence boundary for authenticated runner traffic.
type Store interface {
	AuthenticateRunner(id, token string) (int64, error)
	CurrentRunnerGeneration(id string) (int64, error)
	GetTask(id string) (store.Task, error)
	AcceptRecord(contracts.Accepted) error
	AppendRunnerEvent(contracts.Event) error
	RecordTerminal(contracts.Terminal, int64) error
}
type Reconciler interface {
	ReconcileRunner(string, []contracts.AssignmentEvidence) error
}
type assignmentReader interface {
	CurrentRunnerAssignments(string) ([]store.RunnerAssignment, error)
}
type Options struct {
	Store        Store
	MaxFrame     int64
	Reconciler   Reconciler
	Changed      func()
	Disconnected func(string)
}
type connection struct {
	ws         *websocket.Conn
	generation int64
	ready      atomic.Bool
	writeMu    sync.Mutex
}
type Hub struct {
	store        Store
	max          int64
	reconcile    Reconciler
	changed      func()
	disconnected func(string)
	mu           sync.RWMutex
	conns        map[string]*connection
	closed       bool
}

func New(o Options) *Hub {
	max := o.MaxFrame
	if max <= 0 || max > contracts.MaxRunnerRecordBytes {
		max = contracts.MaxRunnerRecordBytes
	}
	return &Hub{store: o.Store, max: max, reconcile: o.Reconciler, changed: o.Changed, disconnected: o.Disconnected, conns: map[string]*connection{}}
}
func (h *Hub) current(id string, c *connection) bool {
	h.mu.RLock()
	ok := !h.closed && h.conns[id] == c
	h.mu.RUnlock()
	if !ok || !c.ready.Load() || h.store == nil {
		return false
	}
	g, e := h.store.CurrentRunnerGeneration(id)
	return e == nil && g == c.generation
}
func (h *Hub) Available(id string) bool {
	h.mu.RLock()
	c := h.conns[id]
	closed := h.closed
	h.mu.RUnlock()
	return !closed && c != nil && c.ready.Load() && h.store != nil && h.generationOK(id, c.generation)
}
func (h *Hub) generationOK(id string, g int64) bool {
	n, e := h.store.CurrentRunnerGeneration(id)
	return e == nil && n == g
}
func (h *Hub) Status(id string) string {
	if h.Available(id) {
		return "available"
	}
	return "unavailable"
}
func (h *Hub) Dispatch(ctx context.Context, a store.Assignment) error {
	h.mu.RLock()
	c := h.conns[a.RunnerID]
	h.mu.RUnlock()
	if c == nil || !h.current(a.RunnerID, c) {
		return scheduler.ErrUnavailable
	}
	t, e := h.store.GetTask(a.TaskID)
	if e != nil {
		return e
	}
	if t.AssignmentID != a.ID || t.Epoch != a.Epoch {
		return scheduler.ErrUnavailable
	}
	input, e := json.Marshal(ExecutionInput{Task: json.RawMessage(t.Input), Snapshot: json.RawMessage(t.Snapshot)})
	if e != nil {
		return e
	}
	m := contracts.Assign{RunnerRecord: contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: "assign", ID: uuid.NewString(), RunnerID: a.RunnerID, AssignmentID: a.ID, Epoch: a.Epoch}, TaskID: a.TaskID, ExecutionID: a.ExecutionID, Input: input}
	if e = m.Validate(); e != nil {
		return e
	}
	return h.send(ctx, a.RunnerID, c, m)
}

type ExecutionInput struct {
	Task     json.RawMessage `json:"task"`
	Snapshot json.RawMessage `json:"snapshot"`
}

func (h *Hub) send(ctx context.Context, id string, c *connection, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if int64(len(b)) > h.max {
		return errors.New("runner message exceeds frame limit")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if !h.current(id, c) {
		return scheduler.ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, b)
}
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get("X-Runner-ID")
	auth := strings.Fields(r.Header.Get("Authorization"))
	if !validUUID(id) || len(auth) != 2 || !strings.EqualFold(auth[0], "Bearer") || h.store == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	gen, e := h.store.AuthenticateRunner(id, auth[1])
	if e != nil || gen <= 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// coder/websocket's default origin check rejects foreign browser origins; native clients omit Origin.
	ws, e := websocket.Accept(w, r, nil)
	if e != nil {
		return
	}
	ws.SetReadLimit(h.max)
	c := &connection{ws: ws, generation: gen}
	h.mu.Lock()
	if h.closed || h.conns[id] != nil {
		h.mu.Unlock()
		_ = ws.CloseNow()
		return
	}
	h.conns[id] = c
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		removed := h.conns[id] == c
		if removed {
			// Keep the connection reserved until durable disconnect projection
			// completes; a replacement must not reconcile ahead of it.
			if s, ok := h.store.(interface{ MarkRunnerDisconnected(string) error }); ok {
				_ = s.MarkRunnerDisconnected(id)
			}
			delete(h.conns, id)
		}
		h.mu.Unlock()
		if removed && h.disconnected != nil {
			h.disconnected(id)
		}
		_ = ws.CloseNow()
	}()
	hello, reconciled := false, false
	for {
		typ, b, err := ws.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			return
		}
		v, err := contracts.DecodeRunnerRecord(b)
		if err != nil {
			return
		}
		if !h.generationOK(id, gen) {
			return
		}
		if rec, ok := v.(contracts.RunnerRecord); ok && rec.RunnerID != id {
			return
		}
		switch m := v.(type) {
		case *contracts.Control:
			if m.RunnerID != id {
				return
			}
			switch m.Type {
			case "hello":
				if hello || reconciled {
					return
				}
				hello = true
			case "reconcile":
				if !hello || reconciled {
					return
				}
				if h.reconcile != nil {
					if err = h.reconcile.ReconcileRunner(id, m.Assignments); err != nil {
						return
					}
				} else if len(m.Assignments) > 0 {
					return
				}
				h.mu.Lock()
				current := !h.closed && h.conns[id] == c
				if current {
					c.ready.Store(true)
				}
				h.mu.Unlock()
				if !current {
					return
				}
				reconciled = true
				if err = h.send(r.Context(), id, c, contracts.Control{RunnerRecord: contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: "ready", ID: uuid.NewString(), RunnerID: id}}); err != nil {
					return
				}
				if h.changed != nil {
					h.changed()
				}
			case "heartbeat":
				if !c.ready.Load() {
					return
				}
			case "event_ack", "terminal_ack":
				return
			default:
				return
			}
		case *contracts.Accepted:
			if !c.ready.Load() || m.RunnerID != id || h.store.AcceptRecord(*m) != nil {
				return
			}
		case *contracts.Event:
			if !c.ready.Load() || m.RunnerID != id || !h.assignmentCurrent(m.AssignmentID, m.Epoch, id) || h.store.AppendRunnerEvent(*m) != nil {
				return
			}
			ack := contracts.Control{RunnerRecord: contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: "event_ack", ID: uuid.NewString(), RunnerID: id, AssignmentID: m.AssignmentID, Epoch: m.Epoch}, Sequence: m.Sequence, EventID: m.EventID}
			if h.send(r.Context(), id, c, ack) != nil {
				return
			}
		case *contracts.Terminal:
			if !c.ready.Load() || m.RunnerID != id || m.Sequence > math.MaxInt64 || !h.assignmentCurrent(m.AssignmentID, m.Epoch, id) {
				return
			}
			if err = h.store.RecordTerminal(*m, int64(m.Sequence)); err != nil {
				return
			}
			ack := contracts.Control{RunnerRecord: contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: "terminal_ack", ID: uuid.NewString(), RunnerID: id, AssignmentID: m.AssignmentID, Epoch: m.Epoch}, Sequence: m.Sequence, EventID: m.ID}
			if h.send(r.Context(), id, c, ack) != nil {
				return
			}
			if h.changed != nil {
				h.changed()
			}
		default:
			return
		}
	}
}
func (h *Hub) assignmentCurrent(id string, epoch int64, runner string) bool {
	if s, ok := h.store.(assignmentReader); ok {
		as, e := s.CurrentRunnerAssignments(runner)
		if e != nil {
			return false
		}
		for _, a := range as {
			if a.ID == id && a.Epoch == epoch {
				return true
			}
		}
		return false
	}
	return false
}
func validUUID(s string) bool {
	_, e := uuid.Parse(s)
	return e == nil && fmt.Sprint(strings.ToLower(s)) == s
}
func (h *Hub) Cancel(ctx context.Context, runnerID, assignmentID string, epoch int64) error {
	h.mu.RLock()
	c := h.conns[runnerID]
	h.mu.RUnlock()
	if c == nil || !h.current(runnerID, c) {
		return scheduler.ErrUnavailable
	}
	m := contracts.Control{RunnerRecord: contracts.RunnerRecord{Version: contracts.RunnerVersion, Type: "cancel", ID: uuid.NewString(), RunnerID: runnerID, AssignmentID: assignmentID, Epoch: epoch}}
	return h.send(ctx, runnerID, c, m)
}

// Invalidate removes a connection before projecting disconnect, so a replacement cannot reconcile stale state.
func (h *Hub) Invalidate(id string) {
	h.mu.Lock()
	c := h.conns[id]
	if c != nil {
		c.ready.Store(false)
		if s, ok := h.store.(interface{ MarkRunnerDisconnected(string) error }); ok {
			_ = s.MarkRunnerDisconnected(id)
		}
		delete(h.conns, id)
	}
	h.mu.Unlock()
	if c != nil {
		_ = c.ws.CloseNow()
		if h.disconnected != nil {
			h.disconnected(id)
		}
	}
}
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	list := make([]*connection, 0, len(h.conns))
	for id, c := range h.conns {
		delete(h.conns, id)
		list = append(list, c)
	}
	h.mu.Unlock()
	for _, c := range list {
		_ = c.ws.CloseNow()
	}
	return nil
}
