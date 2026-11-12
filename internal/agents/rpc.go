package agents

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrClosed is returned once the host connection is gone.
var ErrClosed = errors.New("agent host connection closed")

// rpcMessage is the union of the four record shapes on the wire.
type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`

	// Transport-specific short keys, kept so the host can emit compact records.
	M string          `json:"m,omitempty"`
	P json.RawMessage `json:"p,omitempty"`
	N string          `json:"n,omitempty"`
}

// notifyHandler receives host-initiated notifications.
type notifyHandler func(method string, params json.RawMessage)

// client is a bidirectional JSON-RPC connection over a pair of streams.
//
// Concurrency model: exactly one goroutine reads the stream and dispatches. All
// writes are serialised by a mutex so a notification can never interleave with a
// response. A caller waiting for a reply parks on its own channel, so a slow
// method never blocks the dispatcher and a cancelled call never leaks.
type client struct {
	w      io.Writer
	closeF func() error

	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan rpcMessage
	serial  uint64
	closed  bool
	done    chan struct{}

	onNotify notifyHandler
}

func newClient(w io.Writer, closeF func() error) *client {
	return &client{w: w, closeF: closeF, pending: map[string]chan rpcMessage{}, done: make(chan struct{})}
}

// readLoop consumes the stream until it ends, routing replies to their callers
// and notifications to the handler. It owns the only read from r.
func (c *client) readLoop(r io.Reader) {
	defer c.shutdown(ErrClosed)
	br := bufio.NewReaderSize(r, 64*1024)
	var pending []byte
	for {
		chunk, err := br.ReadSlice('\n')
		pending = append(pending, chunk...)
		if len(pending) > MaxRecordBytes {
			c.shutdown(fmt.Errorf("agent host record exceeds %d bytes", MaxRecordBytes))
			return
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if len(pending) > 0 {
			line := pending
			pending = nil
			// Trailing CR is tolerated; nothing else is trimmed so an
			// intentional U+2028 inside a JSON string survives.
			if n := len(line); n > 0 && line[n-1] == '\n' {
				line = line[:n-1]
			}
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			if len(line) > 0 {
				c.dispatch(line)
			}
		}
		if err != nil {
			if err != io.EOF {
				c.shutdown(fmt.Errorf("read agent host stream: %w", err))
				return
			}
			return
		}
	}
}

func (c *client) dispatch(line []byte) {
	var m rpcMessage
	if err := json.Unmarshal(line, &m); err != nil {
		// A malformed record is the host's bug; treat it as fatal so Earth does
		// not silently run an agent whose stream is out of sync.
		c.shutdown(fmt.Errorf("invalid agent host record: %w", err))
		return
	}
	method := m.Method
	if method == "" {
		method = m.N
	}
	if method != "" {
		if c.onNotify != nil {
			params := m.Params
			if params == nil {
				params = m.P
			}
			c.onNotify(method, params)
		}
		return
	}
	var id string
	if len(m.ID) > 0 {
		_ = json.Unmarshal(m.ID, &id)
	}
	if id == "" {
		return
	}
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch != nil {
		ch <- m
	}
}

// call sends a request and waits for its reply.
func (c *client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.serial++
	id := fmt.Sprintf("%d", c.serial)
	ch := make(chan rpcMessage, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	body, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if err = c.write(map[string]any{"m": method, "id": id, "p": json.RawMessage(body)}); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed
	case m := <-ch:
		if m.Error != nil {
			return nil, m.Error
		}
		return m.Result, nil
	}
}

func (c *client) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxRecordBytes {
		return fmt.Errorf("agent host request exceeds %d bytes", MaxRecordBytes)
	}
	b = append(b, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.w.Write(b)
	return err
}

// notify sends a fire-and-forget notification.
func (c *client) notify(method string, params any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return c.write(map[string]any{"m": method, "p": json.RawMessage(body)})
}

// shutdown fails every waiting caller exactly once.
func (c *client) shutdown(cause error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	if c.closeF != nil {
		_ = c.closeF()
	}
}
