package environments

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrClosed reports that the host stream ended.
var ErrClosed = errors.New("input host connection closed")

// rpcMessage is one record in either direction. The host uses compact records
// ({n,p} for a notification, {id,result|error} for a reply), which is what the
// embedded JavaScript writes and what the agent protocol already parses.
type rpcMessage struct {
	// Compact and verbose spellings are both accepted, so the host can emit the
	// shorter form while a hand-written test can use the familiar one.
	N      string          `json:"n,omitempty"`
	M      string          `json:"m,omitempty"`
	Method string          `json:"method,omitempty"`
	P      json.RawMessage `json:"p,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	ID     json.RawMessage `json:"id,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

func (m rpcMessage) method() string {
	// The host emits notifications compactly but writes its own requests as full
	// envelopes, so `method` must be read as well as `m`. Reading only the compact
	// spellings makes every host-initiated request look like an unmatched reply,
	// which the host experiences as Sun never answering — start() then hangs with
	// nothing logged on either side.
	if m.M != "" {
		return m.M
	}
	if m.Method != "" {
		return m.Method
	}
	return m.N
}

func (m rpcMessage) params() json.RawMessage {
	if m.P != nil {
		return m.P
	}
	return m.Params
}

// idString decodes an id that may be a number or a string. Sun's own requests use
// numbers and the host's use strings, so both must round-trip.
func (m rpcMessage) idString() string {
	if len(m.ID) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(m.ID, &s) == nil {
		return s
	}
	var n json.Number
	if json.Unmarshal(m.ID, &n) == nil {
		return n.String()
	}
	return ""
}

// RequestHandler answers a request the host initiated. Returning an error sends a
// JSON-RPC error reply, which the host turns into a rejected promise.
type RequestHandler func(ctx context.Context, method string, params json.RawMessage) (any, error)

// NotifyHandler receives a notification. It must not block the read loop.
type NotifyHandler func(method string, params json.RawMessage)

// peer is Sun's side of the channel. It is the mirror of the host's Peer: one
// reader, one writer, a promise per outstanding request, and a handler for the
// requests arriving in the other direction.
type peer struct {
	w       io.Writer
	closeF  func() error
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan rpcMessage
	serial  uint64
	closed  bool
	done    chan struct{}
	// readErr records why the stream ended, so a caller sees the cause rather
	// than a bare "closed".
	readErr error

	onRequest RequestHandler
	onNotify  NotifyHandler
	// replyContext bounds a handler's work; a handler that outlives it would hold
	// the read loop.
	replyContext context.Context
}

func newPeer(w io.Writer, closeF func() error) *peer {
	return &peer{
		w:       w,
		closeF:  closeF,
		pending: map[string]chan rpcMessage{},
		done:    make(chan struct{}),
	}
}

// readLoop consumes the stream until it ends, routing replies to their callers,
// requests to the handler and notifications to theirs. It owns the only read.
func (p *peer) readLoop(r io.Reader) {
	defer p.shutdown(io.EOF)
	br := bufio.NewReaderSize(r, 64*1024)
	var pending []byte
	for {
		chunk, err := br.ReadSlice('\n')
		pending = append(pending, chunk...)
		if len(pending) > MaxRecordBytes {
			p.shutdown(fmt.Errorf("input host record exceeds %d bytes", MaxRecordBytes))
			return
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if len(pending) > 0 {
			line := pending
			pending = nil
			// A trailing CR is tolerated; nothing else is trimmed, so an intentional
			// U+2028 inside a JSON string survives.
			if n := len(line); n > 0 && line[n-1] == '\n' {
				line = line[:n-1]
			}
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			if len(line) > 0 {
				p.dispatch(line)
			}
		}
		if err != nil {
			if err != io.EOF {
				p.shutdown(fmt.Errorf("read input host stream: %w", err))
				return
			}
			return
		}
	}
}

func (p *peer) dispatch(line []byte) {
	var m rpcMessage
	if err := json.Unmarshal(line, &m); err != nil {
		// A malformed record means the stream is out of sync; treating it as fatal
		// is safer than silently ignoring events.
		p.shutdown(fmt.Errorf("invalid input host record: %w", err))
		return
	}
	if method := m.method(); method != "" {
		id := m.idString()
		if id != "" && p.onRequest != nil {
			// A request from the host needs a reply, and the reply must preserve the
			// id's original JSON type, so it is echoed verbatim.
			go p.answerRequest(id, m.ID, method, m.params())
			return
		}
		if p.onNotify != nil {
			p.onNotify(method, m.params())
		}
		return
	}
	id := m.idString()
	if id == "" {
		return
	}
	p.mu.Lock()
	ch := p.pending[id]
	delete(p.pending, id)
	p.mu.Unlock()
	if ch != nil {
		ch <- m
	}
}

// answerRequest runs a handler and writes its reply.
func (p *peer) answerRequest(id string, rawID json.RawMessage, method string, params json.RawMessage) {
	ctx := p.replyContext
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := p.onRequest(ctx, method, params)
	if err != nil {
		var reason string
		var rpcErr *Error
		switch {
		case errors.As(err, &rpcErr):
			reason = rpcErr.Reason
		default:
			reason = "internal_error"
		}
		p.writeRaw(map[string]any{"id": rawID, "error": &Error{Code: -32603, Message: err.Error(), Reason: reason}})
		return
	}
	p.writeRaw(map[string]any{"id": rawID, "result": result})
}

// call sends a request and waits for its reply.
func (p *peer) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	p.serial++
	id := fmt.Sprintf("%d", p.serial)
	ch := make(chan rpcMessage, 1)
	p.pending[id] = ch
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
	}()

	body, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if err = p.write(map[string]any{"m": method, "id": id, "p": json.RawMessage(body)}); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		p.mu.Lock()
		cause := p.readErr
		p.mu.Unlock()
		if cause != nil && !errors.Is(cause, io.EOF) {
			return nil, cause
		}
		return nil, ErrClosed
	case m := <-ch:
		if m.Error != nil {
			return nil, m.Error
		}
		return m.Result, nil
	}
}

func (p *peer) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxRecordBytes {
		return fmt.Errorf("input host request exceeds %d bytes", MaxRecordBytes)
	}
	b = append(b, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err = p.w.Write(b)
	return err
}

// writeRaw marshals a reply. A failure here cannot be reported to anyone, so it
// is dropped: the read loop is already ending.
func (p *peer) writeRaw(v any) {
	_ = p.write(v)
}

func (p *peer) shutdown(cause error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.readErr = cause
	done := p.done
	p.mu.Unlock()
	close(done)
	if p.closeF != nil {
		_ = p.closeF()
	}
}
