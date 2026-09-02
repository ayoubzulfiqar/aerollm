package mesh

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
)

// inMemoryTransport is a simple local transport that satisfies SecureTransport.
// It is suitable for single-process or test environments.
type inMemoryTransport struct {
	mu      sync.RWMutex
	conns   map[PeerID]*inMemoryConn
	peerID  PeerID
}

func newInMemoryTransport(local PeerID) *inMemoryTransport {
	return &inMemoryTransport{
		conns:  make(map[PeerID]*inMemoryConn),
		peerID: local,
	}
}

// NewInMemoryTransport creates an in-memory mesh transport.
func NewInMemoryTransport(local PeerID) SecureTransport { return newInMemoryTransport(local) }

// Dial creates an in-memory connection to a peer.
func (t *inMemoryTransport) Dial(ctx context.Context, peerDesc PeerDescriptor) (PeerConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.conns == nil {
		return nil, io.ErrClosedPipe
	}

	remote, ok := t.conns[peerDesc.ID]
	if !ok || (ok && remote.isClosed()) {
		localCh := make(chan Envelope, 64)
		remoteCh := make(chan Envelope, 64)
		remote = &inMemoryConn{
			peer:    peerDesc.ID,
			outbound: localCh,
			inbound: remoteCh,
		}
		t.conns[peerDesc.ID] = remote
	}

	remote.ref()
	localConn := &inMemoryConn{
		peer:     t.peerID,
		outbound: remote.inbound,
		inbound:  remote.outbound,
		remote:   remote,
	}
	return localConn, nil
}

// Listen satisfies SecureTransport by returning a no-op listener.
func (t *inMemoryTransport) Listen(_ context.Context, _ string) (PeerListener, error) {
	return &inMemoryListener{t: t}, nil
}

// Close satisfies SecureTransport by releasing resources.
func (t *inMemoryTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
	}
	t.conns = nil
	return nil
}

type inMemoryConn struct {
	peer     PeerID
	outbound chan Envelope
	inbound  chan Envelope
	mu       sync.Mutex
	closed   bool
	refs     int32
	remote   *inMemoryConn
}

func (c *inMemoryConn) ref() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refs++
}

func (c *inMemoryConn) unref() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs <= 0 {
		return
	}
	c.refs--
	if c.refs == 0 && c.closed {
		close(c.outbound)
		if c.remote != nil {
			c.remote.closeLocked()
		}
	}
}

func (c *inMemoryConn) closeLocked() {
	if c.closed {
		return
	}
	c.closed = true
	close(c.outbound)
}

func (c *inMemoryConn) Send(_ context.Context, env Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return io.EOF
	}
	env.From = c.peer
	select {
	case c.outbound <- env:
		return nil
	default:
		return io.ErrShortWrite
	}
}

func (c *inMemoryConn) Receive(_ context.Context) (<-chan Envelope, error) {
	return c.inbound, nil
}

func (c *inMemoryConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.unref()
	return nil
}

func (c *inMemoryConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type inMemoryListener struct {
	t *inMemoryTransport
}

func (l *inMemoryListener) Accept(ctx context.Context) (PeerConn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (l *inMemoryListener) Close() error { return nil }

// StreamConn is a simple text-based envelope stream used for debugging.
type StreamConn struct {
	Reader *bufio.Reader
	Writer io.Writer
}

// WriteEnvelope writes a newline-delimited JSON envelope.
func (s *StreamConn) WriteEnvelope(env Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = s.Writer.Write(append(b, '\n'))
	return err
}

// ReadEnvelope reads a newline-delimited JSON envelope.
func (s *StreamConn) ReadEnvelope() (Envelope, error) {
	line, err := s.Reader.ReadBytes('\n')
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	return env, json.Unmarshal(line, &env)
}
