package mesh

// This file contains the only SecureTransport implementation shipped with the
// mesh package: an IN-PROCESS, IN-MEMORY transport.
//
// Despite the file name it is NOT libp2p. It performs no networking, no
// encryption and no peer authentication: transports can only reach other
// transports attached to the same InMemoryNetwork inside the same process.
// It exists so that discovery, gossip and CRDT convergence can run (and be
// tested) end-to-end without a real network stack. A standalone transport
// created with NewInMemoryTransport is attached to a private network of its
// own, so dialing any other peer fails with ErrPeerUnreachable.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	// ErrPeerUnreachable is returned by Dial when the target peer is not attached
	// to the same in-memory network, is closed, or is not listening.
	ErrPeerUnreachable = errors.New("mesh: peer unreachable")
	// ErrTransportClosed is returned when using a closed transport.
	ErrTransportClosed = errors.New("mesh: transport closed")
	// ErrListenerClosed is returned by Accept after the listener was closed.
	ErrListenerClosed = errors.New("mesh: listener closed")
	// ErrAlreadyListening is returned by Listen when the transport already has an
	// active listener.
	ErrAlreadyListening = errors.New("mesh: transport already listening")
	// ErrAcceptBacklogFull is returned by Dial when the target has too many
	// connections waiting to be accepted.
	ErrAcceptBacklogFull = errors.New("mesh: accept backlog full")
	// ErrEnvelopeTooLarge is returned by StreamConn when an envelope exceeds the
	// configured size cap.
	ErrEnvelopeTooLarge = errors.New("mesh: envelope too large")
)

const (
	inMemoryInboxSize     = 64
	inMemoryAcceptBacklog = 64
)

// InMemoryNetwork is an in-process fabric connecting in-memory transports.
// It is safe for concurrent use.
type InMemoryNetwork struct {
	mu    sync.Mutex
	nodes map[PeerID]*inMemoryTransport
}

// NewInMemoryNetwork creates an empty in-process network.
func NewInMemoryNetwork() *InMemoryNetwork {
	return &InMemoryNetwork{nodes: make(map[PeerID]*inMemoryTransport)}
}

// Transport attaches a new transport with the given peer id to the network.
// It fails if id is empty or already attached.
func (n *InMemoryNetwork) Transport(id PeerID) (SecureTransport, error) {
	if id == "" {
		return nil, errors.New("mesh: empty peer id")
	}
	t := &inMemoryTransport{id: id, net: n, conns: make(map[*inMemoryPipe]struct{})}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.nodes[id]; exists {
		return nil, fmt.Errorf("mesh: peer %q already attached to network", id)
	}
	n.nodes[id] = t
	return t, nil
}

func (n *InMemoryNetwork) lookup(id PeerID) *inMemoryTransport {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

func (n *InMemoryNetwork) detach(id PeerID, t *inMemoryTransport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nodes[id] == t {
		delete(n.nodes, id)
	}
}

// NewInMemoryTransport creates a standalone in-memory transport attached to a
// private network. It can only reach itself (loopback); dialing any other peer
// returns ErrPeerUnreachable. Use NewInMemoryNetwork to connect several
// transports within one process.
func NewInMemoryTransport(local PeerID) SecureTransport {
	n := NewInMemoryNetwork()
	t := &inMemoryTransport{id: local, net: n, conns: make(map[*inMemoryPipe]struct{})}
	n.nodes[local] = t
	return t
}

type inMemoryTransport struct {
	id  PeerID
	net *InMemoryNetwork

	mu       sync.Mutex
	closed   bool
	listener *inMemoryListener
	conns    map[*inMemoryPipe]struct{}
}

func (t *inMemoryTransport) track(p *inMemoryPipe) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.conns[p] = struct{}{}
	return true
}

func (t *inMemoryTransport) untrack(p *inMemoryPipe) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, p)
}

// enqueue hands an inbound connection to this transport's listener.
func (t *inMemoryTransport) enqueue(c *inMemoryConn) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.listener == nil {
		return fmt.Errorf("%w: %q is not listening", ErrPeerUnreachable, t.id)
	}
	select {
	case t.listener.accept <- c:
		t.conns[c.pipe] = struct{}{}
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrAcceptBacklogFull, t.id)
	}
}

// Dial opens a connection to peerDesc.ID, which must be attached to the same
// network and listening.
func (t *inMemoryTransport) Dial(ctx context.Context, peerDesc PeerDescriptor) (PeerConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if peerDesc.ID == "" {
		return nil, fmt.Errorf("%w: empty peer id", ErrPeerUnreachable)
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, ErrTransportClosed
	}
	target := t.net.lookup(peerDesc.ID)
	if target == nil {
		return nil, fmt.Errorf("%w: %q", ErrPeerUnreachable, peerDesc.ID)
	}

	p := newInMemoryPipe(t, target)
	local := &inMemoryConn{pipe: p, self: t.id, peer: target.id, out: p.aToB, in: p.bToA}
	remote := &inMemoryConn{pipe: p, self: target.id, peer: t.id, out: p.bToA, in: p.aToB}
	if !t.track(p) {
		p.close()
		return nil, ErrTransportClosed
	}
	if err := target.enqueue(remote); err != nil {
		p.close()
		return nil, err
	}
	return local, nil
}

// Listen starts accepting inbound connections. The address is ignored (the
// in-memory network routes by peer id). Only one listener may be active.
func (t *inMemoryTransport) Listen(_ context.Context, _ string) (PeerListener, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrTransportClosed
	}
	if t.listener != nil {
		return nil, ErrAlreadyListening
	}
	l := &inMemoryListener{
		t:      t,
		accept: make(chan *inMemoryConn, inMemoryAcceptBacklog),
		done:   make(chan struct{}),
	}
	t.listener = l
	return l, nil
}

// Close detaches the transport from its network, closes its listener and every
// open connection. It is idempotent.
func (t *inMemoryTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	l := t.listener
	t.listener = nil
	pipes := make([]*inMemoryPipe, 0, len(t.conns))
	for p := range t.conns {
		pipes = append(pipes, p)
	}
	t.conns = make(map[*inMemoryPipe]struct{})
	t.mu.Unlock()

	t.net.detach(t.id, t)
	if l != nil {
		_ = l.Close()
	}
	for _, p := range pipes {
		p.close()
	}
	return nil
}

// inMemoryPipe is a bidirectional, buffered, in-process channel pair. Closing
// either end closes both directions; buffered envelopes are still delivered to
// receivers before their channel reports closed.
type inMemoryPipe struct {
	mu     sync.RWMutex
	closed bool
	done   chan struct{}
	once   sync.Once
	aToB   chan Envelope
	bToA   chan Envelope
	owners [2]*inMemoryTransport
}

func newInMemoryPipe(a, b *inMemoryTransport) *inMemoryPipe {
	return &inMemoryPipe{
		done:   make(chan struct{}),
		aToB:   make(chan Envelope, inMemoryInboxSize),
		bToA:   make(chan Envelope, inMemoryInboxSize),
		owners: [2]*inMemoryTransport{a, b},
	}
}

func (p *inMemoryPipe) send(ctx context.Context, ch chan Envelope, env Envelope) error {
	// Holding the read lock guarantees the channel is not closed underneath us;
	// close() first signals done so blocked senders release the lock promptly.
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return io.ErrClosedPipe
	}
	select {
	case ch <- env:
		return nil
	case <-p.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *inMemoryPipe) close() {
	p.once.Do(func() {
		close(p.done)
		p.mu.Lock()
		p.closed = true
		close(p.aToB)
		close(p.bToA)
		p.mu.Unlock()
		for _, t := range p.owners {
			if t != nil {
				t.untrack(p)
			}
		}
	})
}

type inMemoryConn struct {
	pipe *inMemoryPipe
	self PeerID
	peer PeerID
	out  chan Envelope
	in   chan Envelope
}

// Send delivers env to the remote end. It blocks while the remote inbox is full
// until ctx is done or the connection closes. From is always overwritten with
// the local peer id, so a peer cannot impersonate another over this transport.
func (c *inMemoryConn) Send(ctx context.Context, env Envelope) error {
	env.From = c.self
	env.Received = time.Now()
	return c.pipe.send(ctx, c.out, env)
}

// Receive returns the inbound envelope channel. It is closed once the
// connection is closed (after any buffered envelopes have been drained).
func (c *inMemoryConn) Receive(_ context.Context) (<-chan Envelope, error) {
	return c.in, nil
}

// RemotePeer returns the id of the peer at the other end.
func (c *inMemoryConn) RemotePeer() PeerID { return c.peer }

// Close closes both directions of the connection. It is idempotent.
func (c *inMemoryConn) Close() error {
	c.pipe.close()
	return nil
}

type inMemoryListener struct {
	t      *inMemoryTransport
	accept chan *inMemoryConn
	done   chan struct{}
	once   sync.Once
}

// Accept returns the next inbound connection, or an error when ctx is done or
// the listener is closed.
func (l *inMemoryListener) Accept(ctx context.Context) (PeerConn, error) {
	select {
	case <-l.done:
		return nil, ErrListenerClosed
	default:
	}
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, ErrListenerClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops accepting connections and closes any that were never accepted.
func (l *inMemoryListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.t.mu.Lock()
		if l.t.listener == l {
			l.t.listener = nil
		}
		l.t.mu.Unlock()
		for {
			select {
			case c := <-l.accept:
				_ = c.Close()
			default:
				return
			}
		}
	})
	return nil
}

// DefaultMaxEnvelopeBytes is the default cap on one newline-delimited envelope
// read or written by StreamConn.
const DefaultMaxEnvelopeBytes = MaxEnvelopePayloadBytes + 64<<10

// StreamConn is a newline-delimited JSON envelope codec over a byte stream,
// used for debugging and for plugging the gossip protocol into real streams.
// Reads are capped at MaxBytes (DefaultMaxEnvelopeBytes when zero). Writes are
// serialised; concurrent reads are not supported.
type StreamConn struct {
	Reader *bufio.Reader
	Writer io.Writer
	// MaxBytes caps the encoded size of one envelope (0 = DefaultMaxEnvelopeBytes).
	MaxBytes int

	wmu sync.Mutex
}

func (s *StreamConn) limit() int {
	if s.MaxBytes > 0 {
		return s.MaxBytes
	}
	return DefaultMaxEnvelopeBytes
}

// WriteEnvelope writes a newline-delimited JSON envelope.
func (s *StreamConn) WriteEnvelope(env Envelope) error {
	if s == nil || s.Writer == nil {
		return errors.New("mesh: stream writer not configured")
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if len(b)+1 > s.limit() {
		return ErrEnvelopeTooLarge
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, err = s.Writer.Write(append(b, '\n'))
	return err
}

// ReadEnvelope reads one newline-delimited JSON envelope. Lines longer than the
// size cap fail with ErrEnvelopeTooLarge; the stream should then be closed.
func (s *StreamConn) ReadEnvelope() (Envelope, error) {
	if s == nil || s.Reader == nil {
		return Envelope{}, errors.New("mesh: stream reader not configured")
	}
	limit := s.limit()
	var line []byte
	for {
		chunk, err := s.Reader.ReadSlice('\n')
		if len(line)+len(chunk) > limit {
			return Envelope{}, ErrEnvelopeTooLarge
		}
		line = append(line, chunk...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return Envelope{}, io.ErrUnexpectedEOF
		}
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
