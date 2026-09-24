package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

// MeshState is the interface for CRDT-backed distributed state.
// Implementations must be safe for concurrent use: Merge is invoked from
// network goroutines while local code keeps mutating the state.
type MeshState interface {
	// Merge incorporates remote state into the local CRDT.
	Merge(ctx context.Context, remote json.RawMessage) error
	// LocalSnapshot returns the current local state as JSON.
	LocalSnapshot(ctx context.Context) (json.RawMessage, error)
	// Type returns the CRDT type name for routing in the gossip protocol.
	Type() string
}

// PeerID uniquely identifies a mesh peer.
type PeerID string

// PeerDescriptor describes a discovered peer.
type PeerDescriptor struct {
	ID      PeerID
	Address string
	Meta    map[string]string
}

// Limits applied to peer descriptors learned from configuration or the network.
const (
	// MaxPeerIDLen caps the length of a peer id.
	MaxPeerIDLen = 256
	// MaxPeerAddressLen caps the length of a peer address.
	MaxPeerAddressLen = 512
	// MaxPeerMetaEntries caps the number of metadata entries kept per peer.
	MaxPeerMetaEntries = 32
	// MaxPeerMetaLen caps the length of each metadata key and value.
	MaxPeerMetaLen = 256
	// MaxEnvelopePayloadBytes caps the payload of a gossip envelope accepted
	// from a peer.
	MaxEnvelopePayloadBytes = MaxMergePayloadBytes
	// DefaultDialTimeout bounds a single outbound dial + send.
	DefaultDialTimeout = 2 * time.Second
)

// ErrInvalidPeer is returned for peer descriptors that fail validation.
var ErrInvalidPeer = errors.New("mesh: invalid peer descriptor")

func hasControl(s string) bool {
	return strings.IndexFunc(s, unicode.IsControl) >= 0
}

// sanitizePeer validates desc and returns a copy with trimmed fields and a
// bounded, copied Meta map. Peers must have a non-empty ID and Address.
func sanitizePeer(desc PeerDescriptor) (PeerDescriptor, error) {
	id := PeerID(strings.TrimSpace(string(desc.ID)))
	addr := strings.TrimSpace(desc.Address)
	switch {
	case id == "":
		return PeerDescriptor{}, fmt.Errorf("%w: empty id", ErrInvalidPeer)
	case addr == "":
		return PeerDescriptor{}, fmt.Errorf("%w: empty address for %q", ErrInvalidPeer, id)
	case len(id) > MaxPeerIDLen || hasControl(string(id)):
		return PeerDescriptor{}, fmt.Errorf("%w: bad id", ErrInvalidPeer)
	case len(addr) > MaxPeerAddressLen || hasControl(addr):
		return PeerDescriptor{}, fmt.Errorf("%w: bad address for %q", ErrInvalidPeer, id)
	}
	out := PeerDescriptor{ID: id, Address: addr}
	if len(desc.Meta) > 0 {
		keys := make([]string, 0, len(desc.Meta))
		for k := range desc.Meta {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out.Meta = make(map[string]string, min(len(keys), MaxPeerMetaEntries))
		for _, k := range keys {
			if len(out.Meta) >= MaxPeerMetaEntries {
				break
			}
			v := desc.Meta[k]
			if k == "" || len(k) > MaxPeerMetaLen || len(v) > MaxPeerMetaLen {
				continue
			}
			out.Meta[k] = v
		}
	}
	return out, nil
}

func metaEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// SecureTransport handles communication with peers.
//
// Implementations must authenticate peers and set Envelope.From on inbound
// envelopes from the authenticated peer identity, never from the sender's
// claim. TLSTransport (NewTLSTransport) is the network implementation:
// mutual TLS 1.3 over TCP with peer ids bound to certificates. The in-memory
// transport (NewInMemoryTransport / InMemoryNetwork) is in-process and
// unencrypted, for tests and single-process deployments.
type SecureTransport interface {
	Dial(ctx context.Context, peer PeerDescriptor) (PeerConn, error)
	Listen(ctx context.Context, address string) (PeerListener, error)
	Close() error
}

// PeerConn is a connection to a peer.
type PeerConn interface {
	Send(ctx context.Context, msg Envelope) error
	Receive(ctx context.Context) (<-chan Envelope, error)
	Close() error
}

// PeerListener accepts incoming peer connections.
type PeerListener interface {
	Accept(ctx context.Context) (PeerConn, error)
	Close() error
}

// Envelope wraps a gossip message.
type Envelope struct {
	From      PeerID
	StateType string
	Payload   json.RawMessage
	Received  time.Time
}

// SyncStats reports cumulative counters of a gossip / sync / discovery worker.
type SyncStats struct {
	// Rounds is the number of periodic rounds executed.
	Rounds uint64
	// Sent is the number of envelopes successfully sent.
	Sent uint64
	// SendErrors counts failed dials and sends.
	SendErrors uint64
	// Received is the number of inbound envelopes processed.
	Received uint64
	// MergeErrors counts inbound envelopes that were rejected or failed to merge.
	MergeErrors uint64
	// LastError is the most recent error message ("" if none).
	LastError string
	// LastErrorAt is when LastError was recorded.
	LastErrorAt time.Time
}

type statsRecorder struct {
	mu      sync.Mutex
	s       SyncStats
	lastErr error
}

func (r *statsRecorder) round() {
	r.mu.Lock()
	r.s.Rounds++
	r.mu.Unlock()
}

func (r *statsRecorder) sent() {
	r.mu.Lock()
	r.s.Sent++
	r.mu.Unlock()
}

func (r *statsRecorder) received() {
	r.mu.Lock()
	r.s.Received++
	r.mu.Unlock()
}

func (r *statsRecorder) setErrLocked(err error) {
	r.lastErr = err
	r.s.LastError = err.Error()
	r.s.LastErrorAt = time.Now()
}

func (r *statsRecorder) sendError(err error) {
	r.mu.Lock()
	r.s.SendErrors++
	r.setErrLocked(err)
	r.mu.Unlock()
}

func (r *statsRecorder) mergeError(err error) {
	r.mu.Lock()
	r.s.MergeErrors++
	r.setErrLocked(err)
	r.mu.Unlock()
}

func (r *statsRecorder) otherError(err error) {
	r.mu.Lock()
	r.setErrLocked(err)
	r.mu.Unlock()
}

func (r *statsRecorder) snapshot() SyncStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.s
}

func (r *statsRecorder) last() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastErr
}

// remotePeerConn is implemented by connections that know the authenticated
// id of the remote peer.
type remotePeerConn interface {
	RemotePeer() PeerID
}

// sendTo dials peer, sends env and closes the connection, bounded by timeout.
func sendTo(ctx context.Context, transport SecureTransport, peer PeerDescriptor, env Envelope, timeout time.Duration) error {
	_, err := sendToPeer(ctx, transport, peer, env, timeout)
	return err
}

// sendToPeer is sendTo that also reports the authenticated id of the peer
// that received env, when the connection exposes it ("" otherwise).
func sendToPeer(ctx context.Context, transport SecureTransport, peer PeerDescriptor, env Envelope, timeout time.Duration) (PeerID, error) {
	if timeout <= 0 {
		timeout = DefaultDialTimeout
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := transport.Dial(dctx, peer)
	if err != nil {
		return "", fmt.Errorf("mesh: dial %q: %w", peer.ID, err)
	}
	defer conn.Close()
	if err := conn.Send(dctx, env); err != nil {
		return "", fmt.Errorf("mesh: send to %q: %w", peer.ID, err)
	}
	var remote PeerID
	if rp, ok := conn.(remotePeerConn); ok {
		remote = rp.RemotePeer()
	}
	return remote, nil
}

// GossipWorker periodically pushes the local state snapshot to a static peer
// list. It is send-only: inbound state is merged by a Discovery listener with a
// handler registered for the state type (see SyncWorker).
type GossipWorker struct {
	mu          sync.Mutex
	state       MeshState
	peers       []PeerDescriptor
	transport   SecureTransport
	interval    time.Duration
	dialTimeout time.Duration
	running     bool
	stopped     bool
	cancel      context.CancelFunc
	stats       statsRecorder
}

// GossipWorkerConfig configures the gossip worker.
type GossipWorkerConfig struct {
	State     MeshState
	Peers     []PeerDescriptor
	Transport SecureTransport
	Interval  time.Duration
	// DialTimeout bounds each dial + send (default DefaultDialTimeout).
	DialTimeout time.Duration
}

// NewGossipWorker creates a new gossip worker. Peers with an empty ID are
// dropped and duplicates are removed.
func NewGossipWorker(cfg GossipWorkerConfig) *GossipWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	seen := make(map[PeerID]struct{}, len(cfg.Peers))
	peers := make([]PeerDescriptor, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		id := PeerID(strings.TrimSpace(string(p.ID)))
		if id == "" || len(id) > MaxPeerIDLen {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		p.ID = id
		peers = append(peers, p)
	}
	return &GossipWorker{
		state:       cfg.State,
		peers:       peers,
		transport:   cfg.Transport,
		interval:    cfg.Interval,
		dialTimeout: cfg.DialTimeout,
	}
}

// Start runs the gossip loop and blocks until ctx is done or Stop is called.
// A worker runs at most once; Start after Stop returns immediately.
func (w *GossipWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running || w.stopped {
		w.mu.Unlock()
		return
	}
	w.running = true
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.mu.Unlock()
	defer w.Stop()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			w.gossip(runCtx)
		}
	}
}

// Stop halts the gossip worker. It is idempotent and safe to call before Start.
func (w *GossipWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	if w.cancel != nil {
		w.cancel()
	}
}

// Peers returns the current peer list.
func (w *GossipWorker) Peers() []PeerDescriptor {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]PeerDescriptor, len(w.peers))
	copy(out, w.peers)
	return out
}

// Stats returns cumulative gossip counters.
func (w *GossipWorker) Stats() SyncStats { return w.stats.snapshot() }

// LastError returns the most recent gossip error, or nil.
func (w *GossipWorker) LastError() error { return w.stats.last() }

func (w *GossipWorker) gossip(ctx context.Context) {
	w.stats.round()
	if w.state == nil || w.transport == nil {
		return
	}
	snapshot, err := w.state.LocalSnapshot(ctx)
	if err != nil {
		w.stats.otherError(fmt.Errorf("mesh: snapshot: %w", err))
		return
	}
	envelope := Envelope{StateType: w.state.Type(), Payload: snapshot}
	for _, peer := range w.Peers() {
		if ctx.Err() != nil {
			return
		}
		if err := sendTo(ctx, w.transport, peer, envelope, w.dialTimeout); err != nil {
			w.stats.sendError(err)
			continue
		}
		w.stats.sent()
	}
}
