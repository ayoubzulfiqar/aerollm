package mesh

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// MeshState is the interface for CRDT-backed distributed state.
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

// PeerDiscovery finds and tracks mesh peers.
type PeerDiscovery interface {
	Discover(ctx context.Context) (<-chan PeerDescriptor, error)
	Advertise(ctx context.Context, descriptor PeerDescriptor) error
	Close() error
}

// SecureTransport handles encrypted communication with peers.
type SecureTransport interface {
	Dial(ctx context.Context, peer PeerDescriptor) (PeerConn, error)
	Listen(ctx context.Context, address string) (PeerListener, error)
	Close() error
}

// PeerConn is an encrypted connection to a peer.
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
	From     PeerID
	StateType string
	Payload  json.RawMessage
	Received time.Time
}

// GossipWorker periodically synchronizes state across peers.
type GossipWorker struct {
	mu         sync.RWMutex
	state      MeshState
	peers     []PeerDescriptor
	transport SecureTransport
	interval   time.Duration
	running    bool
	done       chan struct{}
}

// GossipWorkerConfig configures the gossip worker.
type GossipWorkerConfig struct {
	State     MeshState
	Peers     []PeerDescriptor
	Transport SecureTransport
	Interval  time.Duration
}

// NewGossipWorker creates a new gossip worker.
func NewGossipWorker(cfg GossipWorkerConfig) *GossipWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &GossipWorker{
		state:      cfg.State,
		peers:      cfg.Peers,
		transport:  cfg.Transport,
		interval:   cfg.Interval,
		done:       make(chan struct{}),
	}
}

// Start begins the background gossip loop.
func (w *GossipWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.Stop()
			return
		case <-w.done:
			return
		case <-ticker.C:
			w.gossip(ctx)
		}
	}
}

// Stop halts the gossip worker.
func (w *GossipWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return
	}
	w.running = false
	close(w.done)
}

// Peers returns the current peer list.
func (w *GossipWorker) Peers() []PeerDescriptor {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]PeerDescriptor, len(w.peers))
	copy(out, w.peers)
	return out
}

func (w *GossipWorker) gossip(ctx context.Context) {
	snapshot, err := w.state.LocalSnapshot(ctx)
	if err != nil {
		return
	}

	envelope := Envelope{
		StateType: w.state.Type(),
		Payload:   snapshot,
		Received:  time.Now(),
	}

	for _, peer := range w.Peers() {
		if w.transport == nil {
			continue
		}
		conn, err := w.transport.Dial(ctx, peer)
		if err != nil {
			continue
		}
		_ = conn.Send(ctx, envelope)
		_ = conn.Close()
	}
}
