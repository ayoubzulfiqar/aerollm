package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// DiscoveryConfig configures peer discovery.
type DiscoveryConfig struct {
	LocalID      PeerID
	BindAddress  string
	Peers        []PeerDescriptor
	Advertise    PeerDescriptor
	Transport    SecureTransport
}

// Discovery discovers and tracks mesh peers.
type Discovery struct {
	mu         sync.RWMutex
	cfg        DiscoveryConfig
	peers      map[PeerID]PeerDescriptor
	updates    chan PeerDescriptor
	done       chan struct{}
	running    bool
}

// NewDiscovery creates a new discovery service.
func NewDiscovery(cfg DiscoveryConfig) *Discovery {
	return &Discovery{
		cfg:     cfg,
		peers:  make(map[PeerID]PeerDescriptor),
		updates: make(chan PeerDescriptor, 64),
		done:    make(chan struct{}),
	}
}

// Start begins peer discovery.
func (d *Discovery) Start(ctx context.Context) {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return
	}
	d.running = true
	d.mu.Unlock()

	if d.cfg.Advertise.ID != "" && d.cfg.Transport != nil {
		_, _ = d.cfg.Transport.Listen(ctx, d.cfg.Advertise.Address)
	}

	if d.cfg.Transport != nil && d.cfg.LocalID != "" {
		d.peers[d.cfg.LocalID] = PeerDescriptor{
			ID:      d.cfg.LocalID,
			Address: d.cfg.BindAddress,
			Meta:    map[string]string{"role": "self"},
		}
	}

	go d.discoverLoop(ctx)
}

// Stop halts discovery.
func (d *Discovery) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.running {
		return
	}
	d.running = false
	close(d.done)
}

// Peers returns the current peer set.
func (d *Discovery) Peers() []PeerDescriptor {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]PeerDescriptor, 0, len(d.peers))
	for _, p := range d.peers {
		out = append(out, p)
	}
	return out
}

// Updates returns a channel of peer change events.
func (d *Discovery) Updates() <-chan PeerDescriptor {
	return d.updates
}

func (d *Discovery) discoverLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for _, peer := range d.cfg.Peers {
		d.addPeer(peer)
	}

	for {
		select {
		case <-ctx.Done():
			d.Stop()
			return
		case <-d.done:
			return
		case <-ticker.C:
			for _, peer := range d.cfg.Peers {
				if d.cfg.Transport == nil || d.cfg.LocalID == "" {
					continue
				}
				conn, err := d.cfg.Transport.Dial(ctx, peer)
				if err != nil {
					continue
				}
				recv, err := conn.Receive(ctx)
				if err != nil {
					_ = conn.Close()
					continue
				}
				for env := range recv {
					var desc PeerDescriptor
					if err := json.Unmarshal(env.Payload, &desc); err != nil {
						continue
					}
					desc.ID = env.From
					d.addPeer(desc)
				}
				_ = conn.Close()
			}
		}
	}
}

func (d *Discovery) addPeer(desc PeerDescriptor) {
	if desc.ID == "" || desc.ID == d.cfg.LocalID {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	existing, ok := d.peers[desc.ID]
	if !ok {
		d.peers[desc.ID] = desc
		select {
		case d.updates <- desc:
		default:
		}
		return
	}
	if desc.Address != "" && existing.Address != desc.Address {
		existing.Address = desc.Address
		d.peers[desc.ID] = existing
		select {
		case d.updates <- existing:
		default:
		}
	}
}

// SyncWorker periodically gossips state to discovered peers.
type SyncWorker struct {
	mu          sync.RWMutex
	state       MeshState
	discovery   *Discovery
	interval    time.Duration
	running     bool
	done        chan struct{}
}

// SyncWorkerConfig configures the sync worker.
type SyncWorkerConfig struct {
	State     MeshState
	Discovery *Discovery
	Interval  time.Duration
}

// NewSyncWorker creates a new sync worker.
func NewSyncWorker(cfg SyncWorkerConfig) *SyncWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &SyncWorker{
		state:     cfg.State,
		discovery: cfg.Discovery,
		interval:  cfg.Interval,
		done:      make(chan struct{}),
	}
}

// Start begins the gossip loop.
func (w *SyncWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	if w.discovery != nil {
		w.discovery.Start(ctx)
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.Stop()
			return
		case <-w.done:
			return
		case peer, ok := <-w.discovery.Updates():
			if !ok {
				continue
			}
			w.syncPeer(ctx, peer)
		case <-ticker.C:
			for _, peer := range w.discovery.Peers() {
				w.syncPeer(ctx, peer)
			}
		}
	}
}

// Stop halts the sync worker.
func (w *SyncWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return
	}
	w.running = false
	close(w.done)
	if w.discovery != nil {
		w.discovery.Stop()
	}
}

func (w *SyncWorker) syncPeer(ctx context.Context, peer PeerDescriptor) {
	if w.state == nil || peer.ID == "" {
		return
	}

	snapshot, err := w.state.LocalSnapshot(ctx)
	if err != nil {
		return
	}

	envelope := Envelope{
		StateType: w.state.Type(),
		Payload:   snapshot,
		Received:  time.Now(),
	}

	if w.discovery != nil && w.discovery.cfg.Transport != nil {
		conn, err := w.discovery.cfg.Transport.Dial(ctx, peer)
		if err != nil {
			return
		}
		if err := conn.Send(ctx, envelope); err != nil {
			_ = conn.Close()
			return
		}
		_ = conn.Close()
	}
}

// MeshConfig holds mesh configuration.
type MeshConfig struct {
	Enabled          bool
	BindAddress      string
	PeerAddresses    []string
	GossipInterval   time.Duration
	LocalPeerID      PeerID
}

// DefaultMeshConfig returns default mesh configuration.
func DefaultMeshConfig() MeshConfig {
	return MeshConfig{
		Enabled:        false,
		BindAddress:    "/ip4/0.0.0.0/tcp/0",
		PeerAddresses:  nil,
		GossipInterval: 5 * time.Second,
		LocalPeerID:    PeerID(fmt.Sprintf("node-%d", time.Now().UnixNano())),
	}
}

// PluginRegistrySync bridges plugin registry to mesh gossip.
type PluginRegistrySync struct {
	Registry LWWElementSet
}

// LocalSnapshot serializes plugin metadata.
func (p *PluginRegistrySync) LocalSnapshot(_ context.Context) (json.RawMessage, error) {
	return p.Registry.Snapshot()
}

// Merge incorporates remote plugin registry state.
func (p *PluginRegistrySync) Merge(_ context.Context, remote json.RawMessage) error {
	return p.Registry.Merge(nil, remote)
}

// Type returns the CRDT type name.
func (p *PluginRegistrySync) Type() string { return "plugin-registry" }

// NewPluginRegistrySync creates a plugin-registry-backed mesh state.
func NewPluginRegistrySync() *PluginRegistrySync {
	return &PluginRegistrySync{Registry: *NewLWWElementSet("plugin-registry")}
}
