package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	// DefaultMaxPeers bounds the number of peers tracked by Discovery.
	DefaultMaxPeers = 1024
	// DefaultDiscoveryInterval is how often Discovery re-announces itself.
	DefaultDiscoveryInterval = 2 * time.Second
	// AnnounceStateType is the reserved envelope type used for peer announcements.
	AnnounceStateType = "mesh/announce"

	maxInboundConns    = 64
	inboundIdleTimeout = 30 * time.Second
)

// EnvelopeHandler processes an inbound envelope of a registered state type.
type EnvelopeHandler func(ctx context.Context, env Envelope) error

// DiscoveryConfig configures peer discovery.
type DiscoveryConfig struct {
	LocalID     PeerID
	BindAddress string
	// Peers are seed peers. Entries with an empty ID or Address, or equal to
	// LocalID, are ignored. Seeds are never evicted.
	Peers []PeerDescriptor
	// Advertise is the descriptor announced to peers. Defaults to
	// {ID: LocalID, Address: BindAddress}.
	Advertise PeerDescriptor
	Transport SecureTransport
	// MaxPeers bounds the peer table (default DefaultMaxPeers).
	MaxPeers int
	// Interval between announcement rounds (default DefaultDiscoveryInterval).
	Interval time.Duration
	// DialTimeout bounds each outbound dial + send (default DefaultDialTimeout).
	DialTimeout time.Duration
	// PeerTTL evicts non-seed peers not heard from within this window
	// (default 30 × Interval; negative disables eviction).
	PeerTTL time.Duration
}

type peerEntry struct {
	desc     PeerDescriptor
	seed     bool
	lastSeen time.Time
}

// Discovery tracks mesh peers. When a transport is configured it listens for
// inbound connections, periodically announces itself to known peers, learns
// peers from their announcements and dispatches other envelopes to handlers
// registered with Handle.
//
// A Discovery runs at most once: Start after Stop is a no-op.
type Discovery struct {
	cfg  DiscoveryConfig
	self PeerDescriptor

	mu        sync.RWMutex
	peers     map[PeerID]*peerEntry
	handlers  map[string]EnvelopeHandler
	listener  PeerListener
	started   bool
	stopped   bool
	cancel    context.CancelFunc
	stopAfter func() bool

	updates    chan PeerDescriptor
	inboundSem chan struct{}
	wg         sync.WaitGroup
	exited     chan struct{}
	stats      statsRecorder
}

// NewDiscovery creates a new discovery service.
func NewDiscovery(cfg DiscoveryConfig) *Discovery {
	if cfg.MaxPeers <= 0 {
		cfg.MaxPeers = DefaultMaxPeers
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultDiscoveryInterval
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.PeerTTL == 0 {
		cfg.PeerTTL = 30 * cfg.Interval
	}
	self := cfg.Advertise
	if self.ID == "" {
		self = PeerDescriptor{ID: cfg.LocalID, Address: cfg.BindAddress, Meta: cfg.Advertise.Meta}
	}
	if cfg.LocalID == "" {
		cfg.LocalID = self.ID
	}
	if self.Address == "" {
		self.Address = cfg.BindAddress
	}
	if s, err := sanitizePeer(self); err == nil {
		self = s
	}
	d := &Discovery{
		cfg:        cfg,
		self:       self,
		peers:      make(map[PeerID]*peerEntry),
		handlers:   make(map[string]EnvelopeHandler),
		updates:    make(chan PeerDescriptor, 64),
		inboundSem: make(chan struct{}, maxInboundConns),
		exited:     make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		_, _ = d.addPeer(p, true)
	}
	return d
}

// Self returns the descriptor this node announces. When the bind address
// used port 0, the address reports the port actually bound once Start has
// run (for listeners that expose it, such as TLSTransport's).
func (d *Discovery) Self() PeerDescriptor {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return copyPeer(d.self)
}

// Handle registers h for inbound envelopes of stateType, replacing any previous
// handler. The reserved AnnounceStateType cannot be overridden.
func (d *Discovery) Handle(stateType string, h EnvelopeHandler) error {
	if stateType == "" || stateType == AnnounceStateType {
		return fmt.Errorf("mesh: cannot register handler for state type %q", stateType)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if h == nil {
		delete(d.handlers, stateType)
		return nil
	}
	d.handlers[stateType] = h
	return nil
}

// Start begins peer discovery in background goroutines and returns immediately.
// Discovery stops when ctx is done or Stop is called.
func (d *Discovery) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started || d.stopped {
		return
	}
	d.started = true
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel

	if d.cfg.Transport != nil && d.self.ID != "" {
		// Listen on the bind address; the advertised address may differ
		// (NAT, load balancer, public name).
		listenAddr := d.cfg.BindAddress
		if listenAddr == "" {
			listenAddr = d.self.Address
		}
		l, err := d.cfg.Transport.Listen(runCtx, listenAddr)
		if err != nil {
			d.stats.otherError(fmt.Errorf("mesh: listen: %w", err))
		} else {
			d.listener = l
			if d.self.Address == listenAddr {
				d.self.Address = resolveBoundAddress(d.self.Address, l)
			}
			d.wg.Add(1)
			go d.acceptLoop(runCtx, l)
		}
	}
	d.wg.Add(1)
	go d.refreshLoop(runCtx)
	// Registered last: if ctx is already done, Stop runs in its own goroutine
	// and blocks on d.mu until Start returns.
	d.stopAfter = context.AfterFunc(ctx, d.Stop)
}

// resolveBoundAddress replaces port 0 in a configured address with the port
// the listener actually bound, keeping the configured host (or the bound one
// when none was configured). Other addresses are returned unchanged.
func resolveBoundAddress(configured string, l PeerListener) string {
	al, ok := l.(interface{ Addr() net.Addr })
	if !ok {
		return configured
	}
	hostport, err := ParseTCPAddress(configured)
	if err != nil {
		return configured
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil || port != "0" {
		return configured
	}
	boundHost, boundPort, err := net.SplitHostPort(al.Addr().String())
	if err != nil {
		return configured
	}
	if host == "" {
		host = boundHost
	}
	return net.JoinHostPort(host, boundPort)
}

// Stop halts discovery and closes the listener. It is idempotent, does not
// block, and may be called before Start. Use Stopped to wait for goroutines.
func (d *Discovery) Stop() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	cancel, l, stopAfter := d.cancel, d.listener, d.stopAfter
	d.listener = nil
	d.mu.Unlock()

	if stopAfter != nil {
		stopAfter()
	}
	if cancel != nil {
		cancel()
	}
	if l != nil {
		_ = l.Close()
	}
	go func() {
		d.wg.Wait()
		close(d.exited)
	}()
}

// Stopped returns a channel that is closed once Stop has been called and every
// background goroutine has exited.
func (d *Discovery) Stopped() <-chan struct{} { return d.exited }

// Peers returns the current peer set (excluding self), sorted by ID.
func (d *Discovery) Peers() []PeerDescriptor {
	d.mu.RLock()
	out := make([]PeerDescriptor, 0, len(d.peers))
	for _, e := range d.peers {
		out = append(out, copyPeer(e.desc))
	}
	d.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Updates returns a channel of peer change events (new peers and address or
// metadata changes). Delivery is best-effort: events are dropped when the
// buffer is full. The channel is never closed.
func (d *Discovery) Updates() <-chan PeerDescriptor {
	return d.updates
}

// Stats returns cumulative discovery counters.
func (d *Discovery) Stats() SyncStats { return d.stats.snapshot() }

// LastError returns the most recent discovery error, or nil.
func (d *Discovery) LastError() error { return d.stats.last() }

func copyPeer(p PeerDescriptor) PeerDescriptor {
	if p.Meta != nil {
		m := make(map[string]string, len(p.Meta))
		for k, v := range p.Meta {
			m[k] = v
		}
		p.Meta = m
	}
	return p
}

// addPeer inserts or refreshes a peer. It reports whether the peer table
// changed. Invalid peers, self and peers beyond MaxPeers are rejected.
func (d *Discovery) addPeer(desc PeerDescriptor, seed bool) (bool, error) {
	clean, err := sanitizePeer(desc)
	if err != nil {
		return false, err
	}
	if clean.ID == d.cfg.LocalID || clean.ID == d.self.ID {
		return false, nil
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.peers[clean.ID]; ok {
		e.lastSeen = now
		e.seed = e.seed || seed
		if e.desc.Address == clean.Address && metaEqual(e.desc.Meta, clean.Meta) {
			return false, nil
		}
		e.desc = clean
		d.notifyLocked(clean)
		return true, nil
	}
	if len(d.peers) >= d.cfg.MaxPeers {
		return false, fmt.Errorf("mesh: peer table full (%d), ignoring %q", d.cfg.MaxPeers, clean.ID)
	}
	d.peers[clean.ID] = &peerEntry{desc: clean, seed: seed, lastSeen: now}
	d.notifyLocked(clean)
	return true, nil
}

func (d *Discovery) notifyLocked(desc PeerDescriptor) {
	select {
	case d.updates <- copyPeer(desc):
	default:
	}
}

func (d *Discovery) evictStale() {
	if d.cfg.PeerTTL < 0 {
		return
	}
	cutoff := time.Now().Add(-d.cfg.PeerTTL)
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, e := range d.peers {
		if !e.seed && e.lastSeen.Before(cutoff) {
			delete(d.peers, id)
		}
	}
}

func (d *Discovery) acceptLoop(ctx context.Context, l PeerListener) {
	defer d.wg.Done()
	for {
		conn, err := l.Accept(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, ErrListenerClosed) {
				d.stats.otherError(fmt.Errorf("mesh: accept: %w", err))
			}
			return
		}
		select {
		case d.inboundSem <- struct{}{}:
		default:
			_ = conn.Close()
			d.stats.otherError(errors.New("mesh: too many inbound connections"))
			continue
		}
		d.wg.Add(1)
		go d.serveConn(ctx, conn)
	}
}

func (d *Discovery) serveConn(ctx context.Context, conn PeerConn) {
	defer d.wg.Done()
	defer func() { <-d.inboundSem }()
	defer conn.Close()

	recv, err := conn.Receive(ctx)
	if err != nil {
		d.stats.otherError(fmt.Errorf("mesh: receive: %w", err))
		return
	}
	idle := time.NewTimer(inboundIdleTimeout)
	defer idle.Stop()
	for {
		select {
		case env, ok := <-recv:
			if !ok {
				return
			}
			d.dispatch(ctx, env)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(inboundIdleTimeout)
		case <-idle.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (d *Discovery) dispatch(ctx context.Context, env Envelope) {
	if env.From == "" || env.From == d.cfg.LocalID || env.From == d.self.ID {
		return
	}
	d.stats.received()
	if len(env.Payload) > MaxEnvelopePayloadBytes {
		d.stats.mergeError(fmt.Errorf("mesh: envelope from %q exceeds %d bytes", env.From, MaxEnvelopePayloadBytes))
		return
	}
	if env.StateType == AnnounceStateType {
		var desc PeerDescriptor
		if err := json.Unmarshal(env.Payload, &desc); err != nil {
			d.stats.mergeError(fmt.Errorf("mesh: bad announce from %q: %w", env.From, err))
			return
		}
		// The transport-provided sender id is authoritative; a peer cannot
		// announce on behalf of another id.
		desc.ID = env.From
		if _, err := d.addPeer(desc, false); err != nil {
			d.stats.mergeError(err)
		}
		return
	}
	d.mu.RLock()
	h := d.handlers[env.StateType]
	d.mu.RUnlock()
	if h == nil {
		d.stats.mergeError(fmt.Errorf("mesh: no handler for state type %q from %q", env.StateType, env.From))
		return
	}
	if err := h(ctx, env); err != nil {
		d.stats.mergeError(fmt.Errorf("mesh: handle %q from %q: %w", env.StateType, env.From, err))
	}
}

func (d *Discovery) refreshLoop(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()
	d.announce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.evictStale()
			d.announce(ctx)
		}
	}
}

func (d *Discovery) announce(ctx context.Context) {
	d.stats.round()
	self := d.Self()
	if d.cfg.Transport == nil || self.ID == "" || self.Address == "" {
		return
	}
	payload, err := json.Marshal(self)
	if err != nil {
		d.stats.otherError(err)
		return
	}
	env := Envelope{StateType: AnnounceStateType, Payload: payload}
	for _, peer := range d.Peers() {
		if ctx.Err() != nil {
			return
		}
		remote, err := sendToPeer(ctx, d.cfg.Transport, peer, env, d.cfg.DialTimeout)
		if err != nil {
			d.stats.sendError(err)
			continue
		}
		d.stats.sent()
		if remote != "" && remote != peer.ID {
			d.rekeyPeer(peer, remote)
		}
	}
}

// rekeyPeer replaces a bare-address seed (id == address, see
// MeshConfig.PeerDescriptors) with the authenticated id the transport
// reported for it, so the peer is tracked (and synced) once under its real
// id. Entries with a real id are never re-keyed.
func (d *Discovery) rekeyPeer(old PeerDescriptor, remote PeerID) {
	if !identityUnknown(old) {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.peers[old.ID]
	if !ok || e.desc.Address != old.Address {
		return
	}
	delete(d.peers, old.ID)
	if remote == d.cfg.LocalID || remote == d.self.ID {
		return // the seed was this node itself
	}
	if cur, ok := d.peers[remote]; ok {
		cur.seed = cur.seed || e.seed
		cur.lastSeen = time.Now()
		return
	}
	desc := e.desc
	desc.ID = remote
	clean, err := sanitizePeer(desc)
	if err != nil {
		return
	}
	d.peers[remote] = &peerEntry{desc: clean, seed: e.seed, lastSeen: time.Now()}
	d.notifyLocked(clean)
}

// SyncWorker periodically pushes the local state snapshot to every discovered
// peer and merges inbound snapshots of the same state type (received through
// the Discovery listener).
type SyncWorker struct {
	state       MeshState
	discovery   *Discovery
	interval    time.Duration
	dialTimeout time.Duration

	mu      sync.Mutex
	running bool
	stopped bool
	cancel  context.CancelFunc
	stats   statsRecorder
}

// SyncWorkerConfig configures the sync worker.
type SyncWorkerConfig struct {
	State     MeshState
	Discovery *Discovery
	Interval  time.Duration
	// DialTimeout bounds each dial + send (default DefaultDialTimeout).
	DialTimeout time.Duration
}

// NewSyncWorker creates a new sync worker.
func NewSyncWorker(cfg SyncWorkerConfig) *SyncWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	return &SyncWorker{
		state:       cfg.State,
		discovery:   cfg.Discovery,
		interval:    cfg.Interval,
		dialTimeout: cfg.DialTimeout,
	}
}

// Start starts discovery (if configured) and runs the sync loop, blocking until
// ctx is done or Stop is called. On return discovery has been stopped too.
// A worker runs at most once; Start after Stop returns immediately.
func (w *SyncWorker) Start(ctx context.Context) {
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

	var updates <-chan PeerDescriptor
	if w.discovery != nil {
		if w.state != nil {
			if err := w.discovery.Handle(w.state.Type(), w.mergeEnvelope); err != nil {
				w.stats.otherError(err)
			}
		}
		w.discovery.Start(runCtx)
		updates = w.discovery.Updates()
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.syncAll(runCtx)
	for {
		select {
		case <-runCtx.Done():
			return
		case peer := <-updates:
			w.syncPeer(runCtx, peer)
		case <-ticker.C:
			w.syncAll(runCtx)
		}
	}
}

// Stop halts the sync worker and its discovery. It is idempotent and safe to
// call before Start.
func (w *SyncWorker) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if w.discovery != nil {
		w.discovery.Stop()
	}
}

// Stats returns cumulative sync counters.
func (w *SyncWorker) Stats() SyncStats { return w.stats.snapshot() }

// LastError returns the most recent sync error, or nil.
func (w *SyncWorker) LastError() error { return w.stats.last() }

func (w *SyncWorker) mergeEnvelope(ctx context.Context, env Envelope) error {
	w.stats.received()
	if w.state == nil || env.StateType != w.state.Type() {
		err := fmt.Errorf("mesh: unexpected state type %q", env.StateType)
		w.stats.mergeError(err)
		return err
	}
	if err := w.state.Merge(ctx, env.Payload); err != nil {
		w.stats.mergeError(fmt.Errorf("mesh: merge from %q: %w", env.From, err))
		return err
	}
	return nil
}

func (w *SyncWorker) syncAll(ctx context.Context) {
	w.stats.round()
	if w.discovery == nil {
		return
	}
	for _, peer := range w.discovery.Peers() {
		if ctx.Err() != nil {
			return
		}
		w.syncPeer(ctx, peer)
	}
}

func (w *SyncWorker) syncPeer(ctx context.Context, peer PeerDescriptor) {
	if w.state == nil || peer.ID == "" || w.discovery == nil || w.discovery.cfg.Transport == nil {
		return
	}
	snapshot, err := w.state.LocalSnapshot(ctx)
	if err != nil {
		w.stats.otherError(fmt.Errorf("mesh: snapshot: %w", err))
		return
	}
	env := Envelope{StateType: w.state.Type(), Payload: snapshot}
	if err := sendTo(ctx, w.discovery.cfg.Transport, peer, env, w.dialTimeout); err != nil {
		w.stats.sendError(err)
		return
	}
	w.stats.sent()
}

// MeshConfig holds mesh configuration.
type MeshConfig struct {
	Enabled        bool
	BindAddress    string
	PeerAddresses  []string
	GossipInterval time.Duration
	LocalPeerID    PeerID
}

// MaxPeerAddresses caps the number of configured peer addresses.
const MaxPeerAddresses = 256

// DefaultMeshConfig returns default mesh configuration with a random node id.
func DefaultMeshConfig() MeshConfig {
	return MeshConfig{
		Enabled:        false,
		BindAddress:    "/ip4/0.0.0.0/tcp/0",
		PeerAddresses:  nil,
		GossipInterval: 5 * time.Second,
		LocalPeerID:    PeerID("node-" + randomNodeID()),
	}
}

// Validate reports configuration errors that would prevent the mesh from
// working when it is enabled.
func (c MeshConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(string(c.LocalPeerID)) == "" {
		return errors.New("mesh: LocalPeerID is required")
	}
	if len(c.LocalPeerID) > MaxPeerIDLen {
		return errors.New("mesh: LocalPeerID too long")
	}
	if c.GossipInterval <= 0 {
		return errors.New("mesh: GossipInterval must be positive")
	}
	return nil
}

// ParsePeerAddresses splits a peer list such as the AEROLLM_MESH_PEERS value on
// commas, semicolons and whitespace. Empty, oversized and duplicate entries are
// dropped and at most MaxPeerAddresses entries are returned. An empty input
// yields nil.
func ParsePeerAddresses(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || unicode.IsSpace(r)
	})
	var out []string
	seen := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		if f == "" || len(f) > MaxPeerAddressLen || hasControl(f) {
			continue
		}
		if _, dup := seen[f]; dup {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
		if len(out) >= MaxPeerAddresses {
			break
		}
	}
	return out
}

// PeerDescriptors converts PeerAddresses into seed descriptors for
// DiscoveryConfig.Peers. Each entry may be "id@address" or a bare address (then
// used as the id as well) and may itself be a comma/space separated list.
// Empty, invalid, duplicate and self entries are dropped.
func (c MeshConfig) PeerDescriptors() []PeerDescriptor {
	var out []PeerDescriptor
	seen := make(map[PeerID]struct{})
	for _, entry := range c.PeerAddresses {
		for _, a := range ParsePeerAddresses(entry) {
			id, addr := PeerID(a), a
			if before, after, ok := strings.Cut(a, "@"); ok && before != "" && after != "" {
				id, addr = PeerID(before), after
			}
			desc, err := sanitizePeer(PeerDescriptor{ID: id, Address: addr})
			if err != nil || desc.ID == c.LocalPeerID {
				continue
			}
			if _, dup := seen[desc.ID]; dup {
				continue
			}
			seen[desc.ID] = struct{}{}
			out = append(out, desc)
			if len(out) >= MaxPeerAddresses {
				return out
			}
		}
	}
	return out
}

// PluginRegistrySync bridges plugin registry metadata to mesh gossip using an
// LWW element set keyed by plugin id.
type PluginRegistrySync struct {
	Registry *LWWElementSet
}

var errNilRegistry = errors.New("mesh: plugin registry sync not initialised")

// LocalSnapshot serializes plugin metadata.
func (p *PluginRegistrySync) LocalSnapshot(_ context.Context) (json.RawMessage, error) {
	if p == nil || p.Registry == nil {
		return nil, errNilRegistry
	}
	return p.Registry.Snapshot()
}

// Merge incorporates remote plugin registry state.
func (p *PluginRegistrySync) Merge(ctx context.Context, remote json.RawMessage) error {
	if p == nil || p.Registry == nil {
		return errNilRegistry
	}
	return p.Registry.Merge(ctx, remote)
}

// Type returns the CRDT type name.
func (p *PluginRegistrySync) Type() string { return "plugin-registry" }

// NewPluginRegistrySync creates a plugin-registry-backed mesh state with a
// random replica id.
func NewPluginRegistrySync() *PluginRegistrySync {
	return NewPluginRegistrySyncForNode("")
}

// NewPluginRegistrySyncForNode creates a plugin-registry-backed mesh state whose
// local writes are attributed to node (used for deterministic LWW tie-breaks).
func NewPluginRegistrySyncForNode(node PeerID) *PluginRegistrySync {
	return &PluginRegistrySync{Registry: NewLWWElementSet(string(node))}
}

var _ MeshState = (*PluginRegistrySync)(nil)
