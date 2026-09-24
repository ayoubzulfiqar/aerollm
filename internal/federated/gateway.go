package federated

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Registry limits.
const (
	// MaxNodes bounds the number of distinct registered nodes.
	MaxNodes = 10000
	// MaxRegistrationHistory bounds the retained registration history.
	MaxRegistrationHistory = 1000
	// MaxNodeIDLength is the maximum NodeID length.
	MaxNodeIDLength = 128
	// MaxEndpointLength is the maximum Endpoint URL length.
	MaxEndpointLength = 2048
	// MaxAlgorithms is the maximum number of advertised algorithms.
	MaxAlgorithms = 16
	// MaxAlgorithmLength is the maximum length of a single algorithm name.
	MaxAlgorithmLength = 64
)

var (
	// ErrInvalidRegistration is returned for malformed node registrations.
	ErrInvalidRegistration = errors.New("federated: invalid node registration")
	// ErrRegistryFull is returned when MaxNodes distinct nodes are registered.
	ErrRegistryFull = errors.New("federated: node registry is full")
	// ErrNodeKeyConflict is returned when a registration tries to replace the
	// public key pinned for an existing node. Key rotation requires an explicit
	// Deregister first.
	ErrNodeKeyConflict = errors.New("federated: node already registered with a different public key")
	// ErrAttestationRevoked is returned by RegisterAttested when the
	// operator attestation was issued before the node was last deregistered.
	ErrAttestationRevoked = errors.New("federated: registration attestation predates node deregistration")
)

// Persistence buckets for the registry.
const (
	bucketNodes      = "federated_nodes"
	bucketTombstones = "federated_tombstones"
)

type persistedNode struct {
	NodeID       string    `json:"node_id"`
	Endpoint     string    `json:"endpoint,omitempty"`
	PublicKey    []byte    `json:"public_key,omitempty"`
	Algorithms   []string  `json:"algorithms,omitempty"`
	RegisteredAt time.Time `json:"registered_at"`
}

type persistedTombstone struct {
	DeregisteredAt time.Time `json:"deregistered_at"`
}

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// NodeRegistration represents a federated node registration.
type NodeRegistration struct {
	NodeID     string
	Endpoint   string
	PublicKey  []byte
	Algorithms []string
}

// Validate checks the registration fields.
func (r *NodeRegistration) Validate() error {
	if r == nil {
		return fmt.Errorf("%w: nil registration", ErrInvalidRegistration)
	}
	if r.NodeID == "" || len(r.NodeID) > MaxNodeIDLength || !nodeIDPattern.MatchString(r.NodeID) {
		return fmt.Errorf("%w: node_id must be 1-%d chars of [A-Za-z0-9._:-]", ErrInvalidRegistration, MaxNodeIDLength)
	}
	if r.Endpoint != "" {
		if err := validateEndpoint(r.Endpoint); err != nil {
			return err
		}
	}
	if len(r.PublicKey) != 0 && len(r.PublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: public key must be a %d-byte ed25519 key", ErrInvalidRegistration, ed25519.PublicKeySize)
	}
	if len(r.Algorithms) > MaxAlgorithms {
		return fmt.Errorf("%w: at most %d algorithms", ErrInvalidRegistration, MaxAlgorithms)
	}
	for _, a := range r.Algorithms {
		if a == "" || len(a) > MaxAlgorithmLength || !nodeIDPattern.MatchString(a) {
			return fmt.Errorf("%w: algorithm names must be 1-%d chars of [A-Za-z0-9._:-]", ErrInvalidRegistration, MaxAlgorithmLength)
		}
	}
	return nil
}

func validateEndpoint(endpoint string) error {
	if len(endpoint) > MaxEndpointLength {
		return fmt.Errorf("%w: endpoint too long", ErrInvalidRegistration)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("%w: endpoint is not a valid URL", ErrInvalidRegistration)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: endpoint scheme must be http or https", ErrInvalidRegistration)
	}
	if u.Host == "" || u.Hostname() == "" {
		return fmt.Errorf("%w: endpoint must include a host", ErrInvalidRegistration)
	}
	if u.User != nil {
		return fmt.Errorf("%w: endpoint must not contain credentials", ErrInvalidRegistration)
	}
	return nil
}

// GatewayRegistry stores federated node registrations. It is safe for
// concurrent use and bounded in memory (MaxNodes, MaxRegistrationHistory).
//
// A node's public key is pinned on first registration: later registrations
// for the same NodeID with a different key are rejected with
// ErrNodeKeyConflict, so an unauthenticated caller cannot take over a node's
// signing identity. Registrations with the same key may update the endpoint.
//
// Deregistration leaves a tombstone so that operator attestations issued
// before the deregistration cannot re-register the node (RegisterAttested).
//
// Persistence is opt-in (EnablePersistence): registrations and tombstones
// are written through before the in-memory state changes, so a failed write
// leaves the registry unchanged.
type GatewayRegistry struct {
	mu         sync.RWMutex
	nodes      map[string]*NodeRegistration
	latest     *NodeRegistration
	history    []*NodeRegistration
	tombstones map[string]time.Time
	store      persist.Store
}

// NewGatewayRegistry creates a new registry.
func NewGatewayRegistry() *GatewayRegistry {
	return &GatewayRegistry{nodes: make(map[string]*NodeRegistration)}
}

// Register adds or updates a node registration after validating it. It
// performs no caller authentication; network-facing registration must go
// through RegisterNodeHandlerWithAuth.
func (g *GatewayRegistry) Register(ctx context.Context, reg *NodeRegistration) error {
	return g.register(ctx, reg, time.Time{})
}

// RegisterAttested is Register for registrations authorized by an operator
// attestation issued at issuedAt: it fails with ErrAttestationRevoked if the
// node was deregistered at or after issuedAt, so a captured attestation
// cannot resurrect a removed node.
func (g *GatewayRegistry) RegisterAttested(ctx context.Context, reg *NodeRegistration, issuedAt time.Time) error {
	if issuedAt.IsZero() {
		return fmt.Errorf("%w: missing attestation issue time", ErrInvalidRegistration)
	}
	return g.register(ctx, reg, issuedAt)
}

func (g *GatewayRegistry) register(_ context.Context, reg *NodeRegistration, issuedAt time.Time) error {
	if g == nil {
		return fmt.Errorf("federated: registry not initialized")
	}
	if err := reg.Validate(); err != nil {
		return err
	}
	entry := &NodeRegistration{
		NodeID:     reg.NodeID,
		Endpoint:   reg.Endpoint,
		PublicKey:  append([]byte(nil), reg.PublicKey...),
		Algorithms: append([]string(nil), reg.Algorithms...),
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.nodes == nil {
		g.nodes = make(map[string]*NodeRegistration)
	}
	if !issuedAt.IsZero() {
		if t, ok := g.tombstones[reg.NodeID]; ok && !issuedAt.After(t) {
			return ErrAttestationRevoked
		}
	}
	existing, ok := g.nodes[reg.NodeID]
	if !ok && len(g.nodes) >= MaxNodes {
		return ErrRegistryFull
	}
	if ok && len(existing.PublicKey) > 0 && !bytes.Equal(existing.PublicKey, entry.PublicKey) {
		return ErrNodeKeyConflict
	}
	if g.store != nil {
		rec := persistedNode{
			NodeID:       entry.NodeID,
			Endpoint:     entry.Endpoint,
			PublicKey:    entry.PublicKey,
			Algorithms:   entry.Algorithms,
			RegisteredAt: time.Now().UTC(),
		}
		if err := g.store.Put(bucketNodes, entry.NodeID, rec); err != nil {
			return fmt.Errorf("%w: %v", ErrPersistence, err)
		}
	}
	g.nodes[reg.NodeID] = entry
	g.latest = entry
	g.appendHistoryLocked(entry)
	return nil
}

func (g *GatewayRegistry) appendHistoryLocked(entry *NodeRegistration) {
	if len(g.history) >= MaxRegistrationHistory {
		n := copy(g.history, g.history[len(g.history)-MaxRegistrationHistory+1:])
		for i := n; i < len(g.history); i++ {
			g.history[i] = nil
		}
		g.history = g.history[:n]
	}
	g.history = append(g.history, entry)
}

// Deregister removes a node (e.g. before an administrator-approved key
// rotation). It returns false if the node was not registered or the removal
// could not be persisted; use DeregisterNode to observe the error.
func (g *GatewayRegistry) Deregister(ctx context.Context, nodeID string) bool {
	ok, err := g.DeregisterNode(ctx, nodeID)
	return ok && err == nil
}

// DeregisterNode removes a node and records a tombstone that revokes every
// operator attestation issued up to now for that node. With persistence
// enabled the tombstone and removal are persisted first; on error the
// registry is unchanged.
func (g *GatewayRegistry) DeregisterNode(_ context.Context, nodeID string) (bool, error) {
	if g == nil {
		return false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[nodeID]
	if !ok {
		return false, nil
	}
	now := time.Now().UTC()
	if g.store != nil {
		if err := g.store.Put(bucketTombstones, nodeID, persistedTombstone{DeregisteredAt: now}); err != nil {
			return false, fmt.Errorf("%w: %v", ErrPersistence, err)
		}
		if err := g.store.Delete(bucketNodes, nodeID); err != nil {
			return false, fmt.Errorf("%w: %v", ErrPersistence, err)
		}
	}
	g.setTombstoneLocked(nodeID, now)
	delete(g.nodes, nodeID)
	if g.latest == n {
		g.latest = nil
	}
	return true, nil
}

// setTombstoneLocked records a tombstone, evicting the oldest one when
// MaxNodes tombstones are held.
func (g *GatewayRegistry) setTombstoneLocked(nodeID string, t time.Time) {
	if g.tombstones == nil {
		g.tombstones = make(map[string]time.Time)
	}
	if _, ok := g.tombstones[nodeID]; !ok && len(g.tombstones) >= MaxNodes {
		var oldestID string
		var oldest time.Time
		for id, ts := range g.tombstones {
			if oldestID == "" || ts.Before(oldest) {
				oldestID, oldest = id, ts
			}
		}
		delete(g.tombstones, oldestID)
		if g.store != nil {
			_ = g.store.Delete(bucketTombstones, oldestID)
		}
	}
	g.tombstones[nodeID] = t
}

// DeregisteredAt reports when nodeID was last deregistered.
func (g *GatewayRegistry) DeregisteredAt(nodeID string) (time.Time, bool) {
	if g == nil {
		return time.Time{}, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	t, ok := g.tombstones[nodeID]
	return t, ok
}

// EnablePersistence loads registrations and tombstones from ps (buckets
// "federated_nodes" and "federated_tombstones") and writes later changes
// through to it. It must be called on an empty registry before it serves
// requests. Invalid stored documents are skipped and reported in the
// returned error after all valid documents were loaded.
func (g *GatewayRegistry) EnablePersistence(ps persist.Store) error {
	if g == nil || ps == nil {
		return fmt.Errorf("federated: nil registry or store")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.store != nil {
		return fmt.Errorf("federated: persistence already enabled")
	}
	if len(g.nodes) > 0 || len(g.tombstones) > 0 {
		return fmt.Errorf("federated: enable persistence before registering nodes")
	}
	var bad []string
	type loaded struct {
		reg *NodeRegistration
		at  time.Time
	}
	var nodes []loaded
	err := ps.ForEach(bucketNodes, func(key string, raw json.RawMessage) error {
		var rec persistedNode
		if err := json.Unmarshal(raw, &rec); err != nil {
			bad = append(bad, key)
			return nil
		}
		reg := &NodeRegistration{NodeID: rec.NodeID, Endpoint: rec.Endpoint, PublicKey: rec.PublicKey, Algorithms: rec.Algorithms}
		if rec.NodeID != key || reg.Validate() != nil {
			bad = append(bad, key)
			return nil
		}
		if len(nodes) < MaxNodes {
			nodes = append(nodes, loaded{reg: reg, at: rec.RegisteredAt})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("federated: load nodes: %w", err)
	}
	tombstones := make(map[string]time.Time)
	err = ps.ForEach(bucketTombstones, func(key string, raw json.RawMessage) error {
		var rec persistedTombstone
		if err := json.Unmarshal(raw, &rec); err != nil || rec.DeregisteredAt.IsZero() {
			// Fail closed: an unreadable tombstone revokes every
			// attestation issued before now.
			rec.DeregisteredAt = time.Now().UTC()
			bad = append(bad, key)
		}
		tombstones[key] = rec.DeregisteredAt
		return nil
	})
	if err != nil {
		return fmt.Errorf("federated: load tombstones: %w", err)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if !nodes[i].at.Equal(nodes[j].at) {
			return nodes[i].at.Before(nodes[j].at)
		}
		return nodes[i].reg.NodeID < nodes[j].reg.NodeID
	})
	if g.nodes == nil {
		g.nodes = make(map[string]*NodeRegistration, len(nodes))
	}
	for _, n := range nodes {
		g.nodes[n.reg.NodeID] = n.reg
		g.latest = n.reg
		g.appendHistoryLocked(n.reg)
	}
	g.tombstones = tombstones
	g.store = ps
	if len(bad) > 0 {
		return fmt.Errorf("federated: skipped %d undecodable registry documents (%s)", len(bad), strings.Join(bad, ","))
	}
	return nil
}

// PublicKey returns the pinned ed25519 public key of a node. It implements
// PublicKeyResolver so the registry can back FedAvgAggregatorWithVerify.
func (g *GatewayRegistry) PublicKey(nodeID string) (ed25519.PublicKey, bool) {
	if g == nil {
		return nil, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[nodeID]
	if !ok || len(n.PublicKey) != ed25519.PublicKeySize {
		return nil, false
	}
	return append(ed25519.PublicKey(nil), n.PublicKey...), true
}

func copyRegistration(n *NodeRegistration) *NodeRegistration {
	out := *n
	out.PublicKey = append([]byte(nil), n.PublicKey...)
	out.Algorithms = append([]string(nil), n.Algorithms...)
	return &out
}

// Node returns a node registration by ID.
func (g *GatewayRegistry) Node(nodeID string) (*NodeRegistration, bool) {
	if g == nil {
		return nil, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return copyRegistration(n), true
}

// Latest returns the most recently registered node.
func (g *GatewayRegistry) Latest() *NodeRegistration {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.latest == nil {
		return nil
	}
	return copyRegistration(g.latest)
}

// History returns a deep copy of the retained registration history (at most
// MaxRegistrationHistory entries, oldest first).
func (g *GatewayRegistry) History() []*NodeRegistration {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*NodeRegistration, 0, len(g.history))
	for _, n := range g.history {
		out = append(out, copyRegistration(n))
	}
	return out
}

// Len returns the number of registered nodes.
func (g *GatewayRegistry) Len() int {
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}
