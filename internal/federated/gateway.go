package federated

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sync"
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
)

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
type GatewayRegistry struct {
	mu      sync.RWMutex
	nodes   map[string]*NodeRegistration
	latest  *NodeRegistration
	history []*NodeRegistration
}

// NewGatewayRegistry creates a new registry.
func NewGatewayRegistry() *GatewayRegistry {
	return &GatewayRegistry{nodes: make(map[string]*NodeRegistration)}
}

// Register adds or updates a node registration after validating it.
func (g *GatewayRegistry) Register(_ context.Context, reg *NodeRegistration) error {
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
	existing, ok := g.nodes[reg.NodeID]
	if !ok && len(g.nodes) >= MaxNodes {
		return ErrRegistryFull
	}
	if ok && len(existing.PublicKey) > 0 && !bytes.Equal(existing.PublicKey, entry.PublicKey) {
		return ErrNodeKeyConflict
	}
	g.nodes[reg.NodeID] = entry
	g.latest = entry
	if len(g.history) >= MaxRegistrationHistory {
		n := copy(g.history, g.history[len(g.history)-MaxRegistrationHistory+1:])
		for i := n; i < len(g.history); i++ {
			g.history[i] = nil
		}
		g.history = g.history[:n]
	}
	g.history = append(g.history, entry)
	return nil
}

// Deregister removes a node (e.g. before an administrator-approved key
// rotation). It returns false if the node was not registered.
func (g *GatewayRegistry) Deregister(_ context.Context, nodeID string) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.nodes[nodeID]
	if !ok {
		return false
	}
	delete(g.nodes, nodeID)
	if g.latest == n {
		g.latest = nil
	}
	return true
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
