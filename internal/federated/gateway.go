package federated

import (
	"context"
	"sync"
)

// NodeRegistration represents a federated node registration.
type NodeRegistration struct {
	NodeID     string
	Endpoint   string
	PublicKey  []byte
	Algorithms []string
}

// GatewayRegistry stores federated node registrations.
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

// Register adds or updates a node registration.
func (g *GatewayRegistry) Register(_ context.Context, reg *NodeRegistration) error {
	if reg == nil || reg.NodeID == "" {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	entry := &NodeRegistration{
		NodeID:     reg.NodeID,
		Endpoint:   reg.Endpoint,
		PublicKey:  append([]byte(nil), reg.PublicKey...),
		Algorithms: append([]string(nil), reg.Algorithms...),
	}
	g.nodes[reg.NodeID] = entry
	g.latest = entry
	g.history = append(g.history, entry)
	return nil
}

// Node returns a node registration by ID.
func (g *GatewayRegistry) Node(nodeID string) (*NodeRegistration, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[nodeID]
	if !ok {
		return nil, false
	}
	out := *n
	out.PublicKey = append([]byte(nil), n.PublicKey...)
	out.Algorithms = append([]string(nil), n.Algorithms...)
	return &out, true
}

// Latest returns the most recently registered node.
func (g *GatewayRegistry) Latest() *NodeRegistration {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.latest == nil {
		return nil
	}
	out := *g.latest
	out.PublicKey = append([]byte(nil), g.latest.PublicKey...)
	out.Algorithms = append([]string(nil), g.latest.Algorithms...)
	return &out
}

// History returns a shallow copy of registration history.
func (g *GatewayRegistry) History() []*NodeRegistration {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*NodeRegistration, 0, len(g.history))
	for _, n := range g.history {
		entry := *n
		entry.PublicKey = append([]byte(nil), n.PublicKey...)
		entry.Algorithms = append([]string(nil), n.Algorithms...)
		out = append(out, &entry)
	}
	return out
}
