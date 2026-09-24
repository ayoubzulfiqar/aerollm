package graphrag

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by the graph store in a persist.Store.
const (
	NodesBucket = "graphrag_nodes"
	EdgesBucket = "graphrag_edges"
)

// ErrPartialLoad is wrapped by EnablePersistence errors when some persisted
// nodes or edges could not be decoded. Persistence is enabled and the valid
// documents are loaded; the undecodable ones are left in the persist.Store.
var ErrPartialLoad = errors.New("graphrag: some persisted graph documents could not be loaded")

// NewBboltGraphStoreWithPersistence creates a graph store whose nodes and
// edges are persisted in ps (buckets NodesBucket and EdgesBucket, e.g. a
// persist.OpenBolt file) and reloaded from it. On a partial load the store is
// returned together with an error wrapping ErrPartialLoad; on any other error
// the store is nil. ps stays owned by the caller.
func NewBboltGraphStoreWithPersistence(ps persist.Store) (*BboltGraphStore, error) {
	s := NewBboltGraphStore()
	if err := s.EnablePersistence(ps); err != nil {
		if errors.Is(err, ErrPartialLoad) {
			return s, err
		}
		return nil, err
	}
	return s, nil
}

// EnablePersistence loads the nodes and edges persisted in ps and writes
// every later upsert through to ps. Nodes and edges already in memory are
// written to ps and win over persisted copies with the same ID. On an error
// that does not wrap ErrPartialLoad the graph is unchanged and persistence
// stays disabled.
func (s *bboltGraphStore) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("graphrag: nil persist store")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.ps != nil {
		return errors.New("graphrag: persistence already enabled")
	}
	var badNodes, badEdges []string
	nodes := map[string]Node{}
	err := ps.ForEach(NodesBucket, func(key string, raw json.RawMessage) error {
		var n Node
		if json.Unmarshal(raw, &n) != nil || n.ID != key {
			badNodes = append(badNodes, key)
			return nil
		}
		nodes[key] = n
		return nil
	})
	if err != nil {
		return fmt.Errorf("graphrag: load nodes: %w", err)
	}
	edges := map[string]Edge{}
	err = ps.ForEach(EdgesBucket, func(key string, raw json.RawMessage) error {
		var e Edge
		if json.Unmarshal(raw, &e) != nil || e.ID != key || e.Source == "" || e.Target == "" {
			badEdges = append(badEdges, key)
			return nil
		}
		edges[key] = e
		return nil
	})
	if err != nil {
		return fmt.Errorf("graphrag: load edges: %w", err)
	}

	// Persist what is already in memory (writers are blocked by writeMu).
	s.mu.RLock()
	curNodes := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		curNodes = append(curNodes, n)
	}
	curEdges := make([]Edge, 0, len(s.edges))
	for _, e := range s.edges {
		curEdges = append(curEdges, e)
	}
	s.mu.RUnlock()
	for _, n := range curNodes {
		if err := ps.Put(NodesBucket, n.ID, n); err != nil {
			return fmt.Errorf("graphrag: persist node: %w", err)
		}
	}
	for _, e := range curEdges {
		if err := ps.Put(EdgesBucket, e.ID, e); err != nil {
			return fmt.Errorf("graphrag: persist edge: %w", err)
		}
	}

	s.mu.Lock()
	for id, n := range nodes {
		if _, ok := s.nodes[id]; !ok {
			s.nodes[id] = n
		}
	}
	for id, e := range edges {
		if _, ok := s.edges[id]; !ok {
			s.putEdgeLocked(e)
		}
	}
	s.mu.Unlock()
	s.ps = ps

	if len(badNodes) == 0 && len(badEdges) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d nodes (%s), %d edges (%s)", ErrPartialLoad,
		len(badNodes), clipKeys(badNodes), len(badEdges), clipKeys(badEdges))
}

func clipKeys(keys []string) string {
	const max = 10
	if len(keys) <= max {
		return strings.Join(keys, ",")
	}
	return strings.Join(keys[:max], ",") + fmt.Sprintf(",... %d more", len(keys)-max)
}

// Counts returns the number of nodes and edges in the graph.
func (s *bboltGraphStore) Counts() (nodes, edges int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes), len(s.edges)
}
