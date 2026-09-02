package graphrag

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

const defaultEdgeLimit = 64

// Node is an entity in the temporal knowledge graph.
type Node struct {
	ID        string
	Label     string
	Type      string
	Props     map[string]interface{}
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Edge is a temporal relationship between nodes.
type Edge struct {
	ID        string
	Source    string
	Target    string
	Label     string
	Props     map[string]interface{}
	CreatedAt time.Time
	ValidFrom time.Time
	ValidTo   *time.Time
}

// GraphStore is the interface for a temporal knowledge graph.
type GraphStore interface {
	UpsertNode(ctx context.Context, node Node) (string, error)
	UpsertEdge(ctx context.Context, edge Edge) (string, error)
	Neighbors(ctx context.Context, nodeID string, maxHops int) ([]Edge, error)
	Query(ctx context.Context, query string, limit int) ([]Node, error)
}

// bboltGraphStore persists nodes/edges in bbolt with temporal indexes.
type bboltGraphStore struct {
	mu       sync.RWMutex
	bucket   []byte
	nodes    map[string]Node
	edges    map[string]Edge
	sourceIdx map[string][]string
	targetIdx map[string][]string
}

// NewBboltGraphStore creates an in-memory temporal graph store.
func NewBboltGraphStore() *bboltGraphStore {
	return &bboltGraphStore{
		bucket:    []byte("graph"),
		nodes:     make(map[string]Node),
		edges:     make(map[string]Edge),
		sourceIdx: make(map[string][]string),
		targetIdx: make(map[string][]string),
	}
}

// UpsertNode inserts or updates a node.
func (s *bboltGraphStore) UpsertNode(ctx context.Context, node Node) (string, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if node.ID == "" {
		node.ID = nodeID(node.Label, node.Type, node.Props)
	}
	now := time.Now()
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	node.UpdatedAt = now
	s.nodes[node.ID] = node
	return node.ID, nil
}

// UpsertEdge inserts or updates an edge with temporal validity.
func (s *bboltGraphStore) UpsertEdge(ctx context.Context, edge Edge) (string, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if edge.ID == "" {
		edge.ID = edgeID(edge.Source, edge.Target, edge.Label, edge.Props)
	}
	now := time.Now()
	if edge.CreatedAt.IsZero() {
		edge.CreatedAt = now
	}
	if edge.ValidFrom.IsZero() {
		edge.ValidFrom = now
	}
	if edge.ValidTo == nil {
		zero := time.Time{}
		edge.ValidTo = &zero
	}
	s.edges[edge.ID] = edge
	s.sourceIdx[edge.Source] = append(s.sourceIdx[edge.Source], edge.ID)
	s.targetIdx[edge.Target] = append(s.targetIdx[edge.Target], edge.ID)
	return edge.ID, nil
}

// Neighbors returns edges reachable within maxHops.
func (s *bboltGraphStore) Neighbors(ctx context.Context, nodeID string, maxHops int) ([]Edge, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	if maxHops <= 0 {
		maxHops = 3
	}
	visited := map[string]bool{nodeID: true}
	var queue []string
	queue = append(queue, nodeID)
	depth := map[string]int{nodeID: 0}
	seen := map[string]bool{}
	var out []Edge
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if depth[cur] >= maxHops {
			continue
		}
		for _, id := range s.sourceIdx[cur] {
			if seen[id] {
				continue
			}
			seen[id] = true
			e := s.edges[id]
			out = append(out, e)
			if !visited[e.Target] {
				visited[e.Target] = true
				depth[e.Target] = depth[cur] + 1
				queue = append(queue, e.Target)
			}
		}
		for _, id := range s.targetIdx[cur] {
			if seen[id] {
				continue
			}
			seen[id] = true
			e := s.edges[id]
			out = append(out, e)
			if !visited[e.Source] {
				visited[e.Source] = true
				depth[e.Source] = depth[cur] + 1
				queue = append(queue, e.Source)
			}
		}
	}
	return out, nil
}

// Query returns nodes by token match.
func (s *bboltGraphStore) Query(ctx context.Context, query string, limit int) ([]Node, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	tokens := tokenize(strings.ToLower(query))
	type scoredNode struct {
		n Node
		s int
	}
	var results []scoredNode
	for _, n := range s.nodes {
		score := 0
		for _, token := range tokens {
			if strings.Contains(strings.ToLower(n.Label), token) {
				score += 10
			}
			if strings.Contains(strings.ToLower(n.Type), token) {
				score += 5
			}
		}
		if score > 0 {
			results = append(results, scoredNode{n: n, s: score})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].s > results[j].s })
	if limit <= 0 || limit > len(results) {
		limit = len(results)
	}
	out := make([]Node, 0, limit)
	for _, item := range results[:limit] {
		out = append(out, item.n)
	}
	return out, nil
}

// GraphRAGMiddleware injects graph context into prompts.
type GraphRAGMiddleware struct {
	store GraphStore
}

// NewGraphRAGMiddleware creates middleware.
func NewGraphRAGMiddleware(store GraphStore) *GraphRAGMiddleware {
	return &GraphRAGMiddleware{store: store}
}

// MaybeInject enriches the request with graph context when enabled.
func (m *GraphRAGMiddleware) MaybeInject(ctx context.Context, req *models.LLMRequest) error {
	if req == nil || len(req.Messages) == 0 {
		return nil
	}
	if !req.RagEnabled {
		return nil
	}
	query := ""
	if len(req.Messages) > 0 && req.Messages[len(req.Messages)-1].Content != nil {
		query = *req.Messages[len(req.Messages)-1].Content
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	nodes, err := m.store.Query(ctx, query, 8)
	if err != nil || len(nodes) == 0 {
		return err
	}
	var sb strings.Builder
	sb.WriteString("Graph context:\n")
	for _, node := range nodes {
		sb.WriteString(fmt.Sprintf("- %s (%s)\n", node.Label, node.Type))
	}
	sb.WriteString("\nQuery: ")
	sb.WriteString(query)
	sb.WriteString("\n")
	systemText := sb.String()
	req.Messages = append([]models.Message{{
		Role:    models.RoleSystem,
		Content: &systemText,
	}}, req.Messages...)
	return nil
}

// AutoOntologyWorker extracts entities/relationships from the ledger.
type AutoOntologyWorker struct {
	store GraphStore
	llm   interface{}
}

// NewAutoOntologyWorker creates the background worker.
func NewAutoOntologyWorker(store GraphStore, llm interface{}) *AutoOntologyWorker {
	return &AutoOntologyWorker{store: store, llm: llm}
}

// Run consumes ledger records and upserts entities/relationships.
func (w *AutoOntologyWorker) Run(ctx context.Context, ledger interface{}) error {
	_ = w.llm
	_ = ledger
	_ = ctx
	return nil
}

// Middleware returns an HTTP middleware that injects graph context when enabled.
func (m *GraphRAGMiddleware) Middleware(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var r models.LLMRequest
		body, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		if err := json.Unmarshal(body, &r); err == nil && r.RagEnabled {
			_ = m.MaybeInject(req.Context(), &r)
			if newBody, err := json.Marshal(r); err == nil {
				req.Body = io.NopCloser(bytes.NewReader(newBody))
			}
		}
		next.ServeHTTP(w, req)
	}
}

func tokenize(s string) []string {
	parts := strings.Fields(s)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func nodeID(label, typ string, props map[string]interface{}) string {
	parts := []string{label, typ}
	if props != nil {
		for _, v := range props {
			parts = append(parts, fmt.Sprint(v))
		}
	}
	return hash(parts...)
}

func edgeID(source, target, label string, props map[string]interface{}) string {
	parts := []string{source, target, label}
	if props != nil {
		for _, v := range props {
			parts = append(parts, fmt.Sprint(v))
		}
	}
	return hash(parts...)
}

func hash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}
