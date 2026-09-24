package studio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// DAG represents an agent workflow DAG.
type DAG struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// JSON is the workflow definition. When non-empty it must be valid JSON;
	// if it contains "nodes"/"edges" the graph is validated (unique node IDs,
	// known references, acyclic).
	JSON string `json:"json"`
}

// DAGStore persists DAG definitions.
type DAGStore interface {
	Save(ctx context.Context, dag DAG) error
	Get(ctx context.Context, id string) (DAG, error)
	List(ctx context.Context) ([]DAG, error)
	Delete(ctx context.Context, id string) error
}

const (
	maxDAGNameLen    = 256
	maxDAGVersionLen = 64
	// MaxDAGBodyBytes caps DAG request bodies.
	MaxDAGBodyBytes = 1 << 20
)

var dagIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// InMemoryDAGStore implements DAGStore in memory.
type InMemoryDAGStore struct {
	mu   sync.RWMutex
	dags map[string]DAG
}

// NewInMemoryDAGStore creates a new in-memory DAG store.
func NewInMemoryDAGStore() *InMemoryDAGStore {
	return &InMemoryDAGStore{dags: make(map[string]DAG)}
}

// Save validates and stores a DAG definition. CreatedAt is preserved when an
// existing DAG is updated.
func (s *InMemoryDAGStore) Save(_ context.Context, dag DAG) error {
	if err := ValidateDAG(dag); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := s.dags[dag.ID]; ok {
		dag.CreatedAt = existing.CreatedAt
	} else if dag.CreatedAt.IsZero() || dag.CreatedAt.After(now) {
		dag.CreatedAt = now
	}
	dag.UpdatedAt = now
	s.dags[dag.ID] = dag
	return nil
}

// Get retrieves a DAG by ID.
func (s *InMemoryDAGStore) Get(_ context.Context, id string) (DAG, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dag, ok := s.dags[id]
	if !ok {
		return DAG{}, errDAGNotFound
	}
	return dag, nil
}

// List returns all DAGs sorted by ID.
func (s *InMemoryDAGStore) List(_ context.Context) ([]DAG, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DAG, 0, len(s.dags))
	for _, dag := range s.dags {
		out = append(out, dag)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Delete removes a DAG by ID.
func (s *InMemoryDAGStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dags[id]; !ok {
		return errDAGNotFound
	}
	delete(s.dags, id)
	return nil
}

var errEmptyDAGID = &studioError{message: "dag id is required"}
var errDAGNotFound = &studioError{message: "dag not found"}

// ErrDAGNotFound is returned when a DAG does not exist.
var ErrDAGNotFound error = errDAGNotFound

// ErrInvalidDAG wraps all DAG validation failures.
var ErrInvalidDAG = errors.New("invalid dag")

type studioError struct {
	message string
}

func (e *studioError) Error() string { return e.message }

func invalid(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrInvalidDAG, fmt.Sprintf(format, args...))
}

// ValidateDAG checks the DAG metadata and, when present, the workflow graph
// encoded in the JSON field.
func ValidateDAG(dag DAG) error {
	if dag.ID == "" {
		return fmt.Errorf("%w: %s", ErrInvalidDAG, errEmptyDAGID.Error())
	}
	if !dagIDPattern.MatchString(dag.ID) {
		return invalid("dag id must match %s", dagIDPattern.String())
	}
	if !utf8.ValidString(dag.Name) || utf8.RuneCountInString(dag.Name) > maxDAGNameLen {
		return invalid("name must be valid UTF-8 of at most %d characters", maxDAGNameLen)
	}
	if !utf8.ValidString(dag.Version) || len(dag.Version) > maxDAGVersionLen {
		return invalid("version must be at most %d bytes", maxDAGVersionLen)
	}
	if strings.TrimSpace(dag.JSON) == "" {
		return nil
	}
	if !json.Valid([]byte(dag.JSON)) {
		return invalid("json field is not valid JSON")
	}
	return validateWorkflowGraph([]byte(dag.JSON))
}

type workflowNode struct {
	ID        string   `json:"id"`
	DependsOn []string `json:"depends_on"`
}

type workflowEdge struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Source string `json:"source"`
	Target string `json:"target"`
}

// validateWorkflowGraph validates {"nodes":[{"id","depends_on"}],"edges":[{"from","to"}|{"source","target"}]}.
// Documents without nodes/edges are accepted as opaque JSON.
func validateWorkflowGraph(raw []byte) error {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil // valid JSON but not an object: opaque definition
	}
	nodesRaw, hasNodes := doc["nodes"]
	edgesRaw, hasEdges := doc["edges"]
	if !hasNodes && !hasEdges {
		return nil
	}
	var nodes []workflowNode
	if hasNodes {
		if err := json.Unmarshal(nodesRaw, &nodes); err != nil {
			return invalid("nodes must be an array of objects with string id: %v", err)
		}
	}
	var edges []workflowEdge
	if hasEdges {
		if err := json.Unmarshal(edgesRaw, &edges); err != nil {
			return invalid("edges must be an array of objects: %v", err)
		}
	}
	ids := make(map[string]bool, len(nodes))
	adj := make(map[string][]string)
	for _, n := range nodes {
		if n.ID == "" {
			return invalid("node id is required")
		}
		if ids[n.ID] {
			return invalid("duplicate node id %q", n.ID)
		}
		ids[n.ID] = true
	}
	for _, n := range nodes {
		for _, dep := range n.DependsOn {
			if !ids[dep] {
				return invalid("node %q depends on unknown node %q", n.ID, dep)
			}
			adj[dep] = append(adj[dep], n.ID)
		}
	}
	for i, e := range edges {
		from, to := e.From, e.To
		if from == "" && to == "" {
			from, to = e.Source, e.Target
		}
		if from == "" || to == "" {
			return invalid("edge %d must have from/to (or source/target)", i)
		}
		if !ids[from] || !ids[to] {
			return invalid("edge %d references unknown node (%q -> %q)", i, from, to)
		}
		adj[from] = append(adj[from], to)
	}
	if hasGraphCycle(nodes, adj) {
		return invalid("workflow graph contains a cycle")
	}
	return nil
}

// hasGraphCycle runs an iterative three-color DFS.
func hasGraphCycle(nodes []workflowNode, adj map[string][]string) bool {
	color := make(map[string]int, len(nodes))
	type frame struct {
		id   string
		next int
	}
	for _, n := range nodes {
		if color[n.ID] != 0 {
			continue
		}
		stack := []frame{{id: n.ID}}
		color[n.ID] = 1
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.next < len(adj[top.id]) {
				nxt := adj[top.id][top.next]
				top.next++
				switch color[nxt] {
				case 1:
					return true
				case 0:
					color[nxt] = 1
					stack = append(stack, frame{id: nxt})
				}
				continue
			}
			color[top.id] = 2
			stack = stack[:len(stack)-1]
		}
	}
	return false
}

// DAGHandler handles DAG management requests.
type DAGHandler struct {
	store DAGStore
}

// NewDAGHandler creates a new DAG handler.
func NewDAGHandler(store DAGStore) *DAGHandler {
	return &DAGHandler{store: store}
}

// ListDAGs returns all DAGs, or a single DAG when ?id= is given.
func (h *DAGHandler) ListDAGs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	if h.store == nil {
		if r.URL.Query().Get("id") != "" {
			writeJSONError(w, http.StatusNotFound, "dag not found")
			return
		}
		writeJSON(w, []DAG{})
		return
	}
	if id := r.URL.Query().Get("id"); id != "" {
		dag, err := h.store.Get(r.Context(), id)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, dag)
		return
	}
	dags, err := h.store.List(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list dags")
		return
	}
	writeJSON(w, dags)
}

// SaveDAG saves or updates a DAG and returns the stored version.
func (h *DAGHandler) SaveDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		methodNotAllowed(w, http.MethodPost, http.MethodPut)
		return
	}
	if h.store == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "dag store not configured")
		return
	}
	var dag DAG
	if r.Body == nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxDAGBodyBytes))
	if err := dec.Decode(&dag); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := ValidateDAG(dag); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.Save(r.Context(), dag); err != nil {
		if errors.Is(err, ErrInvalidDAG) {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to save dag")
		return
	}
	stored, err := h.store.Get(r.Context(), dag.ID)
	if err != nil {
		stored = dag
	}
	writeJSON(w, stored)
}

// DeleteDAG deletes the DAG identified by ?id=.
func (h *DAGHandler) DeleteDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w, http.MethodDelete)
		return
	}
	if h.store == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "dag store not configured")
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "id query parameter is required")
		return
	}
	if err := h.store.Delete(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ServeDAGs routes DAG requests by method: GET/HEAD list (or ?id= fetch),
// POST/PUT save, DELETE ?id= delete.
func (h *DAGHandler) ServeDAGs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.ListDAGs(w, r)
	case http.MethodPost, http.MethodPut:
		h.SaveDAG(w, r)
	case http.MethodDelete:
		h.DeleteDAG(w, r)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete)
	}
}

func writeStoreError(w http.ResponseWriter, err error) {
	var se *studioError
	if errors.As(err, &se) && se == errDAGNotFound {
		writeJSONError(w, http.StatusNotFound, "dag not found")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, "dag store error")
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
