package studio

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// DAG represents an agent workflow DAG.
type DAG struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	JSON      string    `json:"json"`
}

// DAGStore persists DAG definitions.
type DAGStore interface {
	Save(ctx context.Context, dag DAG) error
	Get(ctx context.Context, id string) (DAG, error)
	List(ctx context.Context) ([]DAG, error)
	Delete(ctx context.Context, id string) error
}

// InMemoryDAGStore implements DAGStore in memory.
type InMemoryDAGStore struct {
	mu   sync.RWMutex
	dags map[string]DAG
}

// NewInMemoryDAGStore creates a new in-memory DAG store.
func NewInMemoryDAGStore() *InMemoryDAGStore {
	return &InMemoryDAGStore{dags: make(map[string]DAG)}
}

// Save stores a DAG definition.
func (s *InMemoryDAGStore) Save(_ context.Context, dag DAG) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dag.ID == "" {
		return errEmptyDAGID
	}
	now := time.Now().UTC()
	if dag.CreatedAt.IsZero() {
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

// List returns all DAGs.
func (s *InMemoryDAGStore) List(_ context.Context) ([]DAG, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DAG, 0, len(s.dags))
	for _, dag := range s.dags {
		out = append(out, dag)
	}
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

type studioError struct {
	message string
}

func (e *studioError) Error() string { return e.message }

// DAGHandler handles DAG management requests.
type DAGHandler struct {
	store DAGStore
}

// NewDAGHandler creates a new DAG handler.
func NewDAGHandler(store DAGStore) *DAGHandler {
	return &DAGHandler{store: store}
}

// ListDAGs returns all DAGs.
func (h *DAGHandler) ListDAGs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.store == nil {
		writeJSON(w, []DAG{})
		return
	}

	dags, err := h.store.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, dags)
}

// SaveDAG saves or updates a DAG.
func (h *DAGHandler) SaveDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.store == nil {
		http.Error(w, "dag store not configured", http.StatusServiceUnavailable)
		return
	}

	var dag DAG
	if err := json.NewDecoder(r.Body).Decode(&dag); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := h.store.Save(r.Context(), dag); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, dag)
}

// ServeDAGs routes DAG requests by method.
func (h *DAGHandler) ServeDAGs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.ListDAGs(w, r)
	case http.MethodPost:
		h.SaveDAG(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
