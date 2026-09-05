package retention

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// RetentionPolicy defines data retention rules.
type RetentionPolicy struct {
	ID        string        `json:"id"`
	Resource  string        `json:"resource"`
	TTL       time.Duration `json:"ttl"`
	MaxItems  int           `json:"max_items"`
	CreatedAt time.Time     `json:"created_at"`
}

// RetentionStore manages retention policies.
type RetentionStore struct {
	mu      sync.RWMutex
	policies map[string]RetentionPolicy
}

// NewRetentionStore creates a retention store.
func NewRetentionStore() *RetentionStore {
	return &RetentionStore{policies: make(map[string]RetentionPolicy)}
}

// Upsert adds or updates a policy.
func (s *RetentionStore) Upsert(policy RetentionPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now()
	}
	s.policies[policy.ID] = policy
}

// Get retrieves a policy by id.
func (s *RetentionStore) Get(id string) (RetentionPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[id]
	return p, ok
}

// List returns all policies.
func (s *RetentionStore) List() []RetentionPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RetentionPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		out = append(out, p)
	}
	return out
}

// WebhookHandler exposes JSON CRUD for retention policies.
func WebhookHandler(store *RetentionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var p RetentionPolicy
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Upsert(p)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(p)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.List())
				return
			}
			if p, ok := store.Get(id); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(p)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
