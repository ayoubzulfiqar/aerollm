package secrets

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Secret represents a stored secret.
type Secret struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Value     string            `json:"value"`
	Type      string            `json:"type"`
	Metadata  map[string]string `json:"metadata"`
	CreatedAt int64             `json:"created_at"`
}

// Store manages secrets in memory.
type Store struct {
	mu      sync.RWMutex
	secrets map[string]Secret
}

// NewStore creates a secret store.
func NewStore() *Store {
	return &Store{secrets: make(map[string]Secret)}
}

// Upsert adds or updates a secret.
func (s *Store) Upsert(secret Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if secret.ID == "" {
		secret.ID = "sec_" + secret.Name
	}
	if secret.CreatedAt == 0 {
		secret.CreatedAt = time.Now().Unix()
	}
	s.secrets[secret.ID] = secret
}

// Get retrieves a secret by id.
func (s *Store) Get(id string) (Secret, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.secrets[id]
	return sec, ok
}

// List returns all secrets.
func (s *Store) List() []Secret {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Secret, 0, len(s.secrets))
	for _, sec := range s.secrets {
		out = append(out, sec)
	}
	return out
}

// Delete removes a secret by id.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.secrets[id]; ok {
		delete(s.secrets, id)
		return true
	}
	return false
}

// WebhookHandler exposes JSON CRUD for secrets.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var sec Secret
			if err := json.NewDecoder(r.Body).Decode(&sec); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Upsert(sec)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(sec)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.List())
				return
			}
			if sec, ok := store.Get(id); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(sec)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
				return
			}
			if store.Delete(id) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"status":"deleted"})
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
