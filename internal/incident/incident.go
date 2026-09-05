package incident

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Severity levels for incidents.
type Severity string

const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

// Status tracks incident lifecycle.
type Status string

const (
	StatusOpen         Status = "open"
	StatusInvestigating Status = "investigating"
	StatusResolved     Status = "resolved"
	StatusClosed       Status = "closed"
)

// Incident represents an operational incident.
type Incident struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Severity    Severity  `json:"severity"`
	Status      Status    `json:"status"`
	Source      string    `json:"source"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ResolvedAt  time.Time `json:"resolved_at"`
}

// Store manages incidents in memory.
type Store struct {
	mu       sync.RWMutex
	incidents map[string]Incident
}

// NewStore creates an incident store.
func NewStore() *Store {
	return &Store{incidents: make(map[string]Incident)}
}

// Create adds a new incident.
func (s *Store) Create(incident Incident) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if incident.ID == "" {
		incident.ID = "inc_" + time.Now().Format("20060102150405")
	}
	if incident.CreatedAt.IsZero() {
		incident.CreatedAt = time.Now()
	}
	incident.UpdatedAt = time.Now()
	s.incidents[incident.ID] = incident
}

// Get retrieves an incident by id.
func (s *Store) Get(id string) (Incident, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inc, ok := s.incidents[id]
	return inc, ok
}

// List returns all incidents.
func (s *Store) List() []Incident {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		out = append(out, inc)
	}
	return out
}

// Update modifies an existing incident.
func (s *Store) Update(id string, updated Incident) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.incidents[id]; ok {
		updated.ID = id
		if updated.CreatedAt.IsZero() {
			updated.CreatedAt = existing.CreatedAt
		}
		updated.UpdatedAt = time.Now()
		if updated.Status == StatusResolved && existing.Status != StatusResolved {
			updated.ResolvedAt = time.Now()
		}
		s.incidents[id] = updated
	}
}

// Resolve marks an incident as resolved.
func (s *Store) Resolve(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.incidents[id]; ok {
		existing.Status = StatusResolved
		existing.UpdatedAt = time.Now()
		existing.ResolvedAt = time.Now()
		s.incidents[id] = existing
		return true
	}
	return false
}

// WebhookHandler exposes JSON CRUD for incidents.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			if r.URL.Query().Get("resolve") == "true" {
				id := r.URL.Query().Get("id")
				if id == "" {
					http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
					return
				}
				if store.Resolve(id) {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"status":"resolved"})
					return
				}
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			var inc Incident
			if err := json.NewDecoder(r.Body).Decode(&inc); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Create(inc)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(inc)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			if id == "" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.List())
				return
			}
			if inc, ok := store.Get(id); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(inc)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case http.MethodPut:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
				return
			}
			var inc Incident
			if err := json.NewDecoder(r.Body).Decode(&inc); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Update(id, inc)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(inc)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
