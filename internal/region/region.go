package region

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Region defines a deployment region.
type Region struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Primary  bool   `json:"primary"`
}

// ResidencyPolicy defines data residency requirements.
type ResidencyPolicy struct {
	ID        string `json:"id"`
	Region    string `json:"region"`
	DataType  string `json:"data_type"`
	Required  bool   `json:"required"`
}

// RouteRule defines routing rules by region.
type RouteRule struct {
	ID        string   `json:"id"`
	Region    string   `json:"region"`
	Providers []string `json:"providers"`
	Priority  int      `json:"priority"`
	Enabled   bool     `json:"enabled"`
}

// Store manages regions, residency policies, and route rules.
type Store struct {
	mu        sync.RWMutex
	regions   map[string]Region
	policies  map[string]ResidencyPolicy
	rules     map[string]RouteRule
}

// NewStore creates a region store.
func NewStore() *Store {
	return &Store{
		regions:  make(map[string]Region),
		policies: make(map[string]ResidencyPolicy),
		rules:    make(map[string]RouteRule),
	}
}

// UpsertRegion adds or updates a region.
func (s *Store) UpsertRegion(region Region) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regions[region.ID] = region
}

// GetRegion retrieves a region by id.
func (s *Store) GetRegion(id string) (Region, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.regions[id]
	return r, ok
}

// ListRegions returns all regions.
func (s *Store) ListRegions() []Region {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Region, 0, len(s.regions))
	for _, r := range s.regions {
		out = append(out, r)
	}
	return out
}

// UpsertPolicy adds or updates a residency policy.
func (s *Store) UpsertPolicy(policy ResidencyPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policies[policy.ID] = policy
}

// ListPolicies returns all residency policies.
func (s *Store) ListPolicies() []ResidencyPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ResidencyPolicy, 0, len(s.policies))
	for _, p := range s.policies {
		out = append(out, p)
	}
	return out
}

// UpsertRule adds or updates a route rule.
func (s *Store) UpsertRule(rule RouteRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[rule.ID] = rule
}

// ListRules returns all route rules.
func (s *Store) ListRules() []RouteRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RouteRule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	return out
}

// WebhookHandler exposes JSON CRUD for regions, policies, and rules.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/v1/region/routes" || path == "/v1/region/routes/":
			switch r.Method {
			case http.MethodPost:
				var rule RouteRule
				if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store.UpsertRule(rule)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(rule)
			case http.MethodGet:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListRules())
			default:
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			}
		case path == "/v1/region/residency" || path == "/v1/region/residency/":
			switch r.Method {
			case http.MethodPost:
				var policy ResidencyPolicy
				if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store.UpsertPolicy(policy)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(policy)
			case http.MethodGet:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListPolicies())
			default:
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			}
		case path == "/v1/region/regions" || path == "/v1/region/regions/":
			switch r.Method {
			case http.MethodPost:
				var region Region
				if err := json.NewDecoder(r.Body).Decode(&region); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store.UpsertRegion(region)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(region)
			case http.MethodGet:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.ListRegions())
			default:
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			}
		default:
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		}
	}
}
