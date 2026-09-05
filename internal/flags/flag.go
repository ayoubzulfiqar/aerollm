package flags

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

// RolloutStrategy defines how a feature flag is rolled out.
type RolloutStrategy string

const (
	RolloutGlobal      RolloutStrategy = "global"
	RolloutPercentage  RolloutStrategy = "percentage"
	RolloutAllowList   RolloutStrategy = "allowlist"
	RolloutDenyList    RolloutStrategy = "denylist"
)

// FeatureFlag represents a feature flag definition.
type FeatureFlag struct {
	Key         string                 `json:"key"`
	Enabled     bool                   `json:"enabled"`
	Strategy    RolloutStrategy        `json:"strategy"`
	Percentage  int                    `json:"percentage"`
	AllowList   []string               `json:"allow_list"`
	DenyList    []string               `json:"deny_list"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// Store stores feature flags with thread-safe access.
type Store struct {
	mu       sync.RWMutex
	flags    map[string]FeatureFlag
	rollouts map[string]RolloutPolicy
}

// NewStore initializes a feature flag store.
func NewStore() *Store {
	return &Store{
		flags:    make(map[string]FeatureFlag),
		rollouts: make(map[string]RolloutPolicy),
	}
}

// Upsert adds or updates a feature flag.
func (s *Store) Upsert(flag FeatureFlag) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flags[flag.Key] = flag
}

// Get retrieves a feature flag by key.
func (s *Store) Get(key string) (FeatureFlag, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.flags[key]
	return f, ok
}

// List returns all feature flags.
func (s *Store) List() []FeatureFlag {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]FeatureFlag, 0, len(s.flags))
	for _, f := range s.flags {
		out = append(out, f)
	}
	return out
}

// SetRollout stores rollout policy for a key.
func (s *Store) SetRollout(key string, policy RolloutPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollouts[key] = policy
}

// GetRollout retrieves rollout policy for a key.
func (s *Store) GetRollout(key string) (RolloutPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.rollouts[key]
	return p, ok
}

// RolloutPolicy defines rollout weighting.
type RolloutPolicy struct {
	Key       string `json:"key"`
	Weight    int    `json:"weight"`
	CreatedAt string `json:"created_at"`
}

// Evaluate decides if a feature flag is active for a given context.
type Evaluate func(key string, context map[string]string) bool

// Enabled evaluates a feature flag based on strategy.
func (s *Store) Enabled(key string, ctx map[string]string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.flags[key]
	if !ok || !f.Enabled {
		return false
	}
	switch f.Strategy {
	case RolloutGlobal:
		return true
	case RolloutPercentage:
		seed := hashKey(key, ctx)
		return (seed % 100) < f.Percentage
	case RolloutAllowList:
		return inList(ctx["id"], f.AllowList)
	case RolloutDenyList:
		return !inList(ctx["id"], f.DenyList)
	default:
		return false
	}
}

func hashKey(key string, ctx map[string]string) int {
	h := 0
	for _, ch := range key {
		h = h*31 + int(ch)
	}
	for k, v := range ctx {
		for _, ch := range k {
			h = h*17 + int(ch)
		}
		for _, ch := range v {
			h = h*13 + int(ch)
		}
	}
	if h < 0 {
		return -h
	}
	return h % 100
}

func inList(needle string, haystack []string) bool {
	for _, item := range haystack {
		if strings.EqualFold(item, needle) {
			return true
		}
	}
	return false
}

// WebhookHandler exposes JSON-based feature flag CRUD and evaluation.
func WebhookHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var f FeatureFlag
			if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
				http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
				return
			}
			store.Upsert(f)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(f)
		case http.MethodGet:
			key := strings.TrimPrefix(r.URL.Path, "/v1/flags/")
			key = strings.TrimPrefix(key, "/v1/flags")
			if key == "" || key == "/" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(store.List())
				return
			}
			if f, ok := store.Get(key); ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(f)
				return
			}
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
