package tenant

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// QuotaScope defines whether quota applies per tenant, team, or user.
type QuotaScope string

const (
	ScopeTenant QuotaScope = "tenant"
	ScopeTeam   QuotaScope = "team"
	ScopeUser   QuotaScope = "user"
)

// Quota defines usage limits for a scope.
type Quota struct {
	ID       string
	Scope    QuotaScope
	TargetID string
	Limit    int64
	Used     int64
	Burst    int64
	Window   time.Duration
	LastRefill time.Time
}

// QuotaEnforcedError is returned when quota is exceeded.
type QuotaEnforcedError struct {
	Scope    QuotaScope
	TargetID string
	Remaining int64
}

func (e *QuotaEnforcedError) Error() string {
	return fmt.Sprintf("quota exceeded: scope=%s target=%s remaining=%d", e.Scope, e.TargetID, e.Remaining)
}

// InMemoryQuotaStore stores quotas in memory with concurrency safety.
type InMemoryQuotaStore struct {
	mu       sync.RWMutex
	quotas   map[string]*Quota
}

// NewInMemoryQuotaStore creates a new in-memory quota store.
func NewInMemoryQuotaStore() *InMemoryQuotaStore {
	return &InMemoryQuotaStore{quotas: make(map[string]*Quota)}
}

// Upsert inserts or updates a quota.
func (s *InMemoryQuotaStore) Upsert(ctx context.Context, q *Quota) error {
	if q == nil || q.ID == "" {
		return fmt.Errorf("invalid quota")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quotas[q.ID] = q
	return nil
}

// Get retrieves a quota by ID.
func (s *InMemoryQuotaStore) Get(ctx context.Context, id string) (*Quota, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.quotas[id]
	if !ok {
		return nil, fmt.Errorf("quota not found: %s", id)
	}
	return q, nil
}

// ForScope returns a quota by scope and target ID if present.
func (s *InMemoryQuotaStore) ForScope(ctx context.Context, scope QuotaScope, targetID string) (*Quota, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, q := range s.quotas {
		if q.Scope == scope && q.TargetID == targetID {
			return q, nil
		}
	}
	return nil, fmt.Errorf("quota not found: %s:%s", scope, targetID)
}

// Enforce checks and increments quota usage.
func (s *InMemoryQuotaStore) Enforce(ctx context.Context, q *Quota, amount int64) (*Quota, error) {
	if q == nil {
		return nil, fmt.Errorf("nil quota")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.quotas[q.ID]
	if !ok {
		current = q
		current.LastRefill = time.Now()
		s.quotas[q.ID] = current
	}

	if current.Used+amount > current.Limit {
		return current, &QuotaEnforcedError{
			Scope:     current.Scope,
			TargetID:  current.TargetID,
			Remaining: current.Limit - current.Used,
		}
	}

	current.Used += amount
	s.quotas[q.ID] = current
	return current, nil
}

// QuotaKey builds a quota key from scope and target ID.
func QuotaKey(scope QuotaScope, targetID string) string {
	return string(scope) + ":" + targetID
}
