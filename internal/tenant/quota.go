package tenant

import (
	"context"
	"errors"
	"fmt"
	"math"
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
//
// Semantics (fixed window): at most Limit+Burst units may be consumed per
// Window. When Window > 0 the usage resets to zero once Window has elapsed
// since the start of the current window (LastRefill); Window == 0 means the
// quota never resets. Burst is an optional extra allowance on top of Limit.
type Quota struct {
	ID         string        `json:"id"`
	Scope      QuotaScope    `json:"scope"`
	TargetID   string        `json:"target_id"`
	Limit      int64         `json:"limit"`
	Used       int64         `json:"used"`
	Burst      int64         `json:"burst,omitempty"`
	Window     time.Duration `json:"window,omitempty"`
	LastRefill time.Time     `json:"last_refill,omitempty"`
}

// Capacity returns the maximum usage allowed per window.
func (q *Quota) Capacity() int64 {
	if q.Burst > 0 && q.Limit > math.MaxInt64-q.Burst {
		return math.MaxInt64
	}
	if q.Burst > 0 {
		return q.Limit + q.Burst
	}
	return q.Limit
}

// Remaining returns the units still available in the current window.
func (q *Quota) Remaining() int64 {
	r := q.Capacity() - q.Used
	if r < 0 {
		return 0
	}
	return r
}

// ResetAt returns when the current window ends (zero if it never resets).
func (q *Quota) ResetAt() time.Time {
	if q.Window <= 0 || q.LastRefill.IsZero() {
		return time.Time{}
	}
	return q.LastRefill.Add(q.Window)
}

func (q *Quota) validate() error {
	switch {
	case q == nil || q.ID == "":
		return errors.New("invalid quota: missing id")
	case q.Limit < 0:
		return errors.New("invalid quota: negative limit")
	case q.Used < 0:
		return errors.New("invalid quota: negative usage")
	case q.Burst < 0:
		return errors.New("invalid quota: negative burst")
	case q.Window < 0:
		return errors.New("invalid quota: negative window")
	}
	return nil
}

// refresh resets usage if the window has elapsed. Windows stay aligned to
// multiples of Window from the first LastRefill.
func (q *Quota) refresh(now time.Time) {
	if q.Window <= 0 {
		return
	}
	if q.LastRefill.IsZero() {
		q.LastRefill = now
		return
	}
	if elapsed := now.Sub(q.LastRefill); elapsed >= q.Window {
		q.Used = 0
		q.LastRefill = q.LastRefill.Add((elapsed / q.Window) * q.Window)
	}
}

// QuotaEnforcedError is returned when quota is exceeded.
type QuotaEnforcedError struct {
	Scope     QuotaScope
	TargetID  string
	Remaining int64
	ResetAt   time.Time
}

func (e *QuotaEnforcedError) Error() string {
	return fmt.Sprintf("quota exceeded: scope=%s target=%s remaining=%d", e.Scope, e.TargetID, e.Remaining)
}

// ErrInvalidAmount is returned for negative consumption amounts.
var ErrInvalidAmount = errors.New("quota: amount must not be negative")

// InMemoryQuotaStore stores quotas in memory with concurrency safety. All
// methods take and return copies; check-and-consume is atomic.
type InMemoryQuotaStore struct {
	mu     sync.Mutex
	quotas map[string]*Quota
	now    func() time.Time
}

// NewInMemoryQuotaStore creates a new in-memory quota store.
func NewInMemoryQuotaStore() *InMemoryQuotaStore {
	return &InMemoryQuotaStore{quotas: make(map[string]*Quota), now: time.Now}
}

// Upsert inserts or updates a quota definition. When updating, current usage
// and window start are preserved unless q sets them explicitly.
func (s *InMemoryQuotaStore) Upsert(ctx context.Context, q *Quota) error {
	if q == nil {
		return fmt.Errorf("invalid quota")
	}
	if err := q.validate(); err != nil {
		return err
	}
	c := *q
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.quotas[q.ID]; ok {
		if c.Used == 0 {
			c.Used = cur.Used
		}
		if c.LastRefill.IsZero() {
			c.LastRefill = cur.LastRefill
		}
	}
	s.quotas[q.ID] = &c
	return nil
}

// Get retrieves a copy of a quota by ID (with any elapsed window applied).
func (s *InMemoryQuotaStore) Get(ctx context.Context, id string) (*Quota, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.quotas[id]
	if !ok {
		return nil, fmt.Errorf("quota not found: %s", id)
	}
	q.refresh(s.now())
	c := *q
	return &c, nil
}

// ForScope returns a copy of the quota for scope and target ID if present.
func (s *InMemoryQuotaStore) ForScope(ctx context.Context, scope QuotaScope, targetID string) (*Quota, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.quotas {
		if q.Scope == scope && q.TargetID == targetID {
			q.refresh(s.now())
			c := *q
			return &c, nil
		}
	}
	return nil, fmt.Errorf("quota not found: %s:%s", scope, targetID)
}

// Enforce atomically checks and consumes amount units of quota q.ID. If the
// quota is not stored yet, q is registered first (a copy is stored; q is
// never mutated). amount 0 only checks. On success the updated quota is
// returned; when the request would exceed the capacity nothing is consumed
// and a *QuotaEnforcedError is returned together with the current state.
func (s *InMemoryQuotaStore) Enforce(ctx context.Context, q *Quota, amount int64) (*Quota, error) {
	if q == nil {
		return nil, fmt.Errorf("nil quota")
	}
	if amount < 0 {
		return nil, ErrInvalidAmount
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.quotas[q.ID]
	if !ok {
		if err := q.validate(); err != nil {
			return nil, err
		}
		c := *q
		current = &c
		s.quotas[q.ID] = current
	}
	current.refresh(s.now())

	// Overflow-safe: amount > capacity - used.
	if amount > current.Capacity()-current.Used {
		out := *current
		return &out, &QuotaEnforcedError{
			Scope:     current.Scope,
			TargetID:  current.TargetID,
			Remaining: current.Remaining(),
			ResetAt:   current.ResetAt(),
		}
	}
	current.Used += amount
	out := *current
	return &out, nil
}

// Release returns amount units to quota id (e.g. when a reserved request
// failed). Usage never drops below zero.
func (s *InMemoryQuotaStore) Release(ctx context.Context, id string, amount int64) (*Quota, error) {
	if amount < 0 {
		return nil, ErrInvalidAmount
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.quotas[id]
	if !ok {
		return nil, fmt.Errorf("quota not found: %s", id)
	}
	q.refresh(s.now())
	q.Used -= amount
	if q.Used < 0 {
		q.Used = 0
	}
	c := *q
	return &c, nil
}

// Reset clears the usage of quota id and starts a new window.
func (s *InMemoryQuotaStore) Reset(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.quotas[id]
	if !ok {
		return fmt.Errorf("quota not found: %s", id)
	}
	q.Used = 0
	if q.Window > 0 {
		q.LastRefill = s.now()
	}
	return nil
}

// QuotaKey builds a quota key from scope and target ID.
func QuotaKey(scope QuotaScope, targetID string) string {
	return string(scope) + ":" + targetID
}
