package finops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/redis/go-redis/v9"
)

// nanosPerUSD is the fixed-point scale used for all stored money amounts.
// Integer nano-dollars make concurrent accumulation exact (no float drift).
const nanosPerUSD = 1e9

// maxUSD bounds amounts so nano-dollar values fit in int64.
const maxUSD = 9e9

// ErrInvalidAmount is returned for negative, NaN, infinite or oversized amounts.
var ErrInvalidAmount = errors.New("finops: invalid amount")

// toNanos converts USD to integer nano-dollars.
func toNanos(usd float64) (int64, error) {
	if math.IsNaN(usd) || math.IsInf(usd, 0) || usd < 0 || usd > maxUSD {
		return 0, fmt.Errorf("%w: %v", ErrInvalidAmount, usd)
	}
	return int64(math.Round(usd * nanosPerUSD)), nil
}

func fromNanos(n int64) float64 { return float64(n) / nanosPerUSD }

// BudgetPeriod controls when accumulated spend resets.
type BudgetPeriod string

const (
	// PeriodLifetime never resets spend automatically.
	PeriodLifetime BudgetPeriod = ""
	// PeriodDaily resets spend at 00:00 UTC.
	PeriodDaily BudgetPeriod = "daily"
	// PeriodMonthly resets spend on the first day of each month (UTC).
	PeriodMonthly BudgetPeriod = "monthly"
)

// periodKey returns the bucket label for t and how long the bucket must be
// retained (0 = forever).
func periodKey(p BudgetPeriod, t time.Time) (string, time.Duration) {
	t = t.UTC()
	switch p {
	case PeriodDaily:
		end := time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
		return t.Format("20060102"), end.Sub(t) + 24*time.Hour
	case PeriodMonthly:
		end := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		return t.Format("200601"), end.Sub(t) + 24*time.Hour
	default:
		return "all", 0
	}
}

// budgetState is the stored state of one budget.
type budgetState struct {
	SpendNanos int64
	LimitNanos int64
	HasLimit   bool
}

// BudgetStore persists budget limits and accumulated spend. AddSpend must
// be atomic: concurrent callers must observe distinct (prev, new) pairs.
type BudgetStore interface {
	SetLimit(ctx context.Context, id string, limitNanos int64) error
	ClearLimit(ctx context.Context, id string) error
	ResetSpend(ctx context.Context, id, period string) error
	Get(ctx context.Context, id, period string) (budgetState, error)
	// AddSpend atomically adds delta nano-dollars to the spend of (id,
	// period) and returns the new spend with the current limit. retain > 0
	// lets the store expire the period bucket.
	AddSpend(ctx context.Context, id, period string, delta int64, retain time.Duration) (newSpend int64, limit int64, hasLimit bool, err error)
	AppendUsage(ctx context.Context, entries []billing.MeterEntry) error
	DrainUsage(ctx context.Context, customerID string, max int) ([]billing.MeterEntry, error)
}

// ---------------------------------------------------------------------------
// In-memory store (single instance / tests)
// ---------------------------------------------------------------------------

type memBudget struct {
	limit    int64
	hasLimit bool
	spend    map[string]int64 // period -> spend
}

// memoryUsageCap bounds buffered meter entries per customer.
const memoryUsageCap = 100_000

type memoryBudgetStore struct {
	mu      sync.Mutex
	budgets map[string]*memBudget
	usage   map[string][]billing.MeterEntry
}

// NewMemoryBudgetStore returns a process-local BudgetStore.
func NewMemoryBudgetStore() BudgetStore {
	return &memoryBudgetStore{budgets: map[string]*memBudget{}, usage: map[string][]billing.MeterEntry{}}
}

func (m *memoryBudgetStore) get(id string) *memBudget {
	b := m.budgets[id]
	if b == nil {
		b = &memBudget{spend: map[string]int64{}}
		m.budgets[id] = b
	}
	return b
}

func (m *memoryBudgetStore) SetLimit(_ context.Context, id string, limit int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.get(id)
	b.limit, b.hasLimit = limit, true
	return nil
}

func (m *memoryBudgetStore) ClearLimit(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.budgets[id]; b != nil {
		b.limit, b.hasLimit = 0, false
	}
	return nil
}

func (m *memoryBudgetStore) ResetSpend(_ context.Context, id, period string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.budgets[id]; b != nil {
		delete(b.spend, period)
	}
	return nil
}

func (m *memoryBudgetStore) Get(_ context.Context, id, period string) (budgetState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.budgets[id]
	if b == nil {
		return budgetState{}, nil
	}
	return budgetState{SpendNanos: b.spend[period], LimitNanos: b.limit, HasLimit: b.hasLimit}, nil
}

func (m *memoryBudgetStore) AddSpend(_ context.Context, id, period string, delta int64, _ time.Duration) (int64, int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.get(id)
	// Drop stale period buckets so memory does not grow over time.
	for p := range b.spend {
		if p != period {
			delete(b.spend, p)
		}
	}
	cur := b.spend[period]
	if delta > math.MaxInt64-cur {
		return 0, 0, false, fmt.Errorf("%w: spend overflow", ErrInvalidAmount)
	}
	b.spend[period] = cur + delta
	return cur + delta, b.limit, b.hasLimit, nil
}

func (m *memoryBudgetStore) AppendUsage(_ context.Context, entries []billing.MeterEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		q := append(m.usage[e.CustomerID], e)
		if len(q) > memoryUsageCap {
			return fmt.Errorf("finops: usage buffer full for customer %q", e.CustomerID)
		}
		m.usage[e.CustomerID] = q
	}
	return nil
}

func (m *memoryBudgetStore) DrainUsage(_ context.Context, customerID string, max int) ([]billing.MeterEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.usage[customerID]
	if max <= 0 || max > len(q) {
		max = len(q)
	}
	out := append([]billing.MeterEntry(nil), q[:max]...)
	if max == len(q) {
		delete(m.usage, customerID)
	} else {
		m.usage[customerID] = append([]billing.MeterEntry(nil), q[max:]...)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Redis store (multi-instance)
// ---------------------------------------------------------------------------

// RedisBudgetClient is the subset of go-redis used by the Redis budget store.
type RedisBudgetClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	LPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd
}

// addSpendScript atomically increments the spend counter, sets its expiry
// on first use and returns {newSpend, limit|false}.
const addSpendScript = `
local new = redis.call('INCRBY', KEYS[1], ARGV[1])
local ttl = tonumber(ARGV[2])
if ttl > 0 and redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ttl)
end
local limit = redis.call('GET', KEYS[2])
return {new, limit}
`

type redisBudgetStore struct {
	client RedisBudgetClient
	prefix string
}

// NewRedisBudgetStore returns a BudgetStore backed by Redis. Keys are
// namespaced with prefix (default "aerollm:finops:").
func NewRedisBudgetStore(client RedisBudgetClient, prefix string) BudgetStore {
	if prefix == "" {
		prefix = "aerollm:finops:"
	}
	return &redisBudgetStore{client: client, prefix: prefix}
}

// Keys of one budget share the {id} hash tag so the Lua script touches a
// single slot (required by Redis Cluster).
func (r *redisBudgetStore) limitKey(id string) string { return r.prefix + "limit:{" + id + "}" }
func (r *redisBudgetStore) spendKey(id, period string) string {
	return r.prefix + "spend:{" + id + "}:" + period
}
func (r *redisBudgetStore) usageKey(customer string) string { return r.prefix + "usage:" + customer }

func (r *redisBudgetStore) SetLimit(ctx context.Context, id string, limit int64) error {
	return r.client.Set(ctx, r.limitKey(id), strconv.FormatInt(limit, 10), 0).Err()
}

func (r *redisBudgetStore) ClearLimit(ctx context.Context, id string) error {
	return r.client.Del(ctx, r.limitKey(id)).Err()
}

func (r *redisBudgetStore) ResetSpend(ctx context.Context, id, period string) error {
	return r.client.Del(ctx, r.spendKey(id, period)).Err()
}

func parseNanos(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("finops: corrupt budget value %q", s)
	}
	return n, nil
}

func (r *redisBudgetStore) Get(ctx context.Context, id, period string) (budgetState, error) {
	var st budgetState
	spend, err := r.client.Get(ctx, r.spendKey(id, period)).Result()
	switch {
	case errors.Is(err, redis.Nil):
	case err != nil:
		return st, err
	default:
		if st.SpendNanos, err = parseNanos(spend); err != nil {
			return st, err
		}
	}
	limit, err := r.client.Get(ctx, r.limitKey(id)).Result()
	switch {
	case errors.Is(err, redis.Nil):
	case err != nil:
		return st, err
	default:
		if st.LimitNanos, err = parseNanos(limit); err != nil {
			return st, err
		}
		st.HasLimit = true
	}
	return st, nil
}

func (r *redisBudgetStore) AddSpend(ctx context.Context, id, period string, delta int64, retain time.Duration) (int64, int64, bool, error) {
	res, err := r.client.Eval(ctx, addSpendScript, []string{r.spendKey(id, period), r.limitKey(id)}, delta, int64(retain/time.Second)).Result()
	if err != nil {
		return 0, 0, false, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 1 {
		return 0, 0, false, fmt.Errorf("finops: unexpected script reply %T", res)
	}
	newSpend, ok := arr[0].(int64)
	if !ok {
		return 0, 0, false, fmt.Errorf("finops: unexpected spend reply %T", arr[0])
	}
	if len(arr) < 2 || arr[1] == nil {
		return newSpend, 0, false, nil
	}
	s, ok := arr[1].(string)
	if !ok {
		return newSpend, 0, false, nil
	}
	limit, err := parseNanos(s)
	if err != nil {
		return newSpend, 0, false, err
	}
	return newSpend, limit, true, nil
}

func (r *redisBudgetStore) AppendUsage(ctx context.Context, entries []billing.MeterEntry) error {
	byCustomer := map[string][]interface{}{}
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		byCustomer[e.CustomerID] = append(byCustomer[e.CustomerID], b)
	}
	for cust, vals := range byCustomer {
		key := r.usageKey(cust)
		if err := r.client.RPush(ctx, key, vals...).Err(); err != nil {
			return err
		}
		if err := r.client.Expire(ctx, key, 7*24*time.Hour).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (r *redisBudgetStore) DrainUsage(ctx context.Context, customerID string, max int) ([]billing.MeterEntry, error) {
	if max <= 0 {
		max = 1000
	}
	vals, err := r.client.LPopCount(ctx, r.usageKey(customerID), max).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]billing.MeterEntry, 0, len(vals))
	for _, v := range vals {
		var e billing.MeterEntry
		if err := json.Unmarshal([]byte(v), &e); err != nil {
			continue // skip corrupt entries rather than wedging the queue
		}
		out = append(out, e)
	}
	return out, nil
}
