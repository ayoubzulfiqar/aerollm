package finops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used by PersistentBudgetStore.
const (
	// BudgetBucket holds one document per budget ID (limit + period spend).
	BudgetBucket = "finops_budgets"
	// UsageBucket holds buffered billing usage entries.
	UsageBucket = "finops_usage"
)

// budgetDoc is the persisted form of one budget.
type budgetDoc struct {
	LimitNanos int64                `json:"limit_nanos"`
	HasLimit   bool                 `json:"has_limit"`
	Periods    map[string]periodDoc `json:"periods,omitempty"`
}

type periodDoc struct {
	SpendNanos int64 `json:"spend_nanos"`
	// ExpiresUnix is when the period bucket may be discarded (0 = never).
	ExpiresUnix int64 `json:"expires_unix,omitempty"`
}

func (d budgetDoc) clone() budgetDoc {
	c := d
	if d.Periods != nil {
		c.Periods = make(map[string]periodDoc, len(d.Periods))
		for k, v := range d.Periods {
			c.Periods[k] = v
		}
	}
	return c
}

func (d budgetDoc) empty() bool { return !d.HasLimit && len(d.Periods) == 0 }

// dropExpired removes period buckets whose retention has elapsed.
func (d *budgetDoc) dropExpired(now time.Time) {
	for p, pd := range d.Periods {
		if pd.ExpiresUnix > 0 && now.Unix() >= pd.ExpiresUnix {
			delete(d.Periods, p)
		}
	}
}

type persistedBudget struct {
	mu      sync.Mutex // guards doc and version
	doc     budgetDoc
	version uint64

	wmu       sync.Mutex // serialises writes of this budget; guards persisted
	persisted uint64
}

// ErrNotDurable is returned (wrapped, together with the storage error) by
// PersistentBudgetStore.AddSpend when the spend was applied and is enforced
// but could not be written; a later successful write persists it.
var ErrNotDurable = errors.New("finops: budget spend applied but not persisted")

type usageItem struct {
	key   string
	entry billing.MeterEntry
}

// PersistentBudgetStore is a BudgetStore that keeps budgets in memory and
// writes every change through to a persist.Store (e.g. persist.OpenBolt),
// so limits, per-period spend and buffered usage survive restarts without
// Redis. It is single-process: use NewRedisBudgetStore when several
// gateway instances share budgets.
//
// AddSpend is atomic in memory (under the budget's mutex) and returns once
// the new spend is durable. Concurrent spends of one budget are group
// committed: a single write persists every change made before it, so a
// hot key is not limited by storage latency. A failed spend write is
// returned (wrapping ErrNotDurable) but the spend stays applied, because
// the request it pays for has already been served. Limit changes and
// resets are strict: a failed write leaves the budget unchanged.
type PersistentBudgetStore struct {
	ps persist.Store

	mu      sync.Mutex
	budgets map[string]*persistedBudget

	usageMu sync.Mutex
	usage   map[string][]usageItem
	seq     uint64

	now func() time.Time
}

// NewPersistentBudgetStore loads the budgets and usage stored in ps and
// returns a write-through store. It fails when ps cannot be read or holds
// undecodable documents (budgets are never silently reset).
func NewPersistentBudgetStore(ps persist.Store) (*PersistentBudgetStore, error) {
	if ps == nil {
		return nil, fmt.Errorf("finops: nil persist store")
	}
	s := &PersistentBudgetStore{
		ps:      ps,
		budgets: map[string]*persistedBudget{},
		usage:   map[string][]usageItem{},
		now:     time.Now,
	}
	docs, err := persist.LoadAll[budgetDoc](ps, BudgetBucket)
	if err != nil {
		return nil, fmt.Errorf("finops: load budgets: %w", err)
	}
	now := s.now()
	for id, d := range docs {
		d.dropExpired(now)
		s.budgets[id] = &persistedBudget{doc: d}
	}
	var bad []string
	err = ps.ForEach(UsageBucket, func(key string, raw json.RawMessage) error {
		cust, seq, ok := parseUsageKey(key)
		var e billing.MeterEntry
		if !ok || json.Unmarshal(raw, &e) != nil || e.CustomerID != cust {
			bad = append(bad, key)
			return nil
		}
		s.usage[cust] = append(s.usage[cust], usageItem{key: key, entry: e})
		s.seq = max(s.seq, seq)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("finops: load usage: %w", err)
	}
	if len(bad) > 0 {
		return nil, fmt.Errorf("finops: %d undecodable usage documents (%s)", len(bad), strings.Join(bad, ","))
	}
	return s, nil
}

// NewCostTrackerWithPersistence creates a cost tracker whose budgets are
// stored in ps (see NewPersistentBudgetStore).
func NewCostTrackerWithPersistence(ps persist.Store, prices *PricingMap, costMap *intelligence.ModelCostMap) (*CostTracker, error) {
	store, err := NewPersistentBudgetStore(ps)
	if err != nil {
		return nil, err
	}
	return NewCostTrackerWithStore(store, prices, costMap), nil
}

// usageKey sorts by customer, then by insertion order.
func usageKey(customer string, seq uint64) string {
	return customer + "|" + fmt.Sprintf("%020d", seq)
}

func parseUsageKey(key string) (string, uint64, bool) {
	i := strings.LastIndexByte(key, '|')
	if i <= 0 {
		return "", 0, false
	}
	seq, err := strconv.ParseUint(key[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return key[:i], seq, true
}

// budget returns the entry for id, creating it when create is set.
func (s *PersistentBudgetStore) budget(id string, create bool) *persistedBudget {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.budgets[id]
	if b == nil && create {
		b = &persistedBudget{}
		s.budgets[id] = b
	}
	return b
}

// update applies fn to a copy of the budget and persists it before
// publishing it (strict: on failure nothing changes).
func (s *PersistentBudgetStore) update(id string, fn func(d *budgetDoc) error) (budgetDoc, error) {
	b := s.budget(id, true)
	b.wmu.Lock()
	defer b.wmu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	next := b.doc.clone()
	next.dropExpired(s.now())
	if err := fn(&next); err != nil {
		return b.doc, err
	}
	if err := s.write(id, next); err != nil {
		return b.doc, err
	}
	b.doc = next
	b.version++
	b.persisted = b.version
	return next, nil
}

func (s *PersistentBudgetStore) write(id string, d budgetDoc) error {
	var err error
	if d.empty() {
		err = s.ps.Delete(BudgetBucket, id)
	} else {
		err = s.ps.Put(BudgetBucket, id, d)
	}
	if err != nil {
		return fmt.Errorf("finops: persist budget: %w", err)
	}
	return nil
}

// persistUpTo makes version v of the budget durable, writing the latest
// state unless a concurrent writer already covered v.
func (s *PersistentBudgetStore) persistUpTo(id string, b *persistedBudget, v uint64) error {
	b.wmu.Lock()
	defer b.wmu.Unlock()
	if b.persisted >= v {
		return nil
	}
	b.mu.Lock()
	snap, sv := b.doc.clone(), b.version
	b.mu.Unlock()
	if err := s.write(id, snap); err != nil {
		return err
	}
	b.persisted = sv
	return nil
}

// SetLimit implements BudgetStore.
func (s *PersistentBudgetStore) SetLimit(_ context.Context, id string, limit int64) error {
	_, err := s.update(id, func(d *budgetDoc) error {
		d.LimitNanos, d.HasLimit = limit, true
		return nil
	})
	return err
}

// ClearLimit implements BudgetStore.
func (s *PersistentBudgetStore) ClearLimit(_ context.Context, id string) error {
	if s.budget(id, false) == nil {
		return nil
	}
	_, err := s.update(id, func(d *budgetDoc) error {
		d.LimitNanos, d.HasLimit = 0, false
		return nil
	})
	return err
}

// ResetSpend implements BudgetStore.
func (s *PersistentBudgetStore) ResetSpend(_ context.Context, id, period string) error {
	if s.budget(id, false) == nil {
		return nil
	}
	_, err := s.update(id, func(d *budgetDoc) error {
		delete(d.Periods, period)
		return nil
	})
	return err
}

// Get implements BudgetStore.
func (s *PersistentBudgetStore) Get(_ context.Context, id, period string) (budgetState, error) {
	b := s.budget(id, false)
	if b == nil {
		return budgetState{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	st := budgetState{LimitNanos: b.doc.LimitNanos, HasLimit: b.doc.HasLimit}
	if pd, ok := b.doc.Periods[period]; ok && (pd.ExpiresUnix == 0 || s.now().Unix() < pd.ExpiresUnix) {
		st.SpendNanos = pd.SpendNanos
	}
	return st, nil
}

// AddSpend implements BudgetStore. The period bucket expires retain after
// its first spend (like the Redis store's TTL); retain <= 0 keeps it.
func (s *PersistentBudgetStore) AddSpend(_ context.Context, id, period string, delta int64, retain time.Duration) (int64, int64, bool, error) {
	b := s.budget(id, true)
	b.mu.Lock()
	next := b.doc.clone()
	next.dropExpired(s.now())
	if next.Periods == nil {
		next.Periods = map[string]periodDoc{}
	}
	pd, ok := next.Periods[period]
	if !ok && retain > 0 {
		pd.ExpiresUnix = s.now().Add(retain).Unix()
	}
	if delta > math.MaxInt64-pd.SpendNanos {
		b.mu.Unlock()
		return 0, 0, false, fmt.Errorf("%w: spend overflow", ErrInvalidAmount)
	}
	pd.SpendNanos += delta
	next.Periods[period] = pd
	b.doc = next
	b.version++
	v := b.version
	b.mu.Unlock()

	if err := s.persistUpTo(id, b, v); err != nil {
		return pd.SpendNanos, next.LimitNanos, next.HasLimit, fmt.Errorf("%w: %w", ErrNotDurable, err)
	}
	return pd.SpendNanos, next.LimitNanos, next.HasLimit, nil
}

// parallel runs fn(0..n-1) with bounded concurrency and returns the
// per-index errors. Concurrent writes let a batching store (persist.Bolt)
// commit them in one transaction.
func parallel(n int, fn func(i int) error) []error {
	errs := make([]error, n)
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			errs[i] = fn(i)
		}()
	}
	wg.Wait()
	return errs
}

// AppendUsage implements BudgetStore. Entries are persisted one document
// each; when any write fails the entries written by this call are removed
// again and the error is returned (nothing is buffered).
func (s *PersistentBudgetStore) AppendUsage(_ context.Context, entries []billing.MeterEntry) error {
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	perCustomer := map[string]int{}
	for _, e := range entries {
		perCustomer[e.CustomerID]++
	}
	for cust, n := range perCustomer {
		if len(s.usage[cust])+n > memoryUsageCap {
			return fmt.Errorf("finops: usage buffer full for customer %q", cust)
		}
	}
	items := make([]usageItem, len(entries))
	for i, e := range entries {
		s.seq++
		items[i] = usageItem{key: usageKey(e.CustomerID, s.seq), entry: e}
	}
	errs := parallel(len(items), func(i int) error { return s.ps.Put(UsageBucket, items[i].key, items[i].entry) })
	if err := errors.Join(errs...); err != nil {
		parallel(len(items), func(i int) error {
			if errs[i] == nil {
				return s.ps.Delete(UsageBucket, items[i].key)
			}
			return nil
		})
		return fmt.Errorf("finops: persist usage: %w", err)
	}
	for _, it := range items {
		s.usage[it.entry.CustomerID] = append(s.usage[it.entry.CustomerID], it)
	}
	return nil
}

// DrainUsage implements BudgetStore. Entries whose persisted copy cannot
// be deleted stay buffered (and the error is returned together with the
// entries that were drained).
func (s *PersistentBudgetStore) DrainUsage(_ context.Context, customerID string, max int) ([]billing.MeterEntry, error) {
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	q := s.usage[customerID]
	if max <= 0 || max > len(q) {
		max = len(q)
	}
	errs := parallel(max, func(i int) error { return s.ps.Delete(UsageBucket, q[i].key) })
	out := make([]billing.MeterEntry, 0, max)
	var kept []usageItem
	for i := 0; i < max; i++ {
		if errs[i] != nil {
			kept = append(kept, q[i])
			continue
		}
		out = append(out, q[i].entry)
	}
	kept = append(kept, q[max:]...)
	if len(kept) == 0 {
		delete(s.usage, customerID)
	} else {
		s.usage[customerID] = kept
	}
	if err := errors.Join(errs...); err != nil {
		return out, fmt.Errorf("finops: delete drained usage: %w", err)
	}
	return out, nil
}

// BudgetIDs returns the IDs of all stored budgets, sorted.
func (s *PersistentBudgetStore) BudgetIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.budgets))
	for id, b := range s.budgets {
		b.mu.Lock()
		empty := b.doc.empty()
		b.mu.Unlock()
		if !empty {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

var _ BudgetStore = (*PersistentBudgetStore)(nil)
