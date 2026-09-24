package finops

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

func newPersistentTracker(t *testing.T, ps persist.Store) *CostTracker {
	t.Helper()
	c, err := NewCostTrackerWithPersistence(ps, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBudgetLifecyclePersistent(t *testing.T) {
	runBudgetLifecycle(t, newPersistentTracker(t, persist.NewMemory()))
}

func TestConcurrentSpendIsExactPersistent(t *testing.T) {
	concurrentExceeded(t, newPersistentTracker(t, persist.NewMemory()))
}

func TestAppendAndDrainUsagePersistent(t *testing.T) {
	c := newPersistentTracker(t, persist.NewMemory())
	ctx := context.Background()
	if err := c.AppendUsage(ctx, billing.MeterEntry{CustomerID: "c", EventName: "tokens", Value: 3}, billing.MeterEntry{CustomerID: "c", EventName: "tokens", Value: 4}); err != nil {
		t.Fatal(err)
	}
	got, err := c.DrainUsage(ctx, "c", 1)
	if err != nil || len(got) != 1 || got[0].Value != 3 {
		t.Fatalf("drain: %+v %v", got, err)
	}
	got, _ = c.DrainUsage(ctx, "c", 0)
	if len(got) != 1 || got[0].Value != 4 {
		t.Fatalf("drain rest: %+v", got)
	}
}

// Budgets, spend and buffered usage survive a restart (bbolt file closed
// and reopened) and threshold crossings continue from the stored spend.
func TestPersistentBudgetStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	const key = "sk-restart-abcdef123456"

	db, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	c1 := newPersistentTracker(t, db)
	c1.SetBudgetPeriod(PeriodMonthly)
	if err := c1.SetBudget(ctx, key, 1); err != nil {
		t.Fatal(err)
	}
	fd1 := &fakeDispatcher{}
	c1.SetBudgetWebhookConfig(fd1, webhooks.BudgetWebhookConfig{})
	for i := 0; i < 3; i++ {
		if err := c1.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.3}); err != nil {
			t.Fatal(err)
		}
	}
	if len(fd1.events()) != 1 { // 0.9 crossed the 80% threshold
		t.Fatalf("expected threshold event before restart, got %d", len(fd1.events()))
	}
	if err := c1.AppendUsage(ctx, billing.MeterEntry{CustomerID: "cus|1", EventName: "tokens", Value: 42, ID: "u1"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	c2 := newPersistentTracker(t, db2)
	c2.SetBudgetPeriod(PeriodMonthly)
	st, err := c2.GetBudget(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !st.HasLimit || !approxEqual(st.LimitUSD, 1, 1e-12) || !approxEqual(st.SpendUSD, 0.9, 1e-12) {
		t.Fatalf("budget not restored: %+v", st)
	}
	fd2 := &fakeDispatcher{}
	c2.SetBudgetWebhookConfig(fd2, webhooks.BudgetWebhookConfig{})
	_ = c2.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.3})
	evs := fd2.events()
	if len(evs) != 1 || evs[0].Type != webhooks.EventBudgetExceeded {
		t.Fatalf("only the exceeded event may fire after restart, got %+v", evs)
	}
	if _, err := c2.CheckBudget(ctx, key, 0); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("restored budget must be enforced: %v", err)
	}
	usage, err := c2.DrainUsage(ctx, "cus|1", 0)
	if err != nil || len(usage) != 1 || usage[0].ID != "u1" || usage[0].Value != 42 {
		t.Fatalf("usage not restored: %+v %v", usage, err)
	}
	store := c2.store.(*PersistentBudgetStore)
	if ids := store.BudgetIDs(); len(ids) != 1 || ids[0] != BudgetID(key) {
		t.Fatalf("unexpected budget ids %v", ids)
	}
	// Removing the limit and resetting spend deletes the document.
	_ = c2.RemoveBudget(ctx, key)
	_ = c2.ResetSpend(ctx, key)
	if ok, _ := db2.Get(BudgetBucket, BudgetID(key), &budgetDoc{}); ok {
		t.Fatal("empty budget document must be deleted")
	}
	n := 0
	_ = db2.ForEach(UsageBucket, func(string, json.RawMessage) error { n++; return nil })
	if n != 0 {
		t.Fatalf("drained usage must be deleted from the store, %d left", n)
	}
}

// failingStore fails writes while fail is set.
type failingStore struct {
	persist.Store
	fail atomic.Bool
}

var errDiskFull = errors.New("disk full")

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.fail.Load() {
		return errDiskFull
	}
	return f.Store.Put(bucket, key, v)
}

func (f *failingStore) Delete(bucket, key string) error {
	if f.fail.Load() {
		return errDiskFull
	}
	return f.Store.Delete(bucket, key)
}

func TestPersistentBudgetStoreWriteFailureIsSurfaced(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	c := newPersistentTracker(t, fs)
	fd := &fakeDispatcher{}
	c.SetBudgetWebhookConfig(fd, webhooks.BudgetWebhookConfig{})
	ctx := context.Background()
	const key = "sk-failing-abcdef1234"
	_ = c.SetBudget(ctx, key, 1)
	_ = c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.5})

	fs.fail.Store(true)
	// The spend is served already: it stays enforced (and its threshold
	// crossing fires) but the caller learns it is not durable.
	err := c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.35})
	if !errors.Is(err, ErrNotDurable) || !errors.Is(err, errDiskFull) {
		t.Fatalf("write failure must be returned, got %v", err)
	}
	if evs := fd.events(); len(evs) != 1 || evs[0].Type != webhooks.EventBudgetThreshold {
		t.Fatalf("crossing of a non-durable spend must still fire: %+v", evs)
	}
	// Limit changes are strict.
	if err := c.SetBudget(ctx, key, 5); !errors.Is(err, errDiskFull) {
		t.Fatalf("write failure must be returned, got %v", err)
	}
	if err := c.AppendUsage(ctx, billing.MeterEntry{CustomerID: "c", EventName: "e", Value: 1}); !errors.Is(err, errDiskFull) {
		t.Fatalf("usage write failure must be returned, got %v", err)
	}
	st, _ := c.GetBudget(ctx, key)
	if !approxEqual(st.SpendUSD, 0.85, 1e-12) || !approxEqual(st.LimitUSD, 1, 1e-12) {
		t.Fatalf("spend must stay applied, limit unchanged: %+v", st)
	}
	if got, _ := c.DrainUsage(ctx, "c", 0); len(got) != 0 {
		t.Fatalf("failed usage append must not be buffered: %+v", got)
	}

	// The next successful write persists the cumulative state.
	fs.fail.Store(false)
	if err := c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.05}); err != nil {
		t.Fatal(err)
	}
	c2 := newPersistentTracker(t, fs.Store)
	st2, _ := c2.GetBudget(ctx, key)
	if !approxEqual(st2.SpendUSD, 0.9, 1e-12) || !approxEqual(st2.LimitUSD, 1, 1e-12) {
		t.Fatalf("store diverged from memory: %+v", st2)
	}
}

// slowStore delays writes like bbolt's batched commits.
type slowStore struct {
	persist.Store
	puts atomic.Int32
}

func (s *slowStore) Put(bucket, key string, v any) error {
	s.puts.Add(1)
	time.Sleep(2 * time.Millisecond)
	return s.Store.Put(bucket, key, v)
}

// Concurrent spends of one hot budget are group committed: far fewer
// writes than spends, and the stored spend is exact.
func TestPersistentBudgetStoreGroupCommit(t *testing.T) {
	ss := &slowStore{Store: persist.NewMemory()}
	c := newPersistentTracker(t, ss)
	ctx := context.Background()
	const key = "sk-hot-key-abcdef1234"
	_ = c.SetBudget(ctx, key, 1000)
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if err := c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.01}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if n := ss.puts.Load(); n >= 320 {
		t.Fatalf("expected coalesced writes, got %d puts for 320 spends", n)
	}
	c2 := newPersistentTracker(t, ss.Store)
	st, _ := c2.GetBudget(ctx, key)
	if !approxEqual(st.SpendUSD, 3.2, 1e-9) {
		t.Fatalf("durable spend %.9f, want 3.2", st.SpendUSD)
	}
}

func TestPersistentBudgetStorePeriodExpiry(t *testing.T) {
	ps := persist.NewMemory()
	now := time.Date(2026, 9, 24, 23, 0, 0, 0, time.UTC)
	store, err := NewPersistentBudgetStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	c := NewCostTrackerWithStore(store, nil, nil)
	c.now = store.now
	c.SetBudgetPeriod(PeriodDaily)
	ctx := context.Background()
	const key = "sk-daily-abcdef123456"
	_ = c.SetBudget(ctx, key, 1)
	_ = c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 1})
	if _, err := c.CheckBudget(ctx, key, 0); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("expected exhausted budget")
	}
	now = now.Add(2 * time.Hour) // next UTC day: new bucket
	if _, err := c.CheckBudget(ctx, key, 0.5); err != nil {
		t.Fatalf("budget must reset on a new day: %v", err)
	}
	_ = c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.1})
	now = now.Add(72 * time.Hour) // the first day's bucket is past retention
	_ = c.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.1})
	var doc budgetDoc
	if ok, err := ps.Get(BudgetBucket, BudgetID(key), &doc); !ok || err != nil {
		t.Fatalf("missing doc: %v", err)
	}
	if _, stale := doc.Periods["20260924"]; stale || len(doc.Periods) > 2 {
		t.Fatalf("expired period buckets must be dropped: %+v", doc.Periods)
	}
}

func TestPersistentBudgetStoreRejectsCorruptState(t *testing.T) {
	ps := persist.NewMemory()
	_ = ps.Put(BudgetBucket, "id", "not an object")
	if _, err := NewPersistentBudgetStore(ps); err == nil {
		t.Fatal("corrupt budget document must fail loading")
	}
	ps2 := persist.NewMemory()
	_ = ps2.Put(UsageBucket, "no-seq", billing.MeterEntry{CustomerID: "no-seq"})
	if _, err := NewPersistentBudgetStore(ps2); err == nil {
		t.Fatal("corrupt usage key must fail loading")
	}
	if _, err := NewPersistentBudgetStore(nil); err == nil {
		t.Fatal("nil store must be rejected")
	}
}

func TestUsageKeyRoundTrip(t *testing.T) {
	k := usageKey("a|b", 7)
	cust, seq, ok := parseUsageKey(k)
	if !ok || cust != "a|b" || seq != 7 {
		t.Fatalf("parse %q: %q %d %v", k, cust, seq, ok)
	}
	if usageKey("c", 9) >= usageKey("c", 10) {
		t.Fatal("usage keys must sort by sequence")
	}
}

func TestPersistentUsageLargeBatchAndPartialDrainFailure(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	c := newPersistentTracker(t, fs)
	ctx := context.Background()
	entries := make([]billing.MeterEntry, 300)
	for i := range entries {
		entries[i] = billing.MeterEntry{CustomerID: "big", EventName: "tokens", Value: float64(i)}
	}
	if err := c.AppendUsage(ctx, entries...); err != nil {
		t.Fatal(err)
	}
	fs.fail.Store(true)
	if got, err := c.DrainUsage(ctx, "big", 10); err == nil || len(got) != 0 {
		t.Fatalf("failed deletes must keep entries buffered: %d %v", len(got), err)
	}
	fs.fail.Store(false)
	got, err := c.DrainUsage(ctx, "big", 0)
	if err != nil || len(got) != 300 {
		t.Fatalf("drain all: %d %v", len(got), err)
	}
	for i, e := range got {
		if e.Value != float64(i) {
			t.Fatalf("drain must preserve order: index %d has %v", i, e.Value)
		}
	}
	if c2 := newPersistentTracker(t, fs.Store); func() int { g, _ := c2.DrainUsage(ctx, "big", 0); return len(g) }() != 0 {
		t.Fatal("drained entries must be gone from the store")
	}
}
