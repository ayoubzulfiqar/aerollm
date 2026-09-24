package analytics

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

func mustEnable(t *testing.T, a *AnalyticsEngine, ps persist.Store, opts PersistenceOptions) {
	t.Helper()
	if opts.FlushInterval == 0 {
		opts.FlushInterval = time.Hour // tests flush explicitly
	}
	if err := a.EnablePersistence(ps, opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
}

func report(t *testing.T, a *AnalyticsEngine) *SpendReport {
	t.Helper()
	r, err := a.GenerateReport(context.Background(), TimeRange{}, "all")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The spend report survives a restart (bbolt file closed and reopened).
func TestAnalyticsPersistenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "analytics.db")
	db, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	a1 := NewAnalyticsEngine()
	mustEnable(t, a1, db, PersistenceOptions{})
	for i := 0; i < 5; i++ {
		a1.Record(CostEntry{RequestID: "r", APIKey: "sk-live-abcdef123456", Model: "gpt-4o", Provider: "openai", CustomerID: "cus", TeamID: "team",
			PromptTokens: 10, CompletionTokens: 5, CostUSD: 0.5})
	}
	before := report(t, a1)
	if err := a1.Close(); err != nil {
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
	a2 := NewAnalyticsEngine()
	mustEnable(t, a2, db2, PersistenceOptions{})
	after := report(t, a2)
	if after.TotalRequests != 5 || !approx(after.TotalCostUSD, before.TotalCostUSD) || after.TotalTokens != before.TotalTokens ||
		len(after.ByAPIKey) != 1 || after.ByModel["gpt-4o"].Requests != 5 || after.ByTeam["team"] == nil {
		t.Fatalf("report changed across restart:\nbefore %+v\nafter  %+v", before, after)
	}
	logs, _ := a2.QueryLogs(context.Background(), LogQuery{APIKey: "sk-live-abcdef123456"})
	if logs.Total != 5 || logs.Logs[0].APIKey == "sk-live-abcdef123456" {
		t.Fatalf("logs must be restored with redacted keys: %+v", logs)
	}
	// New entries are appended after the restored ones.
	a2.Record(CostEntry{Model: "m2", CostUSD: 1})
	if err := a2.Flush(); err != nil {
		t.Fatal(err)
	}
	if st := a2.PersistenceStats(); !st.Enabled || st.Entries != 6 || st.Pending != 0 || st.LastError != "" {
		t.Fatalf("unexpected stats %+v", st)
	}
}

func approx(a, b float64) bool { d := a - b; return d < 1e-9 && d > -1e-9 }

func TestAnalyticsPersistenceBackgroundFlush(t *testing.T) {
	ps := persist.NewMemory()
	a := NewAnalyticsEngine()
	mustEnable(t, a, ps, PersistenceOptions{FlushInterval: 10 * time.Millisecond})
	a.Record(CostEntry{Model: "m", CostUSD: 1})
	deadline := time.Now().Add(2 * time.Second)
	for a.PersistenceStats().Entries != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("background flush did not persist: %+v", a.PersistenceStats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAnalyticsPersistenceRetention(t *testing.T) {
	ps := persist.NewMemory()
	a := NewAnalyticsEngine()
	mustEnable(t, a, ps, PersistenceOptions{MaxEntries: 5, MaxAge: 24 * time.Hour})
	now := time.Now().UTC()
	// Entries older than MaxAge are dropped from disk on the next flush.
	a.Record(CostEntry{Model: "old", CostUSD: 1, Timestamp: now.Add(-48 * time.Hour)})
	_ = a.Flush()
	if st := a.PersistenceStats(); st.Chunks != 0 {
		t.Fatalf("expired chunk must be deleted: %+v", st)
	}
	for round := 0; round < 3; round++ {
		for i := 0; i < 4; i++ {
			a.Record(CostEntry{Model: "m", CostUSD: float64(round*4 + i), Timestamp: now.Add(time.Duration(round*4+i) * time.Second)})
		}
		if err := a.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	st := a.PersistenceStats()
	if st.Entries > 5 || st.Entries == 0 {
		t.Fatalf("persisted entries must be bounded by MaxEntries: %+v", st)
	}
	b := NewAnalyticsEngine()
	mustEnable(t, b, ps, PersistenceOptions{MaxEntries: 5, MaxAge: 24 * time.Hour})
	r := report(t, b)
	if r.TotalRequests != st.Entries || r.ByModel["old"] != nil {
		t.Fatalf("reload must return the retained entries only: %d vs %+v", r.TotalRequests, r.ByModel)
	}
	logs, _ := b.QueryLogs(context.Background(), LogQuery{PageSize: 1})
	if len(logs.Logs) != 1 || logs.Logs[0].CostUSD != 11 {
		t.Fatalf("newest entry must be retained: %+v", logs.Logs)
	}
}

// failingStore fails writes while fail is set.
type failingStore struct {
	persist.Store
	fail atomic.Bool
}

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(bucket, key, v)
}

func TestAnalyticsPersistenceWriteFailureKeepsPending(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	a := NewAnalyticsEngineWithCapacity(10)
	mustEnable(t, a, fs, PersistenceOptions{})
	fs.fail.Store(true)
	for i := 0; i < 3; i++ {
		a.Record(CostEntry{Model: "m", CostUSD: 1})
	}
	if err := a.Flush(); err == nil {
		t.Fatal("write failure must be reported")
	}
	if st := a.PersistenceStats(); st.Pending != 3 || st.LastError == "" || st.Entries != 0 {
		t.Fatalf("unwritten entries must stay pending: %+v", st)
	}
	// Pending entries are bounded while the store is down.
	for i := 0; i < 20; i++ {
		a.Record(CostEntry{Model: "m", CostUSD: 1})
	}
	if st := a.PersistenceStats(); st.Pending > 10 || st.Pending < 9 || st.Dropped != int64(23-st.Pending) {
		t.Fatalf("pending must be bounded: %+v", st)
	}
	fs.fail.Store(false)
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if st := a.PersistenceStats(); st.Pending != 0 || st.Entries < 9 || st.Entries > 10 || st.LastError != "" {
		t.Fatalf("recovered flush: %+v", st)
	}
}

func TestAnalyticsEnablePersistenceEdgeCases(t *testing.T) {
	ps := persist.NewMemory()
	_ = ps.Put(CostEntryBucket, "00000000000000000001-00000001", "garbage")
	a := NewAnalyticsEngine()
	a.Record(CostEntry{Model: "before", CostUSD: 2})
	mustEnable(t, a, ps, PersistenceOptions{})
	if st := a.PersistenceStats(); st.Corrupt != 1 || st.Pending != 1 {
		t.Fatalf("corrupt chunk must be skipped, earlier entries queued: %+v", st)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if st := a.PersistenceStats(); st.Corrupt != 0 || st.Chunks != 1 || st.Entries != 1 {
		t.Fatalf("corrupt chunk must be removed on flush: %+v", st)
	}
	if err := a.EnablePersistence(ps, PersistenceOptions{}); err == nil {
		t.Fatal("enabling twice must fail")
	}
	_ = a.Close()
	if err := a.EnablePersistence(ps, PersistenceOptions{}); err == nil {
		t.Fatal("re-enabling after Close must fail (entries would be persisted twice)")
	}
	if err := NewAnalyticsEngine().EnablePersistence(nil, PersistenceOptions{}); err == nil {
		t.Fatal("nil store must be rejected")
	}
	plain := NewAnalyticsEngine()
	if plain.Flush() != nil || plain.Close() != nil || plain.PersistenceStats().Enabled {
		t.Fatal("persistence helpers must be no-ops when disabled")
	}
}
