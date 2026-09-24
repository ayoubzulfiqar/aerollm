package meter

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

func enable(t *testing.T, r *Recorder, ps persist.Store, opts PersistenceOptions) {
	t.Helper()
	if opts.FlushInterval == 0 {
		opts.FlushInterval = time.Hour
	}
	if err := r.EnablePersistence(ps, opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
}

func TestMeterPersistenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meter.db")
	db, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := NewRecorder()
	r1.Record(UsageRecord{APIKey: "before-enable", Model: "m0", TokensIn: 1})
	enable(t, r1, db, PersistenceOptions{})
	for i := 0; i < 3; i++ {
		r1.Record(UsageRecord{APIKey: "key-1", Provider: "openai", Model: "gpt-4o", TokensIn: 10, TokensOut: 5, LatencyMs: 12.5})
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	r2 := NewRecorder()
	enable(t, r2, db2, PersistenceOptions{})
	recs := r2.Records()
	if len(recs) != 4 || recs[0].APIKey != "before-enable" || recs[3].LatencyMs != 12.5 || recs[3].Timestamp.IsZero() {
		t.Fatalf("records not restored in order: %+v", recs)
	}
	agg, _ := r2.Aggregate("model", time.Time{})
	if len(agg) != 2 || agg[0].Key != "gpt-4o" || agg[0].TokensIn != 30 {
		t.Fatalf("aggregate after restart: %+v", agg)
	}
	// Clear also removes the persisted records.
	r2.Clear()
	if st := r2.PersistenceStats(); st.Records != 0 || st.Chunks != 0 || st.Pending != 0 {
		t.Fatalf("clear must purge persisted records: %+v", st)
	}
	r3 := NewRecorder()
	enable(t, r3, db2, PersistenceOptions{})
	if r3.Len() != 0 {
		t.Fatalf("cleared records came back: %d", r3.Len())
	}
}

func TestMeterPersistenceRetentionAndBackgroundFlush(t *testing.T) {
	ps := persist.NewMemory()
	r := NewRecorderWithCapacity(100)
	enable(t, r, ps, PersistenceOptions{MaxRecords: 3, MaxAge: time.Hour, FlushInterval: 10 * time.Millisecond})
	r.Record(UsageRecord{Model: "old", Timestamp: time.Now().Add(-2 * time.Hour)})
	for i := 0; i < 6; i++ {
		r.Record(UsageRecord{Model: "new", TokensIn: int64(i)})
		time.Sleep(15 * time.Millisecond) // let the flusher write small chunks
	}
	deadline := time.Now().Add(2 * time.Second)
	for r.PersistenceStats().Pending != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("background flush stalled: %+v", r.PersistenceStats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = r.Flush()
	if st := r.PersistenceStats(); st.Records > 3 || st.Records == 0 {
		t.Fatalf("persisted records must be bounded: %+v", st)
	}
	r2 := NewRecorder()
	enable(t, r2, ps, PersistenceOptions{MaxRecords: 3, MaxAge: time.Hour})
	recs := r2.Records()
	if len(recs) == 0 || len(recs) > 3 || recs[len(recs)-1].TokensIn != 5 {
		t.Fatalf("reload must keep the newest records: %+v", recs)
	}
	for _, u := range recs {
		if u.Model == "old" {
			t.Fatal("records older than MaxAge must not be reloaded")
		}
	}
}

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

func TestMeterPersistenceWriteFailure(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	r := NewRecorder()
	enable(t, r, fs, PersistenceOptions{})
	fs.fail.Store(true)
	r.Record(UsageRecord{Model: "m"})
	if err := r.Flush(); err == nil {
		t.Fatal("write failure must be reported")
	}
	if st := r.PersistenceStats(); st.Pending != 1 || st.LastError == "" {
		t.Fatalf("unexpected stats %+v", st)
	}
	fs.fail.Store(false)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r2 := NewRecorder()
	enable(t, r2, fs.Store, PersistenceOptions{})
	if r2.Len() != 1 {
		t.Fatalf("record must be persisted after recovery, got %d", r2.Len())
	}
	if err := r2.EnablePersistence(fs.Store, PersistenceOptions{}); err == nil {
		t.Fatal("enabling twice must fail")
	}
	var nilRec *Recorder
	if nilRec.EnablePersistence(fs.Store, PersistenceOptions{}) == nil || nilRec.Flush() != nil || nilRec.Close() != nil {
		t.Fatal("nil recorder helpers must be safe")
	}
}
