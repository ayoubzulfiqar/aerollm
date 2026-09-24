package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// flakyStore wraps a persist.Store and fails selected operations on demand.
type flakyStore struct {
	persist.Store
	failPut     atomic.Bool
	failDelete  atomic.Bool
	failForEach atomic.Bool
}

var errDisk = errors.New("disk on fire")

func (f *flakyStore) Put(bucket, key string, v any) error {
	if f.failPut.Load() {
		return errDisk
	}
	return f.Store.Put(bucket, key, v)
}

func (f *flakyStore) Delete(bucket, key string) error {
	if f.failDelete.Load() {
		return errDisk
	}
	return f.Store.Delete(bucket, key)
}

func (f *flakyStore) ForEach(bucket string, fn func(string, json.RawMessage) error) error {
	if f.failForEach.Load() {
		return errDisk
	}
	return f.Store.ForEach(bucket, fn)
}

func newPersistentStore(t *testing.T, ps persist.Store, now time.Time) (*Store, *fakeClock) {
	t.Helper()
	s, err := NewStoreWithPersistence(ps)
	if err != nil {
		t.Fatalf("NewStoreWithPersistence: %v", err)
	}
	clk := &fakeClock{t: now}
	s.now = clk.Now
	return s, clk
}

func TestPersistenceRoundTripBolt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	db, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	now := utc(2026, 1, 1, 0, 0)
	s, _ := newPersistentStore(t, db, now)
	cron, err := s.Create(ScheduledTask{ID: "cron-1", Name: "nightly", Schedule: "0 3 * * *", Timezone: "Europe/London", Payload: `{"k":"v"}`})
	if err != nil {
		t.Fatal(err)
	}
	iv, err := s.Create(ScheduledTask{Name: "tick", Type: TaskInterval, Schedule: "5m"})
	if err != nil {
		t.Fatal(err)
	}
	once, err := s.Create(ScheduledTask{Name: "once", Type: TaskOneTime, RunAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	gone, err := s.Create(ScheduledTask{Name: "gone", Schedule: "@daily"})
	if err != nil {
		t.Fatal(err)
	}
	name := "renamed"
	if _, err := s.Update(iv.ID, TaskUpdate{Name: &name}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStatus(cron.ID, TaskFailed); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(gone.ID); err != nil {
		t.Fatal(err)
	}
	want := s.List()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	s2, _ := newPersistentStore(t, db2, now)
	got := s2.List()
	if len(got) != 3 || len(want) != 3 {
		t.Fatalf("expected 3 tasks after reload, got %d (before %d)", len(got), len(want))
	}
	for i := range want {
		a, b := want[i], got[i]
		if a.ID != b.ID || a.Name != b.Name || a.Type != b.Type || a.Schedule != b.Schedule ||
			a.Payload != b.Payload || a.Status != b.Status || a.Timezone != b.Timezone ||
			!a.CreatedAt.Equal(b.CreatedAt) || !a.NextRun.Equal(b.NextRun) || !a.RunAt.Equal(b.RunAt) {
			t.Fatalf("task %d differs after reload:\nbefore %+v\nafter  %+v", i, a, b)
		}
	}
	if cur, _ := s2.Get(iv.ID); cur.Name != "renamed" {
		t.Fatalf("update not persisted: %+v", cur)
	}
	if cur, _ := s2.Get(cron.ID); cur.Status != TaskFailed {
		t.Fatalf("status not persisted: %+v", cur)
	}
	if _, ok := s2.Get(gone.ID); ok {
		t.Fatal("deleted task resurrected")
	}
	// The reloaded store keeps writing through.
	if _, err := s2.Update(once.ID, TaskUpdate{Name: &name}); err != nil {
		t.Fatal(err)
	}
	var doc ScheduledTask
	if ok, err := db2.Get(BucketTasks, once.ID, &doc); !ok || err != nil || doc.Name != "renamed" {
		t.Fatalf("write-through after reload failed: ok=%v err=%v doc=%+v", ok, err, doc)
	}
}

// TestPersistenceResumesRuntimeState checks that a restart neither re-fires
// a run that already happened nor skips one that was due while down.
func TestPersistenceResumesRuntimeState(t *testing.T) {
	ps := persist.NewMemory()
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newPersistentStore(t, ps, start)
	task, err := s.Create(ScheduledTask{Name: "tick", Type: TaskInterval, Schedule: "10s"})
	if err != nil {
		t.Fatal(err)
	}
	once, err := s.Create(ScheduledTask{Name: "once", Type: TaskOneTime, RunAt: start.Add(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int32
	cancel := startRunner(t, s, func(context.Context, ScheduledTask) error {
		runs.Add(1)
		return nil
	}, RunnerOptions{})
	clk.Advance(10 * time.Second)
	waitFor(t, "both runs", func() bool {
		a, _ := s.Get(task.ID)
		b, _ := s.Get(once.ID)
		return a.Runs == 1 && a.Status == TaskCompleted && b.Status == TaskCompleted
	})
	cancel()

	// Restart 5s later: the interval task is not due yet (next 20s) and the
	// completed one-time task (run time in the past) loads without error.
	s2, _ := newPersistentStore(t, ps, start.Add(15*time.Second))
	a, _ := s2.Get(task.ID)
	if a.Runs != 1 || !a.NextRun.Equal(start.Add(20*time.Second)) || !a.LastRunAt.Equal(start.Add(10*time.Second)) {
		t.Fatalf("runtime state not restored: %+v", a)
	}
	b, _ := s2.Get(once.ID)
	if b.Status != TaskCompleted || !b.NextRun.IsZero() || b.Runs != 1 {
		t.Fatalf("one-time task state not restored: %+v", b)
	}
	var runs2 atomic.Int32
	cancel2 := startRunner(t, s2, func(context.Context, ScheduledTask) error {
		runs2.Add(1)
		return nil
	}, RunnerOptions{})
	time.Sleep(20 * time.Millisecond)
	if runs2.Load() != 0 {
		t.Fatal("restart re-fired a run that already happened")
	}
	cancel2()

	// Restart after a long outage: the missed activation runs exactly once.
	s3, clk3 := newPersistentStore(t, ps, start.Add(95*time.Second))
	var runs3 atomic.Int32
	startRunner(t, s3, func(context.Context, ScheduledTask) error {
		runs3.Add(1)
		return nil
	}, RunnerOptions{})
	waitFor(t, "catch-up run", func() bool {
		cur, _ := s3.Get(task.ID)
		return cur.Runs == 2 && cur.Status == TaskCompleted
	})
	time.Sleep(20 * time.Millisecond)
	cur, _ := s3.Get(task.ID)
	if runs3.Load() != 1 || !cur.NextRun.After(clk3.Now()) {
		t.Fatalf("expected one catch-up run and a future next_run: runs=%d task=%+v", runs3.Load(), cur)
	}
	var doc ScheduledTask
	if ok, _ := ps.Get(BucketTasks, task.ID, &doc); !ok || doc.Runs != 2 || !doc.NextRun.Equal(cur.NextRun) {
		t.Fatalf("run result not persisted: %+v", doc)
	}
}

func TestPersistenceFailuresLeaveMemoryUnchanged(t *testing.T) {
	fs := &flakyStore{Store: persist.NewMemory()}
	now := utc(2026, 1, 1, 0, 0)
	s, _ := newPersistentStore(t, fs, now)
	task, err := s.Create(ScheduledTask{ID: "keep", Name: "keep", Schedule: "@hourly"})
	if err != nil {
		t.Fatal(err)
	}

	fs.failPut.Store(true)
	if _, err := s.Create(ScheduledTask{ID: "new", Name: "new", Schedule: "@daily"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Create: expected ErrPersistence, got %v", err)
	}
	if _, ok := s.Get("new"); ok {
		t.Fatal("failed Create must not store the task")
	}
	if _, err := s.Upsert(ScheduledTask{ID: "keep", Name: "changed", Schedule: "@daily"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Upsert: expected ErrPersistence, got %v", err)
	}
	name := "changed"
	if _, err := s.Update("keep", TaskUpdate{Name: &name}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Update: expected ErrPersistence, got %v", err)
	}
	if err := s.SetStatus("keep", TaskFailed); !errors.Is(err, ErrPersistence) {
		t.Fatalf("SetStatus: expected ErrPersistence, got %v", err)
	}
	s.UpdateStatus("keep", TaskFailed) // logged, not applied
	if cur, _ := s.Get("keep"); cur.Name != "keep" || cur.Schedule != "@hourly" || cur.Status != TaskPending || !cur.NextRun.Equal(task.NextRun) {
		t.Fatalf("failed mutations changed memory: %+v", cur)
	}
	fs.failPut.Store(false)

	fs.failDelete.Store(true)
	if err := s.Remove("keep"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Remove: expected ErrPersistence, got %v", err)
	}
	if s.Delete("keep") {
		t.Fatal("Delete must report false when persistence fails")
	}
	if _, ok := s.Get("keep"); !ok {
		t.Fatal("failed delete must keep the task")
	}

	// HTTP maps persistence failures to 500.
	mux := newTestMux(s)
	rec := do(t, mux, http.MethodDelete, "/v1/schedule/keep", "")
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "persistence failure") {
		t.Fatalf("DELETE: expected 500 persistence failure, got %d %s", rec.Code, rec.Body.String())
	}
	fs.failDelete.Store(false)
	fs.failPut.Store(true)
	rec = do(t, mux, http.MethodPost, "/v1/schedule", `{"name":"x","schedule":"@daily"}`)
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "disk on fire") {
		t.Fatalf("POST: expected opaque 500, got %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, mux, http.MethodPatch, "/v1/schedule/keep", `{"name":"y"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PATCH: expected 500, got %d %s", rec.Code, rec.Body.String())
	}
	fs.failPut.Store(false)
	if rec := do(t, mux, http.MethodDelete, "/v1/schedule/keep", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE after recovery: %d %s", rec.Code, rec.Body.String())
	}
}

// TestPersistenceRunResultFailureDoesNotWedge checks that a failed write of
// a run result still advances the in-memory schedule.
func TestPersistenceRunResultFailureDoesNotWedge(t *testing.T) {
	fs := &flakyStore{Store: persist.NewMemory()}
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newPersistentStore(t, fs, start)
	task, err := s.Create(ScheduledTask{Name: "tick", Type: TaskInterval, Schedule: "10s"})
	if err != nil {
		t.Fatal(err)
	}
	fs.failPut.Store(true)
	var runs atomic.Int32
	startRunner(t, s, func(context.Context, ScheduledTask) error {
		runs.Add(1)
		return nil
	}, RunnerOptions{})
	clk.Advance(10 * time.Second)
	waitFor(t, "first run", func() bool { cur, _ := s.Get(task.ID); return cur.Runs == 1 && cur.Status == TaskCompleted })
	clk.Advance(10 * time.Second)
	waitFor(t, "second run", func() bool { cur, _ := s.Get(task.ID); return cur.Runs == 2 && cur.Status == TaskCompleted })
	var doc ScheduledTask
	if ok, _ := fs.Get(BucketTasks, task.ID, &doc); !ok || doc.Runs != 0 {
		t.Fatalf("persisted doc should still hold the pre-run state: %+v", doc)
	}
}

func TestEnablePersistence(t *testing.T) {
	s := NewStore()
	if err := s.EnablePersistence(nil); err == nil {
		t.Fatal("expected error for nil store")
	}
	if s.PersistenceEnabled() {
		t.Fatal("persistence must not be enabled after a failure")
	}

	ps := persist.NewMemory()
	now := utc(2026, 1, 1, 0, 0)
	// Pre-existing documents: one valid (status running), several invalid.
	valid := ScheduledTask{ID: "valid", Name: "valid", Type: TaskCron, Schedule: "@hourly", Status: TaskRunning,
		CreatedAt: now, NextRun: now.Add(time.Hour)}
	mustPut(t, ps, "valid", valid)
	mustPut(t, ps, "bad-cron", ScheduledTask{ID: "bad-cron", Name: "x", Schedule: "99 * * * *"})
	mustPut(t, ps, "mismatch", ScheduledTask{ID: "other", Name: "x", Schedule: "@daily"})
	mustPut(t, ps, "garbage", "not an object")
	mustPut(t, ps, "shared", ScheduledTask{ID: "shared", Name: "from-disk", Schedule: "@daily", CreatedAt: now, NextRun: now.Add(24 * time.Hour)})

	s.now = func() time.Time { return now }
	mem, err := s.Create(ScheduledTask{ID: "mem-only", Name: "mem", Schedule: "@daily"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ScheduledTask{ID: "shared", Name: "from-memory", Schedule: "@daily"}); err != nil {
		t.Fatal(err)
	}

	err = s.EnablePersistence(ps)
	if !errors.Is(err, ErrInvalidDocuments) {
		t.Fatalf("expected ErrInvalidDocuments, got %v", err)
	}
	for _, key := range []string{"bad-cron", "mismatch", "garbage"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error should name skipped document %q: %v", key, err)
		}
	}
	if !s.PersistenceEnabled() {
		t.Fatal("persistence must be enabled despite skipped documents")
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling twice must fail")
	}
	if cur, ok := s.Get("valid"); !ok || cur.Status != TaskPending || !cur.NextRun.Equal(valid.NextRun) {
		t.Fatalf("valid document not loaded as pending: %+v", cur)
	}
	if cur, _ := s.Get("shared"); cur.Name != "from-disk" {
		t.Fatalf("persisted document must win over memory: %+v", cur)
	}
	var doc ScheduledTask
	if ok, _ := ps.Get(BucketTasks, mem.ID, &doc); !ok || doc.Name != "mem" {
		t.Fatalf("in-memory task not written to the store: ok=%v %+v", ok, doc)
	}
	if n := len(s.List()); n != 3 {
		t.Fatalf("expected 3 tasks, got %d", n)
	}
	// Skipped documents are left untouched in the store.
	var raw json.RawMessage
	if ok, _ := ps.Get(BucketTasks, "garbage", &raw); !ok {
		t.Fatal("invalid documents must not be deleted")
	}

	// A load failure leaves the store unchanged and non-persistent.
	fs := &flakyStore{Store: persist.NewMemory()}
	fs.failForEach.Store(true)
	s2 := NewStore()
	if err := s2.EnablePersistence(fs); !errors.Is(err, ErrPersistence) || s2.PersistenceEnabled() {
		t.Fatalf("expected ErrPersistence and disabled persistence, got %v", err)
	}
	if st, err := NewStoreWithPersistence(fs); st != nil || !errors.Is(err, ErrPersistence) {
		t.Fatalf("NewStoreWithPersistence must fail on load errors: %v %v", st, err)
	}
	// A failure writing in-memory tasks also leaves the store unchanged.
	fs.failForEach.Store(false)
	fs.failPut.Store(true)
	s3 := NewStore()
	if _, err := s3.Create(ScheduledTask{ID: "a", Name: "a", Schedule: "@daily"}); err != nil {
		t.Fatal(err)
	}
	if err := s3.EnablePersistence(fs); !errors.Is(err, ErrPersistence) || s3.PersistenceEnabled() {
		t.Fatalf("expected write failure, got %v", err)
	}
	if _, ok := s3.Get("a"); !ok {
		t.Fatal("in-memory tasks must survive a failed enable")
	}
}

func TestEnablePersistenceRespectsLimit(t *testing.T) {
	ps := persist.NewMemory()
	for _, id := range []string{"a", "b", "c"} {
		mustPut(t, ps, id, ScheduledTask{ID: id, Name: id, Schedule: "@daily"})
	}
	s := NewStore()
	s.maxTasks = 2
	err := s.EnablePersistence(ps)
	if !errors.Is(err, ErrInvalidDocuments) || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected limit error, got %v", err)
	}
	if n := len(s.List()); n != 2 {
		t.Fatalf("expected 2 tasks, got %d", n)
	}
}

func mustPut(t *testing.T, ps persist.Store, key string, v any) {
	t.Helper()
	if err := ps.Put(BucketTasks, key, v); err != nil {
		t.Fatal(err)
	}
}
