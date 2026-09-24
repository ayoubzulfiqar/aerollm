package incident

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// failingStore wraps a Memory store; writes fail while fail is set, and
// the next failPuts Puts fail.
type failingStore struct {
	*persist.Memory
	fail     atomic.Bool
	failPuts atomic.Int32
}

var errDisk = errors.New("disk on fire")

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.fail.Load() || f.failPuts.Add(-1) >= 0 {
		return errDisk
	}
	return f.Memory.Put(bucket, key, v)
}

func (f *failingStore) Delete(bucket, key string) error {
	if f.fail.Load() {
		return errDisk
	}
	return f.Memory.Delete(bucket, key)
}

func TestPersistenceReloadBolt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incidents.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStoreWithPersistence(ps)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Create(Incident{Title: "db down", Severity: SeverityCritical})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(a.ID, StatusAcknowledged); err != nil {
		t.Fatal(err)
	}
	b, err := s.Create(Incident{ID: "inc-manual", Title: "to delete"})
	if err != nil {
		t.Fatal(err)
	}
	h := WebhookHandler(s)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPatch, "/v1/incidents/"+a.ID, strings.NewReader(`{"description":"primary lost quorum"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodDelete, "/v1/incidents/"+b.ID, nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	s2, err := NewStoreWithPersistence(ps2)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get(a.ID)
	if !ok || got.Status != StatusAcknowledged || got.Description != "primary lost quorum" || got.AcknowledgedAt.IsZero() || got.Severity != SeverityCritical {
		t.Fatalf("reloaded incident = %+v, ok=%v", got, ok)
	}
	if _, ok := s2.Get(b.ID); ok {
		t.Fatal("deleted incident came back after reload")
	}
	// The lifecycle is still enforced on reloaded incidents.
	if _, err := s2.Transition(a.ID, StatusClosed); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Transition(a.ID, StatusOpen); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("closed incident reopened: %v", err)
	}
}

func TestPersistenceEvictionIsDurable(t *testing.T) {
	ps := persist.NewMemory()
	s := NewStoreWithLimit(2)
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { clock = clock.Add(time.Second); return clock }
	old, _ := s.Create(Incident{Title: "old", Status: StatusResolved})
	keep, _ := s.Create(Incident{Title: "open"})
	fresh, err := s.Create(Incident{Title: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := ps.Get(BucketIncidents, old.ID, &Incident{}); ok {
		t.Fatal("evicted incident still persisted")
	}
	for _, id := range []string{keep.ID, fresh.ID} {
		if ok, _ := ps.Get(BucketIncidents, id, &Incident{}); !ok {
			t.Fatalf("incident %s not persisted", id)
		}
	}
}

func TestPersistenceEvictionRollbackOnPutFailure(t *testing.T) {
	fs := &failingStore{Memory: persist.NewMemory()}
	s := NewStoreWithLimit(1)
	if err := s.EnablePersistence(fs); err != nil {
		t.Fatal(err)
	}
	old, _ := s.Create(Incident{Title: "old", Status: StatusClosed})
	fs.failPuts.Store(1)
	if _, err := s.Create(Incident{Title: "new"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	if _, ok := s.Get(old.ID); !ok || len(s.List()) != 1 {
		t.Fatal("memory changed despite failed create")
	}
	// The victim deleted from ps before the failed put was restored.
	if ok, _ := fs.Get(BucketIncidents, old.ID, &Incident{}); !ok {
		t.Fatal("evicted incident not restored in durable store")
	}
}

func TestPersistenceMergeAndEnableTwice(t *testing.T) {
	ps := persist.NewMemory()
	_ = ps.Put(BucketIncidents, "inc-shared", Incident{ID: "inc-shared", Title: "persisted", Severity: SeverityLow, Status: StatusOpen})
	s := NewStore()
	_, _ = s.Create(Incident{ID: "inc-shared", Title: "memory"})
	local, _ := s.Create(Incident{ID: "inc-local", Title: "local"})
	var hookCalls atomic.Int32
	s.SetHook(func(Event) { hookCalls.Add(1) })
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if hookCalls.Load() != 0 {
		t.Fatal("hook invoked for loaded incidents")
	}
	if got, _ := s.Get("inc-shared"); got.Title != "persisted" {
		t.Fatalf("persisted document should win, got %q", got.Title)
	}
	if ok, _ := ps.Get(BucketIncidents, local.ID, &Incident{}); !ok {
		t.Fatal("in-memory incident not written to store")
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling persistence twice should fail")
	}
	if st, err := NewStoreWithPersistence(nil); st != nil || err == nil {
		t.Fatal("nil persist store should fail")
	}
}

func TestPersistenceSkipsInvalidDocuments(t *testing.T) {
	ps := persist.NewMemory()
	_ = ps.Put(BucketIncidents, "inc-ok", Incident{ID: "inc-ok", Title: "ok", Status: StatusOpen, Severity: SeverityHigh})
	_ = ps.Put(BucketIncidents, "inc-nostatus", Incident{ID: "inc-nostatus", Title: "x"})
	_ = ps.Put(BucketIncidents, "inc-badsev", Incident{ID: "inc-badsev", Title: "x", Status: StatusOpen, Severity: "apocalyptic"})
	_ = ps.Put(BucketIncidents, "inc-mismatch", Incident{ID: "other", Title: "x", Status: StatusOpen})
	_ = ps.Put(BucketIncidents, "inc-junk", json.RawMessage(`[1,2,3]`))
	s, err := NewStoreWithPersistence(ps)
	if s == nil {
		t.Fatalf("store should be usable: %v", err)
	}
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "skipped 4") {
		t.Fatalf("expected 4 skipped documents, got %v", err)
	}
	if l := s.List(); len(l) != 1 || l[0].ID != "inc-ok" {
		t.Fatalf("unexpected incidents: %+v", l)
	}
}

func TestPersistenceLimitKeepsNewest(t *testing.T) {
	ps := persist.NewMemory()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, id := range []string{"inc-a", "inc-b", "inc-c"} {
		_ = ps.Put(BucketIncidents, id, Incident{ID: id, Title: id, Status: StatusOpen, UpdatedAt: base.Add(time.Duration(i) * time.Hour)})
	}
	s := NewStoreWithLimit(2)
	err := s.EnablePersistence(ps)
	if err == nil || !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected over-limit document to be reported, got %v", err)
	}
	if _, ok := s.Get("inc-a"); ok {
		t.Fatal("oldest incident should have been dropped")
	}
	if len(s.List()) != 2 {
		t.Fatalf("expected 2 incidents, got %d", len(s.List()))
	}
}

func TestPersistenceFailureLeavesMemoryUnchanged(t *testing.T) {
	fs := &failingStore{Memory: persist.NewMemory()}
	s, err := NewStoreWithPersistence(fs)
	if err != nil {
		t.Fatal(err)
	}
	inc, err := s.Create(Incident{Title: "keep"})
	if err != nil {
		t.Fatal(err)
	}
	var events atomic.Int32
	s.SetHook(func(Event) { events.Add(1) })
	fs.fail.Store(true)

	if _, err := s.Create(Incident{Title: "new"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Create err = %v", err)
	}
	if _, err := s.Transition(inc.ID, StatusResolved); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Transition err = %v", err)
	}
	if s.Resolve(inc.ID) {
		t.Fatal("Resolve should fail when not persisted")
	}
	if ok, err := s.DeleteIncident(inc.ID); ok || !errors.Is(err, ErrPersistence) {
		t.Fatalf("DeleteIncident = %v, %v", ok, err)
	}
	if s.Delete(inc.ID) {
		t.Fatal("Delete should report false when not persisted")
	}
	if got, ok := s.Get(inc.ID); !ok || got.Status != StatusOpen || len(s.List()) != 1 {
		t.Fatalf("memory changed despite persistence failure: %+v", s.List())
	}
	if events.Load() != 0 {
		t.Fatal("hook fired for a failed mutation")
	}

	h := WebhookHandler(s)
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/incidents", `{"title":"x"}`},
		{http.MethodPost, "/v1/incidents/" + inc.ID + "/ack", ""},
		{http.MethodPut, "/v1/incidents/" + inc.ID, `{"title":"renamed"}`},
		{http.MethodDelete, "/v1/incidents/" + inc.ID, ""},
	} {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "persistence failure") {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), errDisk.Error()) {
			t.Fatalf("internal error leaked: %s", rec.Body)
		}
	}
}
