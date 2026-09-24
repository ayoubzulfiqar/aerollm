package flags

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// failingStore wraps a Memory store and fails writes while fail is set.
type failingStore struct {
	*persist.Memory
	fail atomic.Bool
}

var errDisk = errors.New("disk on fire")

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.fail.Load() {
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
	path := filepath.Join(t.TempDir(), "flags.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStoreWithPersistence(ps)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(FeatureFlag{Key: "beta", Enabled: true, Strategy: RolloutPercentage, Percentage: 30, Rules: []Rule{{Attribute: "plan", Operator: OpEquals, Values: []string{"pro"}, Serve: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(FeatureFlag{Key: "gone", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRollout("beta", RolloutPolicy{Key: "beta", Weight: 30}); err != nil {
		t.Fatal(err)
	}
	// PATCH through the handler must also be persisted.
	h := WebhookHandler(s)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPatch, "/v1/flags/beta", strings.NewReader(`{"percentage":60}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodDelete, "/v1/flags/gone", nil))
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
	got, ok := s2.Get("beta")
	if !ok || got.Percentage != 60 || len(got.Rules) != 1 || got.Strategy != RolloutPercentage {
		t.Fatalf("reloaded flag = %+v, ok=%v", got, ok)
	}
	if _, ok := s2.Get("gone"); ok {
		t.Fatal("deleted flag came back after reload")
	}
	if p, ok := s2.GetRollout("beta"); !ok || p.Weight != 30 {
		t.Fatalf("rollout not reloaded: %+v %v", p, ok)
	}
	if !s2.Enabled("beta", map[string]string{"plan": "pro"}) {
		t.Fatal("reloaded rule not evaluated")
	}
}

func TestPersistenceWritesExistingEntriesAndPrefersPersisted(t *testing.T) {
	ps := persist.NewMemory()
	if err := ps.Put(BucketFlags, "shared", FeatureFlag{Key: "shared", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	_ = s.Upsert(FeatureFlag{Key: "shared", Enabled: true})
	_ = s.Upsert(FeatureFlag{Key: "local", Enabled: true})
	s.SetRollout("local", RolloutPolicy{Key: "local", Weight: 5})
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if f, _ := s.Get("shared"); f.Enabled {
		t.Fatal("persisted document should win over in-memory entry")
	}
	var f FeatureFlag
	if ok, err := ps.Get(BucketFlags, "local", &f); err != nil || !ok || !f.Enabled {
		t.Fatalf("in-memory flag not written to store: %v %v %+v", ok, err, f)
	}
	var p RolloutPolicy
	if ok, _ := ps.Get(BucketRollouts, "local", &p); !ok || p.Weight != 5 {
		t.Fatal("in-memory rollout not written to store")
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling persistence twice should fail")
	}
	if err := NewStore().EnablePersistence(nil); err == nil {
		t.Fatal("nil persist store should fail")
	}
}

func TestPersistenceSkipsInvalidDocuments(t *testing.T) {
	ps := persist.NewMemory()
	_ = ps.Put(BucketFlags, "ok", FeatureFlag{Key: "ok", Enabled: true})
	_ = ps.Put(BucketFlags, "bad", FeatureFlag{Key: "bad", Percentage: 500})
	_ = ps.Put(BucketFlags, "mismatch", FeatureFlag{Key: "other"})
	_ = ps.Put(BucketFlags, "junk", json.RawMessage(`"not an object"`))
	s, err := NewStoreWithPersistence(ps)
	if s == nil {
		t.Fatalf("store should be usable despite invalid docs: %v", err)
	}
	if !errors.Is(err, ErrInvalidFlag) || !strings.Contains(err.Error(), "skipped 3") {
		t.Fatalf("expected skipped-documents error, got %v", err)
	}
	if len(s.List()) != 1 {
		t.Fatalf("expected only the valid flag, got %+v", s.List())
	}
	// Write-through is active despite the load warning.
	if err := s.Upsert(FeatureFlag{Key: "new", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := ps.Get(BucketFlags, "new", &FeatureFlag{}); !ok {
		t.Fatal("write-through not enabled after partial load")
	}
}

func TestPersistenceFailureLeavesMemoryUnchanged(t *testing.T) {
	fs := &failingStore{Memory: persist.NewMemory()}
	s, err := NewStoreWithPersistence(fs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(FeatureFlag{Key: "keep", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	fs.fail.Store(true)

	if err := s.Upsert(FeatureFlag{Key: "new", Enabled: true}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Upsert err = %v, want ErrPersistence", err)
	}
	if _, ok := s.Get("new"); ok {
		t.Fatal("flag stored in memory despite persistence failure")
	}
	if ok, err := s.DeleteFlag("keep"); ok || !errors.Is(err, ErrPersistence) {
		t.Fatalf("DeleteFlag = %v, %v", ok, err)
	}
	if s.Delete("keep") {
		t.Fatal("Delete should report false when not persisted")
	}
	if _, ok := s.Get("keep"); !ok {
		t.Fatal("flag removed from memory despite persistence failure")
	}
	if err := s.PutRollout("keep", RolloutPolicy{Key: "keep"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("PutRollout err = %v", err)
	}
	s.SetRollout("keep", RolloutPolicy{Key: "keep"})
	if _, ok := s.GetRollout("keep"); ok {
		t.Fatal("rollout stored despite persistence failure")
	}

	h := WebhookHandler(s)
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/v1/flags", `{"key":"x","enabled":true}`},
		{http.MethodPatch, "/v1/flags/keep", `{"enabled":false}`},
		{http.MethodDelete, "/v1/flags/keep", ""},
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
	if f, _ := s.Get("keep"); !f.Enabled {
		t.Fatal("PATCH applied in memory despite persistence failure")
	}
}

func TestPersistenceEnableFailsOnWriteError(t *testing.T) {
	fs := &failingStore{Memory: persist.NewMemory()}
	fs.fail.Store(true)
	s := NewStore()
	_ = s.Upsert(FeatureFlag{Key: "local", Enabled: true})
	if err := s.EnablePersistence(fs); !errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	// Still in-memory only and unchanged.
	fs.fail.Store(false)
	if err := s.Upsert(FeatureFlag{Key: "other", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := fs.Get(BucketFlags, "other", &FeatureFlag{}); ok {
		t.Fatal("store should not write through after failed enable")
	}
	if _, ok := s.Get("local"); !ok {
		t.Fatal("in-memory state lost after failed enable")
	}
	if st, err := NewStoreWithPersistence(nil); st != nil || err == nil {
		t.Fatal("NewStoreWithPersistence(nil) should fail with a nil store")
	}
}
