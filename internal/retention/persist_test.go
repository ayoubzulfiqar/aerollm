package retention

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// faultyStore wraps a persist.Store and fails selected operations.
type faultyStore struct {
	persist.Store
	mu          sync.Mutex
	failPut     bool
	failDelete  bool
	failForEach bool
}

var errInjected = errors.New("injected failure")

func (f *faultyStore) Put(bucket, key string, v any) error {
	f.mu.Lock()
	fail := f.failPut
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.Store.Put(bucket, key, v)
}

func (f *faultyStore) Delete(bucket, key string) error {
	f.mu.Lock()
	fail := f.failDelete
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.Store.Delete(bucket, key)
}

func (f *faultyStore) ForEach(bucket string, fn func(string, json.RawMessage) error) error {
	f.mu.Lock()
	fail := f.failForEach
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.Store.ForEach(bucket, fn)
}

func (f *faultyStore) set(fn func(*faultyStore)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

func mustPersistent(t *testing.T, ps persist.Store) *RetentionStore {
	t.Helper()
	s, err := NewRetentionStoreWithPersistence(ps)
	if err != nil {
		t.Fatalf("NewRetentionStoreWithPersistence: %v", err)
	}
	return s
}

// samePolicies compares policy lists, using time.Equal for CreatedAt (the
// monotonic clock reading does not survive JSON).
func samePolicies(t *testing.T, got, want []RetentionPolicy) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d policies, want %d:\n got %+v\nwant %+v", len(got), len(want), got, want)
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.ID != w.ID || g.Resource != w.Resource || g.TTL != w.TTL || g.MaxItems != w.MaxItems || !g.CreatedAt.Equal(w.CreatedAt) {
			t.Fatalf("policy %d differs:\n got %+v\nwant %+v", i, g, w)
		}
	}
}

func seedPolicies(t *testing.T, s *RetentionStore) {
	t.Helper()
	if _, err := s.Upsert(RetentionPolicy{ID: "logs", Resource: "logs", TTL: 36*time.Hour + 17*time.Second + 3, MaxItems: 500}); err != nil {
		t.Fatal(err)
	}
	h := WebhookHandler(s)
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"id":"audit","resource":"audit/events","ttl":"7d"}`); rec.Code != http.StatusOK {
		t.Fatalf("POST: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"resource":"cache","max_items":10}`); rec.Code != http.StatusOK {
		t.Fatalf("POST: %d %s", rec.Code, rec.Body)
	}
}

func TestPersistenceReloadMemory(t *testing.T) {
	ps := persist.NewMemory()
	s1 := mustPersistent(t, ps)
	seedPolicies(t, s1)
	s2 := mustPersistent(t, ps)
	samePolicies(t, s2.List(), s1.List())
	if ttl, maxItems, ok := s2.Effective("logs"); !ok || ttl != 36*time.Hour+17*time.Second+3 || maxItems != 500 {
		t.Fatalf("effective bounds lost: %v %d %v", ttl, maxItems, ok)
	}
}

func TestPersistenceReloadBolt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s1 := mustPersistent(t, ps)
	seedPolicies(t, s1)
	// PATCH preserves CreatedAt and is persisted.
	if rec := do(t, WebhookHandler(s1), http.MethodPatch, "/v1/retention/logs", `{"max_items":7}`); rec.Code != http.StatusOK {
		t.Fatalf("PATCH: %d %s", rec.Code, rec.Body)
	}
	want := s1.List()
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	s2 := mustPersistent(t, ps2)
	samePolicies(t, s2.List(), want)
	if p, _ := s2.Get("logs"); p.MaxItems != 7 {
		t.Fatalf("patch not persisted: %+v", p)
	}
}

func TestPersistenceDeleteRemovesDocument(t *testing.T) {
	ps := persist.NewMemory()
	s := mustPersistent(t, ps)
	seedPolicies(t, s)
	if rec := do(t, WebhookHandler(s), http.MethodDelete, "/v1/retention/audit", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d %s", rec.Code, rec.Body)
	}
	if !s.Delete("logs") {
		t.Fatal("Delete should report removal")
	}
	if err := s.Remove("logs"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove missing: want ErrNotFound, got %v", err)
	}
	var v json.RawMessage
	for _, id := range []string{"audit", "logs"} {
		if ok, _ := ps.Get(BucketPolicies, id, &v); ok {
			t.Fatalf("%s still persisted after delete", id)
		}
	}
	if n := len(mustPersistent(t, ps).List()); n != 1 {
		t.Fatalf("expected 1 policy after reload, got %d", n)
	}
}

func TestPersistenceWriteFailuresLeaveMemoryUnchanged(t *testing.T) {
	fs := &faultyStore{Store: persist.NewMemory()}
	s := mustPersistent(t, fs)
	seedPolicies(t, s)
	before := s.List()
	h := WebhookHandler(s)

	fs.set(func(f *faultyStore) { f.failPut = true })
	if _, err := s.Upsert(RetentionPolicy{ID: "logs", Resource: "logs", TTL: time.Hour}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Upsert: want ErrPersistence, got %v", err)
	}
	if _, err := s.Upsert(RetentionPolicy{ID: "new", Resource: "x", MaxItems: 1}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Upsert: want ErrPersistence, got %v", err)
	}
	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/retention", `{"id":"n2","resource":"y","max_items":3}`},
		{http.MethodPatch, "/v1/retention/logs", `{"max_items":1}`},
		{http.MethodPut, "/v1/retention/audit", `{"resource":"audit/events","ttl":1}`},
	} {
		rec := do(t, h, c.method, c.path, c.body)
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"persistence failure"`) {
			t.Fatalf("%s %s: want 500 persistence failure, got %d %s", c.method, c.path, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), errInjected.Error()) {
			t.Fatalf("internal error leaked: %s", rec.Body)
		}
	}

	fs.set(func(f *faultyStore) { f.failPut = false; f.failDelete = true })
	if s.Delete("logs") {
		t.Fatal("Delete must report false when the durable delete fails")
	}
	if err := s.Remove("logs"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("Remove: want ErrPersistence, got %v", err)
	}
	if rec := do(t, h, http.MethodDelete, "/v1/retention/audit", ""); rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "persistence failure") {
		t.Fatalf("DELETE: want 500, got %d %s", rec.Code, rec.Body)
	}
	samePolicies(t, s.List(), before)

	fs.set(func(f *faultyStore) { f.failDelete = false })
	samePolicies(t, mustPersistent(t, fs.Store).List(), before)
}

func TestEnablePersistenceErrors(t *testing.T) {
	s := NewRetentionStore()
	if err := s.EnablePersistence(nil); err == nil {
		t.Fatal("nil store should fail")
	}
	if st, err := NewRetentionStoreWithPersistence(nil); st != nil || err == nil {
		t.Fatal("nil store should fail")
	}
	ps := persist.NewMemory()
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("second EnablePersistence should fail")
	}

	fs := &faultyStore{Store: persist.NewMemory(), failForEach: true}
	if st, err := NewRetentionStoreWithPersistence(fs); st != nil || !errors.Is(err, ErrPersistence) {
		t.Fatalf("want nil store and ErrPersistence, got %v %v", st, err)
	}
	s2 := NewRetentionStore()
	if _, err := s2.Upsert(RetentionPolicy{ID: "mem", Resource: "m", MaxItems: 1}); err != nil {
		t.Fatal(err)
	}
	fs.set(func(f *faultyStore) { f.failForEach = false; f.failPut = true })
	if err := s2.EnablePersistence(fs); !errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrPersistence when in-memory policies cannot be written, got %v", err)
	}
	fs.set(func(f *faultyStore) { f.failPut = false })
	if err := s2.EnablePersistence(fs); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	var p RetentionPolicy
	if ok, err := fs.Get(BucketPolicies, "mem", &p); !ok || err != nil || p.MaxItems != 1 {
		t.Fatalf("in-memory policy should be written on enable: %+v %v %v", p, ok, err)
	}
}

func TestEnablePersistenceSkipsInvalidDocuments(t *testing.T) {
	ps := persist.NewMemory()
	puts := []struct {
		key string
		v   any
	}{
		{"good", RetentionPolicy{ID: "good", Resource: "logs", TTL: time.Hour}},
		{"badres", RetentionPolicy{ID: "badres", Resource: "", TTL: time.Hour}},
		{"nobounds", RetentionPolicy{ID: "nobounds", Resource: "x"}},
		{"mismatch", RetentionPolicy{ID: "other", Resource: "x", MaxItems: 1}},
		{"garbage", map[string]string{"ttl_duration": "-5h"}},
	}
	for _, p := range puts {
		if err := ps.Put(BucketPolicies, p.key, p.v); err != nil {
			t.Fatal(err)
		}
	}
	s, err := NewRetentionStoreWithPersistence(ps)
	if s == nil {
		t.Fatalf("store should be usable despite invalid documents: %v", err)
	}
	if !errors.Is(err, ErrInvalid) || errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrInvalid (not ErrPersistence), got %v", err)
	}
	for _, k := range []string{"badres", "nobounds", "mismatch", "garbage"} {
		if !strings.Contains(err.Error(), BucketPolicies+"/"+k) {
			t.Errorf("error should mention %s: %v", k, err)
		}
	}
	if got := s.List(); len(got) != 1 || got[0].ID != "good" {
		t.Fatalf("unexpected loaded policies: %+v", got)
	}
	if _, err := s.Upsert(RetentionPolicy{ID: "later", Resource: "y", MaxItems: 2}); err != nil {
		t.Fatal(err)
	}
	var v json.RawMessage
	if ok, _ := ps.Get(BucketPolicies, "later", &v); !ok {
		t.Fatal("write-through not active after partial load")
	}
}
