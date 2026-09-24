package region

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// faultyStore wraps a persist.Store and fails selected operations.
type faultyStore struct {
	persist.Store
	mu          sync.Mutex
	failPut     bool
	failPutKey  string
	failDelete  bool
	failForEach bool
}

var errInjected = errors.New("injected failure")

func (f *faultyStore) Put(bucket, key string, v any) error {
	f.mu.Lock()
	fail := f.failPut || (f.failPutKey != "" && f.failPutKey == key)
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

func mustPersistent(t *testing.T, ps persist.Store) *Store {
	t.Helper()
	s, err := NewStoreWithPersistence(ps)
	if err != nil {
		t.Fatalf("NewStoreWithPersistence: %v", err)
	}
	return s
}

func seed(t *testing.T, s *Store) {
	t.Helper()
	h := WebhookHandler(s)
	for _, c := range []struct{ path, body string }{
		{"/v1/region/regions", `{"id":"eu","name":"Europe","endpoint":"https://eu.example.com","primary":true}`},
		{"/v1/region/regions", `{"id":"us","name":"US"}`},
		{"/v1/region/residency", `{"id":"gdpr","region":"eu","data_type":"PII","required":true}`},
		{"/v1/region/routes", `{"id":"eu-main","region":"eu","providers":["openai"," anthropic","openai"],"priority":1,"enabled":true}`},
	} {
		if rec := req(t, h, http.MethodPost, c.path, c.body); rec.Code != http.StatusOK {
			t.Fatalf("POST %s: %d %s", c.path, rec.Code, rec.Body)
		}
	}
	if _, err := s.UpsertRule(RouteRule{ID: "us-main", Region: "us", Providers: []string{"mistral"}, Enabled: true}); err != nil {
		t.Fatal(err)
	}
}

func snapshot(s *Store) [3]interface{} {
	return [3]interface{}{s.ListRegions(), s.ListPolicies(), s.ListRules()}
}

func TestPersistenceReloadMemory(t *testing.T) {
	ps := persist.NewMemory()
	s1 := mustPersistent(t, ps)
	seed(t, s1)

	s2 := mustPersistent(t, ps)
	if got, want := snapshot(s2), snapshot(s1); !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded state differs:\n got %+v\nwant %+v", got, want)
	}
	d, err := s2.Resolve("pii", "us")
	if err != nil || d.Region.ID != "eu" || !d.ResidencyEnforced {
		t.Fatalf("residency not enforced after reload: %+v %v", d, err)
	}
}

func TestPersistenceReloadBolt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "region.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s1 := mustPersistent(t, ps)
	seed(t, s1)
	want := snapshot(s1)
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	s2 := mustPersistent(t, ps2)
	if got := snapshot(s2); !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded state differs:\n got %+v\nwant %+v", got, want)
	}
}

func TestPersistencePrimaryDemotionIsDurable(t *testing.T) {
	ps := persist.NewMemory()
	s := mustPersistent(t, ps)
	seed(t, s)
	if _, err := s.UpsertRegion(Region{ID: "us", Primary: true}); err != nil {
		t.Fatal(err)
	}
	var eu Region
	if ok, err := ps.Get(BucketRegions, "eu", &eu); !ok || err != nil || eu.Primary {
		t.Fatalf("demotion of eu not persisted: %+v ok=%v err=%v", eu, ok, err)
	}
	s2 := mustPersistent(t, ps)
	primaries := 0
	for _, r := range s2.ListRegions() {
		if r.Primary {
			primaries++
			if r.ID != "us" {
				t.Fatalf("wrong primary after reload: %s", r.ID)
			}
		}
	}
	if primaries != 1 {
		t.Fatalf("expected exactly one primary, got %d", primaries)
	}
}

func TestPersistenceDeleteRemovesDocuments(t *testing.T) {
	ps := persist.NewMemory()
	s := mustPersistent(t, ps)
	seed(t, s)
	h := WebhookHandler(s)
	for _, p := range []string{"/v1/region/routes/eu-main", "/v1/region/routes/us-main", "/v1/region/residency/gdpr", "/v1/region/regions/eu"} {
		if rec := req(t, h, http.MethodDelete, p, ""); rec.Code != http.StatusNoContent {
			t.Fatalf("DELETE %s: %d %s", p, rec.Code, rec.Body)
		}
	}
	var v json.RawMessage
	for _, c := range []struct{ bucket, key string }{
		{BucketRules, "eu-main"}, {BucketRules, "us-main"}, {BucketPolicies, "gdpr"}, {BucketRegions, "eu"},
	} {
		if ok, _ := ps.Get(c.bucket, c.key, &v); ok {
			t.Fatalf("%s/%s still persisted after delete", c.bucket, c.key)
		}
	}
	if ok, _ := ps.Get(BucketRegions, "us", &v); !ok {
		t.Fatal("us region should still be persisted")
	}
	s2 := mustPersistent(t, ps)
	if n := len(s2.ListRegions()); n != 1 {
		t.Fatalf("expected 1 region after reload, got %d", n)
	}
}

func TestPersistenceWriteFailuresLeaveMemoryUnchanged(t *testing.T) {
	fs := &faultyStore{Store: persist.NewMemory()}
	s := mustPersistent(t, fs)
	seed(t, s)
	before := snapshot(s)
	h := WebhookHandler(s)

	fs.set(func(f *faultyStore) { f.failPut = true })
	if _, err := s.UpsertRegion(Region{ID: "eu", Name: "renamed"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("UpsertRegion: want ErrPersistence, got %v", err)
	}
	if _, err := s.UpsertPolicy(ResidencyPolicy{ID: "p2", Region: "us", DataType: "phi"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("UpsertPolicy: want ErrPersistence, got %v", err)
	}
	if _, err := s.UpsertRule(RouteRule{ID: "r2", Region: "us", Providers: []string{"x"}}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("UpsertRule: want ErrPersistence, got %v", err)
	}
	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/region/regions", `{"id":"ap","name":"APAC"}`},
		{http.MethodPatch, "/v1/region/routes/eu-main", `{"enabled":false}`},
		{http.MethodPut, "/v1/region/residency/gdpr", `{"region":"us","data_type":"pii"}`},
	} {
		rec := req(t, h, c.method, c.path, c.body)
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"persistence failure"`) {
			t.Fatalf("%s %s: want 500 persistence failure, got %d %s", c.method, c.path, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), errInjected.Error()) {
			t.Fatalf("internal error leaked: %s", rec.Body)
		}
	}
	fs.set(func(f *faultyStore) { f.failPut = false; f.failDelete = true })
	if err := s.DeleteRule("us-main"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("DeleteRule: want ErrPersistence, got %v", err)
	}
	if err := s.DeletePolicy("gdpr"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("DeletePolicy: want ErrPersistence, got %v", err)
	}
	if rec := req(t, h, http.MethodDelete, "/v1/region/routes/eu-main", ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE: want 500, got %d %s", rec.Code, rec.Body)
	}
	if got := snapshot(s); !reflect.DeepEqual(got, before) {
		t.Fatalf("memory changed after failed writes:\n got %+v\nwant %+v", got, before)
	}

	// The durable copy matches memory too.
	fs.set(func(f *faultyStore) { f.failDelete = false })
	if got := snapshot(mustPersistent(t, fs.Store)); !reflect.DeepEqual(got, before) {
		t.Fatalf("persisted state diverged:\n got %+v\nwant %+v", got, before)
	}
}

func TestPersistencePrimaryRollback(t *testing.T) {
	fs := &faultyStore{Store: persist.NewMemory()}
	s := mustPersistent(t, fs)
	seed(t, s)
	before := snapshot(s)

	// Promoting a new region writes it first, then demotes eu; failing the
	// demotion must roll the new document back.
	fs.set(func(f *faultyStore) { f.failPutKey = "eu" })
	if _, err := s.UpsertRegion(Region{ID: "ap", Primary: true}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrPersistence, got %v", err)
	}
	var v json.RawMessage
	if ok, _ := fs.Store.Get(BucketRegions, "ap", &v); ok {
		t.Fatal("new region should have been rolled back from the store")
	}
	// Existing region promotion rollback restores its previous document.
	if _, err := s.UpsertRegion(Region{ID: "us", Name: "US", Primary: true}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrPersistence, got %v", err)
	}
	var us Region
	if ok, _ := fs.Store.Get(BucketRegions, "us", &us); !ok || us.Primary {
		t.Fatalf("us should be restored as non-primary, got %+v ok=%v", us, ok)
	}
	if got := snapshot(s); !reflect.DeepEqual(got, before) {
		t.Fatalf("memory changed:\n got %+v\nwant %+v", got, before)
	}
}

func TestEnablePersistenceErrors(t *testing.T) {
	s := NewStore()
	if err := s.EnablePersistence(nil); err == nil {
		t.Fatal("nil store should fail")
	}
	if _, err := NewStoreWithPersistence(nil); err == nil {
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
	s2 := NewStore()
	if _, err := s2.UpsertRegion(Region{ID: "eu"}); err != nil {
		t.Fatal(err)
	}
	if err := s2.EnablePersistence(fs); !errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrPersistence on load failure, got %v", err)
	}
	if st, err := NewStoreWithPersistence(fs); st != nil || !errors.Is(err, ErrPersistence) {
		t.Fatalf("constructor should fail on load failure: %v %v", st, err)
	}
	// Not enabled after a failure: a retry with a healthy store works.
	fs.set(func(f *faultyStore) { f.failForEach = false; f.failPut = true })
	if err := s2.EnablePersistence(fs); !errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrPersistence when in-memory objects cannot be written, got %v", err)
	}
	fs.set(func(f *faultyStore) { f.failPut = false })
	if err := s2.EnablePersistence(fs); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
}

func TestEnablePersistenceMergesInMemoryObjects(t *testing.T) {
	ps := persist.NewMemory()
	if err := ps.Put(BucketRegions, "eu", Region{ID: "eu", Name: "Persisted EU", Primary: true}); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	if _, err := s.UpsertRegion(Region{ID: "eu", Name: "Memory EU"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertRegion(Region{ID: "us", Primary: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	eu, _ := s.GetRegion("eu")
	us, _ := s.GetRegion("us")
	if eu.Name != "Persisted EU" || !eu.Primary {
		t.Fatalf("persisted document should win: %+v", eu)
	}
	if us.Primary {
		t.Fatalf("only one primary may survive the merge: %+v", us)
	}
	var stored Region
	if ok, _ := ps.Get(BucketRegions, "us", &stored); !ok || stored.Primary {
		t.Fatalf("in-memory region should be written (demoted): %+v ok=%v", stored, ok)
	}
}

func TestEnablePersistenceSkipsInvalidDocuments(t *testing.T) {
	ps := persist.NewMemory()
	puts := []struct {
		bucket, key string
		v           any
	}{
		{BucketRegions, "eu", Region{ID: "eu", Endpoint: "https://eu.example.com"}},
		{BucketRegions, "bad", Region{ID: "bad", Endpoint: "ftp://nope"}},
		{BucketRegions, "mismatch", Region{ID: "other"}},
		{BucketRegions, "garbage", "not an object"},
		{BucketPolicies, "ok", ResidencyPolicy{ID: "ok", Region: "eu", DataType: "pii"}},
		{BucketPolicies, "orphan", ResidencyPolicy{ID: "orphan", Region: "bad", DataType: "pii"}},
		{BucketRules, "ok", RouteRule{ID: "ok", Region: "eu", Providers: []string{"a"}}},
		{BucketRules, "noprov", RouteRule{ID: "noprov", Region: "eu"}},
	}
	for _, p := range puts {
		if err := ps.Put(p.bucket, p.key, p.v); err != nil {
			t.Fatal(err)
		}
	}
	s, err := NewStoreWithPersistence(ps)
	if s == nil {
		t.Fatalf("store should be usable despite invalid documents: %v", err)
	}
	if !errors.Is(err, ErrInvalid) || errors.Is(err, ErrPersistence) {
		t.Fatalf("want ErrInvalid (not ErrPersistence), got %v", err)
	}
	for _, k := range []string{"region.regions/bad", "region.regions/mismatch", "region.regions/garbage", "region.residency/orphan", "region.routes/noprov"} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("error should mention %s: %v", k, err)
		}
	}
	if len(s.ListRegions()) != 1 || len(s.ListPolicies()) != 1 || len(s.ListRules()) != 1 {
		t.Fatalf("unexpected loaded state: %+v", snapshot(s))
	}
	// Persistence is enabled: new writes go through.
	if _, err := s.UpsertRegion(Region{ID: "us"}); err != nil {
		t.Fatal(err)
	}
	var v json.RawMessage
	if ok, _ := ps.Get(BucketRegions, "us", &v); !ok {
		t.Fatal("write-through not active after partial load")
	}
}

func TestPersistenceDisabledByDefault(t *testing.T) {
	s := NewStore()
	seed(t, s)
	if s.ps != nil {
		t.Fatal("persistence must be opt-in")
	}
}
