package graphrag

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

func TestGraphSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewBboltGraphStoreWithPersistence(ps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	alice, _ := store.UpsertNode(ctx, Node{Label: "Alice", Type: "person", Props: map[string]interface{}{"team": "core"}})
	acme, _ := store.UpsertNode(ctx, Node{ID: "acme", Label: "Acme Corp", Type: "company"})
	until := time.Now().Add(-time.Hour)
	if _, err := store.UpsertEdge(ctx, Edge{ID: "old-job", Source: alice, Target: "initech", Label: "worked_at", ValidFrom: until.Add(-24 * time.Hour), ValidTo: &until}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertEdge(ctx, Edge{ID: "job", Source: alice, Target: "wrong", Label: "works_at"}); err != nil {
		t.Fatal(err)
	}
	// Moving an edge must persist the new endpoints.
	if _, err := store.UpsertEdge(ctx, Edge{ID: "job", Source: alice, Target: acme, Label: "works_at"}); err != nil {
		t.Fatal(err)
	}
	created := store.nodes[alice].CreatedAt
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	ps2, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps2.Close()
	store2, err := NewBboltGraphStoreWithPersistence(ps2)
	if err != nil {
		t.Fatal(err)
	}
	if n, e := store2.Counts(); n != 2 || e != 2 {
		t.Fatalf("expected 2 nodes / 2 edges after restart, got %d/%d", n, e)
	}
	n, ok := store2.GetNode(ctx, alice)
	if !ok || n.Label != "Alice" || n.Props["team"] != "core" || !n.CreatedAt.Equal(created) {
		t.Fatalf("node not restored faithfully: %+v", n)
	}
	edges, err := store2.Neighbors(ctx, alice, 1)
	if err != nil || len(edges) != 1 || edges[0].Target != acme {
		t.Fatalf("expected only the currently valid, moved edge: %+v %v", edges, err)
	}
	past, _ := store2.NeighborsAt(ctx, alice, 1, until.Add(-time.Hour))
	if len(past) != 1 || past[0].ID != "old-job" || past[0].ValidTo == nil {
		t.Fatalf("temporal validity not restored: %+v", past)
	}
	if _, ok := store2.targetIdx["wrong"]; ok {
		t.Fatal("stale index entry for the moved edge after reload")
	}
	text, err := NewGraphRAGMiddleware(store2).BuildContext(ctx, "Where does Alice work?")
	if err != nil || !strings.Contains(text, "Alice --works_at--> Acme Corp") {
		t.Fatalf("graph context not rebuilt from persisted graph: %q %v", text, err)
	}

	// CreatedAt is preserved by upserts after a reload.
	if _, err := store2.UpsertNode(ctx, Node{ID: alice, Label: "Alice", Type: "person"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := store2.GetNode(ctx, alice); !got.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt changed on upsert after reload: %v vs %v", got.CreatedAt, created)
	}
}

func TestGraphEnablePersistenceMergesAndReportsCorruption(t *testing.T) {
	ps := persist.NewMemory()
	s := NewBboltGraphStore()
	ctx := context.Background()
	_, _ = s.UpsertNode(ctx, Node{ID: "pre", Label: "Before"})
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling persistence twice must fail")
	}
	_ = ps.Put(NodesBucket, "broken", "not a node")
	_ = ps.Put(EdgesBucket, "dangling", Edge{ID: "dangling"}) // no endpoints
	s2, err := NewBboltGraphStoreWithPersistence(ps)
	if !errors.Is(err, ErrPartialLoad) || s2 == nil {
		t.Fatalf("expected a usable store with ErrPartialLoad, got %v %v", s2, err)
	}
	if _, ok := s2.GetNode(ctx, "pre"); !ok {
		t.Fatal("node created before persistence was enabled was not persisted")
	}
	if _, err := NewBboltGraphStoreWithPersistence(nil); err == nil {
		t.Fatal("nil persist store must be rejected")
	}
}

type failingPersist struct {
	persist.Store
	fail atomic.Bool
}

func (f *failingPersist) Put(bucket, key string, v any) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(bucket, key, v)
}

func TestGraphPersistErrorsSurfaced(t *testing.T) {
	fp := &failingPersist{Store: persist.NewMemory()}
	s, err := NewBboltGraphStoreWithPersistence(fp)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	fp.fail.Store(true)
	if _, err := s.UpsertNode(ctx, Node{ID: "a", Label: "A"}); err == nil {
		t.Fatal("node persistence failure must be returned")
	}
	if _, err := s.UpsertEdge(ctx, Edge{ID: "e", Source: "a", Target: "b"}); err == nil {
		t.Fatal("edge persistence failure must be returned")
	}
	if n, e := s.Counts(); n != 0 || e != 0 {
		t.Fatalf("failed writes must not reach the in-memory graph: %d/%d", n, e)
	}
	h := NewGraphHandler(s, nil)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"nodes":[{"label":"X"}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("handler must report persistence failures, got %d", rec.Code)
	}
}

func TestGraphPersistConcurrentWriters(t *testing.T) {
	ps := persist.NewMemory()
	s, _ := NewBboltGraphStoreWithPersistence(ps)
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				target := []string{"x", "y", "z"}[(w+j)%3]
				_, _ = s.UpsertEdge(ctx, Edge{ID: "shared", Source: "a", Target: target})
				_, _ = s.UpsertNode(ctx, Node{ID: "n", Label: target})
				_, _ = s.Neighbors(ctx, "a", 2)
			}
		}(w)
	}
	wg.Wait()
	s2, err := NewBboltGraphStoreWithPersistence(ps)
	if err != nil {
		t.Fatal(err)
	}
	if s.edges["shared"].Target != s2.edges["shared"].Target || s.nodes["n"].Label != s2.nodes["n"].Label {
		t.Fatal("persisted graph diverged from memory under concurrent writers")
	}
}

func TestGraphHandler(t *testing.T) {
	s := NewBboltGraphStore()
	h := NewGraphHandler(s, nil)
	do := func(method, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	rec := do(http.MethodPost, `{
		"nodes":[{"id":"alice","label":"Alice","type":"person"},{"id":"acme","label":"Acme"}],
		"edges":[{"source":"alice","target":"acme","label":"works_at","valid_from":"2020-01-01T00:00:00Z"}],
		"texts":["Bob met Carol in Paris."]}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"nodes":["alice","acme"]`) {
		t.Fatalf("ingest failed: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"entities":3`) {
		t.Fatalf("text entities not extracted: %s", rec.Body.String())
	}
	if rec := do(http.MethodGet, ""); !strings.Contains(rec.Body.String(), `"nodes":5`) || !strings.Contains(rec.Body.String(), `"edges":4`) {
		t.Fatalf("unexpected counts: %s", rec.Body.String())
	}
	for name, tc := range map[string]struct {
		method, body string
		status       int
	}{
		"empty":        {http.MethodPost, `{}`, http.StatusBadRequest},
		"bad json":     {http.MethodPost, `{"nodes":`, http.StatusBadRequest},
		"no endpoints": {http.MethodPost, `{"edges":[{"source":"a"}]}`, http.StatusBadRequest},
		"no label":     {http.MethodPost, `{"nodes":[{"type":"x"}]}`, http.StatusBadRequest},
		"inverted":     {http.MethodPost, `{"edges":[{"source":"a","target":"b","valid_from":"2021-01-01T00:00:00Z","valid_to":"2020-01-01T00:00:00Z"}]}`, http.StatusBadRequest},
		"long id":      {http.MethodPost, `{"nodes":[{"id":"` + strings.Repeat("x", maxGraphIDLen+1) + `"}]}`, http.StatusBadRequest},
		"wrong verb":   {http.MethodDelete, ``, http.StatusMethodNotAllowed},
	} {
		if rec := do(tc.method, tc.body); rec.Code != tc.status {
			t.Errorf("%s: got %d (%s), want %d", name, rec.Code, rec.Body.String(), tc.status)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"nodes":[{"label":"x"}]}`))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", rec.Code)
	}
}
