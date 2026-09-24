package rag

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

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

func openBolt(t *testing.T, path string) *persist.Bolt {
	t.Helper()
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	return ps
}

// TestDocumentsSurviveRestart ingests through the documents handler, "restarts"
// (reopens the bbolt file into fresh stores) and checks retrieval still works.
func TestDocumentsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rag.db")
	ps := openBolt(t, path)
	vs, err := NewInMemoryVectorStoreWithPersistence(ps, nil)
	if err != nil {
		t.Fatal(err)
	}
	ki, err := NewInMemoryKeywordIndexWithPersistence(ps)
	if err != nil {
		t.Fatal(err)
	}
	h := NewDocumentsHandler(vs, ki)
	do := func(method, target, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	rec := do(http.MethodPost, "/", `{"documents":[
		{"id":"routing","content":"AeroLLM routes requests across providers","source":"docs/routing.md","metadata":{"team":"core"}},
		{"id":"billing","content":"Spend is tracked per virtual key"},
		{"content":"anonymous note about caching"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(http.MethodDelete, "/?id=billing", ""); rec.Code != http.StatusOK {
		t.Fatalf("delete: %d", rec.Code)
	}
	// Replacing a document by ID must persist the new content.
	if rec := do(http.MethodPost, "/", `[{"id":"routing","content":"AeroLLM routes requests with weighted fallbacks","source":"docs/routing.md"}]`); rec.Code != http.StatusOK {
		t.Fatalf("replace: %d", rec.Code)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	ps2 := openBolt(t, path)
	defer ps2.Close()
	vs2, err := NewInMemoryVectorStoreWithPersistence(ps2, HashingEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	ki2, err := NewInMemoryKeywordIndexWithPersistence(ps2)
	if err != nil {
		t.Fatal(err)
	}
	if vs2.Len() != 2 || ki2.Len() != 2 {
		t.Fatalf("expected 2 documents after restart (one deleted), got %d/%d", vs2.Len(), ki2.Len())
	}
	docs, err := NewHybridRetriever(vs2, ki2).Retrieve(context.Background(), "weighted fallbacks routing", 1)
	if err != nil || len(docs) != 1 || docs[0].ID != "routing" {
		t.Fatalf("persisted document not retrievable: %+v %v", docs, err)
	}
	if docs[0].Source != "docs/routing.md" || !strings.Contains(docs[0].Content, "weighted") {
		t.Fatalf("replacement not persisted: %+v", docs[0])
	}
	if vs2.Remove("billing") || ki2.Remove("billing") {
		t.Fatal("deleted document came back after restart")
	}
	kdocs, _ := ki2.Search(context.Background(), "anonymous caching", 1)
	if len(kdocs) != 1 || !strings.HasPrefix(kdocs[0].ID, "doc-") {
		t.Fatalf("document ingested without an id not restored: %+v", kdocs)
	}
}

func TestEnablePersistenceMergesExistingDocuments(t *testing.T) {
	ps := persist.NewMemory()
	vs := NewInMemoryVectorStore()
	vs.Add(Document{ID: "early", Content: "added before persistence was enabled"})
	if err := vs.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if err := vs.EnablePersistence(ps); err == nil {
		t.Fatal("enabling persistence twice must fail")
	}
	vs2, err := NewInMemoryVectorStoreWithPersistence(ps, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vs2.Len() != 1 {
		t.Fatalf("pre-existing document was not persisted, got %d", vs2.Len())
	}

	ki := NewInMemoryKeywordIndex()
	ki.Add(Document{Content: "keyword doc without an id"})
	if err := ki.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	ki2, err := NewInMemoryKeywordIndexWithPersistence(ps)
	if err != nil || ki2.Len() != 1 {
		t.Fatalf("keyword document not persisted: %v %d", err, ki2.Len())
	}
	if docs, _ := ki2.Search(context.Background(), "keyword", 1); len(docs) != 1 || docs[0].ID != "" {
		t.Fatalf("anonymous document not restored: %+v", docs)
	}
	if _, err := NewInMemoryKeywordIndexWithPersistence(nil); err == nil {
		t.Fatal("nil persist store must be rejected")
	}
}

type flakyEmbedder struct {
	fail atomic.Bool
}

func (f *flakyEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if f.fail.Load() {
		return nil, errors.New("embedding backend down")
	}
	return HashingEmbedder{}.Embed(ctx, text)
}

func TestPartialLoadIsReported(t *testing.T) {
	ps := persist.NewMemory()
	vs, err := NewInMemoryVectorStoreWithPersistence(ps, nil)
	if err != nil {
		t.Fatal(err)
	}
	vs.Add(Document{ID: "good", Content: "a good document"})
	_ = ps.Put(VectorStoreBucket, "id:corrupt", "not an object")

	emb := &flakyEmbedder{}
	vs2, err := NewInMemoryVectorStoreWithPersistence(ps, emb)
	if !errors.Is(err, ErrPartialLoad) || vs2 == nil {
		t.Fatalf("expected a usable store and ErrPartialLoad, got %v %v", vs2, err)
	}
	if vs2.Len() != 1 {
		t.Fatalf("valid document should still load, got %d", vs2.Len())
	}

	emb.fail.Store(true)
	vs3, err := NewInMemoryVectorStoreWithPersistence(ps, emb)
	if !errors.Is(err, ErrPartialLoad) || !strings.Contains(err.Error(), "embedding backend down") || vs3.Len() != 0 {
		t.Fatalf("embedding failures must be reported: %v", err)
	}
	// Skipped documents stay persisted and load once the embedder recovers.
	emb.fail.Store(false)
	vs4, _ := NewInMemoryVectorStoreWithPersistence(ps, emb)
	if vs4.Len() != 1 {
		t.Fatalf("document lost after a failed load, got %d", vs4.Len())
	}
}

type failingStore struct {
	persist.Store
	failPut, failDelete atomic.Bool
}

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.failPut.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(bucket, key, v)
}

func (f *failingStore) Delete(bucket, key string) error {
	if f.failDelete.Load() {
		return errors.New("disk failure")
	}
	return f.Store.Delete(bucket, key)
}

func TestPersistenceErrorsAreSurfaced(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	vs, err := NewInMemoryVectorStoreWithPersistence(fs, nil)
	if err != nil {
		t.Fatal(err)
	}
	ki, err := NewInMemoryKeywordIndexWithPersistence(fs)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := vs.AddContext(ctx, Document{ID: "a", Content: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := ki.AddContext(ctx, Document{ID: "a", Content: "alpha"}); err != nil {
		t.Fatal(err)
	}

	fs.failPut.Store(true)
	if err := vs.AddContext(ctx, Document{ID: "b", Content: "beta"}); err == nil {
		t.Fatal("vector store must surface persistence errors")
	}
	if err := ki.AddContext(ctx, Document{ID: "b", Content: "beta"}); err == nil {
		t.Fatal("keyword index must surface persistence errors")
	}
	if vs.Len() != 1 || ki.Len() != 1 {
		t.Fatalf("failed writes must not reach the index: %d/%d", vs.Len(), ki.Len())
	}

	h := NewDocumentsHandler(vs, ki)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`[{"id":"c","content":"gamma"}]`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"indexed":0`) {
		t.Fatalf("handler must report persistence failures: %d %s", rec.Code, rec.Body.String())
	}
	fs.failPut.Store(false)

	fs.failDelete.Store(true)
	if ok, err := vs.RemoveContext(ctx, "a"); ok || err == nil {
		t.Fatalf("delete failure must be surfaced: %v %v", ok, err)
	}
	if ok, err := ki.RemoveContext(ctx, "a"); ok || err == nil {
		t.Fatalf("delete failure must be surfaced: %v %v", ok, err)
	}
	req = httptest.NewRequest(http.MethodDelete, "/?id=a", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("handler must report delete failures, got %d", rec.Code)
	}
	if vs.Len() != 1 || ki.Len() != 1 {
		t.Fatal("a document whose persisted copy could not be deleted must stay indexed")
	}
}

func TestPersistentStoresConcurrentWrites(t *testing.T) {
	ps := persist.NewMemory()
	vs, _ := NewInMemoryVectorStoreWithPersistence(ps, nil)
	ki, _ := NewInMemoryKeywordIndexWithPersistence(ps)
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				doc := Document{ID: "d" + string(rune('a'+j)), Content: strings.Repeat("word ", w+1)}
				_ = vs.AddContext(ctx, doc)
				_ = ki.AddContext(ctx, doc)
				_, _ = vs.Search(ctx, "word", 3)
				_, _ = ki.Search(ctx, "word", 3)
				if j%5 == 0 {
					_, _ = vs.RemoveContext(ctx, doc.ID)
					_, _ = ki.RemoveContext(ctx, doc.ID)
				}
			}
		}(w)
	}
	wg.Wait()
	// Memory and disk agree after concurrent writers.
	vs2, err := NewInMemoryVectorStoreWithPersistence(ps, nil)
	if err != nil || vs2.Len() != vs.Len() {
		t.Fatalf("persisted vector docs (%d) differ from memory (%d): %v", vs2.Len(), vs.Len(), err)
	}
	ki2, err := NewInMemoryKeywordIndexWithPersistence(ps)
	if err != nil || ki2.Len() != ki.Len() {
		t.Fatalf("persisted keyword docs (%d) differ from memory (%d): %v", ki2.Len(), ki.Len(), err)
	}
}
