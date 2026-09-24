package cache

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// mockEmbedder provides deterministic embeddings for testing.
type mockEmbedder struct {
	vec []float64
}

func (m *mockEmbedder) Embedding(_ context.Context, _ *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	return &models.EmbeddingResponse{
		Data: []models.Embedding{
			{Embedding: m.vec, Index: 0},
		},
	}, nil
}

// TestSemanticCacheGetAndUpsert tests the round-trip: store, then retrieve.
func TestSemanticCacheGetAndUpsert(t *testing.T) {
	embedder := &mockEmbedder{vec: []float64{0.8, 0.2, 0.5}}
	semCache := NewVectorSemanticCache("test", 60*time.Second, 0.95, embedder)
	ctx := context.Background()

	// Upsert a cached response.
	resp := []byte(`{"reply":"hello world"}`)
	err := semCache.Upsert(ctx, "key1", "hello world", resp, nil)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Search for the same prompt — should get a hit.
	hit, err := semCache.Search(ctx, "hello world")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if hit == nil {
		t.Fatal("expected cache hit, got nil")
	}
	if string(hit.Response) != string(resp) {
		t.Fatal("expected same response")
	}

	// Search with a different prompt — should get a miss (different vector).
	embedder2 := &mockEmbedder{vec: []float64{0.1, 0.9, 0.3}}
	semCache.SetEmbedder(embedder2)
	hit2, err := semCache.Search(ctx, "totally different prompt")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if hit2 != nil {
		t.Fatal("expected no hit for different prompt")
	}
}

// TestSemanticCacheExpiry tests that entries expire after TTL.
func TestSemanticCacheExpiry(t *testing.T) {
	embedder := &mockEmbedder{vec: []float64{1.0, 0.0}}
	semCache := NewVectorSemanticCache("test", 1*time.Millisecond, 0.95, embedder)
	ctx := context.Background()

	resp := []byte(`{"reply":"cached"}`)
	err := semCache.Upsert(ctx, "key1", "cached prompt", resp, nil)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	hit, err := semCache.Search(ctx, "cached prompt")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if hit != nil {
		t.Fatal("expected hit to be nil after expiry")
	}
}

// TestSemanticCacheNoEmbedder tests graceful fallback when no embedder is set.
func TestSemanticCacheNoEmbedder(t *testing.T) {
	semCache := NewVectorSemanticCache("test", 60*time.Second, 0.95, nil)
	ctx := context.Background()

	err := semCache.Upsert(ctx, "key1", "prompt", []byte("resp"), nil)
	if err != nil {
		t.Fatalf("Upsert with nil embedder failed: %v", err)
	}

	hit, err := semCache.Search(ctx, "prompt")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if hit != nil {
		t.Fatal("expected nil hit with nil embedder")
	}
}

// TestSemanticCacheStats tests stats reporting.
func TestSemanticCacheStats(t *testing.T) {
	embedder := &mockEmbedder{vec: []float64{0.5, 0.5}}
	semCache := NewVectorSemanticCache("test", 60*time.Second, 0.95, embedder)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = semCache.Upsert(ctx, "key", "prompt", []byte("resp"), nil)
	}

	stats := semCache.Stats()
	if stats["total_entries"] != 1 {
		t.Errorf("expected 1 entry (upsert replaces), got %d", stats["total_entries"])
	}
	if stats["active_entries"] != 1 {
		t.Errorf("expected 1 active entry, got %d", stats["active_entries"])
	}
}

// TestCosineSimilarity tests the cosine similarity function.
func TestCosineSimilarity(t *testing.T) {
	// Identical vectors → 1.0
	v := []float64{1.0, 2.0, 3.0}
	score := CosineSimilarity(v, v)
	if score < 0.999 || score > 1.001 {
		t.Fatalf("expected ~1.0 for identical vectors, got %f", score)
	}

	// Orthogonal vectors → 0.0
	a := []float64{1.0, 0.0}
	b := []float64{0.0, 1.0}
	score = CosineSimilarity(a, b)
	if score > 0.001 {
		t.Fatalf("expected ~0.0 for orthogonal vectors, got %f", score)
	}

	// Empty vectors → 0.0
	score = CosineSimilarity([]float64{}, []float64{})
	if score != 0 {
		t.Fatalf("expected 0 for empty vectors, got %f", score)
	}
}

func newTestVectorCache(opts VectorCacheOptions) (*VectorSemanticCache, *time.Time) {
	if opts.Embedder == nil {
		opts.Embedder = NewHashingEmbedder(256)
	}
	c := NewVectorSemanticCacheWithOptions(opts)
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }
	return c, &now
}

func TestVectorCacheNamespaceIsolation(t *testing.T) {
	c, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Minute})
	ctx := context.Background()
	if err := c.UpsertNS(ctx, "tenant-a", "k1", "what is the capital of france", []byte("paris"), nil); err != nil {
		t.Fatal(err)
	}
	if hit, _ := c.SearchNS(ctx, "tenant-b", "what is the capital of france"); hit != nil {
		t.Fatal("cross-tenant semantic hit")
	}
	hit, err := c.SearchNS(ctx, "tenant-a", "What is the capital of France?")
	if err != nil || hit == nil || string(hit.Response) != "paris" || hit.Namespace != "tenant-a" {
		t.Fatalf("expected same-tenant hit, got %+v %v", hit, err)
	}
	// Returned entries are copies.
	hit.Response[0] = 'X'
	again, _ := c.SearchNS(ctx, "tenant-a", "what is the capital of france")
	if string(again.Response) != "paris" {
		t.Fatal("caller mutation leaked into the cache")
	}
	if n := c.ClearNamespace("tenant-a"); n != 1 || c.Len() != 0 {
		t.Fatalf("ClearNamespace removed %d, len %d", n, c.Len())
	}
}

func TestVectorCacheTTLAndPurge(t *testing.T) {
	c, now := newTestVectorCache(VectorCacheOptions{TTL: time.Minute})
	ctx := context.Background()
	_ = c.Upsert(ctx, "k", "hello there", []byte("r"), nil)
	*now = now.Add(59 * time.Second)
	if hit, _ := c.Search(ctx, "hello there"); hit == nil {
		t.Fatal("expected hit before expiry")
	}
	*now = now.Add(2 * time.Second)
	if hit, _ := c.Search(ctx, "hello there"); hit != nil {
		t.Fatal("expected miss after expiry")
	}
	if st := c.Snapshot(); st.ActiveEntries != 0 || st.Entries != 1 {
		t.Fatalf("unexpected stats %+v", st)
	}
	if n := c.PurgeExpired(); n != 1 || c.Len() != 0 {
		t.Fatalf("purge removed %d", n)
	}
}

func TestVectorCacheLRUEviction(t *testing.T) {
	c, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour, MaxEntries: 3})
	ctx := context.Background()
	prompts := []string{"alpha one", "bravo two", "charlie three"}
	for i, p := range prompts {
		_ = c.Upsert(ctx, p, p, []byte{byte('a' + i)}, nil)
	}
	// Touch "alpha one" so "bravo two" becomes least recently used.
	if hit, _ := c.Search(ctx, "alpha one"); hit == nil {
		t.Fatal("expected hit")
	}
	_ = c.Upsert(ctx, "delta four", "delta four", []byte("d"), nil)
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}
	if hit, _ := c.Search(ctx, "bravo two"); hit != nil {
		t.Fatal("LRU entry should have been evicted")
	}
	if hit, _ := c.Search(ctx, "alpha one"); hit == nil {
		t.Fatal("recently used entry evicted")
	}
	if c.Snapshot().Evictions != 1 {
		t.Fatalf("expected 1 eviction, got %+v", c.Snapshot())
	}
}

func TestVectorCacheByteLimits(t *testing.T) {
	c, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour, MaxEntryBytes: 10, MaxPromptBytes: 20})
	ctx := context.Background()
	_ = c.Upsert(ctx, "big", "prompt", make([]byte, 11), nil)
	_ = c.Upsert(ctx, "longprompt", strings.Repeat("x ", 20), []byte("ok"), nil)
	if c.Len() != 0 {
		t.Fatalf("oversized entries must not be cached, len=%d", c.Len())
	}
	c2, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour, MaxBytes: 1})
	_ = c2.Upsert(ctx, "a", "prompt a", []byte("x"), nil)
	if c2.Len() != 0 {
		t.Fatalf("MaxBytes not enforced, len=%d", c2.Len())
	}
}

func TestVectorCacheNilEmbedderDisabled(t *testing.T) {
	c := NewVectorSemanticCache("p", time.Minute, 0.95, nil)
	ctx := context.Background()
	_ = c.Upsert(ctx, "k", "prompt", []byte("r"), nil)
	if c.Len() != 0 || c.Enabled() {
		t.Fatal("nil embedder must not store anything")
	}
	if hit, err := c.SearchNS(ctx, "ns", "prompt"); hit != nil || err != nil {
		t.Fatal("nil embedder must miss")
	}
	var nilCache *VectorSemanticCache
	if hit, _ := nilCache.Search(ctx, "x"); hit != nil {
		t.Fatal("nil cache must miss")
	}
}

func TestVectorCacheInspectPagination(t *testing.T) {
	c, now := newTestVectorCache(VectorCacheOptions{TTL: time.Hour})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		*now = now.Add(time.Second)
		_ = c.UpsertNS(ctx, "ns", string(rune('a'+i)), "prompt number "+string(rune('a'+i)), []byte("r"), map[string]interface{}{"model": "gpt-4o", "total_tokens": 7})
	}
	page, next := c.Inspect(0, 2)
	if len(page) != 2 || next != 2 || page[0].Key != "a" || page[0].Model != "gpt-4o" || page[0].TokenCount != 7 {
		t.Fatalf("unexpected first page %+v next=%d", page, next)
	}
	page, next = c.Inspect(next, 10)
	if len(page) != 3 || next != 0 {
		t.Fatalf("unexpected last page %+v next=%d", page, next)
	}
	// Huge cursors must not panic.
	if page, next := c.Inspect(math.MaxUint64, 10); page != nil || next != 0 {
		t.Fatal("expected empty page for out-of-range cursor")
	}
	if page, _ := c.Inspect(0, -1); len(page) != 5 {
		t.Fatalf("non-positive page size should default, got %d", len(page))
	}
}

func TestVectorCacheExportImport(t *testing.T) {
	c, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour})
	ctx := context.Background()
	_ = c.UpsertNS(ctx, "ns", "k", "tell me a joke", []byte("joke"), nil)
	data, err := c.Export()
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour})
	if err := c2.Import(data); err != nil {
		t.Fatal(err)
	}
	if hit, _ := c2.SearchNS(ctx, "ns", "tell me a joke"); hit == nil || string(hit.Response) != "joke" {
		t.Fatal("import lost entry")
	}
	if err := c2.Import([]byte(`[{"key":"","vector":[1]}]`)); err != nil || c2.Len() != 0 {
		t.Fatal("malformed entries must be skipped")
	}
}

func TestHashingEmbedderDeterministicAndDiscriminative(t *testing.T) {
	e := NewHashingEmbedder(0)
	a := e.Vector("The quick brown fox jumps over the lazy dog")
	b := e.Vector("the QUICK brown fox jumps over the lazy dog!")
	if CosineSimilarity(a, b) < 0.999 {
		t.Fatal("case/punctuation-only changes should be near-identical")
	}
	if CosineSimilarity(a, e.Vector("How do I bake sourdough bread at home")) > 0.3 {
		t.Fatal("unrelated prompts should be dissimilar")
	}
	if CosineSimilarity(e.Vector("dog bites man"), e.Vector("man bites dog")) > 0.9 {
		t.Fatal("word order should matter via bigrams")
	}
	if CosineSimilarity(e.Vector(""), e.Vector("")) != 0 {
		t.Fatal("empty text must not match")
	}
	for i := 0; i < 5; i++ {
		v := e.Vector("The quick brown fox jumps over the lazy dog")
		for j := range v {
			if v[j] != a[j] {
				t.Fatal("embedding is not deterministic")
			}
		}
	}
}

func TestVectorCacheConcurrentAccess(t *testing.T) {
	c, _ := newTestVectorCache(VectorCacheOptions{TTL: time.Hour, MaxEntries: 50})
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				ns := "ns" + string(rune('a'+g%3))
				p := "prompt " + string(rune('a'+i%26)) + " " + string(rune('a'+g))
				_ = c.UpsertNS(ctx, ns, p, p, []byte("r"), nil)
				_, _ = c.SearchNS(ctx, ns, p)
				if i%25 == 0 {
					c.Inspect(0, 10)
					c.Snapshot()
					c.PurgeExpired()
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Len() > 50 {
		t.Fatalf("MaxEntries exceeded: %d", c.Len())
	}
}

func TestSemanticThresholdValidation(t *testing.T) {
	c := NewVectorSemanticCache("p", time.Minute, math.NaN(), NewHashingEmbedder(0))
	if c.Threshold() != 0.95 {
		t.Fatalf("NaN threshold should default, got %v", c.Threshold())
	}
	if NewVectorSemanticCache("p", time.Minute, 7, nil).Threshold() != 1 {
		t.Fatal("threshold > 1 should clamp to 1")
	}
}
