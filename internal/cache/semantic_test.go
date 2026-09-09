package cache

import (
	"context"
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
