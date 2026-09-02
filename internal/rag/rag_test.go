package rag

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestHybridRetrieverRRF(t *testing.T) {
	vector := NewInMemoryVectorStore()
	vector.Add(Document{ID: "v1", Content: "hello world", Source: "vector"})
	keyword := NewInMemoryKeywordIndex()
	keyword.Add(Document{ID: "k1", Content: "hello world", Source: "keyword"})

	retriever := NewHybridRetriever(vector, keyword)
	docs, err := retriever.Retrieve(context.Background(), "hello", 5)
	if err != nil {
		t.Fatalf("retrieve error: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected non-empty results")
	}
}

func TestRAGMiddlewareMaybeInjectPrependsSystem(t *testing.T) {
	vector := NewInMemoryVectorStore()
	vector.Add(Document{ID: "v1", Content: "hello world context", Source: "vector"})
	keyword := NewInMemoryKeywordIndex()
	keyword.Add(Document{ID: "k1", Content: "hello world context", Source: "keyword"})
	mw := NewRAGMiddleware(NewHybridRetriever(vector, keyword))

	req := &models.LLMRequest{
		Model: "test-model",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: strPtrRag("hello")},
		},
	}
	if err := mw.MaybeInject(context.Background(), req); err != nil {
		t.Fatalf("inject error: %v", err)
	}
	if len(req.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(req.Messages))
	}
	if req.Messages[0].Role != models.RoleSystem {
		t.Fatalf("expected injected system message, got %s", req.Messages[0].Role)
	}
}

func TestCosineSimilarity(t *testing.T) {
	a := []float64{1, 0, 0}
	b := []float64{0, 1, 0}
	if CosineSimilarity(a, b) != 0 {
		t.Fatalf("expected 0 similarity for orthogonal vectors")
	}
	c := []float64{1, 2, 3}
	d := []float64{1, 2, 3}
	if CosineSimilarity(c, d) != 1.0 {
		t.Fatalf("expected 1.0 similarity for identical vectors")
	}
}

func strPtrRag(s string) *string { return &s }
