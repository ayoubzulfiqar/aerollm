package graphrag

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestGraphStoreLifecycle(t *testing.T) {
	store := NewBboltGraphStore()
	id, err := store.UpsertNode(context.Background(), Node{Label: "city", Type: "entity"})
	if err != nil || id == "" {
		t.Fatalf("upsert node failed: %v", err)
	}
	_, err = store.UpsertEdge(context.Background(), Edge{Source: id, Target: id, Label: "self"})
	if err != nil {
		t.Fatalf("upsert edge failed: %v", err)
	}
	edges, err := store.Neighbors(context.Background(), id, 2)
	if err != nil || len(edges) != 1 {
		t.Fatalf("neighbors failed: %v %d", err, len(edges))
	}
	nodes, err := store.Query(context.Background(), "city", 8)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("query failed: %v %d", err, len(nodes))
	}
}

func TestGraphRAGMiddlewareInject(t *testing.T) {
	store := NewBboltGraphStore()
	_, _ = store.UpsertNode(context.Background(), Node{Label: "weather", Type: "topic"})
	mw := NewGraphRAGMiddleware(store)
	req := &models.LLMRequest{
		RagEnabled: true,
		Messages: []models.Message{{
			Role:    models.RoleUser,
			Content: strPtr("weather forecast"),
		}},
	}
	if err := mw.MaybeInject(context.Background(), req); err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	if len(req.Messages) != 2 {
		t.Fatalf("expected 2 messages after injection, got %d", len(req.Messages))
	}
	if req.Messages[0].Content == nil || *req.Messages[0].Content != "Graph context:\n- weather (topic)\n\nQuery: weather forecast\n" {
		t.Fatalf("unexpected system text: %v", *req.Messages[0].Content)
	}
}

func strPtr(s string) *string {
	return &s
}
