package graphrag

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"encoding/json"
	"strings"
)

func TestGraphStoreNeighborsPropagation(t *testing.T) {
	store := NewBboltGraphStore()
	a, _ := store.UpsertNode(context.Background(), Node{Label: "A", Type: "entity"})
	b, _ := store.UpsertNode(context.Background(), Node{Label: "B", Type: "entity"})
	c, _ := store.UpsertNode(context.Background(), Node{Label: "C", Type: "entity"})
	_, _ = store.UpsertEdge(context.Background(), Edge{Source: a, Target: b, Label: "link"})
	_, _ = store.UpsertEdge(context.Background(), Edge{Source: b, Target: c, Label: "link"})
	edges, err := store.Neighbors(context.Background(), a, 2)
	if err != nil || len(edges) != 2 {
		t.Fatalf("expected 2 edges, got %d err=%v", len(edges), err)
	}
}

func TestGraphStoreQueryRanking(t *testing.T) {
	store := NewBboltGraphStore()
	_, _ = store.UpsertNode(context.Background(), Node{Label: "weather", Type: "topic"})
	_, _ = store.UpsertNode(context.Background(), Node{Label: "news", Type: "topic"})
	nodes, err := store.Query(context.Background(), "weather", 2)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Label != "weather" {
		t.Fatalf("unexpected query ranking: %v", nodes)
	}
}

func TestGraphRAGMiddlewareHTTP(t *testing.T) {
	store := NewBboltGraphStore()
	_, _ = store.UpsertNode(context.Background(), Node{Label: "weather", Type: "topic"})
	mw := NewGraphRAGMiddleware(store)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req models.LLMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != models.RoleSystem {
			t.Fatalf("unexpected messages: %v", req.Messages)
		}
	})
	handler := mw.Middleware(next)
	body := `{"rag_enabled":true,"messages":[{"role":"user","content":"weather forecast"}]}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
}
