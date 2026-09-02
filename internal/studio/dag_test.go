package studio

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInMemoryDAGStoreCRUD(t *testing.T) {
	store := NewInMemoryDAGStore()
	ctx := context.Background()

	dag := DAG{ID: "dag-1", Name: "Test", Version: "v1", JSON: "{}"}
	if err := store.Save(ctx, dag); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := store.Save(ctx, DAG{ID: "", Name: "x"}); err == nil {
		t.Fatalf("expected error for empty id")
	}

	got, err := store.Get(ctx, "dag-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.Name != "Test" {
		t.Fatalf("unexpected name: %s", got.Name)
	}

	list, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 dag, got %d", len(list))
	}

	if err := store.Delete(ctx, "dag-1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := store.Get(ctx, "dag-1"); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

func TestDAGHandlerListAndSave(t *testing.T) {
	store := NewInMemoryDAGStore()
	h := NewDAGHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/v1/studio/dags", nil)
	w := httptest.NewRecorder()
	h.ListDAGs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/studio/dags", nil)
	w = httptest.NewRecorder()
	h.SaveDAG(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty store, got %d", w.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/studio/dags", nil)
	w = httptest.NewRecorder()
	h.SaveDAG(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", w.Code)
	}

	nilHandler := NewDAGHandler(nil)
	req = httptest.NewRequest(http.MethodPost, "/v1/studio/dags", nil)
	w = httptest.NewRecorder()
	nilHandler.SaveDAG(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when store is nil, got %d", w.Code)
	}
}

func TestDAGHandlerServeDAGsRouting(t *testing.T) {
	h := NewDAGHandler(nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/studio/dags", nil)
	w := httptest.NewRecorder()
	h.ServeDAGs(w, req)
	if w.Code != http.StatusMethodNotAllowed && w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}
