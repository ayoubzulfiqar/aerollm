package studio

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

func TestValidateDAGRejectsBadInput(t *testing.T) {
	cases := map[string]DAG{
		"bad id":         {ID: "../etc"},
		"long id":        {ID: strings.Repeat("a", 129)},
		"long name":      {ID: "x", Name: strings.Repeat("n", 257)},
		"invalid json":   {ID: "x", JSON: "{not json"},
		"cycle":          {ID: "x", JSON: `{"nodes":[{"id":"a","depends_on":["b"]},{"id":"b","depends_on":["a"]}]}`},
		"edge cycle":     {ID: "x", JSON: `{"nodes":[{"id":"a"},{"id":"b"}],"edges":[{"source":"a","target":"b"},{"from":"b","to":"a"}]}`},
		"self loop":      {ID: "x", JSON: `{"nodes":[{"id":"a"}],"edges":[{"from":"a","to":"a"}]}`},
		"duplicate node": {ID: "x", JSON: `{"nodes":[{"id":"a"},{"id":"a"}]}`},
		"unknown ref":    {ID: "x", JSON: `{"nodes":[{"id":"a"}],"edges":[{"from":"a","to":"zzz"}]}`},
		"unknown dep":    {ID: "x", JSON: `{"nodes":[{"id":"a","depends_on":["q"]}]}`},
		"empty node id":  {ID: "x", JSON: `{"nodes":[{"id":""}]}`},
		"bad nodes type": {ID: "x", JSON: `{"nodes":"nope"}`},
	}
	for name, dag := range cases {
		if err := ValidateDAG(dag); !errors.Is(err, ErrInvalidDAG) {
			t.Errorf("%s: expected ErrInvalidDAG, got %v", name, err)
		}
	}
	ok := DAG{ID: "wf.v1_a-b", JSON: `{"nodes":[{"id":"a"},{"id":"b","depends_on":["a"]},{"id":"c"}],"edges":[{"from":"b","to":"c"}]}`}
	if err := ValidateDAG(ok); err != nil {
		t.Fatalf("expected valid dag, got %v", err)
	}
	if err := ValidateDAG(DAG{ID: "opaque", JSON: `[1,2,3]`}); err != nil {
		t.Fatalf("opaque json should be accepted, got %v", err)
	}
}

func TestInMemoryDAGStorePreservesCreatedAt(t *testing.T) {
	store := NewInMemoryDAGStore()
	ctx := context.Background()
	if err := store.Save(ctx, DAG{ID: "d", Name: "one"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	first, _ := store.Get(ctx, "d")
	time.Sleep(2 * time.Millisecond)
	if err := store.Save(ctx, DAG{ID: "d", Name: "two", CreatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	second, _ := store.Get(ctx, "d")
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("CreatedAt changed on update: %v -> %v", first.CreatedAt, second.CreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) || second.Name != "two" {
		t.Fatalf("update not applied: %+v", second)
	}
}

func TestDAGHandlerCRUDRouting(t *testing.T) {
	h := NewDAGHandler(NewInMemoryDAGStore())

	w := httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodPost, "/v1/studio/dags", strings.NewReader(`{"id":"d1","name":"n","json":"{\"nodes\":[{\"id\":\"a\"}]}"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("save: expected 200, got %d %s", w.Code, w.Body.String())
	}
	var saved DAG
	if err := json.Unmarshal(w.Body.Bytes(), &saved); err != nil || saved.CreatedAt.IsZero() || saved.UpdatedAt.IsZero() {
		t.Fatalf("expected stored dag with timestamps, got %s (%v)", w.Body.String(), err)
	}

	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodGet, "/v1/studio/dags?id=d1", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"id":"d1"`) {
		t.Fatalf("get: %d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodGet, "/v1/studio/dags?id=missing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("get missing: expected 404, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodPost, "/v1/studio/dags", strings.NewReader(`{"id":"d2","json":"{\"nodes\":[{\"id\":\"a\",\"depends_on\":[\"a\"]}]}"}`)))
	if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("cyclic save: expected 400 JSON, got %d %q", w.Code, w.Header().Get("Content-Type"))
	}

	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodDelete, "/v1/studio/dags?id=d1", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete: expected 204, got %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodDelete, "/v1/studio/dags?id=d1", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete missing: expected 404, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	h.ServeDAGs(w, httptest.NewRequest(http.MethodPatch, "/v1/studio/dags", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
		t.Fatalf("patch: expected 405 with Allow, got %d %q", w.Code, w.Header().Get("Allow"))
	}
}

func TestDAGHandlerBodyTooLarge(t *testing.T) {
	h := NewDAGHandler(NewInMemoryDAGStore())
	big := `{"id":"d","json":"` + strings.Repeat("a", MaxDAGBodyBytes) + `"}`
	w := httptest.NewRecorder()
	h.SaveDAG(w, httptest.NewRequest(http.MethodPost, "/v1/studio/dags", strings.NewReader(big)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}
