package rag

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDocumentsHandlerLifecycle(t *testing.T) {
	vs := NewInMemoryVectorStore()
	ki := NewInMemoryKeywordIndex()
	h := NewDocumentsHandler(vs, ki)

	do := func(method, target, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	rec := do(http.MethodPost, "/", `{"documents":[{"id":"a","content":"AeroLLM routes requests"},{"content":"anonymous doc"}]}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"indexed":2`) {
		t.Fatalf("ingest failed: %d %s", rec.Code, rec.Body.String())
	}
	if vs.Len() != 2 || ki.Len() != 2 {
		t.Fatalf("documents not indexed in every store: %d/%d", vs.Len(), ki.Len())
	}
	docs, _ := NewHybridRetriever(vs, ki).Retrieve(context.Background(), "how does AeroLLM route?", 1)
	if len(docs) != 1 || docs[0].ID != "a" {
		t.Fatalf("ingested document not retrievable: %+v", docs)
	}
	if rec := do(http.MethodGet, "/", ""); !strings.Contains(rec.Body.String(), `"count":2`) {
		t.Fatalf("count wrong: %s", rec.Body.String())
	}
	if rec := do(http.MethodDelete, "/?id=a", ""); rec.Code != http.StatusOK {
		t.Fatalf("delete failed: %d", rec.Code)
	}
	if rec := do(http.MethodDelete, "/?id=a", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("second delete should 404, got %d", rec.Code)
	}

	for name, tc := range map[string]struct {
		method, body string
		status       int
	}{
		"empty":      {http.MethodPost, `{"documents":[]}`, http.StatusBadRequest},
		"no content": {http.MethodPost, `[{"id":"x","content":"  "}]`, http.StatusBadRequest},
		"bad json":   {http.MethodPost, `{"documents":`, http.StatusBadRequest},
		"huge doc":   {http.MethodPost, `[{"content":"` + strings.Repeat("x", MaxDocumentBytes+1) + `"}]`, http.StatusBadRequest},
		"wrong verb": {http.MethodPut, `[]`, http.StatusMethodNotAllowed},
	} {
		rec := do(tc.method, "/", tc.body)
		if rec.Code != tc.status || rec.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s: got %d (%s), want %d", name, rec.Code, rec.Header().Get("Content-Type"), tc.status)
		}
	}
	if rec := do(http.MethodPut, "/", ""); rec.Header().Get("Allow") == "" {
		t.Error("405 must carry an Allow header")
	}
	big := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`[{"content":"`+strings.Repeat("x", MaxIngestBodyBytes)+`"}]`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body should be 413, got %d", rec.Code)
	}
}
