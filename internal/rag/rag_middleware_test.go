package rag

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestRAGHTTPMiddlewareSkipsWhenDisabled(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	mw := RAGHTTPMiddleware(NewHybridRetriever(NewInMemoryVectorStore(), NewInMemoryKeywordIndex()))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	mw(next).ServeHTTP(httptest.NewRecorder(), req)
	if !called {
		t.Fatal("expected next handler to be called when rag_enabled is false")
	}
}

func TestRAGHTTPMiddlewareInjectsWhenEnabled(t *testing.T) {
	store := NewInMemoryVectorStore()
	store.Add(Document{ID: "doc-1", Content: "context about AeroLLM", Score: 0.9})
	received := ""
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received = string(b)
	})
	mw := RAGHTTPMiddleware(NewHybridRetriever(store, NewInMemoryKeywordIndex()))
	payload := `{"model":"gpt-4","messages":[{"role":"user","content":"AeroLLM"}],"rag_enabled":true}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	mw(next).ServeHTTP(httptest.NewRecorder(), req)
	if received == "" {
		t.Fatal("expected downstream handler to receive rewritten body")
	}
	var parsed models.LLMRequest
	if err := json.Unmarshal([]byte(received), &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(parsed.Messages) == 0 {
		t.Fatal("expected messages to be present")
	}
	foundSystem := false
	for _, m := range parsed.Messages {
		if m.Role == models.RoleSystem && m.Content != nil && len(*m.Content) > 0 {
			foundSystem = true
		}
	}
	if !foundSystem {
		t.Fatalf("expected injected system message from RAG, got messages: %+v", parsed.Messages)
	}
}
