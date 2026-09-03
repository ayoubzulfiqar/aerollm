package spatial

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSpatialMiddlewareRewritesAnchors(t *testing.T) {
	h := NewSpatialMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.URL.RawQuery = "session_id=abc"
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"x"`) {
		t.Fatalf("expected rewritten spatial payload, got %s", w.Body.String())
	}
}

func TestSpatialMiddlewarePassthrough(t *testing.T) {
	h := NewSpatialMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(w, r)
	if w.Body.String() != "hello" {
		t.Fatalf("expected passthrough body, got %s", w.Body.String())
	}
}

func TestSpatialMiddlewareMissingNext(t *testing.T) {
	h := NewSpatialMiddleware(nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}
