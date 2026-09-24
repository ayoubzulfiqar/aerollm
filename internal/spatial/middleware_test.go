package spatial

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestSpatialMiddlewareMultiWrite(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"type":"spatial_anchor",`))
		_, _ = w.Write([]byte(`"x":3,"y":2,"z":1}`))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/?session_id=s1", nil))
	var got WebXRPayload
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("expected WebXR JSON, got %q (%v)", w.Body.String(), err)
	}
	if len(got.Anchors) != 1 || got.Anchors[0].X != 3 || got.SessionID != "s1" {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestSpatialMiddlewareRewriteDropsStaleLength(t *testing.T) {
	body := `{"type":"spatial_anchor","x":1}`
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if cl := w.Header().Get("Content-Length"); cl != "" {
		t.Fatalf("stale Content-Length kept: %s", cl)
	}
	if w.Header().Get("ETag") != "" {
		t.Fatal("stale ETag kept")
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %q", w.Header().Get("Content-Type"))
	}
}

func TestSpatialMiddlewarePreservesStatus(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.WriteHeader(http.StatusInternalServerError) // superfluous, ignored
		_, _ = w.Write([]byte(`{"type":"spatial_anchor","x":1}`))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	if w.Body.String() != `{"type":"spatial_anchor","x":1}` {
		t.Fatalf("error responses must not be rewritten, got %s", w.Body.String())
	}
}

func TestSpatialMiddlewareCreatedStatusKeptOnRewrite(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"type":"spatial_anchor","x":1}`))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusCreated || !strings.Contains(w.Body.String(), `"anchors"`) {
		t.Fatalf("unexpected: %d %s", w.Code, w.Body.String())
	}
}

func TestSpatialMiddlewareOverflowPassesThrough(t *testing.T) {
	part1 := `{"type":"spatial_anchor",`
	part2 := `"x":1,"pad":"` + strings.Repeat("p", 64) + `"}`
	h := &SpatialMiddleware{next: func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(part1))
		_, _ = w.Write([]byte(part2))
	}, MaxBufferBytes: 32}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if w.Body.String() != part1+part2 {
		t.Fatalf("oversized response must pass through intact, got %q", w.Body.String())
	}
}

func TestSpatialMiddlewareFlushSwitchesToStreaming(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"type":"spatial_anchor","x":1}`))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush through middleware failed: %v", err)
		}
		_, _ = w.Write([]byte("\nmore"))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if !w.Flushed {
		t.Fatal("expected underlying writer to be flushed")
	}
	if w.Body.String() != `{"type":"spatial_anchor","x":1}`+"\nmore" {
		t.Fatalf("streamed response must pass through, got %q", w.Body.String())
	}
}

func TestSpatialMiddlewareSkipsEncodedBodies(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write([]byte(`{"type":"spatial_anchor","x":1}`))
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if strings.Contains(w.Body.String(), `"anchors"`) {
		t.Fatal("encoded body must not be rewritten")
	}
}

func TestSpatialMiddlewareEmptyResponse(t *testing.T) {
	h := NewSpatialMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatalf("unexpected: %d %q", w.Code, w.Body.String())
	}
}
