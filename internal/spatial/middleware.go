package spatial

import (
	"encoding/json"
	"net/http"
)

// SpatialMiddleware scans responses for spatial anchors and transforms them.
type SpatialMiddleware struct {
	next http.Handler
}

// NewSpatialMiddleware creates a new spatial middleware.
func NewSpatialMiddleware(next http.Handler) *SpatialMiddleware {
	return &SpatialMiddleware{next: next}
}

// ServeHTTP intercepts the response and applies WebXR translation when spatial anchors are detected.
func (m *SpatialMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.next == nil {
		http.Error(w, `{"error":"spatial middleware not configured"}`, http.StatusInternalServerError)
		return
	}
	ctx := r.Context()
	sessionID := r.URL.Query().Get("session_id")
	rec := &spatialResponseRecorder{w: w, sessionID: sessionID}
	m.next.ServeHTTP(rec, r.WithContext(ctx))
	if rec.body != "" {
		anchors := ParseSpatialAnchors(rec.body)
		if len(anchors) > 0 {
			payload := ToWebXR(anchors, sessionID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(payload)
			return
		}
	}
	if rec.code > 0 {
		w.WriteHeader(rec.code)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if rec.body != "" {
		_, _ = w.Write([]byte(rec.body))
	}
}

type spatialResponseRecorder struct {
	w         http.ResponseWriter
	code      int
	body      string
	sessionID string
}

func (r *spatialResponseRecorder) Header() http.Header { return r.w.Header() }
func (r *spatialResponseRecorder) WriteHeader(code int) { r.code = code }
func (r *spatialResponseRecorder) Write(b []byte) (int, error) {
	r.body = string(b)
	return len(b), nil
}
