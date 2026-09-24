package spatial

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// SpatialMiddleware scans responses for spatial anchors and transforms them.
//
// Successful (2xx) uncompressed responses up to MaxBufferBytes are buffered;
// when they contain spatial anchors the body is replaced with a WebXRPayload.
// Larger responses, responses that flush (streams) and everything else pass
// through unchanged.
type SpatialMiddleware struct {
	next http.HandlerFunc
	// MaxBufferBytes caps how much of the downstream response is buffered for
	// anchor detection; 0 selects DefaultMaxParseBytes.
	MaxBufferBytes int
}

// NewSpatialMiddleware creates a new spatial middleware.
func NewSpatialMiddleware(next http.HandlerFunc) *SpatialMiddleware {
	return &SpatialMiddleware{next: next}
}

func (m *SpatialMiddleware) bufferLimit() int {
	if m.MaxBufferBytes <= 0 {
		return DefaultMaxParseBytes
	}
	return m.MaxBufferBytes
}

// ServeHTTP intercepts the response and applies WebXR translation when spatial anchors are detected.
func (m *SpatialMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m == nil || m.next == nil {
		writeJSONError(w, http.StatusInternalServerError, "spatial middleware not configured")
		return
	}
	sessionID := sanitizeSessionID(r.URL.Query().Get("session_id"))
	rec := &spatialResponseRecorder{w: w, limit: m.bufferLimit()}
	m.next.ServeHTTP(rec, r)
	if rec.passthrough {
		return
	}
	code := rec.status()
	if rewritable(code, w.Header()) && rec.buf.Len() > 0 {
		anchors := ParseSpatialAnchors(rec.buf.String())
		if len(anchors) > 0 {
			payload, err := json.Marshal(ToWebXR(anchors, sessionID))
			if err == nil {
				h := w.Header()
				// The body changes, so validators and length computed by the
				// downstream handler no longer apply.
				h.Del("Content-Length")
				h.Del("Etag")
				h.Del("Content-Md5")
				h.Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_, _ = w.Write(append(payload, '\n'))
				return
			}
		}
	}
	w.WriteHeader(code)
	if rec.buf.Len() > 0 {
		_, _ = w.Write(rec.buf.Bytes())
	}
}

// rewritable reports whether a buffered response may be replaced.
func rewritable(code int, h http.Header) bool {
	if code < 200 || code > 299 || code == http.StatusNoContent || code == http.StatusPartialContent {
		return false
	}
	if enc := strings.TrimSpace(h.Get("Content-Encoding")); enc != "" && !strings.EqualFold(enc, "identity") {
		return false
	}
	return true
}

// spatialResponseRecorder buffers a downstream response up to limit bytes and
// switches to pass-through once the limit is exceeded or the handler flushes.
type spatialResponseRecorder struct {
	w           http.ResponseWriter
	code        int
	wroteHeader bool
	buf         bytes.Buffer
	limit       int
	passthrough bool
}

func (r *spatialResponseRecorder) Header() http.Header { return r.w.Header() }

func (r *spatialResponseRecorder) status() int {
	if r.code == 0 {
		return http.StatusOK
	}
	return r.code
}

func (r *spatialResponseRecorder) WriteHeader(code int) {
	// Informational responses (e.g. 103 Early Hints) may be sent any number
	// of times before the final header; forward them untouched.
	if code >= 100 && code <= 199 && code != http.StatusSwitchingProtocols {
		r.w.WriteHeader(code)
		return
	}
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.code = code
}

func (r *spatialResponseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	if r.passthrough {
		return r.w.Write(b)
	}
	if r.buf.Len()+len(b) > r.limit {
		if err := r.startPassthrough(); err != nil {
			return 0, err
		}
		return r.w.Write(b)
	}
	return r.buf.Write(b)
}

// startPassthrough commits the recorded status and buffered bytes to the
// underlying writer; subsequent writes go straight through.
func (r *spatialResponseRecorder) startPassthrough() error {
	if r.passthrough {
		return nil
	}
	r.passthrough = true
	if !r.wroteHeader {
		r.wroteHeader = true
		r.code = http.StatusOK
	}
	r.w.WriteHeader(r.status())
	if r.buf.Len() == 0 {
		return nil
	}
	_, err := r.w.Write(r.buf.Bytes())
	r.buf.Reset()
	return err
}

// FlushError switches to pass-through (a flushing handler is streaming and
// cannot be rewritten) and flushes the underlying writer.
func (r *spatialResponseRecorder) FlushError() error {
	if err := r.startPassthrough(); err != nil {
		return err
	}
	return http.NewResponseController(r.w).Flush()
}

// Flush implements http.Flusher.
func (r *spatialResponseRecorder) Flush() { _ = r.FlushError() }

// Unwrap exposes the underlying writer to http.ResponseController.
func (r *spatialResponseRecorder) Unwrap() http.ResponseWriter { return r.w }
