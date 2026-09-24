package spatial

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
)

const (
	// DefaultStreamChunkSize is the chunk size used when none is configured.
	DefaultStreamChunkSize = 64 << 10
	// MaxStreamChunkSize caps a configured chunk size.
	MaxStreamChunkSize = 4 << 20
	// DefaultMaxStreamBytes caps how many bytes a stream relays by default.
	DefaultMaxStreamBytes int64 = 64 << 20

	// StreamStatusTrailer is the HTTP trailer reporting how a stream ended:
	// "complete", "truncated" (input exceeded MaxBytes), "canceled" (client
	// went away or the request context ended) or "error" (read failure).
	StreamStatusTrailer = "X-Spatial-Stream-Status"

	// maxZeroReads bounds consecutive (0, nil) reads before giving up, as
	// io.Reader implementations are discouraged from returning them.
	maxZeroReads = 100
)

// ErrChunksAlreadyStarted is reported by StreamChunker.Err when Chunks is
// called more than once; the reader can only be consumed once.
var ErrChunksAlreadyStarted = errors.New("spatial: Chunks already called on this chunker")

// StreamChunker splits an io.Reader into bounded chunks without buffering the
// whole input.
type StreamChunker struct {
	chunkSize int
	reader    io.Reader

	once sync.Once
	mu   sync.Mutex
	err  error
}

// NewStreamChunker creates a new chunker. A chunkSize <= 0 selects
// DefaultStreamChunkSize; sizes above MaxStreamChunkSize are clamped.
func NewStreamChunker(r io.Reader, chunkSize int) *StreamChunker {
	if chunkSize <= 0 {
		chunkSize = DefaultStreamChunkSize
	}
	if chunkSize > MaxStreamChunkSize {
		chunkSize = MaxStreamChunkSize
	}
	return &StreamChunker{chunkSize: chunkSize, reader: r}
}

// Chunks yields byte chunks from the underlying reader. The channel is closed
// on EOF, on a read error, or when ctx is done; Err then reports why (nil for
// a clean EOF). The producer goroutine exits as soon as ctx is done unless it
// is blocked inside the underlying Read, so consumers that stop reading early
// must cancel ctx. Chunks may only be called once.
func (c *StreamChunker) Chunks(ctx context.Context) <-chan []byte {
	out := make(chan []byte)
	first := false
	c.once.Do(func() { first = true })
	if !first {
		c.setErr(ErrChunksAlreadyStarted)
		close(out)
		return out
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.reader == nil {
		c.setErr(errors.New("spatial: nil reader"))
		close(out)
		return out
	}
	go func() {
		defer close(out)
		buf := make([]byte, c.chunkSize)
		zeroReads := 0
		for {
			if err := ctx.Err(); err != nil {
				c.setErr(err)
				return
			}
			n, err := c.reader.Read(buf)
			if n > 0 {
				zeroReads = 0
				select {
				case out <- append([]byte(nil), buf[:n]...):
				case <-ctx.Done():
					c.setErr(ctx.Err())
					return
				}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					c.setErr(err)
				}
				return
			}
			if n == 0 {
				zeroReads++
				if zeroReads >= maxZeroReads {
					c.setErr(io.ErrNoProgress)
					return
				}
			}
		}
	}()
	return out
}

// Err returns the first non-EOF error that ended the stream. It is only
// meaningful after the channel returned by Chunks has been closed.
func (c *StreamChunker) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *StreamChunker) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

// Video3DStreamHandler relays a request body to the client as a chunked,
// flushed binary stream.
type Video3DStreamHandler struct {
	// MaxBytes caps how many body bytes are relayed; 0 selects
	// DefaultMaxStreamBytes. Requests with a larger Content-Length are
	// rejected with 413; unbounded bodies are cut off at the cap and the
	// StreamStatusTrailer reports "truncated".
	MaxBytes int64
	// ChunkSize is the read size per chunk; 0 selects DefaultStreamChunkSize.
	ChunkSize int
}

// NewVideo3DStreamHandler creates a new handler with default limits.
func NewVideo3DStreamHandler() *Video3DStreamHandler {
	return &Video3DStreamHandler{MaxBytes: DefaultMaxStreamBytes, ChunkSize: DefaultStreamChunkSize}
}

func (h *Video3DStreamHandler) maxBytes() int64 {
	if h == nil || h.MaxBytes <= 0 {
		return DefaultMaxStreamBytes
	}
	return h.MaxBytes
}

func (h *Video3DStreamHandler) chunkSize() int {
	if h == nil {
		return DefaultStreamChunkSize
	}
	return h.ChunkSize
}

// ServeHTTP is a ready-to-mount handler: POST only (405 with Allow otherwise),
// streaming the capped request body back to the client.
func (h *Video3DStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.StreamResponse(w, r, r.Body)
}

// Handler returns h as an http.HandlerFunc (see ServeHTTP).
func (h *Video3DStreamHandler) Handler() http.HandlerFunc { return h.ServeHTTP }

// StreamResponse writes body to w in flushed chunks. It stops when the body
// ends, when MaxBytes is exceeded, when a write fails, or when the request
// context is canceled (client disconnect). The outcome is reported in the
// StreamStatusTrailer trailer.
//
// Writers that cannot flush (for example wrapped by middleware without an
// Unwrap method) still receive the full stream, just without per-chunk
// flushing, rather than failing the request.
func (h *Video3DStreamHandler) StreamResponse(w http.ResponseWriter, r *http.Request, body io.Reader) {
	if body == nil || body == http.NoBody {
		writeJSONError(w, http.StatusBadRequest, "missing body")
		return
	}
	limit := h.maxBytes()
	parent := context.Background()
	if r != nil {
		if r.ContentLength > limit {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		parent = r.Context()
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	rc, ok := body.(io.ReadCloser)
	if !ok {
		rc = io.NopCloser(body)
	}
	// MaxBytesReader also tells the server to close the connection when the
	// client keeps sending past the cap. It is never closed here: the caller
	// (or net/http, for request bodies) owns the body.
	limited := http.MaxBytesReader(w, rc, limit)

	ctl := http.NewResponseController(w)
	// This handler relays the request body while writing the response. For
	// HTTP/1.x, net/http otherwise discards (or closes) the unread request
	// body as soon as response headers are flushed, which silently turned
	// every real-network stream into an empty 200. Writers that cannot do
	// full duplex (e.g. httptest.ResponseRecorder) return an error we can
	// ignore, since they do not have that restriction.
	_ = ctl.EnableFullDuplex()

	hdr := w.Header()
	hdr.Del("Content-Length")
	hdr.Set("Content-Type", "application/octet-stream")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("X-Accel-Buffering", "no")
	hdr.Set("Trailer", StreamStatusTrailer)
	w.WriteHeader(http.StatusOK)

	canFlush := true
	flush := func() {
		if canFlush && ctl.Flush() != nil {
			canFlush = false
		}
	}
	flush()

	chunker := NewStreamChunker(limited, h.chunkSize())
	status := ""
	for chunk := range chunker.Chunks(ctx) {
		if _, err := w.Write(chunk); err != nil {
			status = "canceled"
			cancel()
			break
		}
		flush()
	}
	if status == "" {
		status = streamStatus(chunker.Err())
	}
	hdr.Set(StreamStatusTrailer, status)
}

func streamStatus(err error) string {
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return "complete"
	case errors.As(err, &tooLarge):
		return "truncated"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "error"
	}
}
