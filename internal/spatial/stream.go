package spatial

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	// DefaultStreamChunkSize is the chunk size used when none is configured.
	DefaultStreamChunkSize = 64 << 10
	// MaxStreamChunkSize caps a configured chunk size.
	MaxStreamChunkSize = 4 << 20
	// DefaultMaxStreamBytes caps how many bytes a stream relays by default.
	DefaultMaxStreamBytes int64 = 64 << 20
	// DefaultStreamIdleTimeout bounds how long a single request-body read or
	// response write may block by default.
	DefaultStreamIdleTimeout = 30 * time.Second
	// DefaultStreamMaxDuration caps the total duration of one stream by
	// default.
	DefaultStreamMaxDuration = 15 * time.Minute

	// StreamStatusTrailer is the HTTP trailer reporting how a stream ended:
	// "complete", "truncated" (input exceeded MaxBytes), "canceled" (client
	// went away or the request context ended), "timeout" (a read or write
	// stalled for longer than IdleTimeout, or the stream exceeded
	// MaxDuration) or "error" (read failure).
	StreamStatusTrailer = "X-Spatial-Stream-Status"

	// streamExpireGrace is how long a response write may still take once a
	// stream has been ended early, so the status trailer can be delivered to
	// a client that is still reading.
	streamExpireGrace = time.Second

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
	// IdleTimeout bounds how long one request-body read or one response
	// write may block; the stream then ends with status "timeout". It is
	// enforced with connection deadlines (http.ResponseController), so a
	// client that stops sending mid-body, or stops reading the response,
	// cannot pin the handler until the connection is closed. Read deadlines
	// apply when the relayed body is the request body. 0 selects
	// DefaultStreamIdleTimeout; a negative value disables it.
	//
	// Per-operation deadlines replace the server's ReadTimeout/WriteTimeout
	// for the duration of the stream (long streams would otherwise be cut
	// off by them); MaxDuration bounds the total instead.
	IdleTimeout time.Duration
	// MaxDuration caps the total stream time (status "timeout"). 0 selects
	// DefaultStreamMaxDuration; a negative value disables it.
	MaxDuration time.Duration
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

func (h *Video3DStreamHandler) idleTimeout() time.Duration {
	if h == nil || h.IdleTimeout == 0 {
		return DefaultStreamIdleTimeout
	}
	if h.IdleTimeout < 0 {
		return 0
	}
	return h.IdleTimeout
}

func (h *Video3DStreamHandler) maxDuration() time.Duration {
	if h == nil || h.MaxDuration == 0 {
		return DefaultStreamMaxDuration
	}
	if h.MaxDuration < 0 {
		return 0
	}
	return h.MaxDuration
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
// ends, when MaxBytes is exceeded, when a write fails, when a read or write
// stalls for longer than IdleTimeout, when MaxDuration elapses, or when the
// request context is canceled (client disconnect). The outcome is reported
// in the StreamStatusTrailer trailer.
//
// When body is the request body, stalled reads are ended by connection read
// deadlines rather than only when the connection closes.
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
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if d := h.maxDuration(); d > 0 {
		ctx, cancel = context.WithTimeout(parent, d)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
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

	dl := &streamDeadlines{ctl: ctl, idle: h.idleTimeout(), reads: r != nil && body == r.Body}
	status := ""
	stopExpire := context.AfterFunc(ctx, dl.expire)
	// Registered after cancel so it runs first: the deadlines must be
	// settled before cancel() would fire dl.expire on a finished stream.
	defer func() {
		stopExpire()
		dl.finish(status == "complete")
	}()

	hdr := w.Header()
	hdr.Del("Content-Length")
	hdr.Set("Content-Type", "application/octet-stream")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("X-Accel-Buffering", "no")
	hdr.Set("Trailer", StreamStatusTrailer)
	dl.beforeWrite()
	w.WriteHeader(http.StatusOK)

	canFlush := true
	flush := func() {
		if canFlush && ctl.Flush() != nil {
			canFlush = false
		}
	}
	flush()

	chunker := NewStreamChunker(&deadlineReader{r: limited, d: dl}, h.chunkSize())
	chunks := chunker.Chunks(ctx)
	for chunk := range chunks {
		dl.beforeWrite()
		if _, err := w.Write(chunk); err != nil {
			status = dl.writeStatus(ctx)
			cancel()
			break
		}
		flush()
	}
	if status == "" {
		status = dl.readStatus(ctx, chunker.Err())
	} else if dl.readsBounded() {
		// The producer may be blocked in a request-body read; the read
		// deadline set by dl.expire (fired by cancel) ends it, so wait for it
		// rather than returning while it still touches the body.
		for range chunks {
		}
	}
	hdr.Set(StreamStatusTrailer, status)
}

// streamDeadlines applies per-operation connection deadlines to one stream
// and forces them into the past when the stream's context ends, which
// unblocks a read or write that is stuck on a stalled client.
type streamDeadlines struct {
	ctl  *http.ResponseController
	idle time.Duration
	// reads is true when the relayed body is the request body, so connection
	// read deadlines affect it.
	reads bool

	mu               sync.Mutex
	finished         bool
	expired          bool
	readUnsupported  bool
	writeUnsupported bool
	readDL           time.Time
	writeDL          time.Time
}

// readsBounded reports whether a blocked body read is guaranteed to end once
// the stream expires.
func (d *streamDeadlines) readsBounded() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads && !d.readUnsupported
}

func (d *streamDeadlines) beforeRead() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.reads || d.idle <= 0 || d.expired || d.finished || d.readUnsupported {
		return
	}
	next := time.Now().Add(d.idle)
	if d.ctl.SetReadDeadline(next) != nil {
		d.readUnsupported = true
		return
	}
	d.readDL = next
}

func (d *streamDeadlines) beforeWrite() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.idle <= 0 || d.expired || d.finished || d.writeUnsupported {
		return
	}
	next := time.Now().Add(d.idle)
	if d.ctl.SetWriteDeadline(next) != nil {
		d.writeUnsupported = true
		return
	}
	d.writeDL = next
}

// expire runs when the stream context ends (client gone, MaxDuration, or the
// handler finishing early): pending reads fail immediately and writes get a
// short grace period to deliver the status trailer.
func (d *streamDeadlines) expire() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return
	}
	d.expired = true
	now := time.Now()
	if d.reads && !d.readUnsupported {
		if d.ctl.SetReadDeadline(now) != nil {
			d.readUnsupported = true
		}
	}
	if !d.writeUnsupported {
		_ = d.ctl.SetWriteDeadline(now.Add(streamExpireGrace))
	}
}

// finish stops deadline management. After a clean stream the read deadline
// is cleared and the response gets one more idle period to be finalised
// (net/http clears the write deadline itself once the response is finished,
// and resets the read deadline before reading the next request on a
// kept-alive connection). After an aborted stream, reads fail immediately
// (net/http then gives up draining the unread body instead of blocking on
// it) and writes get a short grace period: the last per-chunk write deadline
// may already have passed together with the idle read deadline, which would
// otherwise lose the status trailer.
func (d *streamDeadlines) finish(clean bool) {
	d.mu.Lock()
	d.finished = true
	reads := d.reads && !d.readUnsupported
	writes := !d.writeUnsupported && (d.idle > 0 || d.expired)
	d.mu.Unlock()
	if clean {
		if reads {
			_ = d.ctl.SetReadDeadline(time.Time{})
		}
		if writes {
			var next time.Time
			if d.idle > 0 {
				next = time.Now().Add(d.idle)
			}
			_ = d.ctl.SetWriteDeadline(next)
		}
		return
	}
	now := time.Now()
	if reads {
		_ = d.ctl.SetReadDeadline(now)
	}
	if writes {
		_ = d.ctl.SetWriteDeadline(now.Add(streamExpireGrace))
	}
}

// passed reports whether the idle deadline last set for reads (or writes)
// has been reached, i.e. whether a timeout was caused by inactivity rather
// than by expire forcing the deadline early.
func (d *streamDeadlines) passed(write bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	dl := d.readDL
	if write {
		dl = d.writeDL
	}
	return !dl.IsZero() && !time.Now().Before(dl)
}

func (d *streamDeadlines) writeStatus(ctx context.Context) string {
	if d.passed(true) {
		return "timeout"
	}
	return ctxStatus(ctx, "canceled")
}

func (d *streamDeadlines) readStatus(ctx context.Context, err error) string {
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return "complete"
	case errors.As(err, &tooLarge):
		return "truncated"
	case isTimeout(err):
		if d.passed(false) {
			return "timeout"
		}
		return ctxStatus(ctx, "timeout")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ctxStatus(ctx, "canceled")
	default:
		return "error"
	}
}

// ctxStatus maps the stream context's state to a status: MaxDuration elapsing
// is a timeout, any other cancellation means the client went away.
func ctxStatus(ctx context.Context, fallback string) string {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "timeout"
	case ctx.Err() != nil:
		return "canceled"
	default:
		return fallback
	}
}

func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// deadlineReader extends the connection read deadline before every read.
type deadlineReader struct {
	r io.Reader
	d *streamDeadlines
}

func (dr *deadlineReader) Read(p []byte) (int, error) {
	dr.d.beforeRead()
	return dr.r.Read(p)
}
