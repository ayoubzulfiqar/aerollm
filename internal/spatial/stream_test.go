package spatial

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamResponseChunks(t *testing.T) {
	payload := "hello world"
	body := strings.NewReader(payload)
	h := NewVideo3DStreamHandler()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	h.StreamResponse(w, r, body)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("unexpected content type")
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("missing hardening headers: %v", resp.Header)
	}
	if got := w.Body.String(); got != payload {
		t.Fatalf("unexpected body: %s", got)
	}
	if got := resp.Trailer.Get(StreamStatusTrailer); got != "complete" {
		t.Fatalf("expected complete trailer, got %q", got)
	}
	if !w.Flushed {
		t.Fatal("expected response to be flushed")
	}
}

func TestStreamResponseMissingBody(t *testing.T) {
	w := httptest.NewRecorder()
	NewVideo3DStreamHandler().StreamResponse(w, httptest.NewRequest(http.MethodPost, "/", nil), nil)
	if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected JSON 400, got %d %q", w.Code, w.Header().Get("Content-Type"))
	}
}

func TestStreamResponseRejectsDeclaredOversizeBody(t *testing.T) {
	h := &Video3DStreamHandler{MaxBytes: 4}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123456789"))
	h.StreamResponse(w, r, r.Body)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

func TestStreamResponseTruncatesUndeclaredOversizeBody(t *testing.T) {
	h := &Video3DStreamHandler{MaxBytes: 10, ChunkSize: 3}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	h.StreamResponse(w, r, strings.NewReader(strings.Repeat("a", 25)))
	if got := w.Body.Len(); got != 10 {
		t.Fatalf("expected exactly 10 bytes relayed, got %d", got)
	}
	if got := w.Result().Trailer.Get(StreamStatusTrailer); got != "truncated" {
		t.Fatalf("expected truncated trailer, got %q", got)
	}
}

func TestStreamResponseExactlyAtCap(t *testing.T) {
	h := &Video3DStreamHandler{MaxBytes: 10}
	w := httptest.NewRecorder()
	h.StreamResponse(w, httptest.NewRequest(http.MethodPost, "/", nil), strings.NewReader(strings.Repeat("a", 10)))
	if w.Body.Len() != 10 || w.Result().Trailer.Get(StreamStatusTrailer) != "complete" {
		t.Fatalf("unexpected result: %d bytes, trailer %q", w.Body.Len(), w.Result().Trailer.Get(StreamStatusTrailer))
	}
}

func TestStreamResponseReadErrorReported(t *testing.T) {
	w := httptest.NewRecorder()
	body := io.MultiReader(strings.NewReader("abc"), errReader{errors.New("upstream reset")})
	NewVideo3DStreamHandler().StreamResponse(w, httptest.NewRequest(http.MethodPost, "/", nil), body)
	if w.Body.String() != "abc" || w.Result().Trailer.Get(StreamStatusTrailer) != "error" {
		t.Fatalf("unexpected: body=%q trailer=%q", w.Body.String(), w.Result().Trailer.Get(StreamStatusTrailer))
	}
}

// plainWriter implements only http.ResponseWriter (no Flush, no Unwrap).
type plainWriter struct {
	h    http.Header
	code int
	buf  bytes.Buffer
}

func (p *plainWriter) Header() http.Header {
	if p.h == nil {
		p.h = http.Header{}
	}
	return p.h
}
func (p *plainWriter) WriteHeader(code int)        { p.code = code }
func (p *plainWriter) Write(b []byte) (int, error) { return p.buf.Write(b) }

func TestStreamResponseWithoutFlusherStillStreams(t *testing.T) {
	w := &plainWriter{}
	NewVideo3DStreamHandler().StreamResponse(w, httptest.NewRequest(http.MethodPost, "/", nil), strings.NewReader("data"))
	if w.code != http.StatusOK || w.buf.String() != "data" {
		t.Fatalf("expected streamed body without flusher, got %d %q", w.code, w.buf.String())
	}
}

func TestVideo3DStreamHandlerServeHTTP(t *testing.T) {
	h := NewVideo3DStreamHandler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected 405 with Allow, got %d %q", w.Code, w.Header().Get("Allow"))
	}
	w = httptest.NewRecorder()
	h.Handler()(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("xyz")))
	if w.Code != http.StatusOK || w.Body.String() != "xyz" {
		t.Fatalf("unexpected: %d %q", w.Code, w.Body.String())
	}
}

// endlessReader produces data forever and counts reads.
type endlessReader struct{ reads atomic.Int64 }

func (e *endlessReader) Read(p []byte) (int, error) {
	e.reads.Add(1)
	time.Sleep(time.Millisecond)
	for i := range p {
		p[i] = 'z'
	}
	return len(p), nil
}

func TestStreamResponseStopsOnClientDisconnect(t *testing.T) {
	src := &endlessReader{}
	handlerDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		(&Video3DStreamHandler{ChunkSize: 512}).StreamResponse(w, r, src)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader("x"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	buf := make([]byte, 1024)
	if _, err := io.ReadAtLeast(resp.Body, buf, 1024); err != nil {
		t.Fatalf("read: %v", err)
	}
	cancel()
	_ = resp.Body.Close()

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler kept streaming after client disconnect")
	}
	after := src.reads.Load()
	time.Sleep(50 * time.Millisecond)
	if extra := src.reads.Load() - after; extra > 1 {
		t.Fatalf("source still being read after handler returned (%d extra reads)", extra)
	}
}

func TestStreamChunkerEmpty(t *testing.T) {
	chunker := NewStreamChunker(strings.NewReader(""), 1024)
	chunks := 0
	for range chunker.Chunks(context.Background()) {
		chunks++
	}
	if chunks != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", chunks)
	}
	if err := chunker.Err(); err != nil {
		t.Fatalf("expected nil error on EOF, got %v", err)
	}
}

func TestStreamChunkerRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	chunker := NewStreamChunker(strings.NewReader("abc"), 1)
	cancel()
	for range chunker.Chunks(ctx) {
	}
	if !errors.Is(chunker.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", chunker.Err())
	}
}

func TestStreamChunkerZeroChunkSizeUsesDefault(t *testing.T) {
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		for c := range NewStreamChunker(strings.NewReader("payload"), 0).Chunks(context.Background()) {
			sb.Write(c)
		}
		done <- sb.String()
	}()
	select {
	case got := <-done:
		if got != "payload" {
			t.Fatalf("unexpected payload %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("chunkSize 0 did not terminate (busy loop)")
	}
	if c := NewStreamChunker(nil, -5); c.chunkSize != DefaultStreamChunkSize {
		t.Fatalf("negative chunk size not defaulted: %d", c.chunkSize)
	}
	if c := NewStreamChunker(nil, 1<<30); c.chunkSize != MaxStreamChunkSize {
		t.Fatalf("huge chunk size not clamped: %d", c.chunkSize)
	}
}

func TestStreamChunkerAbandonedConsumerDoesNotLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	chunker := NewStreamChunker(&endlessReader{}, 8)
	ch := chunker.Chunks(ctx)
	<-ch // consume one chunk, then walk away
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if !errors.Is(chunker.Err(), context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", chunker.Err())
				}
				return
			}
		case <-deadline:
			t.Fatal("producer goroutine did not exit after cancel")
		}
	}
}

func TestStreamChunkerSurfacesReadError(t *testing.T) {
	boom := errors.New("boom")
	chunker := NewStreamChunker(io.MultiReader(strings.NewReader("ab"), errReader{boom}), 16)
	var got []byte
	for c := range chunker.Chunks(context.Background()) {
		got = append(got, c...)
	}
	if string(got) != "ab" || !errors.Is(chunker.Err(), boom) {
		t.Fatalf("got %q err %v", got, chunker.Err())
	}
}

type zeroReader struct{}

func (zeroReader) Read([]byte) (int, error) { return 0, nil }

func TestStreamChunkerNoProgress(t *testing.T) {
	chunker := NewStreamChunker(zeroReader{}, 16)
	for range chunker.Chunks(context.Background()) {
	}
	if !errors.Is(chunker.Err(), io.ErrNoProgress) {
		t.Fatalf("expected io.ErrNoProgress, got %v", chunker.Err())
	}
}

func TestStreamChunkerSingleUseAndNilReader(t *testing.T) {
	chunker := NewStreamChunker(strings.NewReader("abc"), 16)
	for range chunker.Chunks(context.Background()) {
	}
	second := 0
	for range chunker.Chunks(context.Background()) {
		second++
	}
	if second != 0 {
		t.Fatalf("second Chunks call produced %d chunks", second)
	}
	nilChunker := NewStreamChunker(nil, 16)
	for range nilChunker.Chunks(nil) { //nolint:staticcheck // nil ctx must be tolerated
	}
	if nilChunker.Err() == nil {
		t.Fatal("expected error for nil reader")
	}
}

// TestStreamResponseOverRealHTTP1Server is a regression test: over a real
// HTTP/1.1 connection net/http discards the unread request body once the
// response headers are flushed, so without full-duplex mode the relayed
// stream was silently empty.
func TestStreamResponseOverRealHTTP1Server(t *testing.T) {
	srv := httptest.NewServer(NewVideo3DStreamHandler())
	defer srv.Close()
	payload := strings.Repeat("0123456789", 10_000) // 100 KB, several chunks
	resp, err := http.Post(srv.URL, "application/octet-stream", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(got) != payload {
		t.Fatalf("status %d, relayed %d of %d bytes", resp.StatusCode, len(got), len(payload))
	}
	if tr := resp.Trailer.Get(StreamStatusTrailer); tr != "complete" {
		t.Fatalf("expected complete trailer, got %q", tr)
	}
}
