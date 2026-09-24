package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTraceMiddlewareInstrumentsRequest(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if p.RequestCount() != 1 {
		t.Fatalf("expected 1 request, got %d", p.RequestCount())
	}
	if p.ErrorCount() != 0 {
		t.Fatalf("expected 0 errors, got %d", p.ErrorCount())
	}
	if p.AvgLatency() < 0 {
		t.Fatalf("expected non-negative avg latency, got %f", p.AvgLatency())
	}
}

func TestTraceMiddlewareRecordsNonZeroLatency(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if p.AvgLatency() <= 0 {
		t.Fatalf("expected positive avg latency, got %f", p.AvgLatency())
	}
}

func TestSubMillisecondLatencyNotLost(t *testing.T) {
	p := NewProvider(Config{})
	_, s := p.StartSpan(context.Background(), "fast")
	p.End(context.Background(), s, 500*time.Microsecond, false)
	if got := p.AvgLatency(); got < 0.49 || got > 0.51 {
		t.Fatalf("expected 0.5ms average, got %v", got)
	}
}

func TestTraceMiddlewareRecordsErrors(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	handler := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if p.ErrorCount() != 1 {
		t.Fatalf("expected 1 error, got %d", p.ErrorCount())
	}
}

func TestAvgLatencyZeroWhenNoData(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	if p.AvgLatency() != 0 {
		t.Fatalf("expected 0 avg latency with no data, got %f", p.AvgLatency())
	}
}

func TestIDsAreUniqueAndW3CShaped(t *testing.T) {
	p := NewProvider(Config{})
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		_, s := p.StartSpan(context.Background(), "op")
		if len(s.TraceID) != 32 || len(s.SpanID) != 16 || !isLowerHex(s.TraceID) || !isLowerHex(s.SpanID) {
			t.Fatalf("bad ids %q %q", s.TraceID, s.SpanID)
		}
		if seen[s.TraceID] || seen[s.SpanID] {
			t.Fatalf("duplicate id (old ids were timestamps and collided)")
		}
		seen[s.TraceID], seen[s.SpanID] = true, true
	}
}

func TestChildSpanInheritsTrace(t *testing.T) {
	p := NewProvider(Config{})
	ctx, parent := p.StartSpan(context.Background(), "parent")
	_, child := p.StartSpan(ctx, "child")
	if child.TraceID != parent.TraceID || child.ParentID != parent.SpanID || child.SpanID == parent.SpanID {
		t.Fatalf("child %+v does not continue parent %+v", child, parent)
	}
	p.AddEvent(ctx, child, "cache-miss")
	p.SetAttributes(ctx, child, "model", "gpt-4", "dangling")
	p.End(ctx, child, time.Millisecond, false)
	p.End(ctx, child, time.Millisecond, false) // double End is ignored
	p.End(ctx, parent, 2*time.Millisecond, true)
	if p.RequestCount() != 2 || p.ErrorCount() != 1 {
		t.Fatalf("unexpected counts %d/%d", p.RequestCount(), p.ErrorCount())
	}
	spans := p.Trace(parent.TraceID)
	if len(spans) != 2 || spans[0].Name != "parent" || spans[1].Name != "child" {
		t.Fatalf("unexpected trace: %+v", spans)
	}
	c := spans[1]
	if c.Attributes["model"] != "gpt-4" || c.Attributes["dangling"] != "" || len(c.Events) != 1 || c.Events[0].Name != "cache-miss" {
		t.Fatalf("attributes/events not stored: %+v", c)
	}
	// Mutations after End are ignored.
	p.AddEvent(ctx, child, "late")
	p.SetAttributes(ctx, child, "late", "x")
	if got := p.Trace(parent.TraceID)[1]; len(got.Events) != 1 || got.Attributes["late"] != "" {
		t.Fatalf("span mutated after end: %+v", got)
	}
}

func TestSpanStorageIsBounded(t *testing.T) {
	p := NewProvider(Config{MaxSpans: 10})
	for i := 0; i < 25; i++ {
		_, s := p.StartSpan(context.Background(), fmt.Sprintf("op-%d", i))
		for j := 0; j < MaxEvents+10; j++ {
			p.AddEvent(nil, s, "e")
		}
		for j := 0; j < MaxAttributes+10; j++ {
			p.SetAttributes(nil, s, fmt.Sprintf("k%d", j), strings.Repeat("v", 1000))
		}
		p.End(nil, s, time.Millisecond, false)
	}
	spans := p.Spans()
	if len(spans) != 10 || spans[0].Name != "op-24" || spans[9].Name != "op-15" {
		t.Fatalf("ring buffer broken: %d spans, first=%s", len(spans), spans[0].Name)
	}
	if len(spans[0].Events) != MaxEvents || len(spans[0].Attributes) != MaxAttributes || len(spans[0].Attributes["k0"]) != maxAttrLen {
		t.Fatalf("per-span bounds not applied: events=%d attrs=%d", len(spans[0].Events), len(spans[0].Attributes))
	}
	off := NewProvider(Config{MaxSpans: -1})
	_, s := off.StartSpan(nil, "x")
	off.End(nil, s, time.Millisecond, false)
	if len(off.Spans()) != 0 || off.RequestCount() != 1 {
		t.Fatal("storage disabled must still count metrics")
	}
}

func TestSampling(t *testing.T) {
	p := NewProvider(Config{SampleRate: 0.25})
	for i := 0; i < 400; i++ {
		ctx, s := p.StartSpan(nil, "root")
		_, c := p.StartSpan(ctx, "child")
		p.End(ctx, c, time.Millisecond, false)
		p.End(ctx, s, time.Millisecond, false)
	}
	stored := len(p.Spans())
	if stored == 0 || stored == DefaultMaxSpans || stored%2 != 0 {
		t.Fatalf("expected ~25%% of traces stored whole (parent+child), got %d spans", stored)
	}
	if p.RequestCount() != 800 {
		t.Fatalf("metrics must count unsampled spans, got %d", p.RequestCount())
	}
}

func TestParseTraceparent(t *testing.T) {
	good := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	s, ok := ParseTraceparent(good)
	if !ok || s.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || s.SpanID != "00f067aa0ba902b7" {
		t.Fatalf("failed to parse %q", good)
	}
	for _, bad := range []string{
		"",
		"garbage",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
		"00-4bf92f3577b34da6a3ce929d0e0e47zz-00f067aa0ba902b7-01",
	} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("accepted invalid traceparent %q", bad)
		}
	}
}

func TestMiddlewarePropagatesTraceparentAndSetsHeadersEarly(t *testing.T) {
	p := NewProvider(Config{})
	var inner Span
	srv := httptest.NewServer(p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner, _ = SpanFromContext(r.Context())
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("body"))
	})))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/chat", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Over a real connection headers set after WriteHeader are lost; they
	// must be set before the handler runs.
	if resp.Header.Get("X-Trace-Id") != "4bf92f3577b34da6a3ce929d0e0e4736" || resp.Header.Get("X-Span-Id") == "" {
		t.Fatalf("trace headers missing on the wire: %v", resp.Header)
	}
	if inner.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || inner.ParentID != "00f067aa0ba902b7" {
		t.Fatalf("incoming traceparent not continued: %+v", inner)
	}
	spans := p.Spans()
	if len(spans) != 1 || spans[0].Attributes["http.status_code"] != "418" || spans[0].Name != "GET /v1/chat" {
		t.Fatalf("unexpected stored span: %+v", spans)
	}
}

func TestMiddlewareRecordsPanicAndSupportsFlush(t *testing.T) {
	p := NewProvider(Config{})
	h := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Error("wrapped writer must support Flush for streaming")
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("ResponseController.Flush: %v", err)
		}
		panic("boom")
	}))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic must propagate")
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	if p.ErrorCount() != 1 || p.RequestCount() != 1 {
		t.Fatalf("panic must be recorded as an error: %d/%d", p.ErrorCount(), p.RequestCount())
	}
}

func TestHandlers(t *testing.T) {
	p := NewProvider(Config{ServiceName: "svc"})
	_, s := p.StartSpan(nil, "op")
	p.End(nil, s, 4*time.Millisecond, true)

	rec := httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/trace/metrics", nil))
	var snap MetricsSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil || snap.Service != "svc" || snap.Requests != 1 || snap.ErrorRate != 1 || snap.SpansStored != 1 {
		t.Fatalf("metrics: %v %s", err, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	p.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/trace/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	p.SpansHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/trace/spans?trace_id="+s.TraceID, nil))
	var spans []SpanData
	if err := json.Unmarshal(rec.Body.Bytes(), &spans); err != nil || len(spans) != 1 || spans[0].SpanID != s.SpanID {
		t.Fatalf("spans: %v %s", err, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	p.SpansHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/trace/spans?trace_id=nope", nil))
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected empty list, got %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	p.SpansHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/trace/spans?limit=-1", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad limit, got %d", rec.Code)
	}
}

func TestConcurrentSpans(t *testing.T) {
	p := NewProvider(Config{MaxSpans: 50})
	h := p.TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		s, _ := SpanFromContext(ctx)
		p.AddEvent(ctx, s, "work")
		_, child := p.StartSpan(ctx, "db")
		p.End(ctx, child, time.Microsecond, false)
	}))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
				_ = p.Spans()
				_ = p.Snapshot()
			}
		}()
	}
	wg.Wait()
	if p.RequestCount() != 1600 || len(p.Spans()) != 50 {
		t.Fatalf("unexpected state: %d requests, %d spans", p.RequestCount(), len(p.Spans()))
	}
}
