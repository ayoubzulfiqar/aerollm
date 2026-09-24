package providers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

var (
	_ Prober = (*OpenAIProvider)(nil)
	_ Prober = (*AnthropicProvider)(nil)
	_ Prober = (*LocalProvider)(nil)
)

func TestProbeEndpointClassification(t *testing.T) {
	var status atomic.Int32
	var gotMethod, gotKey atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod.Store(r.Method)
		gotKey.Store(r.Header.Get("Authorization"))
		w.WriteHeader(int(status.Load()))
		io.WriteString(w, `{"error":{"message":"nope"}}`)
	}))
	defer srv.Close()
	h := http.Header{"Authorization": {"Bearer k"}}
	for code, wantOK := range map[int]bool{200: true, 204: true, 404: true, 405: true, 429: true, 401: false, 403: false, 408: false, 500: false, 502: false} {
		status.Store(int32(code))
		err := ProbeEndpoint(context.Background(), nil, "p", srv.URL+"/v1/models", h)
		if (err == nil) != wantOK {
			t.Fatalf("status %d: err=%v", code, err)
		}
		if !wantOK && StatusCode(err) != code {
			t.Fatalf("status %d: want UpstreamError, got %v", code, err)
		}
	}
	if gotMethod.Load() != http.MethodGet || gotKey.Load() != "Bearer k" {
		t.Fatalf("probe request: %v %v", gotMethod.Load(), gotKey.Load())
	}

	// Bounded by the caller's context.
	hang := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	defer close(hang)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := ProbeEndpoint(ctx, nil, "p", slow.URL, nil)
	if err == nil || !IsRetryable(err) || time.Since(start) > 3*time.Second {
		t.Fatalf("probe must honour ctx deadline: %v after %v", err, time.Since(start))
	}
}

func TestHealthTrackerProbeSemantics(t *testing.T) {
	var h HealthTracker
	if !h.Snapshot("p", "t").Healthy {
		t.Fatal("healthy until proven otherwise")
	}
	h.ObserveProbe(time.Millisecond, errors.New("connection refused"))
	s := h.Snapshot("p", "t")
	if s.Healthy || s.ProbeError == "" || s.LastProbe == 0 {
		t.Fatalf("failed probe: %+v", s)
	}
	// A non-retryable call error (e.g. 401) must not mask a failed probe.
	h.Observe(time.Millisecond, &UpstreamError{StatusCode: 401})
	if h.Snapshot("p", "t").Healthy {
		t.Fatal("401 must not clear a failed probe")
	}
	h.Observe(time.Millisecond, nil)
	if s := h.Snapshot("p", "t"); !s.Healthy || s.ProbeError != "" {
		t.Fatalf("successful call clears the probe failure: %+v", s)
	}
	// Passive failures are reset by a successful probe.
	for i := 0; i < unhealthyAfter; i++ {
		h.Observe(time.Millisecond, &UpstreamError{StatusCode: 503})
	}
	if h.Snapshot("p", "t").Healthy {
		t.Fatal("passive failures")
	}
	h.ObserveProbe(time.Millisecond, nil)
	if !h.Snapshot("p", "t").Healthy {
		t.Fatal("successful probe must restore health")
	}
	// Cancelled probes say nothing.
	h.ObserveProbe(0, context.Canceled)
	if !h.Snapshot("p", "t").Healthy {
		t.Fatal("cancelled probe must be ignored")
	}
}

func TestLegacyProviderProbes(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var status atomic.Int32
	status.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path+"|"+r.Header.Get("x-api-key")+r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	ctx := context.Background()
	o := NewOpenAIProvider("o", "k", srv.URL)
	a := NewAnthropicProvider(srv.URL+"/v1", "ak", "claude")
	l := NewLocalProvider(srv.URL, "llama")
	for _, p := range []Prober{o, a, l} {
		if err := p.Probe(ctx); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"/v1/models|Bearer k", "/v1/models|ak", "/v1/models|"}
	mu.Lock()
	got := append([]string(nil), paths...)
	mu.Unlock()
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("probe %d: %s want %s", i, got[i], w)
		}
	}
	status.Store(401)
	if err := o.Probe(ctx); StatusCode(err) != 401 || o.Health().Healthy || o.Health().ProbeError == "" {
		t.Fatalf("openai: %v %+v", err, o.Health())
	}
	if err := a.Probe(ctx); StatusCode(err) != 401 || a.Health().Healthy {
		t.Fatalf("anthropic: %v", err)
	}
	status.Store(500)
	if err := l.Probe(ctx); StatusCode(err) != 500 || l.Health().Healthy {
		t.Fatalf("local: %v", err)
	}
	if u, err := AnthropicModelsURL(""); err != nil || u != "https://api.anthropic.com/v1/models" {
		t.Fatalf("default models URL: %s %v", u, err)
	}
	bad := NewOpenAIProvider("bad", "k", "ftp://x")
	if err := bad.Probe(ctx); StatusCode(err) != 500 {
		t.Fatalf("misconfigured base: %v", err)
	}
}

func TestStartStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Kind", "raw")
		io.WriteString(w, "abc")
	}))
	defer srv.Close()
	newReq := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		return r
	}
	var observed atomic.Int32
	observe := func(time.Duration, error) { observed.Add(1) }

	// Happy path: starter sees headers and part of the body, pump the rest.
	ch, err := StartStream(context.Background(), nil, "p", newReq(), observe, func(h http.Header, body io.Reader) (StreamPump, error) {
		if h.Get("X-Kind") != "raw" {
			return nil, errors.New("headers not passed")
		}
		first := make([]byte, 1)
		if _, err := io.ReadFull(body, first); err != nil {
			return nil, err
		}
		return func(emit func(models.StreamChunk) bool) error {
			rest, err := io.ReadAll(body)
			if err != nil {
				return err
			}
			emit(models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: string(first) + string(rest)}}}})
			return nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if len(chunks) != 1 || chunks[0].Choices[0].Delta.Content != "abc" || chunks[0].Err != nil {
		t.Fatalf("chunks: %+v", chunks)
	}

	// Starter error: returned directly (stream never started).
	startErr := &UpstreamError{Provider: "p", StatusCode: 429, Message: "slow down"}
	if ch, err := StartStream(context.Background(), nil, "p", newReq(), observe, func(http.Header, io.Reader) (StreamPump, error) {
		return nil, startErr
	}); ch != nil || err != startErr {
		t.Fatalf("starter error: %v %v", ch, err)
	}

	// Pump errors: typed errors pass through, others become transport errors.
	for _, perr := range []error{startErr, errors.New("read failed")} {
		ch, err := StartStream(context.Background(), nil, "p", newReq(), observe, func(http.Header, io.Reader) (StreamPump, error) {
			return func(emit func(models.StreamChunk) bool) error { return perr }, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		chunks := collect(t, ch)
		last := chunks[len(chunks)-1]
		var te *TransportError
		if perr == startErr && last.Err != startErr {
			t.Fatalf("typed pump error: %v", last.Err)
		}
		if perr != startErr && (!errors.As(last.Err, &te) || !strings.Contains(last.Err.Error(), "read failed")) {
			t.Fatalf("untyped pump error: %v", last.Err)
		}
	}
	if observed.Load() != 4 {
		t.Fatalf("observe must run once per stream, got %d", observed.Load())
	}

	// Upstream HTTP errors never reach the starter.
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer fail.Close()
	r, _ := http.NewRequest(http.MethodPost, fail.URL, nil)
	if _, err := StartStream(context.Background(), nil, "p", r, nil, func(http.Header, io.Reader) (StreamPump, error) {
		t.Fatal("starter must not run for non-2xx")
		return nil, nil
	}); StatusCode(err) != 503 {
		t.Fatalf("http error: %v", err)
	}
	if _, err := StartStream(context.Background(), nil, "p", r, nil, nil); err == nil {
		t.Fatal("nil starter")
	}
}

func TestStartStreamCancellationAndIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "x")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	reader := func(body io.Reader) StreamPump {
		return func(emit func(models.StreamChunk) bool) error {
			buf := make([]byte, 1)
			for {
				if _, err := body.Read(buf); err != nil {
					return err
				}
				if !emit(models.StreamChunk{Choices: []models.StreamChoice{{Delta: models.MessageDelta{Content: string(buf)}}}}) {
					return nil
				}
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	ch, err := StartStream(ctx, nil, "p", r, nil, func(_ http.Header, body io.Reader) (StreamPump, error) { return reader(body), nil })
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()
	select {
	case c, ok := <-ch:
		if ok && c.Err != nil {
			t.Fatalf("cancellation must not emit an error chunk: %v", c.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not stop after cancel")
	}

	old := StreamIdleTimeout
	StreamIdleTimeout = 100 * time.Millisecond
	defer func() { StreamIdleTimeout = old }()
	r, _ = http.NewRequest(http.MethodPost, srv.URL, nil)
	ch, err = StartStream(context.Background(), nil, "p", r, nil, func(_ http.Header, body io.Reader) (StreamPump, error) { return reader(body), nil })
	if err != nil {
		t.Fatal(err)
	}
	chunks := collect(t, ch)
	if last := chunks[len(chunks)-1]; StatusCode(last.Err) != http.StatusGatewayTimeout {
		t.Fatalf("idle timeout: %+v", chunks)
	}
}
