package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type funcChecker struct {
	name string
	fn   func(ctx context.Context) Check
}

func (f *funcChecker) Name() string                    { return f.name }
func (f *funcChecker) Check(ctx context.Context) Check { return f.fn(ctx) }

func TestCheckLatencyMarshalsMilliseconds(t *testing.T) {
	b, err := json.Marshal(Check{Name: "db", Healthy: true, Latency: 1500 * time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"latency_ms":1.5`) {
		t.Fatalf("expected latency in ms, got %s", b)
	}
	if strings.Contains(string(b), `"error"`) {
		t.Fatalf("empty error must be omitted: %s", b)
	}
	var back Check
	if err := json.Unmarshal(b, &back); err != nil || back.Latency != 1500*time.Microsecond {
		t.Fatalf("round trip failed: %+v %v", back, err)
	}
}

func TestChecksTimeoutPanicAndOrder(t *testing.T) {
	reg := NewRegistry()
	reg.SetTimeout(30 * time.Millisecond)
	block := make(chan struct{})
	defer close(block)
	reg.Register(&funcChecker{name: "c-hang", fn: func(ctx context.Context) Check {
		<-block // ignores ctx entirely
		return Check{Healthy: true}
	}})
	reg.Register(&funcChecker{name: "b-panic", fn: func(ctx context.Context) Check { panic("boom") }})
	reg.Register(&funcChecker{name: "a-ok", fn: func(ctx context.Context) Check { return Check{Healthy: true} }})

	start := time.Now()
	checks := reg.Checks(context.Background())
	if time.Since(start) > time.Second {
		t.Fatal("Checks must be bounded by the per-check timeout")
	}
	if len(checks) != 3 || checks[0].Name != "a-ok" || checks[1].Name != "b-panic" || checks[2].Name != "c-hang" {
		t.Fatalf("unexpected order/results: %+v", checks)
	}
	if !checks[0].Healthy || checks[1].Healthy || checks[2].Healthy {
		t.Fatalf("unexpected health: %+v", checks)
	}
	if !strings.Contains(checks[2].Error, "timed out") || !strings.Contains(checks[1].Error, "panic") {
		t.Fatalf("unexpected errors: %+v", checks)
	}
	if checks[0].CheckedAt.IsZero() {
		t.Fatal("CheckedAt must be filled in")
	}
}

func TestChecksRunConcurrently(t *testing.T) {
	// Each checker only succeeds once all four are running at the same time,
	// which is impossible if Checks runs them sequentially.
	reg := NewRegistry()
	reg.SetTimeout(5 * time.Second)
	var mu sync.Mutex
	started := 0
	all := make(chan struct{})
	for _, n := range []string{"a", "b", "c", "d"} {
		reg.Register(&funcChecker{name: n, fn: func(ctx context.Context) Check {
			mu.Lock()
			started++
			if started == 4 {
				close(all)
			}
			mu.Unlock()
			select {
			case <-all:
				return Check{Healthy: true}
			case <-ctx.Done():
				return Check{Healthy: false, Error: "not concurrent"}
			}
		}})
	}
	for _, c := range reg.Checks(context.Background()) {
		if !c.Healthy {
			t.Fatalf("checks did not run concurrently: %+v", c)
		}
	}
}

func TestNilRegistryAndEmptyReadiness(t *testing.T) {
	var reg *Registry
	reg.Register(&fakeChecker{name: "x"})
	reg.Unregister("x")
	reg.SetTimeout(time.Second)
	if got := reg.Checks(context.Background()); got != nil {
		t.Fatalf("nil registry must return nil, got %v", got)
	}
	out, code := ReadinessResponse(nil)
	if code != http.StatusOK || !strings.Contains(string(out), `"checks":[]`) {
		t.Fatalf("no checks means ready, got %d %s", code, out)
	}
}

func TestServeHTTPMethodsAndHead(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&fakeChecker{name: "ok", healthy: true})

	rec := httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("expected 405 with Allow, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/readyz", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected HEAD response: %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	reg := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				reg.Register(&fakeChecker{name: string(rune('a' + i)), healthy: true})
				reg.Unregister(string(rune('a' + (i+1)%8)))
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = reg.Checks(context.Background())
			}
		}()
	}
	wg.Wait()
}
