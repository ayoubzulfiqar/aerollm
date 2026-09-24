package slo

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestErrorBudgetConsume(t *testing.T) {
	b := NewErrorBudget(2)
	if b.Remaining() != 2 {
		t.Fatalf("expected 2, got %f", b.Remaining())
	}
	b.Consume(1)
	if b.Remaining() != 1 {
		t.Fatalf("expected 1, got %f", b.Remaining())
	}
	b.Consume(2)
	if b.Remaining() != 0 {
		t.Fatalf("expected 0, got %f", b.Remaining())
	}
}

func TestErrorBudgetRejectsBadNumbers(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -3} {
		if got := NewErrorBudget(v).Remaining(); got != 0 {
			t.Errorf("NewErrorBudget(%v) remaining = %v, want 0", v, got)
		}
	}
	b := NewErrorBudget(10)
	b.Consume(math.NaN())
	b.Consume(-5)
	if b.Remaining() != 10 {
		t.Fatalf("NaN/negative consumption must be ignored, got %v", b.Remaining())
	}
	b.Consume(math.Inf(1))
	if b.Remaining() != 0 {
		t.Fatalf("infinite consumption must exhaust, got %v", b.Remaining())
	}
	if NewErrorBudget(0).Snapshot().ConsumedFraction != 1 {
		t.Fatal("zero budget must report fully consumed without dividing by zero")
	}
}

func TestErrorBudgetTryConsumeAndReset(t *testing.T) {
	b := NewErrorBudget(2)
	if !b.TryConsume(2) || b.TryConsume(1) {
		t.Fatal("TryConsume must not overdraw")
	}
	b.Reset()
	if b.Remaining() != 2 {
		t.Fatal("Reset must restore budget")
	}
}

func TestWindowedBudgetReplenishes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	b := NewWindowedErrorBudget(3, time.Hour)
	b.now = func() time.Time { return now }
	b.windowStart = now
	b.Consume(3)
	if b.Remaining() != 0 {
		t.Fatal("expected exhausted budget")
	}
	now = now.Add(59 * time.Minute)
	if b.Remaining() != 0 {
		t.Fatal("budget must not replenish mid-window")
	}
	now = now.Add(2*time.Hour + time.Minute)
	if b.Remaining() != 3 {
		t.Fatalf("budget must replenish in a new window, got %v", b.Remaining())
	}
}

func TestErrorBudgetConcurrent(t *testing.T) {
	b := NewErrorBudget(1000)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Consume(1)
				_ = b.Remaining()
				_ = b.Snapshot()
			}
		}()
	}
	wg.Wait()
	if b.Remaining() != 0 {
		t.Fatalf("expected exactly 1000 consumed, remaining %v", b.Remaining())
	}
}

func TestHandlerBudgetExhausted(t *testing.T) {
	b := NewErrorBudget(0)
	h := Handler(b, "latency")

	req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
	req.Header.Set("x-slo-target", "latency")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}

func TestHandlerBudgetAvailable(t *testing.T) {
	b := NewErrorBudget(10)
	h := Handler(b, "latency")

	req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["target"] != "latency" || body["remaining"] != 10.0 || body["allowed"] != 10.0 {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestHandlerEscapesTargetHeader(t *testing.T) {
	for _, budget := range []float64{0, 5} {
		h := Handler(NewErrorBudget(budget), "latency")
		req := httptest.NewRequest(http.MethodGet, "/v1/slo/budget", nil)
		req.Header.Set("x-slo-target", `x","remaining":999,"pwn":"`+strings.Repeat("a", 500))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var body map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("response must stay valid JSON: %v %s", err, rec.Body.String())
		}
		if _, injected := body["pwn"]; injected || len(body["target"].(string)) > maxTargetLength {
			t.Fatalf("header injection: %v", body)
		}
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(NewErrorBudget(1), "x").ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/slo/budget", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("expected 405 with Allow, got %d %q", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestMiddlewareBlocksWhenBudgetExhausted(t *testing.T) {
	b := NewErrorBudget(0)
	mw := Middleware(b)

	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := mw(mux)

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
}

func TestMiddlewareConsumesOnlyOnServerErrors(t *testing.T) {
	b := NewErrorBudget(2)
	status := http.StatusOK
	h := Middleware(b)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status == 0 {
			panic("boom")
		}
		w.WriteHeader(status)
	}))
	serve := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}
	// Previously every request consumed budget, so healthy traffic exhausted
	// it after N requests and the middleware then rejected everything.
	for i := 0; i < 50; i++ {
		if serve() != http.StatusOK {
			t.Fatal("healthy traffic must not exhaust the budget")
		}
	}
	status = http.StatusNotFound
	serve()
	if b.Remaining() != 2 {
		t.Fatal("4xx must not consume budget")
	}
	status = http.StatusBadGateway
	serve()
	if b.Remaining() != 1 {
		t.Fatalf("5xx must consume budget, remaining %v", b.Remaining())
	}
	status = 0
	func() {
		defer func() { _ = recover() }()
		serve()
	}()
	if b.Remaining() != 0 {
		t.Fatalf("panic must consume budget, remaining %v", b.Remaining())
	}
	status = http.StatusOK
	if serve() != http.StatusTooManyRequests {
		t.Fatal("exhausted budget must reject")
	}
}

func TestParseWindow(t *testing.T) {
	cases := []struct {
		in   Window
		want time.Duration
		ok   bool
	}{
		{Window5Min, 5 * time.Minute, true},
		{Window1Hour, time.Hour, true},
		{Window24H, 24 * time.Hour, true},
		{"custom:90m", 90 * time.Minute, true},
		{"7d", 7 * 24 * time.Hour, true},
		{"custom:30d", 30 * 24 * time.Hour, true},
		{"custom:", 0, false},
		{"-5m", 0, false},
		{"0s", 0, false},
		{"1000d", 0, false},
		{"NaNd", 0, false},
		{"weekly", 0, false},
	}
	for _, tc := range cases {
		got, ok := ParseWindow(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("ParseWindow(%q) = %v,%v want %v,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestNewTrackerValidation(t *testing.T) {
	for _, obj := range []float64{0, 1, 1.5, -0.1, math.NaN()} {
		if _, err := NewTracker(obj, time.Hour); !errors.Is(err, ErrInvalidSLO) {
			t.Errorf("objective %v: expected ErrInvalidSLO, got %v", obj, err)
		}
	}
	for _, win := range []time.Duration{0, -time.Hour, time.Millisecond, MaxWindow + time.Hour} {
		if _, err := NewTracker(0.99, win); !errors.Is(err, ErrInvalidSLO) {
			t.Errorf("window %v: expected ErrInvalidSLO, got %v", win, err)
		}
	}
}

func newTestTracker(t *testing.T, obj float64, window time.Duration) (*Tracker, *time.Time) {
	t.Helper()
	tr, err := NewTracker(obj, window)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	tr.now = func() time.Time { return now }
	return tr, &now
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestTrackerMath(t *testing.T) {
	tr, _ := newTestTracker(t, 0.99, time.Hour)
	s := tr.Snapshot()
	if s.Total != 0 || s.ErrorRate != 0 || s.BurnRate != 0 || s.SLI != 1 || s.BudgetRemaining != 1 {
		t.Fatalf("empty tracker must be healthy without NaN: %+v", s)
	}
	for i := 0; i < 990; i++ {
		tr.Record(true)
	}
	for i := 0; i < 10; i++ {
		tr.Record(false)
	}
	s = tr.Snapshot()
	if s.Total != 1000 || s.Errors != 10 || !approx(s.ErrorRate, 0.01) || !approx(s.BurnRate, 1) || !approx(s.BudgetRemaining, 0) || !approx(s.AllowedErrors, 10) {
		t.Fatalf("unexpected snapshot at exactly-on-budget: %+v", s)
	}
	for i := 0; i < 20; i++ {
		tr.Record(false)
	}
	s = tr.Snapshot()
	if s.BurnRate <= 1 || s.BudgetRemaining >= 0 {
		t.Fatalf("overspent budget must be negative: %+v", s)
	}
	if _, err := json.Marshal(s); err != nil {
		t.Fatalf("snapshot must be JSON encodable: %v", err)
	}
}

func TestTrackerSlidingWindowAndBurnRates(t *testing.T) {
	tr, now := newTestTracker(t, 0.9, time.Hour)
	// Old errors, 50 minutes ago.
	*now = now.Add(-50 * time.Minute)
	for i := 0; i < 10; i++ {
		tr.Record(false)
	}
	*now = now.Add(50 * time.Minute)
	for i := 0; i < 10; i++ {
		tr.Record(true)
	}
	if br := tr.BurnRate(5 * time.Minute); br != 0 {
		t.Fatalf("recent window has no errors, burn %v", br)
	}
	if br := tr.BurnRate(time.Hour); !approx(br, 5) { // 50% errors / 10% allowed
		t.Fatalf("expected burn 5 over 1h, got %v", br)
	}
	if tr.ShouldAlert(5*time.Minute, time.Hour, 2) {
		t.Fatal("multi-window alert must require both windows to burn")
	}
	for i := 0; i < 4; i++ { // 4/14 errors over 5m => burn ~2.9
		tr.Record(false)
	}
	if !tr.ShouldAlert(5*time.Minute, time.Hour, 2) {
		t.Fatal("expected alert when both windows burn fast")
	}
	// Everything ages out of the window.
	*now = now.Add(2 * time.Hour)
	if s := tr.Snapshot(); s.Total != 0 {
		t.Fatalf("events must expire from the window: %+v", s)
	}
	// Events outside the window or in the future are ignored.
	tr.RecordAt(now.Add(-2*time.Hour), false)
	tr.RecordAt(now.Add(time.Hour), false)
	tr.RecordAt(time.Unix(-100, 0), false)
	if s := tr.Snapshot(); s.Total != 0 {
		t.Fatalf("out-of-window events must be ignored: %+v", s)
	}
}

func TestTrackerMiddlewareAndHandler(t *testing.T) {
	tr, err := NewTracker(0.5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code := http.StatusOK
	h := tr.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
	}))
	for _, c := range []int{200, 500, 503, 404} {
		code = c
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	s := tr.Snapshot()
	if s.Total != 4 || s.Errors != 2 {
		t.Fatalf("unexpected counts: %+v", s)
	}
	rec := httptest.NewRecorder()
	tr.Handler("api").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"burn_rate_5m"`) {
		t.Fatalf("handler: %d %s", rec.Code, rec.Body.String())
	}
}

func TestTrackerConcurrent(t *testing.T) {
	tr, err := NewTracker(0.999, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				tr.Record(j%10 != 0)
				_ = tr.BurnRate(5 * time.Second)
			}
		}(i)
	}
	wg.Wait()
	if s := tr.Snapshot(); s.Total != 4000 || s.Errors != 400 {
		t.Fatalf("lost updates: %+v", s)
	}
}
