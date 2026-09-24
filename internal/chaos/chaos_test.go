package chaos

import (
	"context"
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

func TestShouldFaultRespectsPercent(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 0, Enabled: true})
	if inj.ShouldFault() {
		t.Fatalf("expected no fault at 0%%")
	}
}

func TestDisabledByDefault(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100})
	if inj.Enabled() || inj.ShouldFault() {
		t.Fatal("injection must be disabled unless explicitly enabled")
	}
	rec := httptest.NewRecorder()
	if err := inj.Apply(rec, httptest.NewRequest(http.MethodGet, "/", nil)); err != nil || rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("disabled Apply must be a no-op: %v %d", err, rec.Code)
	}
	if err := inj.Configure(Config{Type: FaultError, Percent: 50}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
	// Update from code cannot flip the master switch either.
	inj.Update(Config{Type: FaultError, Percent: 100, Enabled: true})
	if inj.Enabled() {
		t.Fatal("Update must not enable injection")
	}
}

func TestShouldFaultAt100Percent(t *testing.T) {
	// The old byte-based sampler faulted only 255/256 of the time at 100%.
	inj := NewInjector(Config{Type: FaultError, Percent: 100, Enabled: true})
	for i := 0; i < 5000; i++ {
		if !inj.ShouldFault() {
			t.Fatal("100% must always fault")
		}
	}
	half := NewInjector(Config{Type: FaultError, Percent: 50, Enabled: true})
	n := 0
	for i := 0; i < 20000; i++ {
		if half.ShouldFault() {
			n++
		}
	}
	if n < 9000 || n > 11000 {
		t.Fatalf("expected ~50%% faults, got %d/20000", n)
	}
}

func TestApplyErrorWritesJSON(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100, StatusCode: http.StatusBadGateway, Message: "boom", Enabled: true})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	err := inj.Apply(w, r)
	if !errors.Is(err, ErrInjected) {
		t.Fatalf("expected ErrInjected, got %v", err)
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", w.Code)
	}
	if w.Body.String() != `{"error":"boom"}` {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestApplyErrorMessageIsEscaped(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100, Message: `x"}<script>`, Enabled: true})
	w := httptest.NewRecorder()
	_ = inj.Apply(w, httptest.NewRequest(http.MethodGet, "/", nil))
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] != `x"}<script>` {
		t.Fatalf("message must be JSON-escaped: %v %s", err, w.Body.String())
	}
}

func TestApplyLatencyHonorsContext(t *testing.T) {
	inj := NewInjector(Config{Type: FaultLatency, Percent: 100, Duration: MaxLatency, Enabled: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := inj.Apply(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("latency must abort on cancellation: %v after %s", err, time.Since(start))
	}
	short := NewInjector(Config{Type: FaultLatency, Percent: 100, Duration: 5 * time.Millisecond, Enabled: true})
	start = time.Now()
	if err := short.Apply(httptest.NewRecorder(), nil); err != nil || time.Since(start) < 5*time.Millisecond {
		t.Fatalf("expected ~5ms delay, got %v after %s", err, time.Since(start))
	}
}

func TestConstructorClampsValues(t *testing.T) {
	cfg := NewInjector(Config{Type: FaultLatency, Percent: math.NaN(), Duration: time.Hour, StatusCode: 99999, Enabled: true}).Config()
	if cfg.Percent != 0 || cfg.Duration != MaxLatency || cfg.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected clamped config: %+v", cfg)
	}
	if cfg := NewInjector(Config{Type: "meteor", Percent: 500}).Config(); cfg.Type != FaultNone || cfg.Percent != 100 {
		t.Fatalf("unknown type must become none: %+v", cfg)
	}
}

func TestPanicRequiresPermission(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100, Enabled: true})
	if err := inj.Configure(Config{Type: FaultPanic, Percent: 100}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("panic must be rejected without AllowPanic, got %v", err)
	}
	inj.Update(Config{Type: FaultPanic, Percent: 100})
	if inj.Config().Type == FaultPanic {
		t.Fatal("Update must not install a panic fault without AllowPanic")
	}

	allowed := NewInjector(Config{Enabled: true, AllowPanic: true})
	if err := allowed.Configure(Config{Type: FaultPanic, Percent: 100, Message: "kaboom"}); err != nil {
		t.Fatal(err)
	}
	h := RecoverPanic(allowed.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next must not run after an injected panic")
	})))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "kaboom") {
		t.Fatalf("expected recovered 500, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestRecoverPanicDoesNotLeakDetails(t *testing.T) {
	h := RecoverPanic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(`secret db password "hunter2"`)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("panic details leaked: %s", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler {
			t.Fatalf("ErrAbortHandler must be re-raised, got %v", rec)
		}
	}()
	RecoverPanic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestMiddlewareInjectsErrors(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 100, StatusCode: 503, Enabled: true})
	called := false
	h := inj.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 503 || called {
		t.Fatalf("expected short-circuit 503, got %d called=%v", rec.Code, called)
	}
	inj.SetEnabled(false)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 200 || !called {
		t.Fatalf("disabled injector must pass through, got %d", rec.Code)
	}
}

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chaos/fault", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandlerUpdatesInjector(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 0, Enabled: true})
	h := Handler(inj)

	rec := post(h, `{"type":"error","percent":100}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if !inj.ShouldFault() {
		t.Fatalf("expected injector to update")
	}
}

func TestHandlerRejectsWhenDisabled(t *testing.T) {
	inj := NewInjector(Config{})
	rec := post(Handler(inj), `{"type":"error","percent":100,"enabled":true}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d %s", rec.Code, rec.Body.String())
	}
	if inj.Enabled() || inj.ShouldFault() {
		t.Fatal("request body must not be able to enable injection")
	}
}

func TestHandlerValidation(t *testing.T) {
	h := Handler(NewInjector(Config{Enabled: true}))
	bad := []string{
		`{"type":"panic","percent":100}`,
		`{"type":"latency","percent":100,"duration":"1h"}`,
		`{"type":"latency","percent":100,"duration":-5}`,
		`{"type":"latency","percent":100,"duration":"soon"}`,
		`{"type":"error","percent":101}`,
		`{"type":"error","percent":-1}`,
		`{"type":"error","percent":100,"status_code":200}`,
		`{"type":"error","percent":100,"status_code":1000}`,
		`{"type":"nuke","percent":1}`,
		`{"type":"error","message":"` + strings.Repeat("m", MaxMessageLength+1) + `"}`,
		`not json`,
		``,
	}
	for _, body := range bad {
		if rec := post(h, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%.60s: expected 400, got %d", body, rec.Code)
		}
	}
	rec := post(h, `{"type":"latency","percent":25,"duration":"250ms"}`)
	var st StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || rec.Code != http.StatusAccepted || st.Duration != "250ms" || st.Percent != 25 || !st.Enabled {
		t.Fatalf("valid latency config: %d %s", rec.Code, rec.Body.String())
	}
	if rec := post(h, `{"type":"latency","percent":25,"duration":250000000}`); rec.Code != http.StatusAccepted {
		t.Fatalf("legacy nanosecond duration: %d", rec.Code)
	}
	if rec := post(h, `{"type":"error","percent":5,"statuscode":503}`); rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"status_code":503`) {
		t.Fatalf("legacy statuscode field: %d %s", rec.Code, rec.Body.String())
	}
	big := `{"type":"error","message":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	if rec := post(h, big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestHandlerGetDeleteAndMethods(t *testing.T) {
	inj := NewInjector(Config{Type: FaultError, Percent: 30, Enabled: true})
	h := Handler(inj)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/chaos/fault", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"percent":30`) {
		t.Fatalf("GET: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/chaos/fault", nil))
	if rec.Code != http.StatusOK || inj.ShouldFault() {
		t.Fatalf("DELETE must clear the fault: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v1/chaos/fault", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("expected 405 with Allow, got %d", rec.Code)
	}
}

func TestConcurrentConfigureAndApply(t *testing.T) {
	inj := NewInjector(Config{Enabled: true})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = inj.Configure(Config{Type: FaultError, Percent: float64(j % 100)})
				if inj.ShouldFault() {
					_ = inj.Apply(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
				}
				_ = inj.Config()
			}
		}(i)
	}
	wg.Wait()
}
