package aiops

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetaAgentTunerEvaluatesAndApplies(t *testing.T) {
	var applied atomic.Bool
	source := &DefaultMetricsSource{
		requestsFn: func() int64 { return 100 },
		errorsFn:   func() int64 { return 50 },
		latencyFn:  func() float64 { return 500 },
	}
	tuner := NewMetaAgentTuner(source, 10*time.Millisecond, 0)
	tuner.RegisterAction(TunerAction{
		Name: "test-action",
		Apply: func(ctx context.Context) error {
			applied.Store(true)
			return nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	tuner.Run(ctx)
	if !applied.Load() {
		t.Fatalf("expected tuner to apply action under degraded metrics")
	}
}

// fakeSource is a mutable MetricsSource for deterministic tests.
type fakeSource struct {
	mu   sync.Mutex
	snap MetricsSnapshot
}

func (f *fakeSource) set(req, errs int64, latency float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap = MetricsSnapshot{RequestsTotal: req, ErrorsTotal: errs, AvgLatencyMs: latency, P99LatencyMs: latency}
}

func (f *fakeSource) setSnap(s MetricsSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap = s
}

func (f *fakeSource) Snapshot() MetricsSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

type counterAction struct {
	applies, reverts atomic.Int64
}

func (c *counterAction) action(name string) TunerAction {
	return TunerAction{
		Name:   name,
		Apply:  func(context.Context) error { c.applies.Add(1); return nil },
		Revert: func(context.Context) error { c.reverts.Add(1); return nil },
	}
}

func newTestTuner(src MetricsSource) *MetaAgentTuner {
	tn := NewMetaAgentTuner(src, time.Hour, time.Hour)
	tn.cooldown = time.Nanosecond // effectively no cooldown for unit tests
	return tn
}

func TestTunerHealthyTrafficDoesNotApply(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	var a counterAction
	tn.RegisterAction(a.action("a"))

	// Normal traffic: 1% errors, 200ms latency — both below default thresholds.
	for i := int64(1); i <= 10; i++ {
		src.set(i*1000, i*10, 200)
		tn.evaluate(context.Background())
	}
	if got := a.applies.Load(); got != 0 {
		t.Fatalf("healthy traffic must not trigger actions, got %d applies", got)
	}
	if st := tn.Stats(); st.Degraded || st.Evaluations != 10 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestTunerLatencyThresholdApplies(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	var a counterAction
	tn.RegisterAction(a.action("a"))
	src.set(0, 0, 5000)
	tn.evaluate(context.Background())
	if a.applies.Load() != 1 {
		t.Fatalf("expected apply on latency breach")
	}
}

func TestWindowErrorRate(t *testing.T) {
	// First evaluation: cumulative.
	if got := windowErrorRate(nil, MetricsSnapshot{RequestsTotal: 100, ErrorsTotal: 10}); got != 0.1 {
		t.Fatalf("expected cumulative 0.1, got %v", got)
	}
	prev := &MetricsSnapshot{RequestsTotal: 1000, ErrorsTotal: 900}
	// Window: 100 new requests, 1 new error → 0.01 even though cumulative is ~0.82.
	if got := windowErrorRate(prev, MetricsSnapshot{RequestsTotal: 1100, ErrorsTotal: 901}); math.Abs(got-0.01) > 1e-9 {
		t.Fatalf("expected windowed 0.01, got %v", got)
	}
	// No traffic in window.
	if got := windowErrorRate(prev, MetricsSnapshot{RequestsTotal: 1000, ErrorsTotal: 900}); got != 0 {
		t.Fatalf("expected 0 with no traffic, got %v", got)
	}
	// Counter reset → fresh cumulative.
	if got := windowErrorRate(prev, MetricsSnapshot{RequestsTotal: 10, ErrorsTotal: 5}); got != 0.5 {
		t.Fatalf("expected reset to use cumulative 0.5, got %v", got)
	}
	// Source without counters → trust ErrorRate.
	if got := windowErrorRate(&MetricsSnapshot{}, MetricsSnapshot{ErrorRate: 0.3}); got != 0.3 {
		t.Fatalf("expected source rate 0.3, got %v", got)
	}
}

func TestTunerWindowedRateIgnoresOldErrors(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	if err := tn.SetThresholds(10000, 0.05); err != nil {
		t.Fatal(err)
	}
	var a counterAction
	tn.RegisterAction(a.action("a"))
	tn.RegisterAction(a.action("b"))

	// Startup burst of errors → degraded on the first (cumulative) evaluation.
	src.set(100, 90, 10)
	tn.evaluate(context.Background())
	if a.applies.Load() != 1 {
		t.Fatalf("expected one apply, got %d", a.applies.Load())
	}
	// Later windows are clean; cumulative rate is still > 5% but the windowed
	// rate is 0, so no further escalation must happen.
	for i := int64(1); i <= 5; i++ {
		src.set(100+i*1000, 90, 10)
		tn.evaluate(context.Background())
	}
	if a.applies.Load() != 1 {
		t.Fatalf("windowed rate should prevent escalation, got %d applies", a.applies.Load())
	}
}

func TestTunerEscalatesAndRevertsLIFO(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	var order []string
	var mu sync.Mutex
	mk := func(name string) TunerAction {
		return TunerAction{
			Name:   name,
			Apply:  func(context.Context) error { mu.Lock(); order = append(order, "apply:"+name); mu.Unlock(); return nil },
			Revert: func(context.Context) error { mu.Lock(); order = append(order, "revert:"+name); mu.Unlock(); return nil },
		}
	}
	tn.RegisterAction(mk("a"))
	tn.RegisterAction(mk("b"))

	src.set(0, 0, 9000)
	tn.evaluate(context.Background())
	tn.evaluate(context.Background())
	tn.evaluate(context.Background()) // ladder exhausted: no third apply
	if st := tn.Stats(); st.Applied != 2 || len(st.ActiveActions) != 2 {
		t.Fatalf("expected 2 active actions, got %+v", st)
	}

	src.set(0, 0, 10)
	for i := 0; i < DefaultRecoveryEvaluations*2; i++ {
		tn.evaluate(context.Background())
	}
	want := []string{"apply:a", "apply:b", "revert:b", "revert:a"}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != len(want) {
		t.Fatalf("unexpected order: %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("unexpected order: %v", order)
		}
	}
	if st := tn.Stats(); st.Reverted != 2 || len(st.ActiveActions) != 0 {
		t.Fatalf("unexpected stats after recovery: %+v", st)
	}
}

func TestTunerCooldownRespected(t *testing.T) {
	src := &fakeSource{}
	tn := NewMetaAgentTuner(src, time.Hour, time.Hour)
	var a counterAction
	tn.RegisterAction(a.action("a"))
	tn.RegisterAction(a.action("b"))
	src.set(0, 0, 9000)
	for i := 0; i < 5; i++ {
		tn.evaluate(context.Background())
	}
	if a.applies.Load() != 1 {
		t.Fatalf("cooldown must limit to one apply, got %d", a.applies.Load())
	}
}

func TestTunerRecordsApplyErrorsAndPanics(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	tn.RegisterAction(TunerAction{Name: "bad", Apply: func(context.Context) error { return errors.New("boom") }})
	src.set(0, 0, 9000)
	tn.evaluate(context.Background())
	st := tn.Stats()
	if st.ApplyFailures != 1 || st.LastError == "" || len(st.ActiveActions) != 0 {
		t.Fatalf("expected recorded failure, got %+v", st)
	}

	tn2 := newTestTuner(src)
	tn2.RegisterAction(TunerAction{Name: "panic", Apply: func(context.Context) error { panic("kaboom") }})
	tn2.evaluate(context.Background()) // must not crash
	if tn2.Stats().ApplyFailures != 1 {
		t.Fatalf("expected panic to be converted to a failure")
	}
}

func TestTunerSkipsInvalidMetrics(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	var a counterAction
	tn.RegisterAction(a.action("a"))
	for _, s := range []MetricsSnapshot{
		{AvgLatencyMs: math.NaN()},
		{P99LatencyMs: math.Inf(1)},
		{ErrorRate: -1},
		{RequestsTotal: -5},
	} {
		src.setSnap(s)
		tn.evaluate(context.Background())
	}
	if a.applies.Load() != 0 {
		t.Fatalf("invalid metrics must not trigger actions")
	}
	if st := tn.Stats(); st.InvalidMetrics != 4 {
		t.Fatalf("expected 4 invalid snapshots, got %+v", st)
	}
}

func TestTunerConfigValidation(t *testing.T) {
	tn := NewMetaAgentTuner(&fakeSource{}, 0, 0)
	for _, c := range []struct{ lat, er float64 }{
		{0, 0.1}, {-1, 0.1}, {math.NaN(), 0.1}, {math.Inf(1), 0.1}, {100, 0}, {100, 1.5}, {100, math.NaN()},
	} {
		if err := tn.SetThresholds(c.lat, c.er); err == nil {
			t.Fatalf("expected rejection for %+v", c)
		}
	}
	if err := tn.SetThresholds(100, 0.2); err != nil {
		t.Fatalf("valid thresholds rejected: %v", err)
	}
	if cfg := tn.Config(); cfg.LatencyThresholdMs != 100 || cfg.ErrorRateThreshold != 0.2 {
		t.Fatalf("thresholds not stored: %+v", cfg)
	}
	if err := tn.SetConfig(TunerConfig{LatencyThresholdMs: 1, ErrorRateThreshold: 0.1, RecoveryEvaluations: 0}); err == nil {
		t.Fatal("expected rejection for zero recovery evaluations")
	}
}

func TestTunerNilSafety(t *testing.T) {
	var tn *MetaAgentTuner
	tn.RegisterAction(TunerAction{})
	tn.evaluate(context.Background())
	tn.Run(context.Background())
	_ = tn.Stats()
	if err := tn.SetThresholds(1, 0.1); err == nil {
		t.Fatal("expected error for nil tuner")
	}

	noSrc := NewMetaAgentTuner(nil, time.Millisecond, time.Millisecond)
	noSrc.evaluate(context.Background()) // nil source: no panic

	var s *DefaultMetricsSource
	_ = s.Snapshot()

	lit := &MetaAgentTuner{source: &fakeSource{}} // zero interval must not panic
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lit.Run(ctx)
}

func TestTunerConcurrentRegisterAndRun(t *testing.T) {
	src := &fakeSource{}
	src.set(0, 0, 9000)
	tn := NewMetaAgentTuner(src, time.Millisecond, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tn.Run(ctx)
		close(done)
	}()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				tn.RegisterAction(TunerAction{Name: "x", Apply: func(context.Context) error { return nil }})
				_ = tn.Stats()
			}
		}()
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestDefaultMetricsSourceSnapshot(t *testing.T) {
	s := NewDefaultMetricsSource(func() int64 { return 200 }, func() int64 { return 20 }, func() float64 { return 123 })
	snap := s.Snapshot()
	if snap.AvgLatencyMs != 123 || snap.P99LatencyMs != 123 || snap.ErrorRate != 0.1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	if snap.RequestsTotal != 200 || snap.ErrorsTotal != 20 {
		t.Fatalf("unexpected counters: %+v", snap)
	}
}
