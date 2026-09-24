package aiops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// feeder pushes snapshots with cumulative counters into a tuner.
type feeder struct {
	t         *MetaAgentTuner
	clk       *fakeClock
	req, errs int64
	providers map[string]ProviderStats
	step      time.Duration
}

func newFeeder(t *testing.T) *feeder {
	t.Helper()
	clk := newFakeClock()
	tn := NewMetaAgentTuner(nil, time.Hour, time.Hour)
	tn.SetClock(clk.Now)
	return &feeder{t: tn, clk: clk, step: 10 * time.Second}
}

// observe advances the clock by f.step, adds reqs/errs to the global
// counters and evaluates a snapshot with the given p95 latency.
func (f *feeder) observe(p95 float64, reqs, errs int64) {
	f.clk.Advance(f.step)
	f.req += reqs
	f.errs += errs
	f.t.Observe(context.Background(), MetricsSnapshot{
		Timestamp:     f.clk.Now(),
		P95LatencyMs:  p95,
		AvgLatencyMs:  p95 / 2,
		P99LatencyMs:  p95 / 2,
		RequestsTotal: f.req,
		ErrorsTotal:   f.errs,
		Providers:     copyProviders(f.providers),
	})
}

// addProvider adds reqs/errs to a provider's cumulative counters.
func (f *feeder) addProvider(name string, reqs, errs int64) {
	if f.providers == nil {
		f.providers = map[string]ProviderStats{}
	}
	p := f.providers[name]
	p.RequestsTotal += reqs
	p.ErrorsTotal += errs
	f.providers[name] = p
}

type strategyBox struct {
	mu      sync.Mutex
	current string
	sets    []string
	failSet error
}

func (b *strategyBox) get() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current
}

func (b *strategyBox) set(_ context.Context, s string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failSet != nil {
		return b.failSet
	}
	b.sets = append(b.sets, s)
	b.current = s
	return nil
}

func (b *strategyBox) setFail(err error) {
	b.mu.Lock()
	b.failSet = err
	b.mu.Unlock()
}

func (b *strategyBox) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sets)
}

func newLatencyAction(t *testing.T, box *strategyBox, p ActionPolicy) *LatencyStrategyAction {
	t.Helper()
	a, err := NewLatencyStrategyAction(LatencyStrategyConfig{
		HighLatencyMs:    1000,
		RecoverLatencyMs: 500,
		Policy:           p,
		GetStrategy:      box.get,
		SetStrategy:      box.set,
	})
	if err != nil {
		t.Fatalf("NewLatencyStrategyAction: %v", err)
	}
	return a
}

func outcomes(events []AuditEvent) []AuditOutcome {
	out := make([]AuditOutcome, 0, len(events))
	for _, e := range events {
		out = append(out, e.Outcome)
	}
	return out
}

func statusOf(t *testing.T, tn *MetaAgentTuner, action, target string) ActionStatus {
	t.Helper()
	for _, s := range tn.ActionStatuses() {
		if s.Action == action && s.Target == target {
			return s
		}
	}
	t.Fatalf("no status for %s/%s", action, target)
	return ActionStatus{}
}

var fastPolicy = ActionPolicy{SustainEvaluations: 3, RecoveryEvaluations: 2, Cooldown: time.Second}

func TestLatencyActionSustainedVsTransient(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	// Transient spikes interrupted by in-band / healthy samples never apply.
	for _, lat := range []float64{1500, 1500, 700, 1500, 1500, 300, 1500} {
		f.observe(lat, 100, 0)
	}
	if box.calls() != 0 {
		t.Fatalf("transient breaches must not apply, got sets %v", box.sets)
	}
	// Three consecutive breaches (one already counted) apply exactly once.
	f.observe(1500, 100, 0)
	f.observe(1500, 100, 0)
	if got := box.get(); got != "latency" {
		t.Fatalf("expected strategy latency, got %q", got)
	}
	for i := 0; i < 5; i++ {
		f.observe(1500, 100, 0)
	}
	if box.calls() != 1 {
		t.Fatalf("expected exactly one SetStrategy call, got %v", box.sets)
	}
	audit := f.t.Audit()
	if len(audit) != 1 || audit[0].Outcome != OutcomeApplied || audit[0].DryRun {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	if !strings.Contains(audit[0].Detail, `"weighted" -> "latency"`) || !strings.Contains(audit[0].Reason, "latency_ms=1500.0") {
		t.Fatalf("audit lacks detail/reason: %+v", audit[0])
	}
}

func TestLatencyActionHysteresisAndRestore(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	if box.get() != "latency" {
		t.Fatalf("expected apply")
	}
	// Inside the band (500 <= x <= 1000): neither breach nor recovery.
	for i := 0; i < 10; i++ {
		f.observe(800, 100, 0)
	}
	if box.get() != "latency" {
		t.Fatalf("in-band latency must not revert")
	}
	// Healthy once, in-band once: the recovery streak resets.
	f.observe(100, 100, 0)
	f.observe(800, 100, 0)
	f.observe(100, 100, 0)
	if box.get() != "latency" {
		t.Fatalf("interrupted recovery must not revert")
	}
	f.observe(100, 100, 0)
	if got := box.get(); got != "weighted" {
		t.Fatalf("expected previous strategy restored, got %q", got)
	}
	if got := outcomes(f.t.Audit()); len(got) != 2 || got[1] != OutcomeReverted {
		t.Fatalf("unexpected outcomes %v", got)
	}
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); st.Active {
		t.Fatalf("expected inactive after revert: %+v", st)
	}
}

func TestActionCooldown(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	p := ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Minute}
	if err := f.t.Register(newLatencyAction(t, box, p)); err != nil {
		t.Fatal(err)
	}
	f.observe(2000, 100, 0) // applied at T0
	// Recovered immediately (T0+10s..T0+50s), but the cooldown blocks the
	// revert.
	for i := 0; i < 5; i++ {
		f.observe(100, 100, 0)
	}
	if box.get() != "latency" {
		t.Fatalf("revert must wait for cooldown")
	}
	f.observe(100, 100, 0) // T+60s since apply: cooldown elapsed
	if box.get() != "weighted" {
		t.Fatalf("expected revert after cooldown")
	}
	// Re-apply is also rate limited by the cooldown.
	f.observe(2000, 100, 0)
	if box.get() != "weighted" {
		t.Fatalf("re-apply must wait for cooldown")
	}
	f.clk.Advance(time.Minute)
	f.observe(2000, 100, 0)
	if box.get() != "latency" {
		t.Fatalf("expected re-apply after cooldown")
	}
}

func TestDryRunIsDefaultAndRecordsWithoutCallbacks(t *testing.T) {
	f := newFeeder(t)
	if !f.t.DryRun() || !f.t.Stats().DryRun {
		t.Fatalf("new tuners must start in dry-run mode")
	}
	var zero MetaAgentTuner
	if !zero.DryRun() {
		t.Fatalf("zero-value tuner must be in dry-run mode")
	}
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		f.observe(2000, 100, 0)
	}
	if box.calls() != 0 || box.get() != "weighted" {
		t.Fatalf("dry-run must not invoke callbacks: %v", box.sets)
	}
	st := statusOf(t, f.t, LatencyStrategyActionName, "")
	if !st.Active || !st.Simulated {
		t.Fatalf("expected simulated active state: %+v", st)
	}
	f.clk.Advance(time.Second)
	f.observe(100, 100, 0)
	f.observe(100, 100, 0)
	audit := f.t.Audit()
	if got := outcomes(audit); len(got) != 2 || got[0] != OutcomeWouldApply || got[1] != OutcomeWouldRevert {
		t.Fatalf("unexpected dry-run outcomes %v", got)
	}
	for _, e := range audit {
		if !e.DryRun {
			t.Fatalf("dry-run events must be flagged: %+v", e)
		}
	}
	if box.calls() != 0 {
		t.Fatalf("dry-run revert must not invoke callbacks")
	}

	// Simulated state is discarded when going live: live mode must observe
	// its own sustained breach.
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); !st.Simulated {
		t.Fatalf("expected simulated state before going live: %+v", st)
	}
	f.t.SetDryRun(false)
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); st.Active || st.BreachStreak != 0 {
		t.Fatalf("simulated state must be cleared when going live: %+v", st)
	}
	f.clk.Advance(time.Second)
	f.observe(2000, 100, 0)
	f.observe(2000, 100, 0)
	if box.calls() != 0 {
		t.Fatalf("live mode must wait for a full sustained breach")
	}
	f.observe(2000, 100, 0)
	if box.get() != "latency" {
		t.Fatalf("expected live apply")
	}
}

func TestLiveChangeKeptWhenSwitchingToDryRun(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	f.t.SetDryRun(true)
	f.clk.Advance(time.Second)
	for i := 0; i < 4; i++ {
		f.observe(100, 100, 0)
	}
	if box.get() != "latency" {
		t.Fatalf("dry-run must not revert a live change")
	}
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); !st.Active || st.Simulated {
		t.Fatalf("live change must stay tracked: %+v", st)
	}
	f.t.SetDryRun(false)
	f.observe(100, 100, 0)
	f.observe(100, 100, 0)
	if box.get() != "weighted" {
		t.Fatalf("expected revert once live again, got %q", box.get())
	}
}

func TestCallbackErrorLeavesStateUnchanged(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	box.setFail(errors.New("router unavailable"))
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	audit := f.t.Audit()
	if len(audit) != 1 || audit[0].Outcome != OutcomeApplyFailed || !strings.Contains(audit[0].Error, "router unavailable") {
		t.Fatalf("expected apply_failed event: %+v", audit)
	}
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); st.Active {
		t.Fatalf("failed apply must not mark the action active: %+v", st)
	}
	if !strings.Contains(f.t.Stats().LastError, "router unavailable") {
		t.Fatalf("expected LastError to record failure: %q", f.t.Stats().LastError)
	}
	// Still breaching: retried after the cooldown and succeeds.
	box.setFail(nil)
	f.observe(2000, 100, 0)
	if box.get() != "latency" {
		t.Fatalf("expected retry to succeed")
	}

	// Revert failure keeps the action active and is retried later.
	box.setFail(errors.New("still down"))
	f.clk.Advance(time.Second)
	f.observe(100, 100, 0)
	f.observe(100, 100, 0)
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); !st.Active {
		t.Fatalf("failed revert must keep the action active: %+v", st)
	}
	if got := outcomes(f.t.Audit()); got[len(got)-1] != OutcomeRevertFailed {
		t.Fatalf("expected revert_failed, got %v", got)
	}
	box.setFail(nil)
	f.clk.Advance(time.Second)
	f.observe(100, 100, 0)
	f.observe(100, 100, 0)
	if box.get() != "weighted" {
		t.Fatalf("expected revert retry to restore strategy, got %q", box.get())
	}
}

func TestRevertSupersededByOperator(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	_ = box.set(context.Background(), "cost") // operator override
	f.clk.Advance(time.Second)
	f.observe(100, 100, 0)
	f.observe(100, 100, 0)
	if box.get() != "cost" {
		t.Fatalf("operator override must be kept, got %q", box.get())
	}
	last := f.t.Audit()[len(f.t.Audit())-1]
	if last.Outcome != OutcomeSuperseded {
		t.Fatalf("expected superseded, got %+v", last)
	}
	if st := statusOf(t, f.t, LatencyStrategyActionName, ""); st.Active {
		t.Fatalf("superseded revert clears state: %+v", st)
	}
}

func TestNoTrafficIsNoDataForLatency(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	f.observe(2000, 100, 0)
	// Stale high latency without traffic must not count as a breach.
	for i := 0; i < 5; i++ {
		f.observe(2000, 0, 0)
	}
	f.observe(2000, 100, 0)
	f.observe(2000, 100, 0)
	if box.calls() != 0 {
		t.Fatalf("no-traffic windows must reset the breach streak")
	}
}

type limitBox struct {
	mu    sync.Mutex
	limit float64
	sets  []float64
}

func (b *limitBox) get() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit
}

func (b *limitBox) set(_ context.Context, v float64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.limit = v
	b.sets = append(b.sets, v)
	return nil
}

func TestRateLimitTightenAndRelax(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &limitBox{limit: 100}
	a, err := NewRateLimitAction(RateLimitConfig{
		HighErrorRate:    0.2,
		RecoverErrorRate: 0.05,
		MinRequests:      50,
		Factor:           0.25,
		Floor:            30,
		Policy:           fastPolicy,
		GetLimit:         box.get,
		SetLimit:         box.set,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.t.Register(a); err != nil {
		t.Fatal(err)
	}
	// Low-traffic windows with a high error rate are ignored.
	for i := 0; i < 5; i++ {
		f.observe(10, 10, 9)
	}
	if len(box.sets) != 0 {
		t.Fatalf("low-traffic windows must not tighten: %v", box.sets)
	}
	for i := 0; i < 3; i++ {
		f.observe(10, 100, 50)
	}
	if got := box.get(); got != 30 {
		t.Fatalf("expected limit clamped to floor 30, got %v", got)
	}
	// In band (10% errors): stays tightened.
	f.clk.Advance(time.Second)
	for i := 0; i < 5; i++ {
		f.observe(10, 100, 10)
	}
	if box.get() != 30 {
		t.Fatalf("in-band error rate must not relax")
	}
	f.observe(10, 100, 0)
	f.observe(10, 100, 0)
	if got := box.get(); got != 100 {
		t.Fatalf("expected original limit restored, got %v", got)
	}
}

func TestRateLimitOverloadRPS(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &limitBox{limit: 1000}
	a, err := NewRateLimitAction(RateLimitConfig{
		HighErrorRate:    0.5,
		RecoverErrorRate: 0.1,
		HighRPS:          50,
		RecoverRPS:       20,
		Policy:           ActionPolicy{SustainEvaluations: 2, RecoveryEvaluations: 2, Cooldown: time.Second},
		GetLimit:         box.get,
		SetLimit:         box.set,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.t.Register(a); err != nil {
		t.Fatal(err)
	}
	f.observe(10, 100, 0) // first sample: no RPS yet
	f.observe(10, 1000, 0)
	f.observe(10, 1000, 0) // 100 rps for two windows
	if got := box.get(); got != 500 {
		t.Fatalf("expected tightened limit 500 under overload, got %v", got)
	}
	f.observe(10, 100, 0)
	f.observe(10, 100, 0) // 10 rps
	if got := box.get(); got != 1000 {
		t.Fatalf("expected relaxed limit 1000, got %v", got)
	}
}

type circuitBox struct {
	mu     sync.Mutex
	open   map[string]bool
	events []string
}

func (b *circuitBox) openFn(_ context.Context, p string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open == nil {
		b.open = map[string]bool{}
	}
	b.open[p] = true
	b.events = append(b.events, "open:"+p)
	return nil
}

func (b *circuitBox) closeFn(_ context.Context, p string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.open, p)
	b.events = append(b.events, "close:"+p)
	return nil
}

func (b *circuitBox) isOpen(p string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open[p]
}

func TestCircuitBreakerPerProviderIndependence(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &circuitBox{}
	a, err := NewCircuitBreakerAction(CircuitBreakerConfig{
		HighErrorRate:    0.5,
		RecoverErrorRate: 0.1,
		MinRequests:      10,
		MaxOpen:          2,
		Policy:           ActionPolicy{SustainEvaluations: 2, RecoveryEvaluations: 2, Cooldown: 5 * time.Minute},
		OpenCircuit:      box.openFn,
		CloseCircuit:     box.closeFn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.t.Register(a); err != nil {
		t.Fatal(err)
	}
	tick := func(a, b, c [2]int64) {
		f.addProvider("a", a[0], a[1])
		f.addProvider("b", b[0], b[1])
		f.addProvider("c", c[0], c[1])
		f.observe(10, a[0]+b[0]+c[0], a[1]+b[1]+c[1])
	}
	ok := [2]int64{100, 0}
	bad := [2]int64{100, 90}
	idle := [2]int64{0, 0}

	tick(ok, ok, ok) // baseline (cumulative first sample)
	tick(bad, ok, ok)
	tick(bad, ok, ok)
	if !box.isOpen("a") || box.isOpen("b") || box.isOpen("c") {
		t.Fatalf("only a should be open: %v", box.events)
	}
	// b starts failing; a is open (no traffic).
	tick(idle, bad, ok)
	tick(idle, bad, ok)
	if !box.isOpen("b") {
		t.Fatalf("b should be open: %v", box.events)
	}
	// c fails too, but opening it would exceed MaxOpen / leave nothing.
	tick(idle, idle, bad)
	tick(idle, idle, bad)
	if box.isOpen("c") {
		t.Fatalf("guardrail must keep c closed: %v", box.events)
	}
	var blocked bool
	for _, e := range f.t.Audit() {
		if e.Outcome == OutcomeBlocked && e.Target == "c" && strings.Contains(e.Error, "guardrail") {
			blocked = true
		}
	}
	if !blocked {
		t.Fatalf("expected blocked audit event for c: %+v", f.t.Audit())
	}
	// Past the cooldown, idle (open) circuits close after the recovery streak.
	f.clk.Advance(5 * time.Minute)
	tick(idle, idle, ok)
	tick(idle, idle, ok)
	if box.isOpen("a") || box.isOpen("b") {
		t.Fatalf("idle open circuits should close after recovery: %v", box.events)
	}
	if st := statusOf(t, f.t, CircuitBreakerActionName, "c"); st.Active {
		t.Fatalf("c must never have been opened: %+v", st)
	}
}

func TestCircuitGuardNeverOpensLastProvider(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &circuitBox{}
	a, err := NewCircuitBreakerAction(CircuitBreakerConfig{
		HighErrorRate: 0.5, RecoverErrorRate: 0.1, MaxOpen: 5,
		Policy:      ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Second},
		OpenCircuit: box.openFn, CloseCircuit: box.closeFn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.t.Register(a); err != nil {
		t.Fatal(err)
	}
	f.addProvider("only", 100, 100)
	f.observe(10, 100, 100)
	if box.isOpen("only") {
		t.Fatalf("must never open the last provider")
	}
}

func TestAuditTrailBounded(t *testing.T) {
	f := newFeeder(t)
	f.t.SetAuditCapacity(5)
	box := &strategyBox{current: "weighted"}
	p := ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Nanosecond}
	if err := f.t.Register(newLatencyAction(t, box, p)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		f.observe(2000, 100, 0)
		f.observe(100, 100, 0)
	}
	audit := f.t.Audit()
	if len(audit) != 5 {
		t.Fatalf("expected 5 retained events, got %d", len(audit))
	}
	for i := 1; i < len(audit); i++ {
		if audit[i].Seq != audit[i-1].Seq+1 {
			t.Fatalf("events out of order: %+v", audit)
		}
	}
	if audit[len(audit)-1].Seq != 40 {
		t.Fatalf("expected newest seq 40, got %d", audit[len(audit)-1].Seq)
	}
	// Growing keeps what is retained.
	f.t.SetAuditCapacity(10)
	if got := len(f.t.Audit()); got != 5 {
		t.Fatalf("resize lost events: %d", got)
	}
}

func TestRegisterAndConstructorValidation(t *testing.T) {
	tn := NewMetaAgentTuner(nil, time.Hour, time.Hour)
	if err := tn.Register(nil); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("nil action: %v", err)
	}
	box := &strategyBox{}
	a := newLatencyAction(t, box, ActionPolicy{})
	if err := tn.Register(a); err != nil {
		t.Fatal(err)
	}
	if err := tn.Register(a); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("duplicate name must be rejected: %v", err)
	}
	if err := tn.Register(&funcAction{name: "bad name!"}); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("bad name must be rejected: %v", err)
	}
	if err := tn.Register(&funcAction{name: "neg", policy: ActionPolicy{Cooldown: -1}}); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("negative cooldown must be rejected: %v", err)
	}

	cases := []error{}
	_, err := NewLatencyStrategyAction(LatencyStrategyConfig{HighLatencyMs: 500, RecoverLatencyMs: 500, GetStrategy: box.get, SetStrategy: box.set})
	cases = append(cases, err)
	_, err = NewLatencyStrategyAction(LatencyStrategyConfig{HighLatencyMs: 1000, RecoverLatencyMs: 500})
	cases = append(cases, err)
	_, err = NewLatencyStrategyAction(LatencyStrategyConfig{HighLatencyMs: 1000, RecoverLatencyMs: 500, Strategy: "bad strategy\n", GetStrategy: box.get, SetStrategy: box.set})
	cases = append(cases, err)
	lb := &limitBox{}
	_, err = NewRateLimitAction(RateLimitConfig{HighErrorRate: 0.1, RecoverErrorRate: 0.2, GetLimit: lb.get, SetLimit: lb.set})
	cases = append(cases, err)
	_, err = NewRateLimitAction(RateLimitConfig{HighErrorRate: 0.2, RecoverErrorRate: 0.1, Factor: 1.5, GetLimit: lb.get, SetLimit: lb.set})
	cases = append(cases, err)
	_, err = NewRateLimitAction(RateLimitConfig{HighErrorRate: 0.2, RecoverErrorRate: 0.1, HighRPS: 10, RecoverRPS: 20, GetLimit: lb.get, SetLimit: lb.set})
	cases = append(cases, err)
	cb := &circuitBox{}
	_, err = NewCircuitBreakerAction(CircuitBreakerConfig{HighErrorRate: 2, RecoverErrorRate: 0.1, OpenCircuit: cb.openFn, CloseCircuit: cb.closeFn})
	cases = append(cases, err)
	_, err = NewCircuitBreakerAction(CircuitBreakerConfig{HighErrorRate: 0.5, RecoverErrorRate: 0.1})
	cases = append(cases, err)
	for i, err := range cases {
		if !errors.Is(err, ErrInvalidAction) {
			t.Errorf("case %d: expected ErrInvalidAction, got %v", i, err)
		}
	}
}

// funcAction is a generic Action for engine tests.
type funcAction struct {
	name   string
	policy ActionPolicy
	obs    func(Signals) []Observation
	apply  func(context.Context, string) (string, error)
	revert func(context.Context, string) (string, error)
}

func (a *funcAction) Name() string         { return a.name }
func (a *funcAction) Policy() ActionPolicy { return a.policy }
func (a *funcAction) Observe(s Signals) []Observation {
	if a.obs == nil {
		return nil
	}
	return a.obs(s)
}
func (a *funcAction) Apply(ctx context.Context, target string) (string, error) {
	if a.apply == nil {
		return "", nil
	}
	return a.apply(ctx, target)
}
func (a *funcAction) Revert(ctx context.Context, target string) (string, error) {
	if a.revert == nil {
		return "", nil
	}
	return a.revert(ctx, target)
}

func TestActionPanicsAreContained(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	a := &funcAction{
		name:   "panicky",
		policy: ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Nanosecond},
		obs: func(s Signals) []Observation {
			return []Observation{{Condition: ConditionBreach, Reason: "line1\nline2"}}
		},
		apply: func(context.Context, string) (string, error) { panic("boom") },
	}
	if err := f.t.Register(a); err != nil {
		t.Fatal(err)
	}
	obsPanics := &funcAction{name: "observe-panics", obs: func(Signals) []Observation { panic("bad observe") }}
	if err := f.t.Register(obsPanics); err != nil {
		t.Fatal(err)
	}
	f.observe(10, 100, 0)
	audit := f.t.Audit()
	if len(audit) != 1 || audit[0].Outcome != OutcomeApplyFailed || !strings.Contains(audit[0].Error, "panicked") {
		t.Fatalf("expected contained panic: %+v", audit)
	}
	if strings.ContainsAny(audit[0].Reason, "\n\r") {
		t.Fatalf("audit reason must be sanitized: %q", audit[0].Reason)
	}
}

func TestRevertAll(t *testing.T) {
	f := newFeeder(t)
	f.t.SetDryRun(false)
	box := &strategyBox{current: "weighted"}
	if err := f.t.Register(newLatencyAction(t, box, fastPolicy)); err != nil {
		t.Fatal(err)
	}
	cb := &circuitBox{}
	failing := errors.New("close failed")
	ca := &funcAction{
		name:   "sim",
		policy: ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Hour},
		obs: func(Signals) []Observation {
			return []Observation{{Target: "x", Condition: ConditionBreach}}
		},
		apply:  func(ctx context.Context, p string) (string, error) { return "", cb.openFn(ctx, p) },
		revert: func(context.Context, string) (string, error) { return "", failing },
	}
	if err := f.t.Register(ca); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f.observe(2000, 100, 0)
	}
	if box.get() != "latency" || !cb.isOpen("x") {
		t.Fatalf("expected both actions applied")
	}
	f.t.SetDryRun(true) // RevertAll still runs callbacks for live changes
	err := f.t.RevertAll(context.Background())
	if !errors.Is(err, failing) {
		t.Fatalf("expected joined revert error, got %v", err)
	}
	if box.get() != "weighted" {
		t.Fatalf("RevertAll must restore strategy")
	}
	if st := statusOf(t, f.t, "sim", "x"); !st.Active {
		t.Fatalf("failed revert must keep target active: %+v", st)
	}
}

func TestLegacyLadderIsAudited(t *testing.T) {
	src := &fakeSource{}
	tn := newTestTuner(src)
	var a counterAction
	tn.RegisterAction(a.action("ladder"))
	src.set(0, 0, 5000)
	tn.evaluate(context.Background())
	audit := tn.Audit()
	if len(audit) != 1 || audit[0].Action != "ladder" || audit[0].Outcome != OutcomeApplied {
		t.Fatalf("expected ladder apply in audit: %+v", audit)
	}
}

func TestDefaultMetricsSourceHooks(t *testing.T) {
	src := NewDefaultMetricsSource(func() int64 { return 10 }, func() int64 { return 1 }, func() float64 { return 50 }).
		WithP95(func() float64 { return 120 }).
		WithProviders(func() map[string]ProviderStats {
			return map[string]ProviderStats{"openai": {RequestsTotal: 10, ErrorsTotal: 1}}
		})
	snap := src.Snapshot()
	if snap.P95LatencyMs != 120 || snap.Providers["openai"].RequestsTotal != 10 {
		t.Fatalf("hooks not reflected: %+v", snap)
	}
}

func TestWindowProviders(t *testing.T) {
	prev := map[string]ProviderStats{"a": {RequestsTotal: 100, ErrorsTotal: 10}}
	cur := map[string]ProviderStats{
		"a":   {RequestsTotal: 150, ErrorsTotal: 35},
		"b":   {ErrorRate: 0.3, Requests: 40}, // precomputed window
		"bad": {RequestsTotal: -1},
		"":    {Requests: 1},
	}
	got := windowProviders(prev, cur)
	if len(got) != 2 {
		t.Fatalf("expected 2 providers, got %+v", got)
	}
	if a := got["a"]; a.Requests != 50 || a.ErrorRate != 0.5 {
		t.Fatalf("unexpected windowed stats for a: %+v", a)
	}
	if b := got["b"]; b.Requests != 40 || b.ErrorRate != 0.3 {
		t.Fatalf("unexpected stats for b: %+v", b)
	}
	// Counter reset falls back to cumulative.
	reset := windowProviders(prev, map[string]ProviderStats{"a": {RequestsTotal: 10, ErrorsTotal: 5}})
	if a := reset["a"]; a.Requests != 10 || a.ErrorRate != 0.5 {
		t.Fatalf("unexpected reset stats: %+v", a)
	}
}

func TestActionsConcurrentUse(t *testing.T) {
	clk := newFakeClock()
	tn := NewMetaAgentTuner(nil, time.Hour, time.Hour)
	tn.SetClock(clk.Now)
	box := &strategyBox{current: "weighted"}
	p := ActionPolicy{SustainEvaluations: 1, RecoveryEvaluations: 1, Cooldown: time.Nanosecond}
	if err := tn.Register(newLatencyAction(t, box, p)); err != nil {
		t.Fatal(err)
	}
	cb := &circuitBox{}
	ca, err := NewCircuitBreakerAction(CircuitBreakerConfig{
		HighErrorRate: 0.5, RecoverErrorRate: 0.1, MinRequests: 1, MaxOpen: 3,
		Policy:      p,
		OpenCircuit: cb.openFn, CloseCircuit: cb.closeFn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tn.Register(ca); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var n atomic.Int64
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c := n.Add(1)
				clk.Advance(time.Second)
				lat := 100.0
				if i%3 == 0 {
					lat = 5000
				}
				tn.Observe(context.Background(), MetricsSnapshot{
					Timestamp:     clk.Now(),
					P95LatencyMs:  lat,
					RequestsTotal: c * 10,
					Providers: map[string]ProviderStats{
						fmt.Sprintf("p%d", g): {Requests: 10, ErrorRate: float64(i%2) * 0.9},
						"shared":              {Requests: 10},
					},
				})
				if i%50 == 0 {
					tn.SetDryRun(i%100 == 0)
				}
				_ = tn.Audit()
				_ = tn.ActionStatuses()
				_ = tn.Stats()
			}
		}(g)
	}
	wg.Wait()
	if err := tn.RevertAll(context.Background()); err != nil {
		t.Fatalf("RevertAll: %v", err)
	}
	for _, st := range tn.ActionStatuses() {
		if st.Active {
			t.Fatalf("expected all targets reverted: %+v", st)
		}
	}
}
