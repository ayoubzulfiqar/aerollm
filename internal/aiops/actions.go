package aiops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Action engine limits and defaults.
const (
	// DefaultSustainEvaluations is the number of consecutive breaching
	// evaluations required before an Action is applied.
	DefaultSustainEvaluations = 3
	// DefaultActionRecoveryEvaluations is the number of consecutive healthy
	// evaluations required before an applied Action is reverted.
	DefaultActionRecoveryEvaluations = 3
	// DefaultAuditCapacity is the default size of the audit ring buffer.
	DefaultAuditCapacity = 256
	// MaxAuditCapacity bounds SetAuditCapacity.
	MaxAuditCapacity = 100000
	// MaxRegisteredActions bounds the number of Actions a tuner accepts.
	MaxRegisteredActions = 64
	// MaxTargetsPerAction bounds the per-target state kept for one Action
	// (e.g. one entry per provider for the circuit breaker action).
	MaxTargetsPerAction = 1024
	// MaxProviderSignals bounds the number of providers carried in Signals.
	MaxProviderSignals = 1024

	maxStreak      = 1 << 20
	maxAuditString = 512
)

var (
	// ErrSuperseded is returned by Action.Revert when the value the action
	// set was changed externally (e.g. by an operator) since it was applied,
	// so the action deliberately leaves it alone. The engine treats it as a
	// completed revert and records OutcomeSuperseded.
	ErrSuperseded = errors.New("aiops: change superseded externally; not reverted")
	// ErrGuardrail is returned (wrapped) by ActionGuard.CanApply when
	// applying would violate a safety limit.
	ErrGuardrail = errors.New("aiops: action blocked by guardrail")
	// ErrInvalidAction is returned by Register and the action constructors
	// for malformed actions/configurations.
	ErrInvalidAction = errors.New("aiops: invalid action")
)

var actionNamePattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// ProviderStats carries per-provider health.
//
// As an input (MetricsSnapshot.Providers) either supply the cumulative
// counters RequestsTotal/ErrorsTotal (preferred: the tuner derives a windowed
// error rate from the deltas between evaluations), or, when no counters are
// available, a precomputed ErrorRate with Requests as its sample size.
//
// As a signal (Signals.Providers) ErrorRate and Requests always describe the
// latest evaluation window.
type ProviderStats struct {
	RequestsTotal int64   `json:"requests_total,omitempty"`
	ErrorsTotal   int64   `json:"errors_total,omitempty"`
	ErrorRate     float64 `json:"error_rate"`
	Requests      int64   `json:"requests"`
}

// Signals are the derived health signals handed to every Action on each
// evaluation.
type Signals struct {
	Time time.Time `json:"time"`
	// LatencyMs is the primary latency signal: P95LatencyMs when the source
	// provides it, otherwise max(P99LatencyMs, AvgLatencyMs).
	LatencyMs    float64 `json:"latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	// ErrorRate is the windowed error rate (see windowErrorRate).
	ErrorRate float64 `json:"error_rate"`
	// CountersAvailable reports whether the source exposes cumulative
	// request/error counters. When true, WindowRequests is the number of
	// requests observed in this window and can be used to ignore noisy
	// low-traffic windows.
	CountersAvailable bool  `json:"counters_available"`
	WindowRequests    int64 `json:"window_requests"`
	// RPS is the request rate over the window (0 when it cannot be derived,
	// e.g. missing timestamps or counters).
	RPS float64 `json:"rps"`
	// Providers holds windowed per-provider stats keyed by provider name.
	Providers map[string]ProviderStats `json:"providers,omitempty"`
}

// Condition classifies one observation of an Action target.
type Condition int

const (
	// ConditionNeutral: the signal is inside the hysteresis band. Both the
	// breach and the recovery streaks are reset.
	ConditionNeutral Condition = iota
	// ConditionBreach: the signal is beyond the trigger threshold.
	ConditionBreach
	// ConditionHealthy: the signal is below the recovery threshold.
	ConditionHealthy
	// ConditionNoData: there is not enough data to judge (e.g. no traffic).
	// It resets the breach streak and counts as recovery evidence for an
	// applied target (an open circuit receives no traffic, for instance).
	ConditionNoData
)

// String implements fmt.Stringer.
func (c Condition) String() string {
	switch c {
	case ConditionBreach:
		return "breach"
	case ConditionHealthy:
		return "healthy"
	case ConditionNoData:
		return "no_data"
	default:
		return "neutral"
	}
}

// Observation is an Action's classification of one target for one
// evaluation. Target is "" for global actions.
type Observation struct {
	Target    string
	Condition Condition
	// Reason is a short human-readable description of the metric values
	// behind the classification; it is copied into the audit trail and must
	// not contain secrets.
	Reason string
}

// ActionPolicy controls when the engine applies and reverts an Action.
// Zero fields take the defaults.
type ActionPolicy struct {
	// SustainEvaluations: consecutive ConditionBreach observations required
	// before applying (debounces transient spikes).
	SustainEvaluations int `json:"sustain_evaluations"`
	// RecoveryEvaluations: consecutive ConditionHealthy/NoData observations
	// required before reverting.
	RecoveryEvaluations int `json:"recovery_evaluations"`
	// Cooldown is the minimum time between two state changes (apply or
	// revert, including failed attempts) of the same action target.
	Cooldown time.Duration `json:"cooldown"`
}

func (p ActionPolicy) normalize() (ActionPolicy, error) {
	if p.SustainEvaluations == 0 {
		p.SustainEvaluations = DefaultSustainEvaluations
	}
	if p.RecoveryEvaluations == 0 {
		p.RecoveryEvaluations = DefaultActionRecoveryEvaluations
	}
	if p.Cooldown == 0 {
		p.Cooldown = DefaultCooldown
	}
	if p.SustainEvaluations < 1 || p.SustainEvaluations > 10000 {
		return p, fmt.Errorf("%w: sustain_evaluations must be in [1, 10000]", ErrInvalidAction)
	}
	if p.RecoveryEvaluations < 1 || p.RecoveryEvaluations > 10000 {
		return p, fmt.Errorf("%w: recovery_evaluations must be in [1, 10000]", ErrInvalidAction)
	}
	if p.Cooldown < 0 {
		return p, fmt.Errorf("%w: cooldown must not be negative", ErrInvalidAction)
	}
	return p, nil
}

// Action is a closed-loop runtime adjustment managed by MetaAgentTuner.
//
// On every evaluation the engine calls Observe with the current Signals; the
// action classifies each of its targets (a global action uses Target "").
// The engine owns all control logic: sustained-breach and recovery streaks
// (hysteresis comes from the action using a trigger threshold above its
// recovery threshold), per-target cooldowns, dry-run and the audit trail.
// Apply/Revert are only invoked in live mode, serialized (never
// concurrently with each other or another evaluation) and with panics
// converted to errors. A failed Apply leaves the target inactive; a failed
// Revert leaves it active.
type Action interface {
	// Name is a unique identifier matching [A-Za-z0-9._:-]{1,64}.
	Name() string
	// Policy returns the apply/revert policy. It is read once at Register.
	Policy() ActionPolicy
	// Observe classifies the action's targets. It must be fast and free of
	// side effects and must not call back into the tuner.
	Observe(s Signals) []Observation
	// Apply enacts the adjustment for target and returns a short detail
	// for the audit trail (e.g. "strategy weighted -> latency").
	Apply(ctx context.Context, target string) (detail string, err error)
	// Revert undoes a previous successful Apply for target. It may return
	// ErrSuperseded when the value was changed externally meanwhile.
	Revert(ctx context.Context, target string) (detail string, err error)
}

// ActionGuard is optionally implemented by Actions with safety limits. The
// engine calls CanApply (in both live and dry-run mode) before applying a
// target; active lists the targets of this action that are currently applied
// (or simulated in dry-run). A non-nil error blocks the apply and is recorded
// as OutcomeBlocked.
type ActionGuard interface {
	CanApply(target string, active []string) error
}

// AuditOutcome is the result recorded for an action state change.
type AuditOutcome string

// Audit outcomes.
const (
	OutcomeApplied      AuditOutcome = "applied"
	OutcomeReverted     AuditOutcome = "reverted"
	OutcomeApplyFailed  AuditOutcome = "apply_failed"
	OutcomeRevertFailed AuditOutcome = "revert_failed"
	OutcomeWouldApply   AuditOutcome = "would_apply"
	OutcomeWouldRevert  AuditOutcome = "would_revert"
	OutcomeBlocked      AuditOutcome = "blocked"
	OutcomeSuperseded   AuditOutcome = "superseded"
)

// AuditEvent is one entry of the tuner's bounded audit trail.
type AuditEvent struct {
	Seq     uint64       `json:"seq"`
	Time    time.Time    `json:"time"`
	Action  string       `json:"action"`
	Target  string       `json:"target,omitempty"`
	Outcome AuditOutcome `json:"outcome"`
	DryRun  bool         `json:"dry_run"`
	Reason  string       `json:"reason,omitempty"`
	Detail  string       `json:"detail,omitempty"`
	Error   string       `json:"error,omitempty"`
}

// ActionStatus is a point-in-time view of one action target.
type ActionStatus struct {
	Action         string    `json:"action"`
	Target         string    `json:"target,omitempty"`
	Active         bool      `json:"active"`
	Simulated      bool      `json:"simulated"`
	BreachStreak   int       `json:"breach_streak"`
	RecoveryStreak int       `json:"recovery_streak"`
	LastChange     time.Time `json:"last_change"`
}

type targetState struct {
	active     bool // applied live, or simulated in dry-run
	simulated  bool // active only in dry-run bookkeeping (no callback ran)
	breach     int
	recover    int
	lastChange time.Time
	lastSeen   time.Time
}

type registeredAction struct {
	action  Action
	name    string
	policy  ActionPolicy
	targets map[string]*targetState
}

// auditRing is a fixed-capacity ring buffer of audit events.
type auditRing struct {
	buf   []AuditEvent
	start int
	n     int
	seq   uint64
}

func (r *auditRing) add(ev AuditEvent) {
	if len(r.buf) == 0 {
		r.buf = make([]AuditEvent, DefaultAuditCapacity)
	}
	r.seq++
	ev.Seq = r.seq
	if r.n < len(r.buf) {
		r.buf[(r.start+r.n)%len(r.buf)] = ev
		r.n++
		return
	}
	r.buf[r.start] = ev
	r.start = (r.start + 1) % len(r.buf)
}

func (r *auditRing) list() []AuditEvent {
	out := make([]AuditEvent, 0, r.n)
	for i := 0; i < r.n; i++ {
		out = append(out, r.buf[(r.start+i)%len(r.buf)])
	}
	return out
}

func (r *auditRing) resize(capacity int) {
	events := r.list()
	if len(events) > capacity {
		events = events[len(events)-capacity:]
	}
	r.buf = make([]AuditEvent, capacity)
	r.start = 0
	r.n = copy(r.buf, events)
}

// sanitizeAudit bounds the length of a free-form audit string and strips
// control characters so audit output cannot be used for log injection.
func sanitizeAudit(s string) string {
	if len(s) > maxAuditString {
		s = s[:maxAuditString]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}

func (t *MetaAgentTuner) now() time.Time {
	if t.clock != nil {
		return t.clock()
	}
	return time.Now()
}

// SetClock replaces the tuner's time source (for tests). nil restores
// time.Now.
func (t *MetaAgentTuner) SetClock(now func() time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.clock = now
	t.mu.Unlock()
}

// Register adds an Action. Names must be unique per tuner. Actions start in
// the tuner's current mode; new tuners are in dry-run mode (see SetDryRun).
func (t *MetaAgentTuner) Register(a Action) error {
	if t == nil {
		return fmt.Errorf("aiops: tuner is nil")
	}
	if a == nil {
		return fmt.Errorf("%w: nil action", ErrInvalidAction)
	}
	name := a.Name()
	if !actionNamePattern.MatchString(name) {
		return fmt.Errorf("%w: name must match [A-Za-z0-9._:-]{1,64}", ErrInvalidAction)
	}
	policy, err := a.Policy().normalize()
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.registered) >= MaxRegisteredActions {
		return fmt.Errorf("%w: at most %d actions", ErrInvalidAction, MaxRegisteredActions)
	}
	for _, ra := range t.registered {
		if ra.name == name {
			return fmt.Errorf("%w: duplicate action name %q", ErrInvalidAction, name)
		}
	}
	t.registered = append(t.registered, &registeredAction{
		action:  a,
		name:    name,
		policy:  policy,
		targets: make(map[string]*targetState),
	})
	return nil
}

// SetDryRun switches Actions between dry-run (true) and live (false) mode.
// New tuners start in dry-run mode: decisions are recorded in the audit
// trail as would_apply/would_revert and no Apply/Revert callback runs.
//
// Switching dry-run -> live discards simulated state, so live mode must
// observe a sustained breach itself before acting. Switching live -> dry-run
// keeps live changes in place; they are reverted when live mode resumes and
// the target has recovered, or explicitly via RevertAll. The legacy
// TunerAction ladder (RegisterAction) is not affected by dry-run.
func (t *MetaAgentTuner) SetDryRun(dryRun bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	wasDry := !t.live
	t.live = !dryRun
	if wasDry && !dryRun {
		for _, ra := range t.registered {
			for _, st := range ra.targets {
				if st.simulated {
					st.active, st.simulated = false, false
				}
				st.breach, st.recover = 0, 0
			}
		}
	}
}

// DryRun reports whether Actions run in dry-run mode.
func (t *MetaAgentTuner) DryRun() bool {
	if t == nil {
		return true
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.live
}

// SetAuditCapacity resizes the audit ring buffer (clamped to
// [1, MaxAuditCapacity]), keeping the newest events.
func (t *MetaAgentTuner) SetAuditCapacity(n int) {
	if t == nil {
		return
	}
	if n < 1 {
		n = 1
	}
	if n > MaxAuditCapacity {
		n = MaxAuditCapacity
	}
	t.mu.Lock()
	t.audit.resize(n)
	t.mu.Unlock()
}

// Audit returns the retained audit trail, oldest first.
func (t *MetaAgentTuner) Audit() []AuditEvent {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.audit.list()
}

// ActionStatuses returns the state of every known action target in
// registration order (targets sorted by name).
func (t *MetaAgentTuner) ActionStatuses() []ActionStatus {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []ActionStatus
	for _, ra := range t.registered {
		names := make([]string, 0, len(ra.targets))
		for k := range ra.targets {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			st := ra.targets[k]
			out = append(out, ActionStatus{
				Action:         ra.name,
				Target:         k,
				Active:         st.active,
				Simulated:      st.simulated,
				BreachStreak:   st.breach,
				RecoveryStreak: st.recover,
				LastChange:     st.lastChange,
			})
		}
	}
	return out
}

// RevertAll reverts every live-applied action target and clears simulated
// ones, e.g. for an operator "reset to baseline" or before shutdown. It
// invokes Revert callbacks even in dry-run mode because it is an explicit
// request; failures are joined into the returned error and those targets
// stay active.
func (t *MetaAgentTuner) RevertAll(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.evalMu.Lock()
	defer t.evalMu.Unlock()

	type job struct {
		ra     *registeredAction
		target string
	}
	var jobs []job
	t.mu.Lock()
	now := t.now()
	for _, ra := range t.registered {
		names := make([]string, 0, len(ra.targets))
		for k := range ra.targets {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			st := ra.targets[k]
			switch {
			case st.simulated:
				st.active, st.simulated, st.breach, st.recover = false, false, 0, 0
				st.lastChange = now
				t.recordLocked(ra.name, k, OutcomeWouldRevert, true, "manual revert-all", "", nil, now)
			case st.active:
				jobs = append(jobs, job{ra, k})
			}
		}
	}
	t.mu.Unlock()

	var errs []error
	for _, j := range jobs {
		detail, err := safeRevert(ctx, j.ra.action, j.target)
		t.mu.Lock()
		now := t.now()
		st := j.ra.targets[j.target]
		if st != nil {
			st.lastChange = now
		}
		switch {
		case err == nil || errors.Is(err, ErrSuperseded):
			if st != nil {
				st.active, st.breach, st.recover = false, 0, 0
			}
			outcome := OutcomeReverted
			var recErr error
			if err != nil {
				outcome, recErr = OutcomeSuperseded, err
			}
			t.recordLocked(j.ra.name, j.target, outcome, false, "manual revert-all", detail, recErr, now)
		default:
			t.recordLocked(j.ra.name, j.target, OutcomeRevertFailed, false, "manual revert-all", detail, err, now)
			errs = append(errs, fmt.Errorf("revert %s/%s: %w", j.ra.name, j.target, err))
		}
		t.mu.Unlock()
	}
	return errors.Join(errs...)
}

// recordLocked appends an audit event. Caller holds t.mu.
func (t *MetaAgentTuner) recordLocked(action, target string, outcome AuditOutcome, dry bool, reason, detail string, err error, now time.Time) {
	ev := AuditEvent{
		Time:    now,
		Action:  sanitizeAudit(action),
		Target:  sanitizeAudit(target),
		Outcome: outcome,
		DryRun:  dry,
		Reason:  sanitizeAudit(reason),
		Detail:  sanitizeAudit(detail),
	}
	if err != nil {
		ev.Error = sanitizeAudit(err.Error())
		t.stats.LastError = fmt.Sprintf("%s %s/%s: %s", outcome, ev.Action, ev.Target, ev.Error)
	}
	t.audit.add(ev)
}

func safeObserve(a Action, s Signals) (obs []Observation, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aiops: observe panicked: %v", r)
		}
	}()
	return a.Observe(s), nil
}

func safeApply(ctx context.Context, a Action, target string) (detail string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aiops: apply panicked: %v", r)
		}
	}()
	return a.Apply(ctx, target)
}

func safeRevert(ctx context.Context, a Action, target string) (detail string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aiops: revert panicked: %v", r)
		}
	}()
	return a.Revert(ctx, target)
}

func safeGuard(g ActionGuard, target string, active []string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("aiops: guard panicked: %v", r)
		}
	}()
	return g.CanApply(target, active)
}

func cloneSignals(s Signals) Signals {
	if s.Providers != nil {
		p := make(map[string]ProviderStats, len(s.Providers))
		for k, v := range s.Providers {
			p[k] = v
		}
		s.Providers = p
	}
	return s
}

// runActions evaluates every registered Action against sig. Caller holds
// t.evalMu (not t.mu).
func (t *MetaAgentTuner) runActions(ctx context.Context, sig Signals) {
	t.mu.RLock()
	actions := append([]*registeredAction(nil), t.registered...)
	t.mu.RUnlock()
	for _, ra := range actions {
		if ctx.Err() != nil {
			return
		}
		obs, err := safeObserve(ra.action, cloneSignals(sig))
		if err != nil {
			t.mu.Lock()
			t.stats.LastError = fmt.Sprintf("observe %q: %v", ra.name, err)
			t.mu.Unlock()
			continue
		}
		seen := make(map[string]struct{}, len(obs))
		for _, o := range obs {
			if len(seen) >= MaxTargetsPerAction {
				break
			}
			if _, dup := seen[o.Target]; dup {
				continue
			}
			seen[o.Target] = struct{}{}
			t.stepTarget(ctx, ra, o)
		}
	}
}

// targetLocked returns (creating if needed) the state of target, evicting
// the least recently seen inactive target when the action is at capacity.
// It returns nil when no slot can be freed. Caller holds t.mu.
func (ra *registeredAction) targetLocked(target string, now time.Time) *targetState {
	if st, ok := ra.targets[target]; ok {
		st.lastSeen = now
		return st
	}
	if len(ra.targets) >= MaxTargetsPerAction {
		victim := ""
		var oldest time.Time
		found := false
		for k, st := range ra.targets {
			if st.active {
				continue
			}
			if !found || st.lastSeen.Before(oldest) {
				victim, oldest, found = k, st.lastSeen, true
			}
		}
		if !found {
			return nil
		}
		delete(ra.targets, victim)
	}
	st := &targetState{lastSeen: now}
	ra.targets[target] = st
	return st
}

func (ra *registeredAction) activeTargetsLocked() []string {
	var out []string
	for k, st := range ra.targets {
		if st.active {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func bump(n int) int {
	if n < maxStreak {
		return n + 1
	}
	return n
}

// stepTarget advances one target's state machine by one observation and
// performs at most one state change. Caller holds t.evalMu.
func (t *MetaAgentTuner) stepTarget(ctx context.Context, ra *registeredAction, o Observation) {
	t.mu.Lock()
	now := t.now()
	st := ra.targetLocked(o.Target, now)
	if st == nil {
		t.mu.Unlock()
		return
	}
	switch o.Condition {
	case ConditionBreach:
		st.breach, st.recover = bump(st.breach), 0
	case ConditionHealthy:
		st.breach, st.recover = 0, bump(st.recover)
	case ConditionNoData:
		st.breach = 0
		if st.active {
			st.recover = bump(st.recover)
		} else {
			st.recover = 0
		}
	default:
		st.breach, st.recover = 0, 0
	}
	cool := st.lastChange.IsZero() || now.Sub(st.lastChange) >= ra.policy.Cooldown
	dry := !t.live
	wantApply := !st.active && st.breach >= ra.policy.SustainEvaluations && cool
	wantRevert := st.active && st.recover >= ra.policy.RecoveryEvaluations && cool
	switch {
	case wantApply:
		active := ra.activeTargetsLocked()
		t.mu.Unlock()
		t.applyTarget(ctx, ra, st, o, active, dry)
	case wantRevert:
		if st.simulated || dry {
			// Simulated changes are simply forgotten. A live change seen
			// while in dry-run stays in place (no callbacks in dry-run); the
			// streak is reset so the notice repeats at most once per
			// recovery window and cooldown.
			if st.simulated {
				st.active, st.simulated = false, false
			}
			st.breach, st.recover = 0, 0
			st.lastChange = now
			t.recordLocked(ra.name, o.Target, OutcomeWouldRevert, true, o.Reason, "", nil, now)
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()
		detail, err := safeRevert(ctx, ra.action, o.Target)
		t.mu.Lock()
		now = t.now()
		st.lastChange = now
		switch {
		case err == nil || errors.Is(err, ErrSuperseded):
			st.active, st.breach, st.recover = false, 0, 0
			outcome := OutcomeReverted
			var recErr error
			if err != nil {
				outcome, recErr = OutcomeSuperseded, err
			}
			t.recordLocked(ra.name, o.Target, outcome, false, o.Reason, detail, recErr, now)
		default:
			t.recordLocked(ra.name, o.Target, OutcomeRevertFailed, false, o.Reason, detail, err, now)
		}
		t.mu.Unlock()
	default:
		t.mu.Unlock()
	}
}

// applyTarget runs the guard and (in live mode) the Apply callback for one
// target. Caller holds t.evalMu but not t.mu.
func (t *MetaAgentTuner) applyTarget(ctx context.Context, ra *registeredAction, st *targetState, o Observation, active []string, dry bool) {
	if g, ok := ra.action.(ActionGuard); ok {
		if err := safeGuard(g, o.Target, active); err != nil {
			t.mu.Lock()
			now := t.now()
			st.lastChange = now // back off; re-check after the cooldown
			t.recordLocked(ra.name, o.Target, OutcomeBlocked, dry, o.Reason, "", err, now)
			t.mu.Unlock()
			return
		}
	}
	if dry {
		t.mu.Lock()
		now := t.now()
		st.active, st.simulated, st.breach, st.recover = true, true, 0, 0
		st.lastChange = now
		t.recordLocked(ra.name, o.Target, OutcomeWouldApply, true, o.Reason, "", nil, now)
		t.mu.Unlock()
		return
	}
	detail, err := safeApply(ctx, ra.action, o.Target)
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	st.lastChange = now // back off after both success and failure
	if err != nil {
		t.recordLocked(ra.name, o.Target, OutcomeApplyFailed, false, o.Reason, detail, err, now)
		return
	}
	st.active, st.simulated, st.breach, st.recover = true, false, 0, 0
	t.recordLocked(ra.name, o.Target, OutcomeApplied, false, o.Reason, detail, nil, now)
}

// buildSignals derives Signals from the current and previous snapshots.
func buildSignals(prev *MetricsSnapshot, cur MetricsSnapshot, errRate float64, now time.Time) Signals {
	s := Signals{
		Time:         cur.Timestamp,
		P95LatencyMs: cur.P95LatencyMs,
		P99LatencyMs: cur.P99LatencyMs,
		AvgLatencyMs: cur.AvgLatencyMs,
		ErrorRate:    errRate,
	}
	if s.Time.IsZero() {
		s.Time = now
	}
	s.LatencyMs = math.Max(cur.P99LatencyMs, cur.AvgLatencyMs)
	if cur.P95LatencyMs > 0 {
		s.LatencyMs = cur.P95LatencyMs
	}
	s.CountersAvailable = cur.RequestsTotal > 0 || cur.ErrorsTotal > 0 ||
		(prev != nil && (prev.RequestsTotal > 0 || prev.ErrorsTotal > 0))
	if s.CountersAvailable {
		reset := prev == nil || cur.RequestsTotal < prev.RequestsTotal || cur.ErrorsTotal < prev.ErrorsTotal
		if reset {
			s.WindowRequests = cur.RequestsTotal
		} else {
			s.WindowRequests = cur.RequestsTotal - prev.RequestsTotal
			if !cur.Timestamp.IsZero() && !prev.Timestamp.IsZero() {
				if dt := cur.Timestamp.Sub(prev.Timestamp).Seconds(); dt > 0 {
					s.RPS = float64(s.WindowRequests) / dt
				}
			}
		}
	}
	var prevProviders map[string]ProviderStats
	if prev != nil {
		prevProviders = prev.Providers
	}
	s.Providers = windowProviders(prevProviders, cur.Providers)
	return s
}

func validProviderStats(p ProviderStats) bool {
	if p.RequestsTotal < 0 || p.ErrorsTotal < 0 || p.Requests < 0 {
		return false
	}
	return !math.IsNaN(p.ErrorRate) && !math.IsInf(p.ErrorRate, 0) && p.ErrorRate >= 0
}

// windowProviders converts per-provider inputs into windowed signals.
// Invalid entries and empty names are dropped; at most MaxProviderSignals
// providers (lowest names first) are kept.
func windowProviders(prev, cur map[string]ProviderStats) map[string]ProviderStats {
	if len(cur) == 0 {
		return nil
	}
	names := make([]string, 0, len(cur))
	for k := range cur {
		if k != "" {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	out := make(map[string]ProviderStats, len(names))
	for _, name := range names {
		if len(out) >= MaxProviderSignals {
			break
		}
		ps := cur[name]
		if !validProviderStats(ps) {
			continue
		}
		if ps.RequestsTotal == 0 && ps.ErrorsTotal == 0 {
			// No counters: trust the precomputed window values.
			out[name] = ProviderStats{ErrorRate: clampUnit(ps.ErrorRate), Requests: ps.Requests}
			continue
		}
		p, ok := prev[name]
		sig := ProviderStats{RequestsTotal: ps.RequestsTotal, ErrorsTotal: ps.ErrorsTotal}
		if ok && (p.RequestsTotal > 0 || p.ErrorsTotal > 0) &&
			ps.RequestsTotal >= p.RequestsTotal && ps.ErrorsTotal >= p.ErrorsTotal {
			sig.Requests = ps.RequestsTotal - p.RequestsTotal
			if sig.Requests > 0 {
				sig.ErrorRate = clampUnit(float64(ps.ErrorsTotal-p.ErrorsTotal) / float64(sig.Requests))
			}
		} else {
			// First sample or counter reset: use the cumulative counters.
			sig.Requests = ps.RequestsTotal
			if ps.RequestsTotal > 0 {
				sig.ErrorRate = clampUnit(float64(ps.ErrorsTotal) / float64(ps.RequestsTotal))
			}
		}
		out[name] = sig
	}
	return out
}

func copyProviders(in map[string]ProviderStats) map[string]ProviderStats {
	if in == nil {
		return nil
	}
	out := make(map[string]ProviderStats, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
