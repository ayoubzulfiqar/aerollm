package rsi

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// ---------------------------------------------------------------------------
// Opt-in durable state
// ---------------------------------------------------------------------------
//
// By default the orchestrator keeps everything in memory. EnablePersistence
// attaches a persist.Store, restores the previous process's state from it and
// then writes every state change through to it:
//
//	bucket "rsi_cycles"  key %012d cycle ID -> RSICycle
//	bucket "rsi_state"   key "state"        -> cycle counter, cumulative stats,
//	                                           deployed policy per dimension and
//	                                           the rollback stack
//	                     key "config"       -> the last config applied through
//	                                           SetConfig (absent until then)
//
// Restoring never calls OnDeploy or OnRollback: the restored policies become
// the orchestrator's view of what is deployed (the exploration baseline and
// the rollback targets), so a restart neither redeploys the same policy nor
// forgets what Rollback should restore. Whatever the deploy hook applied to
// live components is NOT re-applied automatically; callers that want the
// live gateway to match can iterate DeployedPolicies() after
// EnablePersistence and apply them explicitly.
//
// Policies are interface values, so they are stored as a kind plus JSON
// params. The built-in policy types (RoutingPolicy, CachePolicy,
// GuardrailPolicy, AgentWorkflowPolicy) are always supported; custom Policy
// implementations need a PolicyCodec (RegisterPolicyCodec). A deployed
// policy without a codec still works in memory but is counted as a persist
// error and is not restored after a restart.

// Persistence bucket names.
const (
	PersistBucketCycles = "rsi_cycles"
	PersistBucketState  = "rsi_state"

	persistKeyState     = "state"
	persistKeyConfig    = "config"
	persistStateVersion = 1
)

// Built-in policy kinds recorded in the store.
const (
	PolicyKindRouting       = "routing"
	PolicyKindCache         = "cache"
	PolicyKindGuardrail     = "guardrail"
	PolicyKindAgentWorkflow = "agent_workflow"
)

var (
	// ErrPersistenceEnabled is returned by a second EnablePersistence call.
	ErrPersistenceEnabled = errors.New("rsi: persistence already enabled")
	// ErrPersistAfterStart is returned when EnablePersistence is called after
	// the orchestrator already recorded cycles or deployments; restoring
	// would then mix two histories with colliding cycle IDs.
	ErrPersistAfterStart = errors.New("rsi: EnablePersistence must be called before the first cycle")
	// ErrPartialRestore wraps non-fatal restore problems (undecodable or
	// unknown documents). Persistence IS enabled when EnablePersistence
	// returns an error matching it; everything decodable was restored.
	ErrPartialRestore = errors.New("rsi: some persisted state could not be restored")
	// ErrInvalidPolicyCodec is returned by RegisterPolicyCodec.
	ErrInvalidPolicyCodec = errors.New("rsi: invalid policy codec")
)

// PolicyCodec persists a custom Policy implementation.
type PolicyCodec interface {
	// Kind is the stable identifier stored alongside the params.
	Kind() string
	// Encode reports ok=false when p is not a type this codec handles;
	// otherwise it returns p's params as JSON.
	Encode(p Policy) (params json.RawMessage, ok bool, err error)
	// Decode rebuilds a policy from params produced by Encode.
	Decode(params json.RawMessage) (Policy, error)
}

// RegisterPolicyCodec adds a codec for a custom Policy type. Register codecs
// before EnablePersistence so restored policies of that kind can be decoded.
// Kinds must be unique and must not collide with the built-in kinds.
func (o *RSIOrchestrator) RegisterPolicyCodec(c PolicyCodec) error {
	if o == nil {
		return ErrNilOrchestrator
	}
	if c == nil {
		return fmt.Errorf("%w: nil codec", ErrInvalidPolicyCodec)
	}
	kind := c.Kind()
	if kind == "" {
		return fmt.Errorf("%w: empty kind", ErrInvalidPolicyCodec)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, existing := range o.codecsLocked() {
		if existing.Kind() == kind {
			return fmt.Errorf("%w: kind %q already registered", ErrInvalidPolicyCodec, kind)
		}
	}
	o.codecs = append(o.codecs, c)
	return nil
}

// codecsLocked returns built-in codecs followed by custom ones. Caller holds mu.
func (o *RSIOrchestrator) codecsLocked() []PolicyCodec {
	out := make([]PolicyCodec, 0, len(builtinCodecs)+len(o.codecs))
	out = append(out, builtinCodecs...)
	return append(out, o.codecs...)
}

// PersistErrors returns the number of failed write-throughs since start.
func (o *RSIOrchestrator) PersistErrors() uint64 {
	if o == nil {
		return 0
	}
	return o.persistErrs.Load()
}

// LastPersistError returns the most recent write-through failure, or nil.
func (o *RSIOrchestrator) LastPersistError() error {
	if o == nil {
		return nil
	}
	if p := o.lastPersistErr.Load(); p != nil {
		return p.err
	}
	return nil
}

// DeployedPolicies returns clones of the policy RSI currently considers
// deployed for each dimension (including policies restored by
// EnablePersistence). The map is empty when nothing is deployed.
func (o *RSIOrchestrator) DeployedPolicies() map[HeadroomDimension]Policy {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make(map[HeadroomDimension]Policy, len(o.deployedByDim))
	for dim, p := range o.deployedByDim {
		if p != nil {
			out[dim] = p.Clone()
		}
	}
	return out
}

type persistError struct{ err error }

func (o *RSIOrchestrator) notePersistError(err error) {
	if err == nil {
		return
	}
	o.persistErrs.Add(1)
	o.lastPersistErr.Store(&persistError{err: err})
}

// policyRecord is the stored form of a Policy. Kind is empty when no codec
// could encode the policy (it is then kept for diagnostics only).
type policyRecord struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

type persistedDeployment struct {
	Dimension string        `json:"dimension"`
	CycleID   int           `json:"cycle_id"`
	Policy    policyRecord  `json:"policy"`
	Previous  *policyRecord `json:"previous,omitempty"`
}

type persistedState struct {
	Version     int                     `json:"version"`
	SavedAt     time.Time               `json:"saved_at"`
	CycleID     int                     `json:"cycle_id"`
	Stats       RSIStats                `json:"stats"`
	ByDimension map[string]policyRecord `json:"by_dimension,omitempty"`
	Deployments []persistedDeployment   `json:"deployments,omitempty"`
}

func cycleKey(id int) string { return fmt.Sprintf("%012d", id) }

func encodePolicy(p Policy, codecs []PolicyCodec) (rec policyRecord, err error) {
	rec.Name = policyName(p)
	defer func() {
		if r := recover(); r != nil {
			rec.Kind, rec.Params = "", nil
			err = fmt.Errorf("rsi: encoding %s panicked: %v", rec.Name, r)
		}
	}()
	for _, c := range codecs {
		raw, ok, encErr := c.Encode(p)
		if !ok {
			continue
		}
		if encErr != nil {
			return rec, fmt.Errorf("rsi: encode %s policy: %w", c.Kind(), encErr)
		}
		if !json.Valid(raw) {
			return rec, fmt.Errorf("rsi: codec %q produced invalid JSON", c.Kind())
		}
		rec.Kind = c.Kind()
		rec.Params = raw
		return rec, nil
	}
	return rec, fmt.Errorf("rsi: no PolicyCodec for %s; it will not survive a restart", rec.Name)
}

func decodePolicy(rec policyRecord, codecs []PolicyCodec) (p Policy, err error) {
	defer func() {
		if r := recover(); r != nil {
			p, err = nil, fmt.Errorf("decoding %q policy panicked: %v", rec.Kind, r)
		}
	}()
	if rec.Kind == "" {
		return nil, fmt.Errorf("policy %s was stored without a codec", rec.Name)
	}
	for _, c := range codecs {
		if c.Kind() != rec.Kind {
			continue
		}
		p, err := c.Decode(rec.Params)
		if err != nil {
			return nil, fmt.Errorf("decode %q policy: %w", rec.Kind, err)
		}
		if p == nil {
			return nil, fmt.Errorf("decode %q policy: codec returned nil", rec.Kind)
		}
		return p, nil
	}
	return nil, fmt.Errorf("unknown policy kind %q (%s)", rec.Kind, rec.Name)
}

// ---------------------------------------------------------------------------
// Write-through
// ---------------------------------------------------------------------------

// stateSnapshot captures what saveStateLocked needs while mu is held; policies
// are encoded after mu is released (they are never mutated in place).
type stateSnapshot struct {
	cycleID     int
	stats       RSIStats
	byDim       map[HeadroomDimension]Policy
	deployments []deploymentRecord
	codecs      []PolicyCodec
}

func (o *RSIOrchestrator) snapshotState() stateSnapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	snap := stateSnapshot{
		cycleID:     o.cycleID,
		stats:       o.stats,
		byDim:       make(map[HeadroomDimension]Policy, len(o.deployedByDim)),
		deployments: append([]deploymentRecord(nil), o.deployments...),
		codecs:      o.codecsLocked(),
	}
	for dim, p := range o.deployedByDim {
		snap.byDim[dim] = p
	}
	return snap
}

func buildStateDoc(snap stateSnapshot) (persistedState, []error) {
	var errs []error
	encode := func(p Policy) policyRecord {
		rec, err := encodePolicy(p, snap.codecs)
		if err != nil {
			errs = append(errs, err)
		}
		return rec
	}
	st := persistedState{
		Version: persistStateVersion,
		SavedAt: time.Now().UTC(),
		CycleID: snap.cycleID,
		Stats:   snap.stats,
	}
	// Runtime-only gauges are never persisted.
	st.Stats.CycleInProgress = false
	st.Stats.HistorySize = 0
	st.Stats.DeployedPolicy = ""
	st.Stats.Persistent = false
	st.Stats.PersistErrors = 0
	if len(snap.byDim) > 0 {
		st.ByDimension = make(map[string]policyRecord, len(snap.byDim))
		for dim, p := range snap.byDim {
			if p != nil {
				st.ByDimension[string(dim)] = encode(p)
			}
		}
	}
	for _, d := range snap.deployments {
		pd := persistedDeployment{Dimension: string(d.dimension), CycleID: d.cycleID, Policy: encode(d.policy)}
		if d.previous != nil {
			prev := encode(d.previous)
			pd.Previous = &prev
		}
		st.Deployments = append(st.Deployments, pd)
	}
	return st, errs
}

// saveStateLocked writes the state document. Caller holds persistMu and ps
// is non-nil.
func (o *RSIOrchestrator) saveStateLocked() {
	doc, encErrs := buildStateDoc(o.snapshotState())
	for _, err := range encErrs {
		o.notePersistError(err)
	}
	if err := o.ps.Put(PersistBucketState, persistKeyState, doc); err != nil {
		o.notePersistError(fmt.Errorf("rsi: persist state: %w", err))
	}
}

func (o *RSIOrchestrator) deleteCyclesLocked(ids []int) {
	for _, id := range ids {
		if err := o.ps.Delete(PersistBucketCycles, cycleKey(id)); err != nil {
			o.notePersistError(fmt.Errorf("rsi: delete cycle %d: %w", id, err))
		}
	}
}

// persistState writes the state document when persistence is enabled.
func (o *RSIOrchestrator) persistState() {
	o.persistMu.Lock()
	defer o.persistMu.Unlock()
	if o.ps == nil {
		return
	}
	o.saveStateLocked()
}

// persistCycle writes a recorded cycle, deletes trimmed ones and saves state.
func (o *RSIOrchestrator) persistCycle(c RSICycle, removed []int) {
	o.persistCycles([]RSICycle{c}, removed)
}

func (o *RSIOrchestrator) persistCycles(cycles []RSICycle, removed []int) {
	o.persistMu.Lock()
	defer o.persistMu.Unlock()
	if o.ps == nil {
		return
	}
	dropped := make(map[int]bool, len(removed))
	for _, id := range removed {
		dropped[id] = true
	}
	for _, c := range cycles {
		if dropped[c.ID] {
			continue
		}
		if err := o.ps.Put(PersistBucketCycles, cycleKey(c.ID), c); err != nil {
			o.notePersistError(fmt.Errorf("rsi: persist cycle %d: %w", c.ID, err))
		}
	}
	o.deleteCyclesLocked(removed)
	o.saveStateLocked()
}

// persistRollback rewrites the rolled-back cycle (RolledBack=true) and state.
func (o *RSIOrchestrator) persistRollback(cycleID int) {
	o.persistMu.Lock()
	defer o.persistMu.Unlock()
	if o.ps == nil {
		return
	}
	var (
		c     RSICycle
		found bool
	)
	o.mu.RLock()
	for i := range o.cycles {
		if o.cycles[i].ID == cycleID {
			c, found = o.cycles[i].clone(), true
			break
		}
	}
	o.mu.RUnlock()
	if found {
		if err := o.ps.Put(PersistBucketCycles, cycleKey(c.ID), c); err != nil {
			o.notePersistError(fmt.Errorf("rsi: persist cycle %d: %w", c.ID, err))
		}
	}
	o.saveStateLocked()
}

// persistConfig writes the current config (read under persistMu so the
// stored value always matches memory) and deletes trimmed cycles.
func (o *RSIOrchestrator) persistConfig(removed []int) {
	o.persistMu.Lock()
	defer o.persistMu.Unlock()
	if o.ps == nil {
		return
	}
	o.mu.RLock()
	cfg := o.config
	o.mu.RUnlock()
	if err := o.ps.Put(PersistBucketState, persistKeyConfig, cfg); err != nil {
		o.notePersistError(fmt.Errorf("rsi: persist config: %w", err))
	}
	if len(removed) > 0 {
		o.deleteCyclesLocked(removed)
	}
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// EnablePersistence attaches ps, restores the state a previous process
// persisted there and writes every later change through to it (see the
// comment at the top of persistence.go for the layout).
//
// Call it once, after RegisterPolicyCodec/SetHooks and before the first
// cycle (it returns ErrPersistAfterStart once cycles or deployments exist).
// It is safe to call while Run is active but before a cycle completed: it
// waits for an in-flight cycle and blocks new ones until the restore is done.
//
// Restore semantics:
//   - history is restored and bounded by MaxHistory (excess documents are
//     deleted); the cycle ID counter continues after the highest stored ID;
//   - cumulative stats are restored;
//   - deployed policies and the rollback stack are restored WITHOUT calling
//     OnDeploy/OnRollback (see DeployedPolicies to re-apply them);
//   - a config previously applied through SetConfig is restored, except that
//     DryRun is the logical OR of the stored and the constructor value: a
//     restart can switch deployment off but never silently on.
//
// A store read failure is fatal: persistence stays disabled and the error is
// returned. Undecodable or unknown documents are skipped; persistence is
// then enabled and the returned error wraps ErrPartialRestore.
func (o *RSIOrchestrator) EnablePersistence(ps persist.Store) error {
	if o == nil {
		return ErrNilOrchestrator
	}
	if ps == nil {
		return errors.New("rsi: nil persist store")
	}
	// Lock order: cycleMu -> persistMu -> mu.
	o.cycleMu.Lock()
	defer o.cycleMu.Unlock()
	o.persistMu.Lock()
	defer o.persistMu.Unlock()
	if o.ps != nil {
		return ErrPersistenceEnabled
	}

	o.mu.RLock()
	fresh := len(o.cycles) == 0 && o.cycleID == 0 && len(o.deployments) == 0 && len(o.deployedByDim) == 0
	cfg := o.config
	codecs := o.codecsLocked()
	o.mu.RUnlock()
	if !fresh {
		return ErrPersistAfterStart
	}

	var problems []error

	// 1. Config applied at runtime through SetConfig.
	configRestored := false
	var stored RSIConfig
	found, err := ps.Get(PersistBucketState, persistKeyConfig, &stored)
	switch {
	case err != nil && !found:
		return fmt.Errorf("rsi: load config: %w", err)
	case err != nil:
		problems = append(problems, fmt.Errorf("config: %w", err))
	case found:
		stored = stored.withDefaults()
		if vErr := stored.Validate(); vErr != nil {
			problems = append(problems, fmt.Errorf("config: %w", vErr))
		} else {
			stored.DryRun = stored.DryRun || cfg.DryRun
			cfg = stored
			configRestored = true
		}
	}

	// 2. State document.
	var st persistedState
	found, err = ps.Get(PersistBucketState, persistKeyState, &st)
	stateOK := found && err == nil
	switch {
	case err != nil && !found:
		return fmt.Errorf("rsi: load state: %w", err)
	case err != nil:
		problems = append(problems, fmt.Errorf("state: %w", err))
		st = persistedState{}
	case found && st.Version != persistStateVersion:
		problems = append(problems, fmt.Errorf("state: unsupported version %d", st.Version))
		st, stateOK = persistedState{}, false
	}

	// 3. Cycle history.
	type storedCycle struct {
		key   string
		cycle RSICycle
	}
	var (
		cycles  []storedCycle
		badKeys []string
	)
	err = ps.ForEach(PersistBucketCycles, func(key string, raw json.RawMessage) error {
		var c RSICycle
		if uErr := json.Unmarshal(raw, &c); uErr != nil || c.ID <= 0 {
			badKeys = append(badKeys, key)
			return nil
		}
		cycles = append(cycles, storedCycle{key: key, cycle: c})
		return nil
	})
	if err != nil {
		return fmt.Errorf("rsi: load cycles: %w", err)
	}
	if len(badKeys) > 0 {
		problems = append(problems, fmt.Errorf("skipped %d undecodable cycle documents (%s)", len(badKeys), joinKeys(badKeys)))
	}
	sort.SliceStable(cycles, func(i, j int) bool { return cycles[i].cycle.ID < cycles[j].cycle.ID })
	// Keep one document per ID (the last in key order wins).
	dedup := cycles[:0]
	for _, c := range cycles {
		if n := len(dedup); n > 0 && dedup[n-1].cycle.ID == c.cycle.ID {
			dedup[n-1] = c
			continue
		}
		dedup = append(dedup, c)
	}
	cycles = dedup
	maxHistory := cfg.sanitized().MaxHistory
	if excess := len(cycles) - maxHistory; excess > 0 {
		for _, c := range cycles[:excess] {
			if dErr := ps.Delete(PersistBucketCycles, c.key); dErr != nil {
				problems = append(problems, fmt.Errorf("trim cycle %d: %w", c.cycle.ID, dErr))
			}
		}
		cycles = cycles[excess:]
	}
	history := make([]RSICycle, len(cycles))
	maxID := 0
	for i, c := range cycles {
		history[i] = c.cycle
		if c.cycle.ID > maxID {
			maxID = c.cycle.ID
		}
	}

	// 4. Deployed policies and the rollback stack.
	byDim := make(map[HeadroomDimension]Policy, len(st.ByDimension))
	for dim, rec := range st.ByDimension {
		if dim == "" {
			problems = append(problems, errors.New("deployed policy with empty dimension skipped"))
			continue
		}
		p, dErr := decodePolicy(rec, codecs)
		if dErr != nil {
			problems = append(problems, fmt.Errorf("deployed %s policy skipped: %w", dim, dErr))
			continue
		}
		byDim[HeadroomDimension(dim)] = p
	}
	deployments := make([]deploymentRecord, 0, len(st.Deployments))
	for i, d := range st.Deployments {
		rec := deploymentRecord{dimension: HeadroomDimension(d.Dimension), cycleID: d.CycleID}
		var dErr error
		if d.Dimension == "" {
			dErr = errors.New("empty dimension")
		}
		if dErr == nil {
			rec.policy, dErr = decodePolicy(d.Policy, codecs)
		}
		if dErr == nil && d.Previous != nil {
			rec.previous, dErr = decodePolicy(*d.Previous, codecs)
		}
		if dErr != nil {
			// Rolling back past an entry we cannot rebuild would restore the
			// wrong state, so the stack keeps only the entries above it.
			problems = append(problems, fmt.Errorf("rollback stack truncated at entry %d (cycle %d): %w", i, d.CycleID, dErr))
			deployments = deployments[:0]
			continue
		}
		deployments = append(deployments, rec)
	}
	if len(deployments) > maxDeploymentStack {
		deployments = deployments[len(deployments)-maxDeploymentStack:]
	}

	// 5. Apply.
	o.mu.Lock()
	if configRestored {
		o.config = cfg
	}
	o.cycles = history
	o.cycleID = max(st.CycleID, maxID)
	rejected := o.stats.CyclesRejected
	if stateOK {
		o.stats = st.Stats
		o.stats.CycleInProgress = false
		o.stats.HistorySize = 0
		o.stats.DeployedPolicy = ""
		o.stats.Persistent = false
		o.stats.PersistErrors = 0
	} else if n := len(history); n > 0 {
		last := history[n-1]
		o.stats = RSIStats{LastCycleID: last.ID, LastCycleAt: last.Timestamp, LastCycleDuration: last.DurationMs}
	}
	o.stats.CyclesRejected += rejected
	o.deployedByDim = byDim
	o.deployments = deployments
	o.deployed = nil
	if n := len(deployments); n > 0 {
		o.deployed = deployments[n-1].policy
	}
	o.mu.Unlock()

	o.ps = ps
	o.persistOn.Store(true)
	if configRestored {
		select {
		case o.wake <- struct{}{}:
		default:
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %w", ErrPartialRestore, errors.Join(problems...))
	}
	return nil
}

func joinKeys(keys []string) string {
	const maxShown = 10
	out := ""
	for i, k := range keys {
		if i == maxShown {
			return out + ", ..." + strconv.Itoa(len(keys)-maxShown) + " more"
		}
		if i > 0 {
			out += ", "
		}
		out += strconv.Quote(k)
	}
	return out
}

// ---------------------------------------------------------------------------
// Built-in policy codecs
// ---------------------------------------------------------------------------

// jsonFloat maps non-finite values (which JSON cannot encode) to values the
// policies treat identically: NaN -> 0, ±Inf -> ±MaxFloat64.
func jsonFloat(v float64) float64 {
	switch {
	case math.IsNaN(v):
		return 0
	case math.IsInf(v, 1):
		return math.MaxFloat64
	case math.IsInf(v, -1):
		return -math.MaxFloat64
	}
	return v
}

type funcCodec struct {
	kind string
	enc  func(Policy) (any, bool, error)
	dec  func(json.RawMessage) (Policy, error)
}

func (c funcCodec) Kind() string { return c.kind }

func (c funcCodec) Encode(p Policy) (json.RawMessage, bool, error) {
	v, ok, err := c.enc(p)
	if !ok || err != nil {
		return nil, ok, err
	}
	raw, err := json.Marshal(v)
	return raw, true, err
}

func (c funcCodec) Decode(raw json.RawMessage) (Policy, error) { return c.dec(raw) }

var errNilPolicy = errors.New("nil policy")

type routingPolicyDoc struct {
	Weights      map[string]float64 `json:"weights"`
	CacheEnabled bool               `json:"cache_enabled"`
	CostAware    bool               `json:"cost_aware"`
	Seed         int64              `json:"seed"`
}

type cachePolicyDoc struct {
	CacheableThreshold float64 `json:"cacheable_threshold"`
	MaxCacheSizeMB     int     `json:"max_cache_size_mb"`
	Seed               int64   `json:"seed"`
}

type guardrailPolicyDoc struct {
	InjectionSensitivity float64 `json:"injection_sensitivity"`
	PIISensitivity       float64 `json:"pii_sensitivity"`
	BudgetLimitUSD       float64 `json:"budget_limit_usd"`
	Seed                 int64   `json:"seed"`
}

type agentWorkflowPolicyDoc struct {
	MaxTools    int   `json:"max_tools"`
	MaxDepth    int   `json:"max_depth"`
	UsePlanning bool  `json:"use_planning"`
	Seed        int64 `json:"seed"`
}

func decodeDoc[T any](raw json.RawMessage) (T, error) {
	var v T
	if len(raw) == 0 {
		return v, errors.New("missing params")
	}
	err := json.Unmarshal(raw, &v)
	return v, err
}

var builtinCodecs = []PolicyCodec{
	funcCodec{
		kind: PolicyKindRouting,
		enc: func(p Policy) (any, bool, error) {
			rp, ok := p.(*RoutingPolicy)
			if !ok {
				return nil, false, nil
			}
			if rp == nil {
				return nil, true, errNilPolicy
			}
			// Non-finite weights are ignored by selectProvider; drop them.
			weights := make(map[string]float64, len(rp.Weights))
			for k, w := range rp.Weights {
				if isFinite(w) {
					weights[k] = w
				}
			}
			return routingPolicyDoc{Weights: weights, CacheEnabled: rp.CacheEnabled, CostAware: rp.CostAware, Seed: rp.seed}, true, nil
		},
		dec: func(raw json.RawMessage) (Policy, error) {
			d, err := decodeDoc[routingPolicyDoc](raw)
			if err != nil {
				return nil, err
			}
			if d.Weights == nil {
				d.Weights = map[string]float64{}
			}
			return &RoutingPolicy{Weights: d.Weights, CacheEnabled: d.CacheEnabled, CostAware: d.CostAware, seed: d.Seed}, nil
		},
	},
	funcCodec{
		kind: PolicyKindCache,
		enc: func(p Policy) (any, bool, error) {
			cp, ok := p.(*CachePolicy)
			if !ok {
				return nil, false, nil
			}
			if cp == nil {
				return nil, true, errNilPolicy
			}
			return cachePolicyDoc{CacheableThreshold: jsonFloat(cp.CacheableThreshold), MaxCacheSizeMB: cp.MaxCacheSizeMB, Seed: cp.seed}, true, nil
		},
		dec: func(raw json.RawMessage) (Policy, error) {
			d, err := decodeDoc[cachePolicyDoc](raw)
			if err != nil {
				return nil, err
			}
			return &CachePolicy{CacheableThreshold: d.CacheableThreshold, MaxCacheSizeMB: d.MaxCacheSizeMB, seed: d.Seed}, nil
		},
	},
	funcCodec{
		kind: PolicyKindGuardrail,
		enc: func(p Policy) (any, bool, error) {
			gp, ok := p.(*GuardrailPolicy)
			if !ok {
				return nil, false, nil
			}
			if gp == nil {
				return nil, true, errNilPolicy
			}
			return guardrailPolicyDoc{
				InjectionSensitivity: jsonFloat(gp.InjectionSensitivity),
				PIISensitivity:       jsonFloat(gp.PIISensitivity),
				BudgetLimitUSD:       jsonFloat(gp.BudgetLimitUSD),
				Seed:                 gp.seed,
			}, true, nil
		},
		dec: func(raw json.RawMessage) (Policy, error) {
			d, err := decodeDoc[guardrailPolicyDoc](raw)
			if err != nil {
				return nil, err
			}
			return &GuardrailPolicy{
				InjectionSensitivity: d.InjectionSensitivity,
				PIISensitivity:       d.PIISensitivity,
				BudgetLimitUSD:       d.BudgetLimitUSD,
				seed:                 d.Seed,
			}, nil
		},
	},
	funcCodec{
		kind: PolicyKindAgentWorkflow,
		enc: func(p Policy) (any, bool, error) {
			ap, ok := p.(*AgentWorkflowPolicy)
			if !ok {
				return nil, false, nil
			}
			if ap == nil {
				return nil, true, errNilPolicy
			}
			return agentWorkflowPolicyDoc{MaxTools: ap.MaxTools, MaxDepth: ap.MaxDepth, UsePlanning: ap.UsePlanning, Seed: ap.seed}, true, nil
		},
		dec: func(raw json.RawMessage) (Policy, error) {
			d, err := decodeDoc[agentWorkflowPolicyDoc](raw)
			if err != nil {
				return nil, err
			}
			return &AgentWorkflowPolicy{MaxTools: d.MaxTools, MaxDepth: d.MaxDepth, UsePlanning: d.UsePlanning, seed: d.Seed}, nil
		},
	},
}
