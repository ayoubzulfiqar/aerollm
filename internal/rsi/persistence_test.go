package rsi

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPolicyCodec lets the test-only mockPolicy be persisted.
type mockPolicyCodec struct{}

type mockPolicyDoc struct {
	Provider  string  `json:"provider"`
	Cached    bool    `json:"cached"`
	ErrorFlag bool    `json:"error_flag"`
	Latency   float64 `json:"latency"`
	Cost      float64 `json:"cost"`
}

func (mockPolicyCodec) Kind() string { return "test_mock" }

func (mockPolicyCodec) Encode(p Policy) (json.RawMessage, bool, error) {
	m, ok := p.(*mockPolicy)
	if !ok {
		return nil, false, nil
	}
	raw, err := json.Marshal(mockPolicyDoc{Provider: m.provider, Cached: m.cached, ErrorFlag: m.errorFlag, Latency: m.latency, Cost: m.cost})
	return raw, true, err
}

func (mockPolicyCodec) Decode(raw json.RawMessage) (Policy, error) {
	var d mockPolicyDoc
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &mockPolicy{provider: d.Provider, cached: d.Cached, errorFlag: d.ErrorFlag, latency: d.Latency, cost: d.Cost}, nil
}

// backends returns store factories: open yields the first handle and a
// reopen func that simulates a process restart.
func backends() map[string]func(t *testing.T) (persist.Store, func() persist.Store) {
	return map[string]func(t *testing.T) (persist.Store, func() persist.Store){
		"memory": func(t *testing.T) (persist.Store, func() persist.Store) {
			m := persist.NewMemory()
			return m, func() persist.Store { return m }
		},
		"bolt": func(t *testing.T) (persist.Store, func() persist.Store) {
			path := filepath.Join(t.TempDir(), "rsi.db")
			first, err := persist.OpenBolt(path)
			require.NoError(t, err)
			current := first
			t.Cleanup(func() { _ = current.Close() })
			return first, func() persist.Store {
				require.NoError(t, current.Close())
				next, err := persist.OpenBolt(path)
				require.NoError(t, err)
				current = next
				return next
			}
		},
	}
}

func countDocs(t *testing.T, s persist.Store, bucket string) int {
	t.Helper()
	n := 0
	require.NoError(t, s.ForEach(bucket, func(string, json.RawMessage) error { n++; return nil }))
	return n
}

func cyclesJSON(t *testing.T, cs []RSICycle) string {
	t.Helper()
	b, err := json.Marshal(cs)
	require.NoError(t, err)
	return string(b)
}

func mustEncode(t *testing.T, p Policy) policyRecord {
	t.Helper()
	rec, err := encodePolicy(p, builtinCodecs)
	require.NoError(t, err)
	return rec
}

func TestPersistence_RestartRestoresHistoryDeploymentAndStats(t *testing.T) {
	ctx := context.Background()
	for name, open := range backends() {
		t.Run(name, func(t *testing.T) {
			store, reopen := open(t)

			orch1 := deployableOrchestrator(t, nil)
			require.NoError(t, orch1.RegisterPolicyCodec(mockPolicyCodec{}))
			require.NoError(t, orch1.EnablePersistence(store))
			var deploys1 atomic.Int32
			orch1.SetHooks(
				func(context.Context, Policy, RSICycle) error { deploys1.Add(1); return nil },
				func(context.Context, Policy, RSICycle) error { return nil },
				nil,
			)

			c1, err := orch1.RunCycle(ctx)
			require.NoError(t, err)
			require.True(t, c1.Deployed)
			c2, err := orch1.RunCycle(ctx)
			require.NoError(t, err)
			require.False(t, c2.Deployed, "the deployed policy is the new baseline")
			require.NoError(t, orch1.Rollback(ctx))
			c3, err := orch1.RunCycle(ctx)
			require.NoError(t, err)
			require.True(t, c3.Deployed)
			require.EqualValues(t, 2, deploys1.Load())

			wantCycles := cyclesJSON(t, orch1.Cycles())
			wantStats := orch1.Stats()
			wantPolicy := orch1.DeployedPolicy().(*mockPolicy)
			assert.True(t, wantStats.Persistent)
			assert.Zero(t, orch1.PersistErrors(), "%v", orch1.LastPersistError())

			// --- restart ---
			store2 := reopen()
			orch2 := deployableOrchestrator(t, nil)
			require.NoError(t, orch2.RegisterPolicyCodec(mockPolicyCodec{}))
			var deploys2 atomic.Int32
			var restored []Policy
			var restoredCycle RSICycle
			orch2.SetHooks(
				func(context.Context, Policy, RSICycle) error { deploys2.Add(1); return nil },
				func(_ context.Context, p Policy, c RSICycle) error {
					restored = append(restored, p)
					restoredCycle = c
					return nil
				},
				nil,
			)
			require.NoError(t, orch2.EnablePersistence(store2))
			assert.Zero(t, deploys2.Load(), "restoring must not call the deploy hook")

			assert.JSONEq(t, wantCycles, cyclesJSON(t, orch2.Cycles()))
			assert.True(t, orch2.Cycles()[0].RolledBack)

			got, ok := orch2.DeployedPolicy().(*mockPolicy)
			require.True(t, ok)
			assert.Equal(t, *wantPolicy, *got)
			assert.Len(t, orch2.DeployedPolicies(), 1)

			s := orch2.Stats()
			assert.Equal(t, wantStats.CyclesRun, s.CyclesRun)
			assert.Equal(t, wantStats.Deployments, s.Deployments)
			assert.Equal(t, wantStats.Rollbacks, s.Rollbacks)
			assert.Equal(t, wantStats.CandidatesRejected, s.CandidatesRejected)
			assert.Equal(t, wantStats.LastCycleID, s.LastCycleID)
			assert.True(t, wantStats.LastCycleAt.Equal(s.LastCycleAt))
			assert.True(t, wantStats.LastDeployedAt.Equal(s.LastDeployedAt))
			assert.Equal(t, wantStats.DeployedPolicy, s.DeployedPolicy)
			assert.Equal(t, 3, s.HistorySize)
			assert.True(t, s.Persistent)
			assert.False(t, s.CycleInProgress)

			// The restored policy is the baseline: the next cycle continues
			// the numbering and does not redeploy it.
			c4, err := orch2.RunCycle(ctx)
			require.NoError(t, err)
			assert.Equal(t, 4, c4.ID)
			assert.False(t, c4.Deployed)
			assert.Zero(t, deploys2.Load())

			// The restored rollback stack still works: cycle 3 had nothing
			// deployed before it (cycle 1 was rolled back).
			require.NoError(t, orch2.Rollback(ctx))
			require.Len(t, restored, 1)
			assert.Nil(t, restored[0])
			assert.Equal(t, 3, restoredCycle.ID)
			assert.Nil(t, orch2.DeployedPolicy())
			require.ErrorIs(t, orch2.Rollback(ctx), ErrNothingToRollback)

			// And that rollback was written through.
			orch3 := deployableOrchestrator(t, nil)
			require.NoError(t, orch3.RegisterPolicyCodec(mockPolicyCodec{}))
			require.NoError(t, orch3.EnablePersistence(reopen()))
			assert.Nil(t, orch3.DeployedPolicy())
			assert.Empty(t, orch3.DeployedPolicies())
			assert.Len(t, orch3.Cycles(), 4)
			assert.True(t, orch3.Cycles()[2].RolledBack)
			assert.Equal(t, uint64(2), orch3.Stats().Rollbacks)
		})
	}
}

func TestPersistence_RestoresRollbackStackWithPreviousPolicy(t *testing.T) {
	store := persist.NewMemory()
	p1 := &RoutingPolicy{Weights: map[string]float64{"openai": 0.9, "anthropic": 0.1}, CacheEnabled: true, seed: 11}
	p2 := &RoutingPolicy{Weights: map[string]float64{"openai": 0.2, "anthropic": 0.8}, CostAware: true, seed: 22}
	prev := mustEncode(t, p1)
	require.NoError(t, store.Put(PersistBucketState, persistKeyState, persistedState{
		Version:     persistStateVersion,
		CycleID:     7,
		ByDimension: map[string]policyRecord{string(DimensionRouting): mustEncode(t, p2)},
		Deployments: []persistedDeployment{
			{Dimension: string(DimensionRouting), CycleID: 5, Policy: mustEncode(t, p1)},
			{Dimension: string(DimensionRouting), CycleID: 7, Policy: mustEncode(t, p2), Previous: &prev},
		},
		Stats: RSIStats{Deployments: 2, LastCycleID: 7, CycleInProgress: true},
	}))

	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	var calls []Policy
	var cycleIDs []int
	orch.SetHooks(nil, func(_ context.Context, p Policy, c RSICycle) error {
		calls = append(calls, p)
		cycleIDs = append(cycleIDs, c.ID)
		return nil
	}, nil)
	require.NoError(t, orch.EnablePersistence(store))

	assert.False(t, orch.CycleInProgress(), "a persisted in-progress flag must not survive a restart")
	assert.Equal(t, uint64(2), orch.Stats().Deployments)
	assert.True(t, reflect.DeepEqual(p2, orch.DeployedPolicy()))
	assert.True(t, reflect.DeepEqual(p2, orch.DeployedPolicies()[DimensionRouting]))
	// Exploration starts from the restored policy.
	assert.True(t, reflect.DeepEqual(p2, orch.currentPolicyClone(DimensionRouting)))

	require.NoError(t, orch.Rollback(context.Background()))
	require.NoError(t, orch.Rollback(context.Background()))
	require.ErrorIs(t, orch.Rollback(context.Background()), ErrNothingToRollback)
	require.Len(t, calls, 2)
	assert.True(t, reflect.DeepEqual(p1, calls[0]), "first rollback restores the previous policy")
	assert.Nil(t, calls[1])
	assert.Equal(t, []int{7, 5}, cycleIDs)
	assert.Nil(t, orch.DeployedPolicy())
	assert.Empty(t, orch.DeployedPolicies())

	// Next cycle ID continues after the persisted counter.
	c, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 8, c.ID)
}

func TestPersistence_UnknownAndCorruptDocumentsAreSkipped(t *testing.T) {
	store := persist.NewMemory()
	good := &RoutingPolicy{Weights: map[string]float64{"openai": 1}, seed: 3}
	require.NoError(t, store.Put(PersistBucketCycles, cycleKey(1), RSICycle{ID: 1, DeployDecision: DecisionBelowThreshold}))
	require.NoError(t, store.Put(PersistBucketCycles, cycleKey(2), "not a cycle"))
	require.NoError(t, store.Put(PersistBucketCycles, "zzz", RSICycle{ID: 0}))
	require.NoError(t, store.Put(PersistBucketCycles, cycleKey(3), RSICycle{ID: 3, Deployed: true, DeployDecision: DecisionDeployed}))
	alien := policyRecord{Kind: "alien", Name: "*x.Alien", Params: json.RawMessage(`{}`)}
	require.NoError(t, store.Put(PersistBucketState, persistKeyState, persistedState{
		Version: persistStateVersion,
		CycleID: 3,
		ByDimension: map[string]policyRecord{
			string(DimensionRouting): mustEncode(t, good),
			string(DimensionCache):   alien,
		},
		Deployments: []persistedDeployment{
			{Dimension: string(DimensionCache), CycleID: 1, Policy: alien},
			{Dimension: string(DimensionRouting), CycleID: 3, Policy: mustEncode(t, good)},
		},
	}))

	orch := deployableOrchestrator(t, nil)
	err := orch.EnablePersistence(store)
	require.ErrorIs(t, err, ErrPartialRestore)
	assert.Contains(t, err.Error(), `unknown policy kind "alien"`)
	assert.Contains(t, err.Error(), "2 undecodable cycle documents")
	assert.True(t, orch.Stats().Persistent, "partial restore still enables persistence")

	var ids []int
	for _, c := range orch.Cycles() {
		ids = append(ids, c.ID)
	}
	assert.Equal(t, []int{1, 3}, ids)
	pols := orch.DeployedPolicies()
	require.Len(t, pols, 1)
	assert.True(t, reflect.DeepEqual(good, pols[DimensionRouting]))
	assert.True(t, reflect.DeepEqual(good, orch.DeployedPolicy()))

	// The stack was truncated above the undecodable entry: one rollback left.
	orch.SetHooks(nil, func(context.Context, Policy, RSICycle) error { return nil }, nil)
	require.NoError(t, orch.Rollback(context.Background()))
	require.ErrorIs(t, orch.Rollback(context.Background()), ErrNothingToRollback)

	// Write-through continues after a partial restore.
	c, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, c.ID)
	var stored RSICycle
	found, err := store.Get(PersistBucketCycles, cycleKey(4), &stored)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, c.DeployDecision, stored.DeployDecision)
}

func TestPersistence_CorruptOrUnsupportedStateDocument(t *testing.T) {
	for name, state := range map[string]any{
		"corrupt":     "garbage",
		"new_version": persistedState{Version: persistStateVersion + 1, CycleID: 99},
	} {
		t.Run(name, func(t *testing.T) {
			store := persist.NewMemory()
			ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			require.NoError(t, store.Put(PersistBucketState, persistKeyState, state))
			require.NoError(t, store.Put(PersistBucketCycles, cycleKey(5), RSICycle{ID: 5, Timestamp: ts, DurationMs: 12}))

			orch := newTestOrchestratorNoData(DefaultRSIConfig())
			require.ErrorIs(t, orch.EnablePersistence(store), ErrPartialRestore)
			s := orch.Stats()
			assert.Equal(t, 5, s.LastCycleID)
			assert.True(t, ts.Equal(s.LastCycleAt))
			assert.Equal(t, 1, s.HistorySize)
			assert.Nil(t, orch.DeployedPolicy())

			c, err := orch.RunCycle(context.Background())
			require.NoError(t, err)
			assert.Equal(t, 6, c.ID, "the counter continues after the highest stored cycle")
		})
	}
}

// faultyStore wraps a Store and fails selected operations.
type faultyStore struct {
	persist.Store
	failGet, failForEach, failPut, failDelete atomic.Bool
}

var errInjected = errors.New("injected store failure")

func (f *faultyStore) Get(bucket, key string, v any) (bool, error) {
	if f.failGet.Load() {
		return false, errInjected
	}
	return f.Store.Get(bucket, key, v)
}

func (f *faultyStore) ForEach(bucket string, fn func(string, json.RawMessage) error) error {
	if f.failForEach.Load() {
		return errInjected
	}
	return f.Store.ForEach(bucket, fn)
}

func (f *faultyStore) Put(bucket, key string, v any) error {
	if f.failPut.Load() {
		return errInjected
	}
	return f.Store.Put(bucket, key, v)
}

func (f *faultyStore) Delete(bucket, key string) error {
	if f.failDelete.Load() {
		return errInjected
	}
	return f.Store.Delete(bucket, key)
}

func TestPersistence_ReadFailureIsFatalAndRetryable(t *testing.T) {
	for _, mode := range []string{"get", "foreach"} {
		t.Run(mode, func(t *testing.T) {
			fs := &faultyStore{Store: persist.NewMemory()}
			if mode == "get" {
				fs.failGet.Store(true)
			} else {
				fs.failForEach.Store(true)
			}
			orch := newTestOrchestratorNoData(DefaultRSIConfig())
			err := orch.EnablePersistence(fs)
			require.ErrorIs(t, err, errInjected)
			assert.NotErrorIs(t, err, ErrPartialRestore)
			assert.False(t, orch.Stats().Persistent)

			// Nothing was attached, so a retry is possible.
			fs.failGet.Store(false)
			fs.failForEach.Store(false)
			require.NoError(t, orch.EnablePersistence(fs))
			assert.True(t, orch.Stats().Persistent)
		})
	}
}

func TestPersistence_WriteFailuresAreCountedNotFatal(t *testing.T) {
	fs := &faultyStore{Store: persist.NewMemory()}
	orch := deployableOrchestrator(t, nil)
	require.NoError(t, orch.RegisterPolicyCodec(mockPolicyCodec{}))
	require.NoError(t, orch.EnablePersistence(fs))
	orch.OnDeploy = func(context.Context, Policy, RSICycle) error { return nil }
	assert.Nil(t, orch.LastPersistError())

	fs.failPut.Store(true)
	c, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.True(t, c.Deployed, "the cycle itself is unaffected by store failures")
	assert.NotZero(t, orch.PersistErrors())
	assert.Equal(t, orch.PersistErrors(), orch.Stats().PersistErrors)
	require.ErrorIs(t, orch.LastPersistError(), errInjected)
	require.NoError(t, orch.SetConfig(deployableConfig()))
}

func TestPersistence_EnableOrdering(t *testing.T) {
	var nilOrch *RSIOrchestrator
	require.ErrorIs(t, nilOrch.EnablePersistence(persist.NewMemory()), ErrNilOrchestrator)

	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	require.Error(t, orch.EnablePersistence(nil))
	require.NoError(t, orch.EnablePersistence(persist.NewMemory()))
	require.ErrorIs(t, orch.EnablePersistence(persist.NewMemory()), ErrPersistenceEnabled)

	started := newTestOrchestratorNoData(DefaultRSIConfig())
	_, err := started.RunCycle(context.Background())
	require.NoError(t, err)
	require.ErrorIs(t, started.EnablePersistence(persist.NewMemory()), ErrPersistAfterStart)
	assert.False(t, started.Stats().Persistent)
}

func TestPersistence_EnableWhileRunLoopActive(t *testing.T) {
	store := persist.NewMemory()
	require.NoError(t, store.Put(PersistBucketCycles, cycleKey(41), RSICycle{ID: 41}))
	cfg := DefaultRSIConfig()
	cfg.CycleInterval = time.Hour
	orch := newTestOrchestratorNoData(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { orch.Run(ctx); close(done) }()
	require.NoError(t, orch.EnablePersistence(store))
	cancel()
	<-done
	c, err := orch.RunCycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 42, c.ID)
}

func TestPersistence_HistoryBoundedOnLoadAndWrite(t *testing.T) {
	store := persist.NewMemory()
	for id := 1; id <= 10; id++ {
		require.NoError(t, store.Put(PersistBucketCycles, cycleKey(id), RSICycle{ID: id}))
	}
	cfg := DefaultRSIConfig()
	cfg.MaxHistory = 3
	orch := newTestOrchestratorNoData(cfg)
	require.NoError(t, orch.EnablePersistence(store))

	var ids []int
	for _, c := range orch.Cycles() {
		ids = append(ids, c.ID)
	}
	assert.Equal(t, []int{8, 9, 10}, ids)
	assert.Equal(t, 3, countDocs(t, store, PersistBucketCycles), "excess documents are deleted on load")

	for i := 0; i < 2; i++ {
		_, err := orch.RunCycle(context.Background())
		require.NoError(t, err)
	}
	assert.Equal(t, 3, countDocs(t, store, PersistBucketCycles), "write-through keeps the store bounded")
	ok, err := store.Get(PersistBucketCycles, cycleKey(12), &RSICycle{})
	require.NoError(t, err)
	assert.True(t, ok)

	// Shrinking MaxHistory at runtime trims the store too.
	cfg.MaxHistory = 1
	require.NoError(t, orch.SetConfig(cfg))
	assert.Equal(t, 1, countDocs(t, store, PersistBucketCycles))

	// RestoreHistory writes through as well.
	cfg.MaxHistory = 10
	require.NoError(t, orch.SetConfig(cfg))
	orch.RestoreHistory([]RSICycle{{ID: 2}})
	ok, err = store.Get(PersistBucketCycles, cycleKey(2), &RSICycle{})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestPersistence_RuntimeConfigSurvivesRestartButNeverEnablesDeploy(t *testing.T) {
	store := persist.NewMemory()
	live := DefaultRSIConfig()
	live.DryRun = false
	orch1 := newTestOrchestratorNoData(live)
	require.NoError(t, orch1.EnablePersistence(store))
	changed := live
	changed.CycleInterval = 42 * time.Second
	changed.MaxHistory = 7
	changed.ImprovementThresholdPct = 12.5
	require.NoError(t, orch1.SetConfig(changed))

	// Restarted in dry-run mode: runtime tuning is kept, dry run wins.
	dry := DefaultRSIConfig()
	dry.DryRun = true
	orch2 := newTestOrchestratorNoData(dry)
	require.NoError(t, orch2.EnablePersistence(store))
	got := orch2.Config()
	assert.Equal(t, 42*time.Second, got.CycleInterval)
	assert.Equal(t, 7, got.MaxHistory)
	assert.Equal(t, 12.5, got.ImprovementThresholdPct)
	assert.True(t, got.DryRun)

	// A stored dry_run=true is honoured even when the constructor says deploy.
	dry.CycleInterval = time.Minute
	require.NoError(t, orch2.SetConfig(dry))
	orch3 := newTestOrchestratorNoData(live)
	require.NoError(t, orch3.EnablePersistence(store))
	assert.True(t, orch3.Config().DryRun)
	assert.Equal(t, time.Minute, orch3.Config().CycleInterval)

	// An invalid stored config is reported and ignored.
	require.NoError(t, store.Put(PersistBucketState, persistKeyConfig, map[string]any{"k_fold": 1}))
	orch4 := newTestOrchestratorNoData(live)
	require.ErrorIs(t, orch4.EnablePersistence(store), ErrPartialRestore)
	assert.Equal(t, live.CycleInterval, orch4.Config().CycleInterval)
}

func TestPersistence_BuiltinPolicyCodecsRoundTrip(t *testing.T) {
	policies := []Policy{
		&RoutingPolicy{Weights: map[string]float64{"openai": 0.4, "local": 0.6}, CacheEnabled: true, CostAware: true, seed: 42},
		&CachePolicy{CacheableThreshold: 0.37, MaxCacheSizeMB: 256, seed: -7},
		&GuardrailPolicy{InjectionSensitivity: 0.2, PIISensitivity: 0.9, BudgetLimitUSD: 0.05, seed: 99},
		&AgentWorkflowPolicy{MaxTools: 4, MaxDepth: 2, UsePlanning: true, seed: 5},
	}
	for _, dim := range AllDimensions() {
		policies = append(policies, defaultPolicyFor(dim))
	}
	for _, p := range policies {
		rec, err := encodePolicy(p, builtinCodecs)
		require.NoError(t, err)
		require.NotEmpty(t, rec.Kind)
		raw, err := json.Marshal(rec)
		require.NoError(t, err)
		var back policyRecord
		require.NoError(t, json.Unmarshal(raw, &back))
		decoded, err := decodePolicy(back, builtinCodecs)
		require.NoError(t, err)
		assert.True(t, reflect.DeepEqual(p, decoded), "%s: %#v != %#v", rec.Kind, p, decoded)
		// The seed survives, so exploration stays deterministic.
		assert.True(t, reflect.DeepEqual(p.Mutate(), decoded.Mutate()))
	}

	// Non-finite values are stored as equivalent finite values.
	rec, err := encodePolicy(&GuardrailPolicy{InjectionSensitivity: math.NaN(), BudgetLimitUSD: math.Inf(1)}, builtinCodecs)
	require.NoError(t, err)
	_, err = json.Marshal(rec)
	require.NoError(t, err)
	rec, err = encodePolicy(&RoutingPolicy{Weights: map[string]float64{"a": math.NaN(), "b": 1}}, builtinCodecs)
	require.NoError(t, err)
	p, err := decodePolicy(rec, builtinCodecs)
	require.NoError(t, err)
	assert.Equal(t, map[string]float64{"b": 1}, p.(*RoutingPolicy).Weights)

	// Failures.
	_, err = encodePolicy(&mockPolicy{}, builtinCodecs)
	require.ErrorContains(t, err, "no PolicyCodec")
	_, err = encodePolicy((*RoutingPolicy)(nil), builtinCodecs)
	require.Error(t, err)
	_, err = decodePolicy(policyRecord{Kind: PolicyKindCache, Params: json.RawMessage(`[1]`)}, builtinCodecs)
	require.Error(t, err)
	_, err = decodePolicy(policyRecord{Kind: PolicyKindCache}, builtinCodecs)
	require.Error(t, err)
	_, err = decodePolicy(policyRecord{Name: "x"}, builtinCodecs)
	require.ErrorContains(t, err, "without a codec")
}

type panicCodec struct{ kind string }

func (c panicCodec) Kind() string { return c.kind }
func (panicCodec) Encode(Policy) (json.RawMessage, bool, error) {
	panic("encode boom")
}
func (panicCodec) Decode(json.RawMessage) (Policy, error) { panic("decode boom") }

func TestPersistence_RegisterPolicyCodec(t *testing.T) {
	orch := newTestOrchestratorNoData(DefaultRSIConfig())
	require.ErrorIs(t, orch.RegisterPolicyCodec(nil), ErrInvalidPolicyCodec)
	require.ErrorIs(t, orch.RegisterPolicyCodec(panicCodec{}), ErrInvalidPolicyCodec)
	require.ErrorIs(t, orch.RegisterPolicyCodec(panicCodec{kind: PolicyKindRouting}), ErrInvalidPolicyCodec)
	require.NoError(t, orch.RegisterPolicyCodec(mockPolicyCodec{}))
	require.ErrorIs(t, orch.RegisterPolicyCodec(mockPolicyCodec{}), ErrInvalidPolicyCodec)
	var nilOrch *RSIOrchestrator
	require.ErrorIs(t, nilOrch.RegisterPolicyCodec(mockPolicyCodec{}), ErrNilOrchestrator)

	// Panicking codecs are contained.
	_, err := encodePolicy(&mockPolicy{}, []PolicyCodec{panicCodec{kind: "p"}})
	require.ErrorContains(t, err, "panicked")
	_, err = decodePolicy(policyRecord{Kind: "p"}, []PolicyCodec{panicCodec{kind: "p"}})
	require.ErrorContains(t, err, "panicked")
}

func TestPersistence_PolicyWithoutCodecIsReportedAndNotRestored(t *testing.T) {
	store := persist.NewMemory()
	orch1 := deployableOrchestrator(t, nil) // mockPolicy, no codec registered
	require.NoError(t, orch1.EnablePersistence(store))
	orch1.OnDeploy = func(context.Context, Policy, RSICycle) error { return nil }
	c, err := orch1.RunCycle(context.Background())
	require.NoError(t, err)
	require.True(t, c.Deployed, "missing codecs never block a deployment")
	assert.NotZero(t, orch1.PersistErrors())
	require.ErrorContains(t, orch1.LastPersistError(), "no PolicyCodec")

	orch2 := deployableOrchestrator(t, nil)
	err = orch2.EnablePersistence(store)
	require.ErrorIs(t, err, ErrPartialRestore)
	assert.Nil(t, orch2.DeployedPolicy())
	assert.Empty(t, orch2.DeployedPolicies())
	require.Len(t, orch2.Cycles(), 1, "history is still restored")
	assert.Equal(t, uint64(1), orch2.Stats().Deployments)
}

func TestPersistence_ConcurrentUseIsRaceFree(t *testing.T) {
	store := persist.NewMemory()
	orch := deployableOrchestrator(t, nil)
	require.NoError(t, orch.RegisterPolicyCodec(mockPolicyCodec{}))
	require.NoError(t, orch.EnablePersistence(store))
	orch.SetHooks(
		func(context.Context, Policy, RSICycle) error { return nil },
		func(context.Context, Policy, RSICycle) error { return nil },
		nil,
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				_, _ = orch.RunCycle(ctx)
				_ = orch.Rollback(ctx)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cfg := deployableConfig()
		for j := 0; j < 20; j++ {
			cfg.MaxHistory = 5 + j%3
			_ = orch.SetConfig(cfg)
			_ = orch.Stats()
			_ = orch.Cycles()
			_ = orch.DeployedPolicies()
			_ = orch.LastPersistError()
		}
	}()
	wg.Wait()
	require.Zero(t, orch.PersistErrors(), "%v", orch.LastPersistError())

	restarted := deployableOrchestrator(t, nil)
	require.NoError(t, restarted.RegisterPolicyCodec(mockPolicyCodec{}))
	require.NoError(t, restarted.EnablePersistence(store))
	assert.JSONEq(t, cyclesJSON(t, orch.Cycles()), cyclesJSON(t, restarted.Cycles()))
	assert.Equal(t, orch.Stats().Deployments, restarted.Stats().Deployments)
	assert.Equal(t, orch.Stats().Rollbacks, restarted.Stats().Rollbacks)
	assert.Equal(t, orch.Config(), restarted.Config())
	if p := orch.DeployedPolicy(); p != nil {
		assert.Equal(t, *p.(*mockPolicy), *restarted.DeployedPolicy().(*mockPolicy))
	} else {
		assert.Nil(t, restarted.DeployedPolicy())
	}
}
