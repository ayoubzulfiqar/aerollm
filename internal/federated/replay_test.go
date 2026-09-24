package federated

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// fakeClock is a mutable clock for tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// failingStore wraps a Memory store and can be told to fail writes.
type failingStore struct {
	*persist.Memory
	failPut    atomic.Bool
	failDelete atomic.Bool
}

func newFailingStore() *failingStore { return &failingStore{Memory: persist.NewMemory()} }

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.failPut.Load() {
		return errors.New("disk full")
	}
	return f.Memory.Put(bucket, key, v)
}

func (f *failingStore) Delete(bucket, key string) error {
	if f.failDelete.Load() {
		return errors.New("disk full")
	}
	return f.Memory.Delete(bucket, key)
}

type testNode struct {
	id   string
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
}

func newTestNodes(t *testing.T, reg *GatewayRegistry, ids ...string) []testNode {
	t.Helper()
	out := make([]testNode, 0, len(ids))
	for _, id := range ids {
		pub, priv, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		require.NoError(t, reg.Register(context.Background(), &NodeRegistration{NodeID: id, PublicKey: pub}))
		out = append(out, testNode{id: id, pub: pub, priv: priv})
	}
	return out
}

func newTestAggregator(t *testing.T, reg *GatewayRegistry, clk *fakeClock) *SecureAggregator {
	t.Helper()
	agg, err := NewSecureAggregator(reg, SecureAggregatorConfig{Now: clk.Now, MaxClockSkew: time.Minute})
	require.NoError(t, err)
	return agg
}

func (n testNode) sign(t *testing.T, round string, seq uint64, ts time.Time, data ...float64) *SignedUpdate {
	t.Helper()
	su, err := NewSignedUpdate(n.priv, &LoRAMatrix{Rows: 1, Cols: len(data), Data: data, Owner: n.id}, round, seq, ts)
	require.NoError(t, err)
	return su
}

func TestDataDigestIsLanguageNeutral(t *testing.T) {
	m := &LoRAMatrix{Rows: 1, Cols: 2, Data: []float64{1, -0.5}}
	raw, _ := hex.DecodeString("0000000000000001" + "0000000000000002" + "3ff0000000000000" + "bfe0000000000000")
	want := sha256.Sum256(raw)
	require.Equal(t, hex.EncodeToString(want[:]), m.DataDigest())

	// Large matrices cross the internal buffer boundary.
	big := &LoRAMatrix{Rows: 1, Cols: 1000, Data: make([]float64, 1000)}
	for i := range big.Data {
		big.Data[i] = float64(i)
	}
	h := sha256.New()
	var b [8]byte
	put := func(u uint64) {
		for i := 7; i >= 0; i-- {
			b[i] = byte(u)
			u >>= 8
		}
		h.Write(b[:])
	}
	put(1)
	put(1000)
	for _, v := range big.Data {
		put(math.Float64bits(v))
	}
	require.Equal(t, hex.EncodeToString(h.Sum(nil)), big.DataDigest())

	// Negative zero is distinguished from zero.
	require.NotEqual(t, (&LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{0}}).DataDigest(),
		(&LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{math.Copysign(0, -1)}}).DataDigest())
}

func TestSignedUpdatePayloadFormat(t *testing.T) {
	m := &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "node-1"}
	su := &SignedUpdate{Update: m, RoundID: "r1", Sequence: 7, TimestampMs: 1234}
	want := "aerollm/federated/update/v2\nnode=node-1\nround=r1\nseq=7\nts_ms=1234\nrows=1\ncols=1\ndigest=" + m.DataDigest()
	require.Equal(t, want, string(SignedUpdatePayload(su)))
	require.Nil(t, SignedUpdatePayload(nil))
}

func TestSignedUpdateValidate(t *testing.T) {
	ok := func() *SignedUpdate {
		return &SignedUpdate{Update: &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "n1"}, RoundID: "r1", Sequence: 1, TimestampMs: 1}
	}
	require.NoError(t, ok().Validate())
	cases := map[string]func(*SignedUpdate){
		"no matrix":      func(s *SignedUpdate) { s.Update = nil },
		"no owner":       func(s *SignedUpdate) { s.Update.Owner = "" },
		"newline owner":  func(s *SignedUpdate) { s.Update.Owner = "n1\nround=r2" },
		"bad round":      func(s *SignedUpdate) { s.RoundID = "r 1" },
		"empty round":    func(s *SignedUpdate) { s.RoundID = "" },
		"zero seq":       func(s *SignedUpdate) { s.Sequence = 0 },
		"zero timestamp": func(s *SignedUpdate) { s.TimestampMs = 0 },
		"bad matrix":     func(s *SignedUpdate) { s.Update.Cols = 3 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			su := ok()
			mut(su)
			require.Error(t, su.Validate())
		})
	}
	var nilSU *SignedUpdate
	require.ErrorIs(t, nilSU.Validate(), ErrInvalidSignedUpdate)
	_, priv, _ := ed25519.GenerateKey(nil)
	require.Error(t, ok().Sign(nil))
	require.NoError(t, ok().Sign(priv))
}

func TestNewSecureAggregatorRequiresResolver(t *testing.T) {
	_, err := NewSecureAggregator(nil, SecureAggregatorConfig{})
	require.ErrorIs(t, err, ErrVerificationNotConfigured)
	_, err = NewSecureAggregator(NewGatewayRegistry(), SecureAggregatorConfig{MaxClockSkew: -1})
	require.Error(t, err)

	var nilAgg *SecureAggregator
	require.ErrorIs(t, nilAgg.Submit(context.Background(), nil), ErrVerificationNotConfigured)
	require.ErrorIs(t, nilAgg.OpenRound("r1"), ErrVerificationNotConfigured)
	_, err = nilAgg.AggregateRound(context.Background())
	require.ErrorIs(t, err, ErrVerificationNotConfigured)
	require.False(t, nilAgg.Round().Open)
}

func TestSecureAggregatorRoundLifecycle(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2", "n3")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	ctx := context.Background()

	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2)), ErrNoOpenRound)
	_, err := agg.AggregateRound(ctx)
	require.ErrorIs(t, err, ErrNoOpenRound)

	require.ErrorIs(t, agg.OpenRound("bad round"), ErrInvalidRoundID)
	require.NoError(t, agg.OpenRound("r1"))
	_, err = agg.AggregateRound(ctx)
	require.ErrorIs(t, err, ErrNoUpdates, "an empty round cannot be finalized")

	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2)))
	require.NoError(t, agg.Submit(ctx, nodes[1].sign(t, "r1", 10, clk.Now(), 3, 4)))
	require.NoError(t, agg.Submit(ctx, nodes[2].sign(t, "r1", 99, clk.Now(), 5, 6)))
	st := agg.Round()
	require.True(t, st.Open)
	require.Equal(t, "r1", st.RoundID)
	require.Equal(t, []string{"n1", "n2", "n3"}, st.Contributors)
	require.Equal(t, 2, st.Cols)

	// A round with contributions cannot be replaced.
	require.ErrorIs(t, agg.OpenRound("r2"), ErrRoundInProgress)

	res, err := agg.AggregateRound(ctx)
	require.NoError(t, err)
	require.Equal(t, "r1", res.RoundID)
	require.Equal(t, []string{"n1", "n2", "n3"}, res.Contributors)
	require.InDeltaSlice(t, []float64{3, 4}, res.Aggregate.Data, 1e-12)
	require.False(t, agg.Round().Open)

	// Round IDs cannot be reopened.
	require.ErrorIs(t, agg.OpenRound("r1"), ErrRoundReused)

	// An empty round can be replaced, and aborting discards contributions.
	require.NoError(t, agg.OpenRound("r2"))
	require.NoError(t, agg.OpenRound("r3"))
	require.ErrorIs(t, agg.OpenRound("r2"), ErrRoundReused)
	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r3", 2, clk.Now(), 1, 1)))
	n, err := agg.AbortRound()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = agg.AbortRound()
	require.ErrorIs(t, err, ErrNoOpenRound)
}

func TestSecureAggregatorRejectsReplays(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	ctx := context.Background()

	require.NoError(t, agg.OpenRound("r1"))
	su := nodes[0].sign(t, "r1", 5, clk.Now(), 1)
	require.NoError(t, agg.Submit(ctx, su))

	// Exact replay in the same round.
	require.ErrorIs(t, agg.Submit(ctx, su), ErrReplay)
	// A fresh sequence in the same round is still a second contribution.
	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 6, clk.Now(), 2)), ErrAlreadyContributed)

	_, err := agg.AggregateRound(ctx)
	require.NoError(t, err)
	require.NoError(t, agg.OpenRound("r2"))

	// The old update is bound to r1: stale.
	require.ErrorIs(t, agg.Submit(ctx, su), ErrStaleRound)
	// Re-signing for r2 with an old sequence is a replay.
	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r2", 5, clk.Now(), 1)), ErrReplay)
	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r2", 4, clk.Now(), 1)), ErrReplay)
	// A strictly greater sequence is accepted (gaps are allowed).
	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r2", 1000, clk.Now(), 1)))

	st, ok := agg.NodeState("n1")
	require.True(t, ok)
	require.Equal(t, uint64(1000), st.LastSequence)
	require.Equal(t, "r2", st.LastRound)
}

func TestSecureAggregatorTimestampWindow(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk) // skew 1m
	ctx := context.Background()
	require.NoError(t, agg.OpenRound("r1"))
	now := clk.Now()

	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, now.Add(-2*time.Minute), 1)), ErrTimestampOutOfWindow)
	require.ErrorIs(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 2, now.Add(2*time.Minute), 1)), ErrTimestampOutOfWindow)

	// A signed update captured and delayed past the window is rejected.
	delayed := nodes[0].sign(t, "r1", 3, now, 1)
	clk.Advance(90 * time.Second)
	require.ErrorIs(t, agg.Submit(ctx, delayed), ErrTimestampOutOfWindow)

	// Within the window it is accepted; rejections did not consume sequences.
	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, clk.Now().Add(-30*time.Second), 1)))
}

func TestSecureAggregatorRejectsUpdatesPredatingRound(t *testing.T) {
	// With a monotonic clock the skew window already implies this bound;
	// it matters when the gateway clock steps backwards (e.g. NTP
	// correction) after a round was opened.
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk) // skew 1m
	require.NoError(t, agg.OpenRound("r1"))
	clk.Advance(-30 * time.Minute)
	require.ErrorIs(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", 1, clk.Now(), 1)), ErrTimestampOutOfWindow)
	clk.Advance(30 * time.Minute)
	require.NoError(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", 1, clk.Now(), 1)))
}

func TestSecureAggregatorAuthentication(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2")
	require.NoError(t, reg.Register(context.Background(), &NodeRegistration{NodeID: "keyless"}))
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	ctx := context.Background()
	require.NoError(t, agg.OpenRound("r1"))

	// n2 cannot sign on behalf of n1.
	forged, err := NewSignedUpdate(nodes[1].priv, &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "n1"}, "r1", 1, clk.Now())
	require.NoError(t, err)
	require.ErrorIs(t, agg.Submit(ctx, forged), ErrInvalidSignature)

	// Unknown and keyless owners fail closed.
	stranger, _ := NewSignedUpdate(nodes[0].priv, &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "stranger"}, "r1", 1, clk.Now())
	require.ErrorIs(t, agg.Submit(ctx, stranger), ErrUnknownOwner)
	keyless, _ := NewSignedUpdate(nodes[0].priv, &LoRAMatrix{Rows: 1, Cols: 1, Data: []float64{1}, Owner: "keyless"}, "r1", 1, clk.Now())
	require.ErrorIs(t, agg.Submit(ctx, keyless), ErrUnknownOwner)

	// Every signed field is bound: tampering invalidates the signature.
	tamper := map[string]func(*SignedUpdate){
		"data":      func(s *SignedUpdate) { s.Update.Data[0] = 42 },
		"sequence":  func(s *SignedUpdate) { s.Sequence++ },
		"timestamp": func(s *SignedUpdate) { s.TimestampMs++ },
		"signature": func(s *SignedUpdate) { s.Signature[0] ^= 0xff },
		"short sig": func(s *SignedUpdate) { s.Signature = s.Signature[:10] },
		"no sig":    func(s *SignedUpdate) { s.Signature = nil },
	}
	for name, mut := range tamper {
		t.Run(name, func(t *testing.T) {
			su := nodes[0].sign(t, "r1", 1, clk.Now(), 1)
			mut(su)
			require.ErrorIs(t, agg.Submit(ctx, su), ErrInvalidSignature)
		})
	}
	// A round rebinding is caught (update signed for r0 relabelled as r1).
	relabelled := nodes[0].sign(t, "r0", 1, clk.Now(), 1)
	relabelled.RoundID = "r1"
	require.ErrorIs(t, agg.Submit(ctx, relabelled), ErrInvalidSignature)

	// Failed attempts do not consume the sequence.
	_, ok := agg.NodeState("n1")
	require.False(t, ok)
	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, clk.Now(), 1)))
}

func TestSecureAggregatorShapeAndLimits(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2", "n3")
	clk := newFakeClock()
	agg, err := NewSecureAggregator(reg, SecureAggregatorConfig{Now: clk.Now, MaxContributors: 2})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, agg.OpenRound("r1"))
	require.NoError(t, agg.Submit(ctx, nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2)))
	require.ErrorIs(t, agg.Submit(ctx, nodes[1].sign(t, "r1", 1, clk.Now(), 1, 2, 3)), ErrDimensionMismatch)
	require.NoError(t, agg.Submit(ctx, nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4)))
	require.ErrorIs(t, agg.Submit(ctx, nodes[2].sign(t, "r1", 1, clk.Now(), 5, 6)), ErrRoundFull)
}

func TestSecureAggregatorBatchIsAtomic(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2", "n3")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	ctx := context.Background()
	require.NoError(t, agg.OpenRound("r1"))

	good1 := nodes[0].sign(t, "r1", 1, clk.Now(), 1, 1)
	good2 := nodes[1].sign(t, "r1", 1, clk.Now(), 3, 3)
	bad := nodes[2].sign(t, "r1", 1, clk.Now(), 5, 5)
	bad.Signature[3] ^= 1

	require.ErrorIs(t, agg.SubmitBatch(ctx, []*SignedUpdate{good1, good2, bad}), ErrInvalidSignature)
	require.Empty(t, agg.Round().Contributors)
	_, ok := agg.NodeState("n1")
	require.False(t, ok, "a failed batch must not consume sequences")

	require.ErrorIs(t, agg.SubmitBatch(ctx, []*SignedUpdate{good1, good1}), ErrAlreadyContributed)
	require.ErrorIs(t, agg.SubmitBatch(ctx, nil), ErrNoUpdates)
	require.ErrorIs(t, agg.SubmitBatch(ctx, []*SignedUpdate{good1, nodes[2].sign(t, "r1", 2, clk.Now(), 1)}), ErrDimensionMismatch)

	require.NoError(t, agg.SubmitBatch(ctx, []*SignedUpdate{good1, good2}))
	res, err := agg.AggregateRound(ctx)
	require.NoError(t, err)
	require.InDeltaSlice(t, []float64{2, 2}, res.Aggregate.Data, 1e-12)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, agg.OpenRound("r2"))
	require.ErrorIs(t, agg.SubmitBatch(cctx, []*SignedUpdate{nodes[2].sign(t, "r2", 9, clk.Now(), 1, 1)}), context.Canceled)
}

func TestSecureAggregatorConcurrentDuplicates(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	require.NoError(t, agg.OpenRound("r1"))
	su := nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2, 3)

	var wg sync.WaitGroup
	var accepted atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if agg.Submit(context.Background(), su) == nil {
				accepted.Add(1)
			}
			_ = agg.Round()
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), accepted.Load())
}

func TestSecureAggregatorRunningMeanMatchesFedAvg(t *testing.T) {
	reg := NewGatewayRegistry()
	ids := make([]string, 25)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%02d", i)
	}
	nodes := newTestNodes(t, reg, ids...)
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	require.NoError(t, agg.OpenRound("r1"))

	rng := rand.New(rand.NewSource(1))
	var plain []*LoRAMatrix
	for i, n := range nodes {
		data := make([]float64, 6)
		for j := range data {
			data[j] = rng.NormFloat64() * 1e6
		}
		if i == 0 {
			data[0] = math.MaxFloat64
		}
		if i == 1 {
			data[0] = -math.MaxFloat64
		}
		plain = append(plain, &LoRAMatrix{Rows: 2, Cols: 3, Data: append([]float64(nil), data...)})
		su, err := NewSignedUpdate(n.priv, &LoRAMatrix{Rows: 2, Cols: 3, Data: data, Owner: n.id}, "r1", 1, clk.Now())
		require.NoError(t, err)
		require.NoError(t, agg.Submit(context.Background(), su))
	}
	want, err := NewFedAvgAggregator().Aggregate(context.Background(), plain)
	require.NoError(t, err)
	res, err := agg.AggregateRound(context.Background())
	require.NoError(t, err)
	for j := range want.Data {
		require.False(t, math.IsInf(res.Aggregate.Data[j], 0))
		if j == 0 {
			// Averaging +MaxFloat64 and -MaxFloat64 with ordinary values is
			// ill-conditioned: any evaluation order (or fused multiply-add
			// on arm64) is only accurate relative to the input magnitude.
			require.InDelta(t, want.Data[j], res.Aggregate.Data[j], 1e-9*math.MaxFloat64, "index %d", j)
			continue
		}
		require.InEpsilon(t, want.Data[j], res.Aggregate.Data[j], 1e-9, "index %d", j)
	}
}

func TestSecureAggregatorPersistenceSurvivesRestart(t *testing.T) {
	stores := map[string]func(t *testing.T) persist.Store{
		"memory": func(t *testing.T) persist.Store { return persist.NewMemory() },
		"bolt": func(t *testing.T) persist.Store {
			b, err := persist.OpenBolt(filepath.Join(t.TempDir(), "fed.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			return b
		},
	}
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			ps := mk(t)
			reg := NewGatewayRegistry()
			nodes := newTestNodes(t, reg, "n1", "n2")
			clk := newFakeClock()
			ctx := context.Background()

			agg := newTestAggregator(t, reg, clk)
			require.NoError(t, agg.EnablePersistence(ps))
			require.Error(t, agg.EnablePersistence(ps), "enabling twice is rejected")
			require.NoError(t, agg.OpenRound("r1"))
			su1 := nodes[0].sign(t, "r1", 7, clk.Now(), 1)
			require.NoError(t, agg.Submit(ctx, su1))
			_, err := agg.AggregateRound(ctx)
			require.NoError(t, err)
			require.NoError(t, agg.OpenRound("r2"))
			su2 := nodes[1].sign(t, "r2", 3, clk.Now(), 1)
			require.NoError(t, agg.Submit(ctx, su2))

			// Restart.
			agg2 := newTestAggregator(t, reg, clk)
			require.NoError(t, agg2.EnablePersistence(ps))
			st := agg2.Round()
			require.True(t, st.Open)
			require.Equal(t, "r2", st.RoundID)
			require.Empty(t, st.Contributors, "partial aggregates are not persisted")

			require.ErrorIs(t, agg2.Submit(ctx, su2), ErrReplay, "sequence survives restart")
			require.ErrorIs(t, agg2.Submit(ctx, nodes[0].sign(t, "r2", 7, clk.Now(), 1)), ErrReplay)
			require.NoError(t, agg2.Submit(ctx, nodes[1].sign(t, "r2", 4, clk.Now(), 1)))
			_, err = agg2.AggregateRound(ctx)
			require.NoError(t, err)
			require.ErrorIs(t, agg2.OpenRound("r1"), ErrRoundReused, "used rounds survive restart")
			require.ErrorIs(t, agg2.OpenRound("r2"), ErrRoundReused)

			// Using an aggregator before enabling persistence is refused.
			agg3 := newTestAggregator(t, reg, clk)
			require.NoError(t, agg3.OpenRound("zz"))
			require.Error(t, agg3.EnablePersistence(ps))

			// ResetNode is written through.
			require.NoError(t, agg2.ResetNode("n1"))
			agg4 := newTestAggregator(t, reg, clk)
			require.NoError(t, agg4.EnablePersistence(ps))
			_, ok := agg4.NodeState("n1")
			require.False(t, ok)
			st2, ok := agg4.NodeState("n2")
			require.True(t, ok)
			require.Equal(t, uint64(4), st2.LastSequence)
			require.False(t, agg4.Round().Open)
		})
	}
}

func TestSecureAggregatorPersistenceFailsClosed(t *testing.T) {
	ps := newFailingStore()
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	require.NoError(t, agg.EnablePersistence(ps))
	require.NoError(t, agg.OpenRound("r1"))

	ps.failPut.Store(true)
	require.ErrorIs(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", 1, clk.Now(), 1)), ErrPersistence)
	require.Empty(t, agg.Round().Contributors)
	_, ok := agg.NodeState("n1")
	require.False(t, ok)
	require.ErrorIs(t, agg.OpenRound("r9"), ErrPersistence)

	ps.failPut.Store(false)
	require.NoError(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", 1, clk.Now(), 1)))
	ps.failPut.Store(true)
	_, err := agg.AggregateRound(context.Background())
	require.ErrorIs(t, err, ErrPersistence)
	require.True(t, agg.Round().Open, "round stays open when closing cannot be persisted")
	ps.failPut.Store(false)
	// r9 was never persisted and can still be used later.
	_, err = agg.AggregateRound(context.Background())
	require.NoError(t, err)
	require.NoError(t, agg.OpenRound("r9"))

	ps.failDelete.Store(true)
	require.ErrorIs(t, agg.ResetNode("n1"), ErrPersistence)
	_, ok = agg.NodeState("n1")
	require.True(t, ok)
}

func TestSecureAggregatorCorruptReplayStateQuarantinesNode(t *testing.T) {
	ps := persist.NewMemory()
	require.NoError(t, ps.Put(bucketReplay, "n1", "not an object"))
	require.NoError(t, ps.Put(bucketReplay, "n2", NodeReplayState{LastSequence: 5}))
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	err := agg.EnablePersistence(ps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "n1")

	require.NoError(t, agg.OpenRound("r1"))
	require.ErrorIs(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", math.MaxUint64-1, clk.Now(), 1)), ErrReplay)
	require.ErrorIs(t, agg.Submit(context.Background(), nodes[1].sign(t, "r1", 5, clk.Now(), 1)), ErrReplay)
	require.NoError(t, agg.Submit(context.Background(), nodes[1].sign(t, "r1", 6, clk.Now(), 1)))

	require.NoError(t, agg.ResetNode("n1"))
	require.NoError(t, agg.Submit(context.Background(), nodes[0].sign(t, "r1", 1, clk.Now(), 1)))

	// A corrupt round document refuses to start.
	bad := persist.NewMemory()
	require.NoError(t, bad.Put(bucketRounds, roundsKey, []int{1}))
	require.Error(t, newTestAggregator(t, reg, clk).EnablePersistence(bad))
}
