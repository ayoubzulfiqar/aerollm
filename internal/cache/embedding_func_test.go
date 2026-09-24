package cache

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// hashingFunc embeds with the deterministic hashing embedder, standing in
// for a real /v1/embeddings backend.
func hashingFunc(dims int) EmbeddingFunc {
	h := NewHashingEmbedder(dims)
	return func(ctx context.Context, text string) ([]float64, error) {
		return h.Vector(text), ctx.Err()
	}
}

func TestEmbeddingFuncBacksSemanticCache(t *testing.T) {
	c := NewVectorSemanticCache("sem", time.Minute, 0.95, EmbeddingFunc(hashingFunc(256)))
	ctx := context.Background()
	if err := c.UpsertNS(ctx, "t", "k", "what is the capital of france", []byte("paris"), nil); err != nil {
		t.Fatal(err)
	}
	hit, err := c.SearchNS(ctx, "t", "what is the capital of france")
	if err != nil || hit == nil || string(hit.Response) != "paris" {
		t.Fatalf("expected hit, got %+v %v", hit, err)
	}
}

func TestEmbeddingFailuresDegradeToMiss(t *testing.T) {
	failing := EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		return nil, errors.New("embeddings backend down")
	})
	c := NewVectorSemanticCache("sem", time.Minute, 0.9, failing)
	ctx := context.Background()
	if err := c.UpsertNS(ctx, "t", "k", "prompt", []byte("resp"), nil); err != nil {
		t.Fatalf("store must be skipped silently, got %v", err)
	}
	if hit, err := c.SearchNS(ctx, "t", "prompt"); hit != nil || err != nil {
		t.Fatalf("lookup must be a plain miss, got %+v %v", hit, err)
	}
	st := c.Snapshot()
	if st.EmbedErrors != 2 || st.Misses != 1 || st.Entries != 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
	// Invalid vectors are failures too.
	for name, fn := range map[string]EmbeddingFunc{
		"empty": func(context.Context, string) ([]float64, error) { return nil, nil },
		"nan":   func(context.Context, string) ([]float64, error) { return []float64{math.NaN(), 1}, nil },
		"zero":  func(context.Context, string) ([]float64, error) { return []float64{0, 0}, nil },
		"huge": func(context.Context, string) ([]float64, error) {
			return make([]float64, DefaultEmbeddingMaxDimensions+1), nil
		},
		"panic": func(context.Context, string) ([]float64, error) { panic("boom") },
	} {
		_, err := fn.Embedding(ctx, &models.EmbeddingRequest{Input: "x"})
		if !errors.Is(err, ErrEmbeddingUnavailable) {
			t.Errorf("%s: expected ErrEmbeddingUnavailable, got %v", name, err)
		}
	}
}

func TestEmbeddingAdapterInputCapAndTimeout(t *testing.T) {
	var calls atomic.Int32
	slow := EmbeddingFunc(func(ctx context.Context, text string) ([]float64, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	a := NewEmbeddingAdapter(slow, EmbeddingAdapterOptions{Timeout: 20 * time.Millisecond, MaxInputBytes: 10, FailureThreshold: -1})
	ctx := context.Background()
	if _, err := a.Embedding(ctx, &models.EmbeddingRequest{Input: strings.Repeat("x", 11)}); !errors.Is(err, ErrEmbeddingUnavailable) {
		t.Fatalf("oversized input must be rejected: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("oversized input must not reach the backend")
	}
	start := time.Now()
	if _, err := a.Embedding(ctx, &models.EmbeddingRequest{Input: "short"}); !errors.Is(err, ErrEmbeddingUnavailable) {
		t.Fatalf("timeout must be reported as unavailable: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("timeout not applied (%s)", time.Since(start))
	}
	st := a.Stats()
	if st.RejectedInputs != 1 || st.Timeouts != 1 || st.Calls != 1 {
		t.Fatalf("unexpected stats %+v", st)
	}
	// The semantic cache applies the same budget on its request path.
	c := NewVectorSemanticCache("sem", time.Minute, 0.9, a)
	if hit, err := c.SearchNS(ctx, "t", "short"); hit != nil || err != nil {
		t.Fatalf("slow embedder must degrade to a miss: %+v %v", hit, err)
	}
}

func TestEmbeddingAdapterDimensionConsistency(t *testing.T) {
	dims := atomic.Int32{}
	dims.Store(4)
	fn := EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		v := make([]float64, dims.Load())
		v[0] = 1
		return v, nil
	})
	ctx := context.Background()
	req := &models.EmbeddingRequest{Input: "x"}

	pinned := NewEmbeddingAdapter(fn, EmbeddingAdapterOptions{Dimensions: 8})
	if _, err := pinned.Embedding(ctx, req); !errors.Is(err, ErrEmbeddingUnavailable) {
		t.Fatalf("pinned dimension mismatch must fail: %v", err)
	}

	learning := NewEmbeddingAdapter(fn, EmbeddingAdapterOptions{})
	if _, err := learning.Embedding(ctx, req); err != nil || learning.Dimensions() != 4 {
		t.Fatalf("first vector fixes the dimension: %v %d", err, learning.Dimensions())
	}
	dims.Store(6)
	for i := 0; i < relearnAfter-1; i++ {
		if _, err := learning.Embedding(ctx, req); !errors.Is(err, ErrEmbeddingUnavailable) {
			t.Fatalf("mismatch %d must fail: %v", i, err)
		}
	}
	// Enough consistent vectors of the new size: the model changed.
	if _, err := learning.Embedding(ctx, req); err != nil || learning.Dimensions() != 6 {
		t.Fatalf("adapter must re-learn the dimension: %v %d", err, learning.Dimensions())
	}
	if st := learning.Stats(); st.DimMismatches != relearnAfter-1 {
		t.Fatalf("unexpected stats %+v", st)
	}
	// Array input: every element embedded, indexes preserved.
	resp, err := learning.Embedding(ctx, &models.EmbeddingRequest{Inputs: []string{"a", "b"}})
	if err != nil || len(resp.Data) != 2 || resp.Data[1].Index != 1 {
		t.Fatalf("array input: %+v %v", resp, err)
	}
}

func TestEmbeddingAdapterCooldown(t *testing.T) {
	var calls atomic.Int32
	var healthy atomic.Bool
	fn := EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		calls.Add(1)
		if healthy.Load() {
			return []float64{1, 0}, nil
		}
		return nil, errors.New("503")
	})
	now := time.Unix(1_790_000_000, 0)
	a := NewEmbeddingAdapter(fn, EmbeddingAdapterOptions{FailureThreshold: 3, Cooldown: time.Minute})
	a.now = func() time.Time { return now }
	ctx := context.Background()
	req := &models.EmbeddingRequest{Input: "x"}
	for i := 0; i < 5; i++ {
		_, _ = a.Embedding(ctx, req)
	}
	if calls.Load() != 3 || a.Stats().ShortCircuited != 2 || !a.Stats().CoolingDown {
		t.Fatalf("cooldown must stop calling the backend: calls=%d stats=%+v", calls.Load(), a.Stats())
	}
	now = now.Add(2 * time.Minute)
	healthy.Store(true)
	if _, err := a.Embedding(ctx, req); err != nil {
		t.Fatalf("probe after cooldown must succeed: %v", err)
	}
	if a.Stats().CoolingDown {
		t.Fatal("success must close the cooldown")
	}
	// Caller cancellation is not a backend failure.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	b := NewEmbeddingAdapter(EmbeddingFunc(func(ctx context.Context, _ string) ([]float64, error) { return nil, ctx.Err() }), EmbeddingAdapterOptions{FailureThreshold: 1})
	_, _ = b.Embedding(cctx, req)
	if b.Stats().CoolingDown {
		t.Fatal("a cancelled caller must not trip the cooldown")
	}
	var nilFn EmbeddingFunc
	if _, err := nilFn.Embedding(ctx, req); !errors.Is(err, ErrEmbeddingUnavailable) {
		t.Fatalf("nil func: %v", err)
	}
}
