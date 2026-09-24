package cache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ErrEmbeddingUnavailable marks an embedding failure that callers should
// treat as "no semantic answer" rather than as a request error: the
// VectorSemanticCache turns it into a cache miss (SearchNS) or a skipped
// store (UpsertNS). Errors returned by EmbeddingFunc and EmbeddingAdapter
// always wrap it.
var ErrEmbeddingUnavailable = errors.New("cache: embedding unavailable")

// Embedding adapter defaults.
const (
	DefaultEmbeddingTimeout       = 2 * time.Second
	DefaultEmbeddingMaxInputBytes = maxEmbedInputBytes
	DefaultEmbeddingMaxDimensions = 16384
	defaultFailureThreshold       = 5
	defaultFailureCooldown        = 30 * time.Second
	// relearnAfter is how many consecutive vectors of one new dimension a
	// learning adapter must see before it adopts that dimension (the
	// upstream embedding model was changed).
	relearnAfter = 20
)

// EmbeddingFunc adapts a plain embedding function — for example a call into
// the gateway's own /v1/embeddings pipeline — to EmbeddingProvider.
//
// Used directly it applies the default input cap (32 KiB; longer texts
// are not embedded), a 2s timeout and vector validation (non-empty, at
// most 16384 finite components, non-zero); every failure is returned
// wrapping ErrEmbeddingUnavailable so the semantic cache degrades to a
// miss. Use NewEmbeddingAdapter for dimension pinning, a failure cooldown
// and statistics.
type EmbeddingFunc func(ctx context.Context, text string) ([]float64, error)

// Embedding implements EmbeddingProvider.
func (f EmbeddingFunc) Embedding(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	if f == nil {
		return nil, fmt.Errorf("%w: nil embedding function", ErrEmbeddingUnavailable)
	}
	return NewEmbeddingAdapter(f, EmbeddingAdapterOptions{FailureThreshold: -1}).Embedding(ctx, req)
}

// EmbeddingAdapterOptions configures an EmbeddingAdapter. Zero values select
// the defaults.
type EmbeddingAdapterOptions struct {
	// Timeout bounds each embedding call (default 2s); the semantic cache
	// sits on the request path, so keep it short.
	Timeout time.Duration
	// MaxInputBytes rejects longer texts without calling the function
	// (default 32 KiB). Truncating would embed a different prompt.
	MaxInputBytes int
	// Dimensions pins the expected vector length. When 0 the adapter
	// learns it from the first valid vector and re-learns only after
	// several consecutive vectors agree on a new length.
	Dimensions int
	// MaxDimensions rejects longer vectors (default 16384).
	MaxDimensions int
	// FailureThreshold consecutive failures make the adapter skip calls
	// for Cooldown (defaults 5 and 30s), so an unavailable embedding
	// backend does not add its timeout to every request. Negative
	// disables the cooldown.
	FailureThreshold int
	Cooldown         time.Duration
	// Model is reported in responses (default "aerollm-embedding-func").
	Model string
}

// EmbeddingAdapterStats counts adapter outcomes.
type EmbeddingAdapterStats struct {
	Calls          int64 `json:"calls"`
	Failures       int64 `json:"failures"`
	Timeouts       int64 `json:"timeouts"`
	DimMismatches  int64 `json:"dimension_mismatches"`
	InvalidVectors int64 `json:"invalid_vectors"`
	RejectedInputs int64 `json:"rejected_inputs"`
	ShortCircuited int64 `json:"short_circuited"`
	Dimensions     int   `json:"dimensions"`
	CoolingDown    bool  `json:"cooling_down"`
}

// EmbeddingAdapter wraps an EmbeddingFunc with input caps, timeouts,
// dimension consistency and a failure cooldown. It is safe for concurrent
// use. Every error it returns wraps ErrEmbeddingUnavailable.
type EmbeddingAdapter struct {
	fn   EmbeddingFunc
	opts EmbeddingAdapterOptions

	mu          sync.Mutex
	dims        int
	candidate   int
	candidateN  int
	consecFails int
	openUntil   time.Time

	calls, failures, timeouts, dimMismatch, invalid, rejected, shorted atomic.Int64

	now func() time.Time
}

// NewEmbeddingAdapter returns an adapter around fn.
func NewEmbeddingAdapter(fn EmbeddingFunc, opts EmbeddingAdapterOptions) *EmbeddingAdapter {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultEmbeddingTimeout
	}
	if opts.MaxInputBytes <= 0 {
		opts.MaxInputBytes = DefaultEmbeddingMaxInputBytes
	}
	if opts.MaxDimensions <= 0 {
		opts.MaxDimensions = DefaultEmbeddingMaxDimensions
	}
	if opts.Dimensions < 0 || opts.Dimensions > opts.MaxDimensions {
		opts.Dimensions = 0
	}
	if opts.FailureThreshold == 0 {
		opts.FailureThreshold = defaultFailureThreshold
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = defaultFailureCooldown
	}
	if opts.Model == "" {
		opts.Model = "aerollm-embedding-func"
	}
	return &EmbeddingAdapter{fn: fn, opts: opts, dims: opts.Dimensions, now: time.Now}
}

func unavailable(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrEmbeddingUnavailable}, args...)...)
}

// Embedding implements EmbeddingProvider. Array inputs are embedded one by
// one within the same timeout.
func (a *EmbeddingAdapter) Embedding(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	if a == nil || a.fn == nil {
		return nil, unavailable("no embedding function")
	}
	if req == nil {
		return nil, unavailable("nil request")
	}
	inputs := req.InputList()
	if len(inputs) == 0 {
		a.rejected.Add(1)
		return nil, unavailable("empty input")
	}
	for _, in := range inputs {
		if in == "" || len(in) > a.opts.MaxInputBytes {
			a.rejected.Add(1)
			return nil, unavailable("input of %d bytes outside 1..%d", len(in), a.opts.MaxInputBytes)
		}
	}
	if a.coolingDown() {
		a.shorted.Add(1)
		return nil, unavailable("embedding backend cooling down after repeated failures")
	}
	ctx, cancel := context.WithTimeout(ctx, a.opts.Timeout)
	defer cancel()
	resp := &models.EmbeddingResponse{Object: "list", Model: a.opts.Model, Data: make([]models.Embedding, 0, len(inputs))}
	for i, in := range inputs {
		vec, err := a.embedOne(ctx, in)
		if err != nil {
			return nil, err
		}
		resp.Data = append(resp.Data, models.Embedding{Object: "embedding", Embedding: vec, Index: i})
	}
	return resp, nil
}

func (a *EmbeddingAdapter) embedOne(ctx context.Context, text string) (vec []float64, err error) {
	a.calls.Add(1)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("embedding function panicked: %v", r)
			}
		}()
		vec, err = a.fn(ctx, text)
	}()
	if err != nil {
		a.failures.Add(1)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			a.timeouts.Add(1)
		}
		// A caller cancelling its own request says nothing about the backend.
		if !errors.Is(ctx.Err(), context.Canceled) {
			a.recordFailure()
		}
		return nil, unavailable("%v", err)
	}
	if err := validVector(vec, a.opts.MaxDimensions); err != nil {
		a.invalid.Add(1)
		a.recordFailure()
		return nil, unavailable("%v", err)
	}
	if !a.acceptDims(len(vec)) {
		a.dimMismatch.Add(1)
		return nil, unavailable("vector has %d dimensions, expected %d", len(vec), a.Dimensions())
	}
	a.recordSuccess()
	return vec, nil
}

func validVector(vec []float64, maxDims int) error {
	if len(vec) == 0 {
		return errors.New("empty vector")
	}
	if len(vec) > maxDims {
		return fmt.Errorf("vector has %d dimensions (max %d)", len(vec), maxDims)
	}
	var norm float64
	for _, x := range vec {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return errors.New("non-finite vector component")
		}
		norm += x * x
	}
	if norm == 0 || math.IsInf(norm, 0) {
		return errors.New("zero or overflowing vector norm")
	}
	return nil
}

// acceptDims enforces dimension consistency. A pinned dimension never
// changes; a learned one is replaced only after relearnAfter consecutive
// vectors agree on a new length.
func (a *EmbeddingAdapter) acceptDims(n int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.dims == 0:
		a.dims = n
		return true
	case n == a.dims:
		a.candidate, a.candidateN = 0, 0
		return true
	case a.opts.Dimensions != 0:
		return false
	}
	if n != a.candidate {
		a.candidate, a.candidateN = n, 0
	}
	a.candidateN++
	if a.candidateN >= relearnAfter {
		a.dims, a.candidate, a.candidateN = n, 0, 0
		return true
	}
	return false
}

func (a *EmbeddingAdapter) coolingDown() bool {
	if a.opts.FailureThreshold < 0 {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now().Before(a.openUntil)
}

func (a *EmbeddingAdapter) recordFailure() {
	if a.opts.FailureThreshold < 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecFails++
	if a.consecFails >= a.opts.FailureThreshold {
		// After the cooldown one call probes the backend again; another
		// failure re-opens the cooldown immediately.
		a.openUntil = a.now().Add(a.opts.Cooldown)
		a.consecFails = a.opts.FailureThreshold - 1
	}
}

func (a *EmbeddingAdapter) recordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecFails = 0
	a.openUntil = time.Time{}
}

// Dimensions returns the pinned or learned vector length (0 before the
// first successful call of a learning adapter).
func (a *EmbeddingAdapter) Dimensions() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dims
}

// Stats returns a snapshot of the adapter counters.
func (a *EmbeddingAdapter) Stats() EmbeddingAdapterStats {
	return EmbeddingAdapterStats{
		Calls:          a.calls.Load(),
		Failures:       a.failures.Load(),
		Timeouts:       a.timeouts.Load(),
		DimMismatches:  a.dimMismatch.Load(),
		InvalidVectors: a.invalid.Load(),
		RejectedInputs: a.rejected.Load(),
		ShortCircuited: a.shorted.Load(),
		Dimensions:     a.Dimensions(),
		CoolingDown:    a.coolingDown(),
	}
}

var (
	_ EmbeddingProvider = EmbeddingFunc(nil)
	_ EmbeddingProvider = (*EmbeddingAdapter)(nil)
)
