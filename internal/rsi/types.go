// Package rsi implements the Recursive Self-Improvement (RSI) engine for AeroLLM.
//
// The RSI engine enables the system to assess its own headroom for improvement
// (via the HCI — Headroom-Closed Index from "The Last AI Built by Humans"),
// evolve policies through replay simulation (via Dream-RSI's replay-simulator
// paradigm), and continuously refine routing, caching, guardrails, and agent-tool
// strategies without making redundant live API calls.
//
// The engine is organized into five files:
//   - types.go:   shared types and the Policy interface
//   - hci.go:     headroom assessment engine
//   - dream.go:   replay simulator for offline policy evaluation
//   - modular.go: benchmark-disjoint evaluation to prevent overfitting
//   - explore.go: broad-then-deep autonomous policy exploration
//
// All RSI components are designed to be non-invasive: they read from existing
// internal/ledger records and internal/trace metrics, never modifying the
// request path directly. Policy deployment (when improvement exceeds a
// threshold) is delegated to internal/aiops.
package rsi

import (
	"context"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// TimeRange represents a temporal window for filtering historical data.
// It is used by the DreamSimulator to scope which ledger records to load
// into the replay pool.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// Contains reports whether a timestamp falls within this time range.
// If both Start and End are zero values, the range matches all timestamps.
// This allows callers to pass a zero-value TimeRange to mean "all time".
func (tr TimeRange) Contains(t time.Time) bool {
	if tr.Start.IsZero() && tr.End.IsZero() {
		return true
	}
	if !tr.Start.IsZero() && t.Before(tr.Start) {
		return false
	}
	if !tr.End.IsZero() && t.After(tr.End) {
		return false
	}
	return true
}

// Policy is the interface for an RSI-evolvable policy.
//
// A Policy makes decisions about routing, caching, guardrails, or agent
// tool execution. It is the fundamental unit of evolution in the RSI loop:
// the explorer generates mutated variants, the dream simulator evaluates
// them offline via replay, and the best variants are deployed through
// internal/aiops.
//
// Apply must NOT make real LLM calls. In the replay simulator context,
// Apply returns a policy decision (provider selection, cache decision)
// that the simulator uses to look up a recorded response. This mirrors
// the Dream-RSI principle that "evaluating a new strategy requires only
// reading past records without rerunning the underlying discovery agent."
type Policy interface {
	// Apply applies the policy to a request and returns the simulated
	// decision/outcome. The returned Response captures routing, caching,
	// cost, and latency decisions without invoking any LLM provider.
	Apply(ctx context.Context, req *models.LLMRequest) (*Response, error)

	// Mutate creates a mutated variant of this policy for exploration.
	// Each concrete implementation defines its own mutation strategy
	// (e.g., random weight perturbation for a routing policy).
	Mutate() Policy

	// Clone creates a deep copy of this policy so that mutations
	// do not affect the original. This is critical for the broad-then-deep
	// exploration loop where multiple variations are derived from a base.
	Clone() Policy
}

// Response represents a policy's decision and simulated outcome for a single request.
//
// It is the return type of Policy.Apply and is consumed by DreamSimulator.ReplayTraffic
// to compute aggregate SimulatedMetrics. The Response does NOT carry LLM-generated
// text — that is stored separately in DreamReplay.Response and replayed by the simulator.
type Response struct {
	// Provider is the provider name selected by the routing policy
	// (e.g., "openai", "anthropic", "claude-3-sonnet").
	Provider string

	// Cached indicates whether the policy decided to serve the request
	// from cache (exact or semantic). When true, the simulator uses
	// near-zero latency and zero marginal cost.
	Cached bool

	// LatencyMs is the simulated latency budget allocated for this decision.
	// The simulator may override this with recorded latency data.
	LatencyMs float64

	// CostUSD is the simulated cost allocated for this decision.
	// The simulator may override this with cost-map-derived values.
	CostUSD float64

	// Error indicates whether the simulated request resulted in an error
	// (e.g., provider unavailable, budget exceeded). The simulator uses
	// this to compute the ErrorRate metric.
	Error bool
}

// LedgerReader is the interface for reading ledger records.
// The existing ledger.LedgerStore interface satisfies this — the rsi package
// only needs read access (All and Latest), never write.
type LedgerReader interface {
	// All returns all stored ledger records. Implementations may return
	// records in chronological order.
	All(ctx context.Context) ([]ledger.LedgerRecord, error)
	// Latest returns the most recent ledger record, or an error if none exist.
	Latest(ctx context.Context) (*ledger.LedgerRecord, error)
}

// MetricsProvider provides current runtime performance metrics.
// The existing trace.Provider satisfies this interface via its
// RequestCount, ErrorCount, and AvgLatency methods.
type MetricsProvider interface {
	// RequestCount returns the total number of requests processed.
	RequestCount() int64
	// ErrorCount returns the total number of error responses.
	ErrorCount() int64
	// AvgLatency returns the average request latency in milliseconds.
	AvgLatency() float64
}

// CostCalculator estimates the USD cost of an LLM request given a model
// and its token usage. The existing finops.CostTracker satisfies this
// interface.
type CostCalculator interface {
	// CalculateCost computes the USD cost for a request given the model
	// name and token usage statistics.
	CalculateCost(model string, usage *models.Usage) float64
}

// ProviderLister provides information about registered LLM providers.
// The existing router.Router satisfies this via its Providers() method,
// though the rsi package only needs the count and names.
type ProviderLister interface {
	// ProviderNames returns the names of all registered, currently
	// available providers.
	ProviderNames() []string
}
