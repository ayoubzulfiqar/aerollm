package providers

import (
	"context"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ProviderType identifies the LLM provider.
type ProviderType string

const (
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderLocal     ProviderType = "local"
)

// Provider is the interface that all LLM providers must implement.
//
// ChatCompletions must return a *UpstreamError for non-2xx upstream
// responses and a *TransportError for failures to reach the upstream so that
// callers can map status codes and decide on retries (see IsRetryable).
type Provider interface {
	Name() string
	Type() ProviderType
	ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
	Health() ProviderHealth
	Close() error
}

// DefaultModeler is optionally implemented by providers that are bound to a
// default model (used when a request does not name one, and by cost routing).
type DefaultModeler interface {
	DefaultModel() string
}

// ProviderHealth is a point-in-time health snapshot of a provider.
type ProviderHealth struct {
	Name        string       `json:"name"`
	Type        ProviderType `json:"type"`
	Healthy     bool         `json:"healthy"`
	LatencyMs   float64      `json:"latency_ms"`
	Failures    int64        `json:"failures"`
	LastChecked int64        `json:"last_checked"`
	CircuitOpen bool         `json:"circuit_open"`
	// LastProbe is the Unix time of the last active health probe (0 when
	// probing is not used).
	LastProbe int64 `json:"last_probe,omitempty"`
	// ProbeError is the error of the last probe while it is failing.
	ProbeError string `json:"probe_error,omitempty"`
}

// Prober is optionally implemented by providers that support an active
// health probe: a cheap authenticated request (typically GET /models) that
// proves the upstream is reachable and accepts the configured credentials.
// Probes are opt-in: nothing calls Probe unless the operator runs them (e.g.
// on a ticker). The result is reflected in Health(); a provider is healthy
// until a probe (or real traffic) proves otherwise.
type Prober interface {
	Probe(ctx context.Context) error
}

// ProviderMetrics holds performance metrics for a provider.
type ProviderMetrics struct {
	TotalRequests  int64
	TotalErrors    int64
	TotalLatencyMs float64
}
