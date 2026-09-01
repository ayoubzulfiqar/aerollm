package providers

import (
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"context"
)

// ProviderType identifies the LLM provider.
type ProviderType string

const (
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderLocal     ProviderType = "local"
)

// Provider is the interface that all LLM providers must implement.
type Provider interface {
	Name() string
	Type() ProviderType
	ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
	Health() ProviderHealth
	Close() error
}

type ProviderHealth struct {
	Name         string    `json:"name"`
	Type         ProviderType `json:"type"`
	Healthy      bool      `json:"healthy"`
	LatencyMs    float64   `json:"latency_ms"`
	Failures     int64     `json:"failures"`
	LastChecked  int64     `json:"last_checked"`
	CircuitOpen  bool      `json:"circuit_open"`
}

// ProviderMetrics holds performance metrics for a provider.
type ProviderMetrics struct {
	TotalRequests int64
	TotalErrors   int64
	TotalLatencyMs float64
}
