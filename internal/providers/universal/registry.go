package universal

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ModelEntry binds a provider name to a model alias.
type ModelEntry struct {
	ProviderName string
	Model        string
}

// ProviderRegistry dynamically manages provider adapters.
type ProviderRegistry struct {
	mu       sync.RWMutex
	adapters map[string]ProviderAdapter
	models   []ModelEntry
}

// NewProviderRegistry creates a new registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		adapters: make(map[string]ProviderAdapter),
		models:   make([]ModelEntry, 0),
	}
}

// Register adds or replaces an adapter and optional model aliases.
func (r *ProviderRegistry) Register(adapter ProviderAdapter, aliases ...string) error {
	if adapter == nil {
		return fmt.Errorf("adapter cannot be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[adapter.Name()] = adapter
	for _, model := range aliases {
		r.models = append(r.models, ModelEntry{ProviderName: adapter.Name(), Model: model})
	}
	return nil
}

// RegisterFromConfig registers adapters from dynamic provider configs.
func (r *ProviderRegistry) RegisterFromConfig(cfgs []config.ProviderConfig) error {
	for _, cfg := range cfgs {
		name := cfg.ResolvedName()
		switch cfg.Type {
		case "openai", "openai-compatible":
			if err := r.Register(NewOpenAICompatibleAdapter(name, cfg.Type, cfg.APIKey, cfg.BaseURL), cfg.Models...); err != nil {
				return err
			}
		case "groq":
			if err := r.Register(NewGroqAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		case "cohere":
			if err := r.Register(NewCohereAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		case "deepseek":
			if err := r.Register(NewDeepSeekAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		case "anthropic":
			if err := r.Register(NewAnthropicAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		case "bedrock":
			if err := r.Register(NewBedrockAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		case "gemini":
			if err := r.Register(NewGeminiAdapter(cfg.APIKey, cfg.Endpoint()), cfg.Models...); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported provider type: %s", cfg.Type)
		}
	}
	return nil
}

// Get returns an adapter by name.
func (r *ProviderRegistry) Get(name string) (ProviderAdapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[name]
	return a, ok
}

// All returns all registered adapter names.
func (r *ProviderRegistry) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		out = append(out, name)
	}
	return out
}

// ChatCompletion routes to the named adapter or returns an error.
func (r *ProviderRegistry) ChatCompletion(ctx context.Context, name string, req *models.LLMRequest) (*models.LLMResponse, error) {
	a, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("adapter %q not registered", name)
	}
	return a.ChatCompletions(ctx, req)
}

// ResolveProviderByModel resolves a provider adapter for the given model alias.
func (r *ProviderRegistry) ResolveProviderByModel(model string) (ProviderAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	modelLower := strings.ToLower(strings.TrimSpace(model))
	for _, entry := range r.models {
		if strings.ToLower(entry.Model) == modelLower {
			if a, ok := r.adapters[entry.ProviderName]; ok {
				return a, nil
			}
			return nil, fmt.Errorf("provider %q not found for model %q", entry.ProviderName, model)
		}
	}
	return nil, fmt.Errorf("model %q not registered", model)
}
