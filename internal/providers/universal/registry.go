package universal

import (
	"context"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// ProviderRegistry dynamically manages provider adapters.
type ProviderRegistry struct {
	mu       sync.RWMutex
	adapters map[string]ProviderAdapter
}

// NewProviderRegistry creates a new registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{adapters: make(map[string]ProviderAdapter)}
}

// Register adds or replaces an adapter.
func (r *ProviderRegistry) Register(adapter ProviderAdapter) error {
	if adapter == nil {
		return fmt.Errorf("adapter cannot be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[adapter.Name()] = adapter
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
