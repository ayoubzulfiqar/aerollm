package tenant

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryTenantResolver resolves API keys to tenants from an in-memory map.
type InMemoryTenantResolver struct {
	mu      sync.RWMutex
	apiKeys map[string]*APIKey
}

// NewInMemoryTenantResolver creates a new in-memory tenant resolver.
func NewInMemoryTenantResolver() *InMemoryTenantResolver {
	return &InMemoryTenantResolver{apiKeys: make(map[string]*APIKey)}
}

// Add adds an API key mapping.
func (r *InMemoryTenantResolver) Add(key *APIKey) {
	if key == nil || key.HashedKey == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := *key
	r.apiKeys[key.HashedKey] = &entry
}

// ResolveByAPIKey looks up the API key.
func (r *InMemoryTenantResolver) ResolveByAPIKey(_ context.Context, apiKey string) (*APIKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.apiKeys[apiKey]
	if !ok {
		return nil, fmt.Errorf("tenant: api key not found")
	}
	entry := *key
	return &entry, nil
}
