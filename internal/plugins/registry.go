package plugins

import (
	"fmt"
	"sync"
)

// InMemoryRegistry stores plugin metadata in memory.
type InMemoryRegistry struct {
	mu       sync.RWMutex
	metadata map[string]Metadata
}

// NewInMemoryRegistry creates a new in-memory plugin registry.
func NewInMemoryRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{metadata: make(map[string]Metadata)}
}

// Register adds a plugin.
func (r *InMemoryRegistry) Register(m Metadata) error {
	if m.ID == "" {
		return fmt.Errorf("plugin id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.metadata[m.ID]; ok {
		return fmt.Errorf("plugin %q already exists", m.ID)
	}
	r.metadata[m.ID] = m
	return nil
}

// Unregister removes a plugin.
func (r *InMemoryRegistry) Unregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.metadata[id]; !ok {
		return fmt.Errorf("plugin %q not found", id)
	}
	delete(r.metadata, id)
	return nil
}

// Get returns metadata by id.
func (r *InMemoryRegistry) Get(id string) (Metadata, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.metadata[id]
	return m, ok
}

// List returns all metadata.
func (r *InMemoryRegistry) List() []Metadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Metadata, 0, len(r.metadata))
	for _, m := range r.metadata {
		out = append(out, m)
	}
	return out
}

// SetEnabled toggles enabled state.
func (r *InMemoryRegistry) SetEnabled(id string, enabled bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metadata[id]
	if !ok {
		return fmt.Errorf("plugin %q not found", id)
	}
	m.Enabled = enabled
	m.UpdatedAt = 0
	r.metadata[id] = m
	return nil
}
