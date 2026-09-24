package plugins

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// InMemoryRegistry stores plugin metadata in memory. It is safe for
// concurrent use.
type InMemoryRegistry struct {
	mu       sync.RWMutex
	metadata map[string]Metadata
	now      func() time.Time
}

// NewInMemoryRegistry creates a new in-memory plugin registry.
func NewInMemoryRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{metadata: make(map[string]Metadata), now: time.Now}
}

// Register adds a plugin. The ID must satisfy ValidateID; CreatedAt and
// UpdatedAt default to the current Unix time.
func (r *InMemoryRegistry) Register(m Metadata) error {
	if m.ID == "" {
		return fmt.Errorf("plugin id is required")
	}
	if err := ValidateID(m.ID); err != nil {
		return err
	}
	if m.SizeBytes < 0 {
		return fmt.Errorf("plugins: negative size for %q", m.ID)
	}
	now := r.now().Unix()
	if m.CreatedAt == 0 {
		m.CreatedAt = now
	}
	if m.UpdatedAt == 0 {
		m.UpdatedAt = m.CreatedAt
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.metadata[m.ID]; ok {
		return fmt.Errorf("%w: %q", ErrPluginExists, m.ID)
	}
	r.metadata[m.ID] = m
	return nil
}

// Unregister removes a plugin.
func (r *InMemoryRegistry) Unregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.metadata[id]; !ok {
		return fmt.Errorf("%w: %q", ErrPluginNotFound, id)
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

// List returns all metadata sorted by ID.
func (r *InMemoryRegistry) List() []Metadata {
	r.mu.RLock()
	out := make([]Metadata, 0, len(r.metadata))
	for _, m := range r.metadata {
		out = append(out, m)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SetEnabled toggles enabled state and bumps UpdatedAt.
func (r *InMemoryRegistry) SetEnabled(id string, enabled bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metadata[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrPluginNotFound, id)
	}
	m.Enabled = enabled
	m.UpdatedAt = r.now().Unix()
	r.metadata[id] = m
	return nil
}

var _ Registry = (*InMemoryRegistry)(nil)
