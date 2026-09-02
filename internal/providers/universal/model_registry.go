package universal

import (
	"context"
	"sync"
	"time"
)

// ModelCard describes a registered model.
type ModelCard struct {
	ID           string
	Provider     string
	Type         string
	Capabilities []string
	Pricing      map[string]float64
	CreatedAt    time.Time
}

// ModelRegistry stores available models and their capabilities.
type ModelRegistry struct {
	mu       sync.RWMutex
	models   map[string]ModelCard
	provider map[string][]string
}

// NewModelRegistry creates a new registry.
func NewModelRegistry() *ModelRegistry {
	return &ModelRegistry{
		models:   make(map[string]ModelCard),
		provider: make(map[string][]string),
	}
}

// Register adds or updates a model card.
func (r *ModelRegistry) Register(ctx context.Context, card ModelCard) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	if card.ID == "" {
		return ErrEmptyModelID
	}
	if card.CreatedAt.IsZero() {
		card.CreatedAt = time.Now()
	}
	r.models[card.ID] = card
	if card.Provider != "" {
		r.provider[card.Provider] = append(r.provider[card.Provider], card.ID)
	}
	return nil
}

// Get retrieves a model by ID.
func (r *ModelRegistry) Get(id string) (ModelCard, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.models[id]
	return m, ok
}

// List returns all registered model IDs.
func (r *ModelRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.models))
	for id := range r.models {
		out = append(out, id)
	}
	return out
}

// ByProvider returns models for a provider.
func (r *ModelRegistry) ByProvider(provider string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.provider[provider]))
	out = append(out, r.provider[provider]...)
	return out
}

// Models returns all model cards.
func (r *ModelRegistry) Models() []ModelCard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ModelCard, 0, len(r.models))
	for _, m := range r.models {
		out = append(out, m)
	}
	return out
}

var ErrEmptyModelID = NewRegistryError("model id is empty")

// RegistryError wraps registry failures.
type RegistryError struct {
	msg string
}

func (e *RegistryError) Error() string { return e.msg }

// NewRegistryError creates a registry error.
func NewRegistryError(msg string) *RegistryError {
	return &RegistryError{msg: msg}
}
