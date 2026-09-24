package universal

import (
	"context"
	"sort"
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

func (c ModelCard) clone() ModelCard {
	c.Capabilities = append([]string(nil), c.Capabilities...)
	if c.Pricing != nil {
		p := make(map[string]float64, len(c.Pricing))
		for k, v := range c.Pricing {
			p[k] = v
		}
		c.Pricing = p
	}
	return c
}

// ModelRegistry stores available models and their capabilities. It is safe
// for concurrent use; returned cards are copies.
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

func removeString(list []string, s string) []string {
	out := list[:0]
	for _, v := range list {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}

// Register adds or updates a model card. Re-registering an ID replaces the
// card (and moves it to its new provider).
func (r *ModelRegistry) Register(ctx context.Context, card ModelCard) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if card.ID == "" {
		return ErrEmptyModelID
	}
	card = card.clone()
	if card.CreatedAt.IsZero() {
		card.CreatedAt = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.models[card.ID]; ok && old.Provider != "" {
		r.provider[old.Provider] = removeString(r.provider[old.Provider], card.ID)
		if len(r.provider[old.Provider]) == 0 {
			delete(r.provider, old.Provider)
		}
	}
	r.models[card.ID] = card
	if card.Provider != "" {
		r.provider[card.Provider] = append(r.provider[card.Provider], card.ID)
	}
	return nil
}

// Unregister removes a model card; it reports whether it existed.
func (r *ModelRegistry) Unregister(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.models[id]
	if !ok {
		return false
	}
	delete(r.models, id)
	if old.Provider != "" {
		r.provider[old.Provider] = removeString(r.provider[old.Provider], id)
		if len(r.provider[old.Provider]) == 0 {
			delete(r.provider, old.Provider)
		}
	}
	return true
}

// Get retrieves a model by ID.
func (r *ModelRegistry) Get(id string) (ModelCard, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.models[id]
	if !ok {
		return ModelCard{}, false
	}
	return m.clone(), true
}

// List returns all registered model IDs, sorted.
func (r *ModelRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.models))
	for id := range r.models {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ByProvider returns models for a provider, sorted.
func (r *ModelRegistry) ByProvider(provider string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := append([]string(nil), r.provider[provider]...)
	sort.Strings(out)
	return out
}

// Models returns all model cards sorted by ID.
func (r *ModelRegistry) Models() []ModelCard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ModelCard, 0, len(r.models))
	for _, m := range r.models {
		out = append(out, m.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ErrEmptyModelID is returned when registering a card without an ID.
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
