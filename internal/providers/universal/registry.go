package universal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// ErrModelNotFound is returned (wrapped) when no provider serves a model.
var ErrModelNotFound = errors.New("model not found")

// ErrAliasCycle is returned when model aliases form a cycle.
var ErrAliasCycle = errors.New("model alias cycle")

// maxAliasDepth bounds alias chains.
const maxAliasDepth = 16

// ModelEntry binds a provider name to a model name or pattern.
type ModelEntry struct {
	ProviderName string
	// Model is an exact model name or a prefix pattern ending in "*".
	Model string
	// Target is the upstream model name to send; empty means the requested
	// model name is sent unchanged.
	Target string
}

func (e ModelEntry) isPattern() bool { return strings.HasSuffix(e.Model, "*") }

// Resolution is the result of resolving a requested model.
type Resolution struct {
	Adapter      ProviderAdapter
	ProviderName string
	// Model is the upstream model name to put in the request.
	Model string
}

// ProviderRegistry dynamically manages provider adapters and the models they
// serve. All methods are safe for concurrent use.
type ProviderRegistry struct {
	mu       sync.RWMutex
	adapters map[string]ProviderAdapter
	models   []ModelEntry
	aliases  map[string]string // lower(alias) -> target model name
}

// NewProviderRegistry creates a new registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		adapters: make(map[string]ProviderAdapter),
		models:   make([]ModelEntry, 0),
		aliases:  make(map[string]string),
	}
}

// parseModelSpec parses "model", "prefix*" or "alias=upstream-model".
func parseModelSpec(provider, spec string) (ModelEntry, bool, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return ModelEntry{}, false, nil
	}
	e := ModelEntry{ProviderName: provider, Model: spec}
	if name, target, ok := strings.Cut(spec, "="); ok {
		e.Model, e.Target = strings.TrimSpace(name), strings.TrimSpace(target)
		if e.Model == "" || e.Target == "" {
			return ModelEntry{}, false, fmt.Errorf("invalid model alias %q: want alias=model", spec)
		}
		if strings.Contains(e.Target, "*") {
			return ModelEntry{}, false, fmt.Errorf("invalid model alias %q: target cannot be a pattern", spec)
		}
	}
	if i := strings.Index(e.Model, "*"); i >= 0 {
		if i != len(e.Model)-1 {
			return ModelEntry{}, false, fmt.Errorf("invalid model pattern %q: '*' is only allowed at the end", spec)
		}
		if e.Target != "" {
			return ModelEntry{}, false, fmt.Errorf("invalid model alias %q: a pattern cannot have a target", spec)
		}
	}
	return e, true, nil
}

// Register adds or replaces an adapter and the models it serves. Each alias
// is an exact model name ("gpt-4o"), a prefix pattern ("gpt-4o*") or an
// alias mapped to an upstream model ("fast=gpt-4o-mini"). Registering an
// adapter under an existing name replaces it and its model entries.
func (r *ProviderRegistry) Register(adapter ProviderAdapter, aliases ...string) error {
	if adapter == nil {
		return fmt.Errorf("adapter cannot be nil")
	}
	name := adapter.Name()
	if name == "" {
		return fmt.Errorf("adapter name cannot be empty")
	}
	entries := make([]ModelEntry, 0, len(aliases))
	for _, a := range aliases {
		e, ok, err := parseModelSpec(name, a)
		if err != nil {
			return err
		}
		if ok {
			entries = append(entries, e)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(name)
	r.adapters[name] = adapter
	r.models = append(r.models, entries...)
	return nil
}

func (r *ProviderRegistry) removeLocked(name string) {
	delete(r.adapters, name)
	kept := r.models[:0]
	for _, e := range r.models {
		if e.ProviderName != name {
			kept = append(kept, e)
		}
	}
	r.models = kept
}

// Unregister removes an adapter and its model entries. It reports whether the
// adapter was registered. The adapter is not closed.
func (r *ProviderRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.adapters[name]
	r.removeLocked(name)
	return ok
}

// SetAlias maps a model alias to another model name (which may itself be an
// alias or a model served by a provider). Cycles are rejected.
func (r *ProviderRegistry) SetAlias(alias, target string) error {
	alias, target = strings.TrimSpace(alias), strings.TrimSpace(target)
	if alias == "" || target == "" {
		return fmt.Errorf("alias and target must be non-empty")
	}
	if strings.Contains(alias, "*") || strings.Contains(target, "*") {
		return fmt.Errorf("aliases cannot be patterns")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.ToLower(alias)
	// Walk the chain from target; reaching alias means a cycle.
	cur := strings.ToLower(target)
	for depth := 0; ; depth++ {
		if cur == key {
			return fmt.Errorf("%w: %q -> %q", ErrAliasCycle, alias, target)
		}
		next, ok := r.aliases[cur]
		if !ok {
			break
		}
		if depth >= maxAliasDepth {
			return fmt.Errorf("%w: chain from %q is too long", ErrAliasCycle, target)
		}
		cur = strings.ToLower(next)
	}
	r.aliases[key] = target
	return nil
}

// RemoveAlias deletes a model alias.
func (r *ProviderRegistry) RemoveAlias(alias string) {
	r.mu.Lock()
	delete(r.aliases, strings.ToLower(strings.TrimSpace(alias)))
	r.mu.Unlock()
}

// NewAdapterFromConfig builds the adapter for one provider config entry.
// Adapter names come from cfg.ResolvedName().
func NewAdapterFromConfig(cfg config.ProviderConfig) (ProviderAdapter, error) {
	name := cfg.ResolvedName()
	if name == "" {
		return nil, fmt.Errorf("provider config needs a name or type")
	}
	var client *http.Client
	if cfg.Timeout > 0 {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	switch cfg.Type {
	case "openai", "openai-compatible", "local", "vllm", "ollama", "groq", "cohere", "deepseek", "gemini", "azure":
		typ := cfg.Type
		if typ == "gemini" {
			typ = "google"
		}
		if typ == "azure" && strings.TrimSpace(cfg.BaseURL) == "" {
			return nil, fmt.Errorf("provider %q: azure requires base_url (https://<resource>.openai.azure.com/openai/v1)", name)
		}
		base := cfg.Endpoint()
		if typ == "local" || typ == "vllm" || typ == "ollama" {
			if strings.TrimSpace(cfg.BaseURL) == "" {
				return nil, fmt.Errorf("provider %q: type %s requires base_url", name, cfg.Type)
			}
			base = cfg.BaseURL
		}
		a := NewOpenAICompatibleAdapter(name, typ, cfg.APIKey, base)
		if a.baseErr != nil {
			return nil, a.baseErr
		}
		a.SetHTTPClient(client)
		return a, nil
	case "anthropic":
		a := NewAnthropicAdapterV2(cfg.APIKey, cfg.Endpoint())
		if a.endpointErr != nil {
			return nil, fmt.Errorf("provider %q: %w", name, a.endpointErr)
		}
		a.name = name
		a.SetHTTPClient(client)
		return a, nil
	case "bedrock":
		a := NewBedrockAdapter(cfg.APIKey, cfg.Endpoint())
		if a.baseErr != nil {
			return nil, fmt.Errorf("provider %q: %w", name, a.baseErr)
		}
		a.name = name
		a.SetHTTPClient(client)
		return a, nil
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", cfg.Type)
	}
}

type registryState struct {
	adapters map[string]ProviderAdapter
	models   []ModelEntry
}

func buildState(cfgs []config.ProviderConfig) (*registryState, error) {
	st := &registryState{adapters: make(map[string]ProviderAdapter, len(cfgs))}
	for _, cfg := range cfgs {
		a, err := NewAdapterFromConfig(cfg)
		if err != nil {
			return nil, err
		}
		if _, dup := st.adapters[a.Name()]; dup {
			return nil, fmt.Errorf("duplicate provider name %q", a.Name())
		}
		st.adapters[a.Name()] = a
		for _, spec := range cfg.Models {
			e, ok, err := parseModelSpec(a.Name(), spec)
			if err != nil {
				return nil, fmt.Errorf("provider %q: %w", a.Name(), err)
			}
			if ok {
				st.models = append(st.models, e)
			}
		}
	}
	return st, nil
}

// RegisterFromConfig registers adapters from provider configs. It validates
// every entry first and registers nothing when any entry is invalid.
func (r *ProviderRegistry) RegisterFromConfig(cfgs []config.ProviderConfig) error {
	st, err := buildState(cfgs)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cfg := range cfgs {
		name := cfg.ResolvedName()
		r.removeLocked(name)
		r.adapters[name] = st.adapters[name]
	}
	r.models = append(r.models, st.models...)
	return nil
}

// ReloadFromConfig atomically replaces all adapters and provider model
// entries with those built from cfgs (aliases set with SetAlias are kept).
// On error the registry is left unchanged. Adapters that are replaced or
// removed have their idle connections closed.
func (r *ProviderRegistry) ReloadFromConfig(cfgs []config.ProviderConfig) error {
	st, err := buildState(cfgs)
	if err != nil {
		return err
	}
	r.mu.Lock()
	old := r.adapters
	r.adapters = st.adapters
	r.models = st.models
	r.mu.Unlock()
	for name, a := range old {
		if st.adapters[name] != a {
			_ = a.Close()
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

// All returns all registered adapter names, sorted.
func (r *ProviderRegistry) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Models returns a copy of the model entries in registration order.
func (r *ProviderRegistry) Models() []ModelEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]ModelEntry(nil), r.models...)
}

// ChatCompletion routes to the named adapter or returns an error.
func (r *ProviderRegistry) ChatCompletion(ctx context.Context, name string, req *models.LLMRequest) (*models.LLMResponse, error) {
	a, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("adapter %q not registered", name)
	}
	return a.ChatCompletions(ctx, req)
}

// resolveAliasLocked follows global aliases from model.
func (r *ProviderRegistry) resolveAliasLocked(model string) (string, error) {
	cur := model
	seen := map[string]bool{}
	for depth := 0; depth <= maxAliasDepth; depth++ {
		key := strings.ToLower(cur)
		if seen[key] {
			return "", fmt.Errorf("%w at %q", ErrAliasCycle, cur)
		}
		seen[key] = true
		next, ok := r.aliases[key]
		if !ok {
			return cur, nil
		}
		cur = next
	}
	return "", fmt.Errorf("%w: chain from %q is too long", ErrAliasCycle, model)
}

// ResolveAll returns every provider able to serve model, best match first:
// global aliases are followed, then exact entries (registration order), then
// prefix patterns (longest prefix first). Useful for fallback across
// providers serving the same model.
func (r *ProviderRegistry) ResolveAll(model string) ([]Resolution, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, fmt.Errorf("%w: model is required", ErrModelNotFound)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	resolved, err := r.resolveAliasLocked(model)
	if err != nil {
		return nil, err
	}
	var out []Resolution
	add := func(e ModelEntry) {
		a, ok := r.adapters[e.ProviderName]
		if !ok {
			return
		}
		up := resolved
		if e.Target != "" {
			up = e.Target
		}
		out = append(out, Resolution{Adapter: a, ProviderName: e.ProviderName, Model: up})
	}
	for _, e := range r.models {
		if !e.isPattern() && strings.EqualFold(e.Model, resolved) {
			add(e)
		}
	}
	if len(out) == 0 {
		var patterns []ModelEntry
		lower := strings.ToLower(resolved)
		for _, e := range r.models {
			if e.isPattern() && strings.HasPrefix(lower, strings.ToLower(strings.TrimSuffix(e.Model, "*"))) {
				patterns = append(patterns, e)
			}
		}
		sort.SliceStable(patterns, func(i, j int) bool { return len(patterns[i].Model) > len(patterns[j].Model) })
		for _, e := range patterns {
			add(e)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %q is not served by any registered provider", ErrModelNotFound, model)
	}
	return out, nil
}

// Resolve returns the best provider for model and the upstream model name
// to send (which differs from model for aliases).
func (r *ProviderRegistry) Resolve(model string) (Resolution, error) {
	all, err := r.ResolveAll(model)
	if err != nil {
		return Resolution{}, err
	}
	return all[0], nil
}

// ResolveProviderByModel resolves a provider adapter for the given model
// (exact names, "prefix*" patterns and aliases). Errors wrap
// ErrModelNotFound or ErrAliasCycle. When the model is an alias for a
// different upstream model, the returned adapter rewrites req.Model (see
// ResolveAdapter).
func (r *ProviderRegistry) ResolveProviderByModel(model string) (ProviderAdapter, error) {
	return r.ResolveAdapter(model)
}

// ResolveAdapter is like ResolveProviderByModel, but when the requested
// model is an alias for a different upstream model the returned adapter
// rewrites req.Model before calling the provider, so aliases work without
// caller changes.
func (r *ProviderRegistry) ResolveAdapter(model string) (ProviderAdapter, error) {
	res, err := r.Resolve(model)
	if err != nil {
		return nil, err
	}
	if res.Model == strings.TrimSpace(model) {
		return res.Adapter, nil
	}
	return &modelRewriteAdapter{ProviderAdapter: res.Adapter, model: res.Model}, nil
}

// modelRewriteAdapter forwards to an adapter with a fixed upstream model.
type modelRewriteAdapter struct {
	ProviderAdapter
	model string
}

// Unwrap returns the underlying adapter.
func (m *modelRewriteAdapter) Unwrap() ProviderAdapter { return m.ProviderAdapter }

// UpstreamModel returns the model name sent upstream.
func (m *modelRewriteAdapter) UpstreamModel() string { return m.model }

func (m *modelRewriteAdapter) withModel(req *models.LLMRequest) *models.LLMRequest {
	if req == nil {
		return nil
	}
	cp := *req
	cp.Model = m.model
	return &cp
}

func (m *modelRewriteAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return m.ProviderAdapter.ChatCompletions(ctx, m.withModel(req))
}

func (m *modelRewriteAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	return m.ProviderAdapter.Stream(ctx, m.withModel(req))
}

func (m *modelRewriteAdapter) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if sp, ok := m.ProviderAdapter.(StreamProvider); ok {
		return sp.StreamChatCompletions(ctx, m.withModel(req))
	}
	return nil, providers.ErrStreamingNotSupported
}

func (m *modelRewriteAdapter) unsupported(what string) error {
	return &providers.UpstreamError{Provider: m.Name(), StatusCode: http.StatusNotImplemented, Type: "unsupported", Message: what + " not supported by provider"}
}

func (m *modelRewriteAdapter) Embeddings(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	e, ok := m.ProviderAdapter.(interface {
		Embeddings(context.Context, *models.EmbeddingRequest) (*models.EmbeddingResponse, error)
	})
	if !ok {
		return nil, m.unsupported("embeddings")
	}
	if req != nil {
		cp := *req
		cp.Model = m.model
		req = &cp
	}
	return e.Embeddings(ctx, req)
}

func (m *modelRewriteAdapter) ImageGenerations(ctx context.Context, req *models.ImageRequest) (*models.ImageResponse, error) {
	i, ok := m.ProviderAdapter.(interface {
		ImageGenerations(context.Context, *models.ImageRequest) (*models.ImageResponse, error)
	})
	if !ok {
		return nil, m.unsupported("image generation")
	}
	if req != nil {
		cp := *req
		cp.Model = m.model
		req = &cp
	}
	return i.ImageGenerations(ctx, req)
}

func (m *modelRewriteAdapter) AudioTranscriptions(ctx context.Context, req *models.AudioRequest) (*models.AudioResponse, error) {
	a, ok := m.ProviderAdapter.(interface {
		AudioTranscriptions(context.Context, *models.AudioRequest) (*models.AudioResponse, error)
	})
	if !ok {
		return nil, m.unsupported("audio transcription")
	}
	if req != nil {
		cp := *req
		cp.Model = m.model
		req = &cp
	}
	return a.AudioTranscriptions(ctx, req)
}

func (m *modelRewriteAdapter) Responses(ctx context.Context, req *models.ResponsesRequest) (*models.ResponsesResponse, error) {
	rp, ok := m.ProviderAdapter.(interface {
		Responses(context.Context, *models.ResponsesRequest) (*models.ResponsesResponse, error)
	})
	if !ok {
		return nil, m.unsupported("responses")
	}
	if req != nil {
		cp := *req
		cp.Model = m.model
		req = &cp
	}
	return rp.Responses(ctx, req)
}
