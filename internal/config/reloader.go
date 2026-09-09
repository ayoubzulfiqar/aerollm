package config

import (
	"context"
	"sync"
	"sync/atomic"
)

// ModelInfo describes a loaded model and its capabilities for /model/info.
type ModelInfo struct {
	Model      string   `json:"model"`
	Provider   string   `json:"provider"`
	ProviderType string `json:"provider_type"`
	Capabilities []string `json:"capabilities"`
	OwnedTokens int64   `json:"owned_tokens"`
	ContextWindow int   `json:"context_window"`
}

// ConfigReloader safely swaps the active configuration and provider registry
// without dropping active connections or causing race conditions.
// It uses atomic pointers for lock-free reads by the hot path.
type ConfigReloader struct {
	current  atomic.Pointer[ReloadedState]
	mu       sync.Mutex
	onReload func(ctx context.Context, cfg *Config) error // called to rebuild registry on reload
}

// ReloadedState is the snapshot that replaces the current config atomically.
// All reads in the hot path go through the atomic pointer.
type ReloadedState struct {
	Config *Config
	Models []ModelInfo
}


// NewConfigReloader creates a new reloader with the given initial config.
func NewConfigReloader(initial *Config) *ConfigReloader {
	r := &ConfigReloader{}
	r.current.Store(&ReloadedState{
		Config: initial,
		Models: buildModelInfo(initial),
	})
	return r
}

// Current returns the atomically-loaded current state.
func (r *ConfigReloader) Current() *ReloadedState {
	return r.current.Load()
}

// SetReloadCallback registers a function that rebuilds the provider registry
// when a hot-reload occurs. This must be called after NewConfigReloader.
func (r *ConfigReloader) SetReloadCallback(fn func(ctx context.Context, cfg *Config) error) {
	r.onReload = fn
}

// Reload atomically swaps in a new config + model list.
// If a reload callback is registered, it is invoked to rebuild the provider registry.
func (r *ConfigReloader) Reload(ctx context.Context, cfg *Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Rebuild the provider registry if a callback is set.
	if r.onReload != nil {
		if err := r.onReload(ctx, cfg); err != nil {
			return err
		}
	}

	state := &ReloadedState{
		Config: cfg,
		Models: buildModelInfo(cfg),
	}
	r.current.Store(state)
	return nil
}

// GetModels returns the current list of model info entries.
func (r *ConfigReloader) GetModels() []ModelInfo {
	s := r.current.Load()
	if s == nil {
		return nil
	}
	return s.Models
}

// MaskSecrets returns a copy of the config with all API keys redacted.
func (r *ConfigReloader) MaskSecrets() *Config {
	s := r.current.Load()
	if s == nil || s.Config == nil {
		return nil
	}
	cfg := *s.Config
	for i := range cfg.Providers {
		cfg.Providers[i].APIKey = "***REDACTED***"
	}
	return &cfg
}

// buildModelInfo constructs a list of model descriptors from provider config.
func buildModelInfo(cfg *Config) []ModelInfo {
	if cfg == nil {
		return nil
	}
	out := make([]ModelInfo, 0, len(cfg.Providers)*8)
	for _, p := range cfg.Providers {
		for _, m := range p.Models {
			out = append(out, ModelInfo{
				Model:         m,
				Provider:      p.ResolvedName(),
				ProviderType:  p.Type,
				Capabilities:  capabilitiesFor(p.Type),
				OwnedTokens:   0,
				ContextWindow: defaultContextWindow(m),
			})
		}
	}
	return out
}

// capabilitiesFor returns known capabilities for a provider type.
func capabilitiesFor(ptype string) []string {
	switch ptype {
	case "anthropic":
		return []string{"chat", "tools", "vision", "caching"}
	case "bedrock":
		return []string{"chat", "embeddings", "tools", "image"}
	case "gemini":
		return []string{"chat", "embeddings", "image", "audio"}
	case "groq":
		return []string{"chat", "embeddings"}
	default:
		return []string{"chat", "embeddings", "image", "audio", "responses"}
	}
}

// defaultContextWindow returns a rough default context window for a model name.
func defaultContextWindow(model string) int {
	// heuristic: check for common model families
	switch {
	case contains(model, "gpt-4o") || contains(model, "claude-3-5-sonnet-20240620"):
		return 200000
	case contains(model, "gpt-4") || contains(model, "claude-3"):
		return 100000
	case contains(model, "gpt-3.5"):
		return 16000
	case contains(model, "gemini-1.5-pro"):
		return 2000000
	case contains(model, "gemini-1.5"):
		return 1000000
	case contains(model, "gemini"):
		return 30720
	default:
		return 4096
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
