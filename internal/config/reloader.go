package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// RedactedValue replaces secrets in configs returned to clients. Submitting
// it back through an update keeps the previously configured secret.
const RedactedValue = "***REDACTED***"

// ModelInfo describes a loaded model and its capabilities for /model/info.
type ModelInfo struct {
	Model         string   `json:"model"`
	Provider      string   `json:"provider"`
	ProviderType  string   `json:"provider_type"`
	Capabilities  []string `json:"capabilities"`
	OwnedTokens   int64    `json:"owned_tokens"`
	ContextWindow int      `json:"context_window"`
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
// All reads in the hot path go through the atomic pointer. Treat it as
// immutable.
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
// when a hot-reload occurs.
func (r *ConfigReloader) SetReloadCallback(fn func(ctx context.Context, cfg *Config) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onReload = fn
}

// Reload validates cfg and atomically swaps it in. If a reload callback is
// registered it runs first; a callback error leaves the current config active.
func (r *ConfigReloader) Reload(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.onReload != nil {
		if err := r.onReload(ctx, cfg); err != nil {
			return err
		}
	}

	r.current.Store(&ReloadedState{
		Config: cfg,
		Models: buildModelInfo(cfg),
	})
	return nil
}

// ApplyUpdate merges a JSON document onto a copy of the current config (keys
// absent from the document keep their current values; arrays such as
// providers are replaced wholesale), restores secrets submitted as
// RedactedValue, validates the result and reloads it.
func (r *ConfigReloader) ApplyUpdate(ctx context.Context, patch []byte) (*Config, error) {
	r.mu.Lock()
	cur := r.current.Load()
	r.mu.Unlock()

	var base *Config
	if cur != nil && cur.Config != nil {
		base = cur.Config
	} else {
		base = &Config{}
	}
	next, err := cloneConfig(base)
	if err != nil {
		return nil, err
	}
	// Providers are replaced, not merged element-wise, so a shorter list
	// actually removes providers.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(patch, &probe); err != nil {
		return nil, fmt.Errorf("invalid config JSON: %w", err)
	}
	if _, ok := probe["providers"]; ok {
		next.Providers = nil
	}
	dec := json.NewDecoder(strings.NewReader(string(patch)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(next); err != nil {
		return nil, fmt.Errorf("invalid config JSON: %w", err)
	}
	restoreSecrets(next, base)
	ExpandEnv(next)
	if err := r.Reload(ctx, next); err != nil {
		return nil, err
	}
	return next, nil
}

// GetModels returns the current list of model info entries.
func (r *ConfigReloader) GetModels() []ModelInfo {
	s := r.current.Load()
	if s == nil {
		return nil
	}
	return s.Models
}

// MaskSecrets returns a deep copy of the current config with every secret
// redacted. The live config is never modified.
func (r *ConfigReloader) MaskSecrets() *Config {
	s := r.current.Load()
	if s == nil || s.Config == nil {
		return nil
	}
	cfg, err := cloneConfig(s.Config)
	if err != nil {
		return nil
	}
	mask := func(v *string) {
		if *v != "" {
			*v = RedactedValue
		}
	}
	for i := range cfg.Providers {
		mask(&cfg.Providers[i].APIKey)
	}
	mask(&cfg.Redis.Password)
	mask(&cfg.Auth.MasterKey)
	for i := range cfg.Auth.AdminKeys {
		mask(&cfg.Auth.AdminKeys[i])
	}
	for i := range cfg.Auth.APIKeys {
		mask(&cfg.Auth.APIKeys[i])
	}
	mask(&cfg.Callbacks.Webhook.Secret)
	mask(&cfg.Callbacks.Langfuse.APIKey)
	mask(&cfg.Callbacks.Langfuse.SecretKey)
	mask(&cfg.Callbacks.Datadog.APIKey)
	return cfg
}

func cloneConfig(c *Config) (*Config, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var out Config
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// restoreSecrets puts back secrets that a client echoed as RedactedValue.
func restoreSecrets(next, prev *Config) {
	keep := func(dst *string, src string) {
		if *dst == RedactedValue {
			*dst = src
		}
	}
	prevProviders := make(map[string]string, len(prev.Providers))
	for _, p := range prev.Providers {
		prevProviders[p.ResolvedName()] = p.APIKey
	}
	for i := range next.Providers {
		if next.Providers[i].APIKey == RedactedValue {
			next.Providers[i].APIKey = prevProviders[next.Providers[i].ResolvedName()]
		}
	}
	keep(&next.Redis.Password, prev.Redis.Password)
	keep(&next.Auth.MasterKey, prev.Auth.MasterKey)
	keep(&next.Callbacks.Webhook.Secret, prev.Callbacks.Webhook.Secret)
	keep(&next.Callbacks.Langfuse.APIKey, prev.Callbacks.Langfuse.APIKey)
	keep(&next.Callbacks.Langfuse.SecretKey, prev.Callbacks.Langfuse.SecretKey)
	keep(&next.Callbacks.Datadog.APIKey, prev.Callbacks.Datadog.APIKey)
	next.Auth.AdminKeys = restoreList(next.Auth.AdminKeys, prev.Auth.AdminKeys)
	next.Auth.APIKeys = restoreList(next.Auth.APIKeys, prev.Auth.APIKeys)
}

// restoreList keeps redacted entries by position and drops ones that have no
// previous value, so a redacted placeholder can never become a real key.
func restoreList(next, prev []string) []string {
	out := next[:0]
	for i, v := range next {
		if v == RedactedValue {
			if i < len(prev) {
				out = append(out, prev[i])
			}
			continue
		}
		out = append(out, v)
	}
	return out
}

// buildModelInfo constructs a list of model descriptors from provider config.
func buildModelInfo(cfg *Config) []ModelInfo {
	if cfg == nil {
		return nil
	}
	out := make([]ModelInfo, 0, len(cfg.Providers)*8)
	for _, p := range cfg.Providers {
		if !p.Usable() {
			continue // not served (e.g. missing API key)
		}
		for _, m := range p.Models {
			out = append(out, ModelInfo{
				Model:         m,
				Provider:      p.ResolvedName(),
				ProviderType:  p.Type,
				Capabilities:  capabilitiesFor(p.Type),
				ContextWindow: DefaultContextWindow(m),
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
		return []string{"chat", "tools"}
	case "cohere":
		return []string{"chat", "embeddings"}
	case "deepseek":
		return []string{"chat", "tools"}
	default:
		return []string{"chat", "embeddings", "image", "audio", "responses"}
	}
}

// DefaultContextWindow returns the context window (in tokens) for well-known
// model families, matched by the most specific prefix/substring.
func DefaultContextWindow(model string) int {
	m := strings.ToLower(model)
	// Ordered from most to least specific.
	table := []struct {
		match  string
		window int
	}{
		{"gpt-4.1", 1047576},
		{"gpt-4o", 128000},
		{"gpt-4-turbo", 128000},
		{"gpt-4-32k", 32768},
		{"gpt-4", 8192},
		{"gpt-3.5-turbo", 16385},
		{"o1", 200000},
		{"o3", 200000},
		{"o4", 200000},
		{"claude", 200000},
		{"gemini-1.5-pro", 2097152},
		{"gemini-1.5", 1048576},
		{"gemini-2", 1048576},
		{"gemini", 32768},
		{"llama-3.1", 131072},
		{"llama-3.2", 131072},
		{"llama-3.3", 131072},
		{"llama3", 8192},
		{"llama-3", 8192},
		{"mixtral", 32768},
		{"deepseek", 65536},
		{"command-r", 128000},
	}
	for _, e := range table {
		if strings.Contains(m, e.match) {
			return e.window
		}
	}
	return 4096
}
