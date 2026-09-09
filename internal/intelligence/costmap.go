package intelligence

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// CostScale defines the billing precision for token pricing.
// Prices are expressed as USD per 1M tokens, matching LiteLLM's convention.
const CostScale = 1_000_000.0

// ModelCost holds pricing and capability metadata for a single model.
type ModelCost struct {
	ModelName         string  `json:"model_name"`
	Provider          string  `json:"provider"`
	InputCostPer1M    float64 `json:"input_cost_per_1m_tokens"`
	OutputCostPer1M   float64 `json:"output_cost_per_1m_tokens"`
	MaxContextWindow  int     `json:"max_context_window"`
	MaxOutputTokens   int     `json:"max_output_tokens"`
	IsEmbedding       bool    `json:"is_embedding"`
	IsImage           bool    `json:"is_image"`
	IsAudio           bool    `json:"is_audio"`
	SupportsVision    bool    `json:"supports_vision"`
	SupportsFunction  bool    `json:"supports_function_calling"`
	SupportsStreaming bool    `json:"supports_streaming"`
}

// ModelCostMap loads and queries a JSON configuration of model costs and capabilities.
// It supports exact-match lookups, prefix/wildcard matching, and default fallback
// pricing for unknown models — similar to LiteLLM's model_prices_and_context_window.json.
type ModelCostMap struct {
	mu       sync.RWMutex
	models   map[string]ModelCost
	prefixes []prefixEntry // sorted longest-first for deterministic prefix matching
	defaults ModelCost     // fallback pricing for unknown models
	hasDefault bool
}

// prefixEntry pairs a prefix string with a ModelCost.
type prefixEntry struct {
	prefix string
	cost   ModelCost
}

// NewModelCostMap creates a new empty cost map with sensible fallback defaults.
func NewModelCostMap() *ModelCostMap {
	m := &ModelCostMap{
		models:   make(map[string]ModelCost),
		prefixes: make([]prefixEntry, 0),
	}
	// Conservative default pricing for unknown models.
	m.defaults = ModelCost{
		InputCostPer1M:   0.01,
		OutputCostPer1M:  0.02,
		MaxContextWindow: 4096,
	}
	m.hasDefault = true
	return m
}

// LoadFromBytes parses a JSON cost map (as bytes) and populates the map.
// The JSON should be an object mapping model name (or prefix) to ModelCost.
// Keys ending with "*" are treated as prefix matches (e.g. "gpt-4-*").
// A key named "default" provides fallback pricing for unknown models.
func (m *ModelCostMap) LoadFromBytes(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("failed to parse cost map JSON: %w", err)
	}

	loaded := make(map[string]ModelCost)
	var prefixList []prefixEntry

	for key, rawMsg := range raw {
		var cost ModelCost
		if err := json.Unmarshal(rawMsg, &cost); err != nil {
			return fmt.Errorf("failed to parse model entry for %q: %w", key, err)
		}
		cost.ModelName = key

		if strings.HasSuffix(key, "*") {
			prefix := strings.TrimSuffix(key, "*")
			prefixList = append(prefixList, prefixEntry{prefix: prefix, cost: cost})
		} else {
			loaded[key] = cost
		}
	}

	// Sort prefixes longest-first so more specific prefixes win.
	sortPrefixes(prefixList)

	m.mu.Lock()
	m.models = loaded
	m.prefixes = prefixList
	m.mu.Unlock()

	return nil
}

// LoadFromDefault loads a curated set of common model prices.
// This provides baseline pricing for the most widely-used LLMs.
func (m *ModelCostMap) LoadFromDefault() {
	defaults := map[string]ModelCost{
		// OpenAI
		"gpt-4":                 {InputCostPer1M: 30.0, OutputCostPer1M: 60.0, MaxContextWindow: 8192, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gpt-4-turbo":           {InputCostPer1M: 10.0, OutputCostPer1M: 30.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gpt-4o":                {InputCostPer1M: 5.0, OutputCostPer1M: 15.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gpt-4o-mini":           {InputCostPer1M: 0.15, OutputCostPer1M: 0.6, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gpt-3.5-turbo":         {InputCostPer1M: 0.5, OutputCostPer1M: 1.5, MaxContextWindow: 16385, SupportsStreaming: true, SupportsFunction: true},
		"text-embedding-3-small": {InputCostPer1M: 0.02, OutputCostPer1M: 0.02, MaxContextWindow: 8192, IsEmbedding: true},
		"text-embedding-3-large": {InputCostPer1M: 0.13, OutputCostPer1M: 0.13, MaxContextWindow: 8192, IsEmbedding: true},

		// Anthropic
		"claude-3-opus":         {InputCostPer1M: 15.0, OutputCostPer1M: 75.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"claude-3-sonnet":       {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"claude-3-haiku":        {InputCostPer1M: 0.25, OutputCostPer1M: 1.25, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"claude-3-5-sonnet":     {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},

		// Google
		"gemini-1.5-pro":        {InputCostPer1M: 7.0, OutputCostPer1M: 21.0, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gemini-1.5-flash":      {InputCostPer1M: 0.35, OutputCostPer1M: 1.05, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gemini-2.0-flash":      {InputCostPer1M: 0.075, OutputCostPer1M: 0.30, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},

		// Llama
		"llama-3-8b":            {InputCostPer1M: 0.15, OutputCostPer1M: 0.15, MaxContextWindow: 8192, SupportsStreaming: true, SupportsFunction: true},
		"llama-3-70b":           {InputCostPer1M: 0.88, OutputCostPer1M: 0.88, MaxContextWindow: 8192, SupportsStreaming: true, SupportsFunction: true},

		// Mistral
		"mistral-large":         {InputCostPer1M: 3.0, OutputCostPer1M: 9.0, MaxContextWindow: 32000, SupportsStreaming: true, SupportsFunction: true},
		"mistral-medium":        {InputCostPer1M: 0.7, OutputCostPer1M: 2.1, MaxContextWindow: 32000, SupportsStreaming: true, SupportsFunction: true},

		// Cohere
		"command-r-plus":        {InputCostPer1M: 3.5, OutputCostPer1M: 5.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsFunction: true},

		// Fallback prefix entries for variants
		"gpt-4-":                {InputCostPer1M: 10.0, OutputCostPer1M: 30.0, MaxContextWindow: 32768, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gpt-3.5-turbo-":        {InputCostPer1M: 0.5, OutputCostPer1M: 1.5, MaxContextWindow: 16385, SupportsStreaming: true, SupportsFunction: true},
		"claude-3-":             {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
		"gemini-":               {InputCostPer1M: 0.35, OutputCostPer1M: 1.05, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for name, cost := range defaults {
		if strings.HasSuffix(name, "-") {
			prefix := strings.TrimSuffix(name, "-")
			m.prefixes = append(m.prefixes, prefixEntry{prefix: prefix, cost: cost})
		} else {
			m.models[name] = cost
		}
	}

	// Sort prefixes longest-first.
	sortPrefixes(m.prefixes)
}

// Lookup returns the ModelCost for a given model name.
// Resolution order: exact match -> prefix match (longest prefix wins) -> default.
func (m *ModelCostMap) Lookup(model string) (ModelCost, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. Exact match.
	if cost, ok := m.models[model]; ok {
		return cost, true
	}

	// 2. Prefix match (longest prefix wins).
	for _, p := range m.prefixes {
		if strings.HasPrefix(model, p.prefix) {
			return p.cost, true
		}
	}

	// 3. Default fallback.
	if m.hasDefault {
		return m.defaults, true
	}

	return ModelCost{}, false
}

// CalculateCost computes the USD cost for a request given usage.
// Uses the model's pricing from the cost map. Costs are per 1M tokens.
func (m *ModelCostMap) CalculateCost(model string, usage *models.Usage) float64 {
	if usage == nil {
		return 0.0
	}
	cost, ok := m.Lookup(model)
	if !ok {
		return 0.0
	}
	inputCost := float64(usage.PromptTokens) / CostScale * cost.InputCostPer1M
	outputCost := float64(usage.CompletionTokens) / CostScale * cost.OutputCostPer1M
	return inputCost + outputCost
}

// MaxContextWindow returns the context window for a model, or 0 if unknown.
func (m *ModelCostMap) MaxContextWindow(model string) int {
	cost, ok := m.Lookup(model)
	if !ok {
		return 0
	}
	return cost.MaxContextWindow
}

// HasModel returns true if the model exists in the map (exact or prefix).
func (m *ModelCostMap) HasModel(model string) bool {
	_, ok := m.Lookup(model)
	return ok
}

// sortPrefixes sorts prefix entries by length descending (longest first).
func sortPrefixes(entries []prefixEntry) {
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if len(entries[j].prefix) > len(entries[i].prefix) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
}

// RefreshPeriodically reloads the cost map at the given interval.
// The refresh function is called to obtain fresh price data.
func (m *ModelCostMap) RefreshPeriodically(interval time.Duration, refreshFn func() ([]byte, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			data, err := refreshFn()
			if err != nil {
				continue
			}
			_ = m.LoadFromBytes(data)
		}
	}
}
