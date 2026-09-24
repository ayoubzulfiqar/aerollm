package intelligence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
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

// validate rejects negative or non-finite prices and limits.
func (c ModelCost) validate() error {
	for _, v := range []float64{c.InputCostPer1M, c.OutputCostPer1M} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return fmt.Errorf("invalid price %v", v)
		}
	}
	if c.MaxContextWindow < 0 || c.MaxOutputTokens < 0 {
		return errors.New("negative token limit")
	}
	return nil
}

// ModelCostMap loads and queries a JSON configuration of model costs and capabilities.
// It supports exact-match lookups, prefix/wildcard matching, and default fallback
// pricing for unknown models — similar to LiteLLM's model_prices_and_context_window.json.
//
// Model names are matched case-insensitively after normalization: provider
// prefixes ("openai/gpt-4o"), Bedrock vendor/version decorations
// ("anthropic.claude-3-haiku-20240307-v1:0"), Vertex "@date" suffixes, date
// suffixes ("gpt-4o-2024-08-06", "claude-3-opus-20240229", "gpt-4-0613") and
// "-latest" are stripped before matching. It is safe for concurrent use.
type ModelCostMap struct {
	mu         sync.RWMutex
	models     map[string]ModelCost
	prefixes   []prefixEntry // sorted longest-first for deterministic prefix matching
	defaults   ModelCost     // fallback pricing for unknown models
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

// LoadFromBytes parses a JSON cost map (as bytes) and replaces the map's
// model and prefix entries. The JSON should be an object mapping model name
// (or prefix) to ModelCost. Keys ending with "*" are treated as prefix
// matches (e.g. "gpt-4-*"). A key named "default" provides fallback pricing
// for unknown models (existing defaults are kept when it is absent).
// Negative or non-finite prices are rejected and nothing is changed.
func (m *ModelCostMap) LoadFromBytes(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("failed to parse cost map JSON: %w", err)
	}

	loaded := make(map[string]ModelCost)
	var prefixList []prefixEntry
	var def *ModelCost

	for key, rawMsg := range raw {
		var cost ModelCost
		if err := json.Unmarshal(rawMsg, &cost); err != nil {
			return fmt.Errorf("failed to parse model entry for %q: %w", key, err)
		}
		if err := cost.validate(); err != nil {
			return fmt.Errorf("invalid model entry for %q: %w", key, err)
		}
		cost.ModelName = key
		norm := strings.ToLower(strings.TrimSpace(key))
		switch {
		case norm == "":
			return errors.New("cost map contains an empty model name")
		case norm == "default":
			c := cost
			def = &c
		case strings.HasSuffix(norm, "*"):
			prefix := strings.TrimSuffix(norm, "*")
			if prefix == "" {
				// "*" alone is equivalent to "default".
				c := cost
				def = &c
				continue
			}
			prefixList = append(prefixList, prefixEntry{prefix: prefix, cost: cost})
		default:
			loaded[norm] = cost
		}
	}

	// Sort prefixes longest-first so more specific prefixes win.
	sortPrefixes(prefixList)

	m.mu.Lock()
	m.models = loaded
	m.prefixes = prefixList
	if def != nil {
		m.defaults = *def
		m.hasDefault = true
	}
	m.mu.Unlock()

	return nil
}

// defaultPrices is the curated baseline price list (USD per 1M tokens).
// Keys ending in "-" are prefix entries matching any model that starts with
// the key (e.g. "gpt-4-" matches "gpt-4-1106-preview" but not "gpt-4o").
var defaultPrices = map[string]ModelCost{
	// OpenAI
	"gpt-4":                  {InputCostPer1M: 30.0, OutputCostPer1M: 60.0, MaxContextWindow: 8192, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4-turbo":            {InputCostPer1M: 10.0, OutputCostPer1M: 30.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4o":                 {InputCostPer1M: 2.5, OutputCostPer1M: 10.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4o-mini":            {InputCostPer1M: 0.15, OutputCostPer1M: 0.6, MaxContextWindow: 128000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4.1":                {InputCostPer1M: 2.0, OutputCostPer1M: 8.0, MaxContextWindow: 1047576, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4.1-mini":           {InputCostPer1M: 0.4, OutputCostPer1M: 1.6, MaxContextWindow: 1047576, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-4.1-nano":           {InputCostPer1M: 0.1, OutputCostPer1M: 0.4, MaxContextWindow: 1047576, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"o1":                     {InputCostPer1M: 15.0, OutputCostPer1M: 60.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"o1-mini":                {InputCostPer1M: 1.1, OutputCostPer1M: 4.4, MaxContextWindow: 128000, SupportsStreaming: true},
	"o3-mini":                {InputCostPer1M: 1.1, OutputCostPer1M: 4.4, MaxContextWindow: 200000, SupportsStreaming: true, SupportsFunction: true},
	"o4-mini":                {InputCostPer1M: 1.1, OutputCostPer1M: 4.4, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-3.5-turbo":          {InputCostPer1M: 0.5, OutputCostPer1M: 1.5, MaxContextWindow: 16385, SupportsStreaming: true, SupportsFunction: true},
	"text-embedding-3-small": {InputCostPer1M: 0.02, OutputCostPer1M: 0.02, MaxContextWindow: 8192, IsEmbedding: true},
	"text-embedding-3-large": {InputCostPer1M: 0.13, OutputCostPer1M: 0.13, MaxContextWindow: 8192, IsEmbedding: true},

	// Anthropic
	"claude-3-opus":     {InputCostPer1M: 15.0, OutputCostPer1M: 75.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-3-sonnet":   {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-3-haiku":    {InputCostPer1M: 0.25, OutputCostPer1M: 1.25, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-3-5-sonnet": {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-3-5-haiku":  {InputCostPer1M: 0.8, OutputCostPer1M: 4.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsFunction: true},
	"claude-3-7-sonnet": {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-sonnet-4":   {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"claude-opus-4":     {InputCostPer1M: 15.0, OutputCostPer1M: 75.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},

	// Google
	"gemini-1.5-pro":   {InputCostPer1M: 7.0, OutputCostPer1M: 21.0, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gemini-1.5-flash": {InputCostPer1M: 0.35, OutputCostPer1M: 1.05, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gemini-2.0-flash": {InputCostPer1M: 0.075, OutputCostPer1M: 0.30, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gemini-2.5-pro":   {InputCostPer1M: 1.25, OutputCostPer1M: 10.0, MaxContextWindow: 1048576, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gemini-2.5-flash": {InputCostPer1M: 0.30, OutputCostPer1M: 2.50, MaxContextWindow: 1048576, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},

	// Llama
	"llama-3-8b":  {InputCostPer1M: 0.15, OutputCostPer1M: 0.15, MaxContextWindow: 8192, SupportsStreaming: true, SupportsFunction: true},
	"llama-3-70b": {InputCostPer1M: 0.88, OutputCostPer1M: 0.88, MaxContextWindow: 8192, SupportsStreaming: true, SupportsFunction: true},

	// Mistral
	"mistral-large":  {InputCostPer1M: 3.0, OutputCostPer1M: 9.0, MaxContextWindow: 32000, SupportsStreaming: true, SupportsFunction: true},
	"mistral-medium": {InputCostPer1M: 0.7, OutputCostPer1M: 2.1, MaxContextWindow: 32000, SupportsStreaming: true, SupportsFunction: true},

	// Cohere
	"command-r-plus": {InputCostPer1M: 3.5, OutputCostPer1M: 5.0, MaxContextWindow: 128000, SupportsStreaming: true, SupportsFunction: true},

	// Fallback prefix entries for variants
	"gpt-4-":         {InputCostPer1M: 10.0, OutputCostPer1M: 30.0, MaxContextWindow: 32768, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gpt-3.5-turbo-": {InputCostPer1M: 0.5, OutputCostPer1M: 1.5, MaxContextWindow: 16385, SupportsStreaming: true, SupportsFunction: true},
	"claude-3-":      {InputCostPer1M: 3.0, OutputCostPer1M: 15.0, MaxContextWindow: 200000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
	"gemini-":        {InputCostPer1M: 0.35, OutputCostPer1M: 1.05, MaxContextWindow: 1000000, SupportsStreaming: true, SupportsVision: true, SupportsFunction: true},
}

// LoadFromDefault merges a curated set of common model prices into the map.
// This provides baseline pricing for the most widely-used LLMs. It is
// idempotent: calling it repeatedly does not duplicate prefix entries.
func (m *ModelCostMap) LoadFromDefault() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.models == nil {
		m.models = make(map[string]ModelCost)
	}
	for name, cost := range defaultPrices {
		cost.ModelName = name
		if strings.HasSuffix(name, "-") {
			replaced := false
			for i := range m.prefixes {
				if m.prefixes[i].prefix == name {
					m.prefixes[i].cost = cost
					replaced = true
					break
				}
			}
			if !replaced {
				m.prefixes = append(m.prefixes, prefixEntry{prefix: name, cost: cost})
			}
		} else {
			m.models[name] = cost
		}
	}

	// Sort prefixes longest-first.
	sortPrefixes(m.prefixes)
}

var (
	isoDateSuffix     = regexp.MustCompile(`-\d{4}-\d{2}-\d{2}$`)
	compactDateSuffix = regexp.MustCompile(`-\d{8}$`)
	mmddSuffix        = regexp.MustCompile(`-\d{4}$`)
	bedrockVersion    = regexp.MustCompile(`(-v\d+(\.\d+)?)?(:\d+)?$`)
)

// bedrockVendors are Bedrock model-id vendor prefixes ("anthropic.claude-...").
var bedrockVendors = []string{"anthropic.", "amazon.", "meta.", "cohere.", "ai21.", "mistral.", "deepseek.", "writer.", "stability."}

// normalizeCandidates returns the progressively normalized forms of a model
// name, most specific first. Each form is tried for an exact match.
func normalizeCandidates(model string) []string {
	s := strings.ToLower(strings.TrimSpace(model))
	out := []string{s}
	add := func(v string) {
		if v != "" && v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	// Provider routing prefixes: "openai/gpt-4o", "vertex_ai/gemini-1.5-pro".
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
		add(s)
	}
	// Bedrock ids: "[us.|eu.|apac.]anthropic.claude-3-haiku-20240307-v1:0".
	for _, region := range []string{"us.", "eu.", "apac.", "global."} {
		if strings.HasPrefix(s, region) {
			rest := s[len(region):]
			for _, v := range bedrockVendors {
				if strings.HasPrefix(rest, v) {
					s = rest
					break
				}
			}
			break
		}
	}
	for _, v := range bedrockVendors {
		if strings.HasPrefix(s, v) {
			s = strings.TrimPrefix(s, v)
			s = bedrockVersion.ReplaceAllString(s, "")
			add(s)
			break
		}
	}
	// Vertex AI: "claude-3-5-sonnet@20240620".
	if i := strings.Index(s, "@"); i > 0 {
		s = s[:i]
		add(s)
	}
	if strings.HasSuffix(s, "-latest") {
		s = strings.TrimSuffix(s, "-latest")
		add(s)
	}
	for _, re := range []*regexp.Regexp{isoDateSuffix, compactDateSuffix, mmddSuffix} {
		if re.MatchString(s) {
			s = re.ReplaceAllString(s, "")
			add(s)
			break
		}
	}
	return out
}

// lookupKnownLocked resolves a model without the default fallback.
// m.mu must be held (read or write).
func (m *ModelCostMap) lookupKnownLocked(model string) (ModelCost, bool) {
	cands := normalizeCandidates(model)
	// 1. Exact match on any normalized form.
	for _, c := range cands {
		if cost, ok := m.models[c]; ok {
			return cost, true
		}
	}
	// 2. Longest prefix match. Explicit prefix entries match literally; exact
	// model names act as prefixes only on a '-' boundary ("gpt-4o" matches
	// "gpt-4o-audio-preview" but "gpt-4" never matches "gpt-4o"). On equal
	// length an explicit prefix entry wins.
	for _, name := range []string{cands[len(cands)-1], cands[0]} {
		bestLen := -1
		var best ModelCost
		for _, p := range m.prefixes { // sorted longest first
			if strings.HasPrefix(name, p.prefix) {
				bestLen = len(p.prefix)
				best = p.cost
				break
			}
		}
		// Model-name boundaries, longest first: a match on name[:i] consumes
		// i+1 bytes (the key plus the '-').
		for i := len(name) - 1; i > 0 && i+1 > bestLen; i-- {
			if name[i] != '-' {
				continue
			}
			if cost, ok := m.models[name[:i]]; ok {
				bestLen = i + 1
				best = cost
				break
			}
		}
		if bestLen >= 0 {
			return best, true
		}
	}
	return ModelCost{}, false
}

// Lookup returns the ModelCost for a given model name.
// Resolution order: exact match -> prefix match (longest prefix wins) -> default.
func (m *ModelCostMap) Lookup(model string) (ModelCost, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if cost, ok := m.lookupKnownLocked(model); ok {
		return cost, true
	}
	if m.hasDefault {
		return m.defaults, true
	}
	return ModelCost{}, false
}

// LookupKnown is like Lookup but never falls back to default pricing: ok is
// false when the model matches no exact or prefix entry.
func (m *ModelCostMap) LookupKnown(model string) (ModelCost, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lookupKnownLocked(model)
}

// usageCost converts token usage into USD, clamping negative counts to 0.
func usageCost(cost ModelCost, usage *models.Usage) float64 {
	prompt := usage.PromptTokens
	if prompt < 0 {
		prompt = 0
	}
	completion := usage.CompletionTokens
	if completion < 0 {
		completion = 0
	}
	inputCost := float64(prompt) / CostScale * cost.InputCostPer1M
	outputCost := float64(completion) / CostScale * cost.OutputCostPer1M
	return inputCost + outputCost
}

// CalculateCost computes the USD cost for a request given usage.
// Uses the model's pricing from the cost map (falling back to default pricing
// for unknown models). Costs are per 1M tokens.
func (m *ModelCostMap) CalculateCost(model string, usage *models.Usage) float64 {
	if usage == nil {
		return 0.0
	}
	cost, ok := m.Lookup(model)
	if !ok {
		return 0.0
	}
	return usageCost(cost, usage)
}

// CostFor returns the USD cost of usage for model using known pricing only.
// ok is false (and usd 0) when usage is nil or the model has no exact or
// prefix entry (i.e. only default pricing would apply).
func (m *ModelCostMap) CostFor(model string, usage *models.Usage) (usd float64, ok bool) {
	if m == nil || usage == nil {
		return 0, false
	}
	cost, ok := m.LookupKnown(model)
	if !ok {
		return 0, false
	}
	return usageCost(cost, usage), true
}

// MaxContextWindow returns the context window for a model, or 0 if unknown.
func (m *ModelCostMap) MaxContextWindow(model string) int {
	cost, ok := m.Lookup(model)
	if !ok {
		return 0
	}
	return cost.MaxContextWindow
}

// HasModel returns true if the model resolves to pricing (exact, prefix, or
// the default fallback). Use LookupKnown to exclude the default.
func (m *ModelCostMap) HasModel(model string) bool {
	_, ok := m.Lookup(model)
	return ok
}

// sortPrefixes sorts prefix entries by length descending (longest first),
// breaking ties lexically for determinism.
func sortPrefixes(entries []prefixEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if len(entries[i].prefix) != len(entries[j].prefix) {
			return len(entries[i].prefix) > len(entries[j].prefix)
		}
		return entries[i].prefix < entries[j].prefix
	})
}

// RefreshPeriodically reloads the cost map at the given interval, forever.
// It returns immediately if interval <= 0 or refreshFn is nil.
//
// Deprecated: use RefreshPeriodicallyContext, which can be stopped.
func (m *ModelCostMap) RefreshPeriodically(interval time.Duration, refreshFn func() ([]byte, error)) {
	_ = m.RefreshPeriodicallyContext(context.Background(), interval, refreshFn)
}

// RefreshPeriodicallyContext reloads the cost map from refreshFn every
// interval until ctx is done, then returns ctx.Err(). Failed fetches or
// invalid data keep the previous prices. It returns an error immediately for
// a non-positive interval or nil refreshFn.
func (m *ModelCostMap) RefreshPeriodicallyContext(ctx context.Context, interval time.Duration, refreshFn func() ([]byte, error)) error {
	if interval <= 0 {
		return fmt.Errorf("costmap refresh: interval must be positive, got %v", interval)
	}
	if refreshFn == nil {
		return errors.New("costmap refresh: nil refresh function")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			data, err := refreshFn()
			if err != nil {
				continue
			}
			_ = m.LoadFromBytes(data)
		}
	}
}
