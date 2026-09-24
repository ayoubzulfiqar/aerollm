package finops

import (
	"math"
	"regexp"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// Pricing source labels reported in CostBreakdown.PricingSource.
const (
	SourceOverride   = "override"
	SourceCostMap    = "cost_map"
	SourcePricingMap = "pricing_map"
	SourceFallback   = "fallback"
)

// DefaultUnknownModelPrice is charged for models that no pricing table
// knows. It is deliberately conservative (USD per 1M tokens) so that
// budgets cannot be bypassed by requesting an unpriced model name.
var DefaultUnknownModelPrice = ModelPrice{InputPer1M: 10, OutputPer1M: 30}

// ModelPrice is a resolved per-model price in USD per 1M tokens.
type ModelPrice struct {
	// Model is the name the price was found under.
	Model       string  `json:"model"`
	InputPer1M  float64 `json:"input_per_1m"`
	OutputPer1M float64 `json:"output_per_1m"`
	Source      string  `json:"source"`
	Known       bool    `json:"known"`
}

// CostBreakdown is the itemised cost of one request.
type CostBreakdown struct {
	Model            string  `json:"model"`
	PricedAs         string  `json:"priced_as"`
	PromptTokens     int     `json:"prompt_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	InputCostUSD     float64 `json:"input_cost_usd"`
	OutputCostUSD    float64 `json:"output_cost_usd"`
	TotalCostUSD     float64 `json:"total_cost_usd"`
	PricingSource    string  `json:"pricing_source"`
	KnownModel       bool    `json:"known_model"`
}

var (
	// versionSuffixRe matches dated / versioned model suffixes such as
	// -2024-08-06, -20240620, @20240620, -0613, -08-2024, -latest, -preview,
	// -v1:0 and :0.
	versionSuffixRe = regexp.MustCompile(`(?:[-@](?:\d{4}-\d{2}-\d{2}|\d{2}-\d{4}|\d{8}|\d{4}|latest|preview|v\d+(?::\d+)?)|:\d+)$`)
	// bedrockVendors are "vendor." prefixes used by AWS Bedrock model IDs.
	bedrockVendors = []string{"anthropic.", "meta.", "amazon.", "cohere.", "mistral.", "ai21.", "us.anthropic.", "eu.anthropic."}
)

// NormalizeModelName strips provider routing prefixes ("openai/gpt-4o"),
// Bedrock vendor prefixes and date/version suffixes, returning the base
// model family name in lower case (e.g. "gpt-4o-2024-08-06" -> "gpt-4o").
func NormalizeModelName(model string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	for _, v := range bedrockVendors {
		if strings.HasPrefix(m, v) {
			m = strings.TrimPrefix(m, v)
			break
		}
	}
	for i := 0; i < 4; i++ {
		loc := versionSuffixRe.FindStringIndex(m)
		if loc == nil || loc[0] == 0 {
			break
		}
		m = m[:loc[0]]
	}
	return m
}

// modelCandidates returns lookup names from most literal to most normalised.
func modelCandidates(model string) []string {
	out := make([]string, 0, 4)
	add := func(s string) {
		if s == "" {
			return
		}
		for _, o := range out {
			if o == s {
				return
			}
		}
		out = append(out, s)
	}
	add(model)
	lower := strings.ToLower(strings.TrimSpace(model))
	add(lower)
	if i := strings.LastIndex(lower, "/"); i >= 0 {
		add(lower[i+1:])
	}
	add(NormalizeModelName(model))
	return out
}

func validPrice(in, out float64) bool {
	return in >= 0 && out >= 0 && !math.IsNaN(in) && !math.IsNaN(out) && !math.IsInf(in, 0) && !math.IsInf(out, 0)
}

const unknownModelProbe = "\x00aerollm-unknown-model\x00"

func sameCost(a, b intelligence.ModelCost) bool {
	return a.ModelName == b.ModelName && a.InputCostPer1M == b.InputCostPer1M &&
		a.OutputCostPer1M == b.OutputCostPer1M && a.MaxContextWindow == b.MaxContextWindow
}

// ResolvePricing finds the price for model. Resolution order:
//  1. explicit overrides (SetModelPrice) on any candidate name;
//  2. an exact entry in the ModelCostMap loaded from JSON;
//  3. the ModelCostMap (exact or prefix) for the normalised name first,
//     then the literal name — so "gpt-4o-2024-08-06" is priced as "gpt-4o"
//     instead of matching the shorter "gpt-4" prefix;
//  4. the legacy PricingMap (USD per 1K tokens) on any candidate name;
//  5. the conservative fallback price (Known=false).
//
// The ModelCostMap's catch-all default is never used for unknown models.
func (c *CostTracker) ResolvePricing(model string) ModelPrice {
	cands := modelCandidates(model)

	c.priceMu.RLock()
	for _, cand := range cands {
		if p, ok := c.overrides[cand]; ok {
			c.priceMu.RUnlock()
			p.Model, p.Source, p.Known = cand, SourceOverride, true
			return p
		}
	}
	unknown := c.unknownPrice
	c.priceMu.RUnlock()

	if c.costMap != nil {
		def, hasDef := c.costMap.Lookup(unknownModelProbe)
		isDefault := func(mc intelligence.ModelCost) bool { return hasDef && sameCost(mc, def) }
		for _, cand := range cands {
			if mc, ok := c.costMap.Lookup(cand); ok && mc.ModelName == cand && !isDefault(mc) && validPrice(mc.InputCostPer1M, mc.OutputCostPer1M) {
				return ModelPrice{Model: cand, InputPer1M: mc.InputCostPer1M, OutputPer1M: mc.OutputCostPer1M, Source: SourceCostMap, Known: true}
			}
		}
		for i := len(cands) - 1; i >= 0; i-- {
			cand := cands[i]
			if mc, ok := c.costMap.Lookup(cand); ok && !isDefault(mc) && validPrice(mc.InputCostPer1M, mc.OutputCostPer1M) {
				return ModelPrice{Model: cand, InputPer1M: mc.InputCostPer1M, OutputPer1M: mc.OutputCostPer1M, Source: SourceCostMap, Known: true}
			}
		}
	}
	if c.prices != nil {
		for _, cand := range cands {
			if p, ok := c.prices.Get(cand); ok && validPrice(p.PromptPrice, p.CompletionPrice) {
				return ModelPrice{Model: cand, InputPer1M: p.PromptPrice * 1000, OutputPer1M: p.CompletionPrice * 1000, Source: SourcePricingMap, Known: true}
			}
		}
	}
	unknown.Model, unknown.Source, unknown.Known = NormalizeModelName(model), SourceFallback, false
	return unknown
}

// SetModelPrice installs an explicit price override (USD per 1M tokens)
// that wins over every pricing table. Invalid (negative/NaN/Inf) prices are
// rejected.
func (c *CostTracker) SetModelPrice(model string, inputPer1M, outputPer1M float64) bool {
	if model == "" || !validPrice(inputPer1M, outputPer1M) {
		return false
	}
	c.priceMu.Lock()
	defer c.priceMu.Unlock()
	c.overrides[strings.ToLower(strings.TrimSpace(model))] = ModelPrice{InputPer1M: inputPer1M, OutputPer1M: outputPer1M}
	return true
}

// SetUnknownModelPrice sets the price charged for unrecognised models.
func (c *CostTracker) SetUnknownModelPrice(inputPer1M, outputPer1M float64) bool {
	if !validPrice(inputPer1M, outputPer1M) {
		return false
	}
	c.priceMu.Lock()
	defer c.priceMu.Unlock()
	c.unknownPrice = ModelPrice{InputPer1M: inputPer1M, OutputPer1M: outputPer1M}
	return true
}

// CostBreakdown prices usage for model. Negative token counts are treated
// as zero. A nil usage yields a zero-cost breakdown.
func (c *CostTracker) CostBreakdown(model string, usage *models.Usage) CostBreakdown {
	b := CostBreakdown{Model: model}
	if usage == nil {
		return b
	}
	b.PromptTokens = max(usage.PromptTokens, 0)
	b.CompletionTokens = max(usage.CompletionTokens, 0)
	p := c.ResolvePricing(model)
	b.PricedAs, b.PricingSource, b.KnownModel = p.Model, p.Source, p.Known
	b.InputCostUSD = float64(b.PromptTokens) * p.InputPer1M / 1e6
	b.OutputCostUSD = float64(b.CompletionTokens) * p.OutputPer1M / 1e6
	b.TotalCostUSD = b.InputCostUSD + b.OutputCostUSD
	return b
}

// EstimateCost returns the USD cost of usage for model (see CostBreakdown).
func (c *CostTracker) EstimateCost(model string, usage *models.Usage) float64 {
	return c.CostBreakdown(model, usage).TotalCostUSD
}

// CalculateCost computes cost in USD for a request. It is kept for
// backward compatibility and is identical to EstimateCost.
func (c *CostTracker) CalculateCost(model string, usage *models.Usage) float64 {
	return c.EstimateCost(model, usage)
}

// EstimateRequestCost is an upper-bound estimate for a request that has not
// run yet: promptTokens at the input price plus maxCompletionTokens at the
// output price. Use it for CheckBudget pre-checks.
func (c *CostTracker) EstimateRequestCost(model string, promptTokens, maxCompletionTokens int) float64 {
	return c.EstimateCost(model, &models.Usage{PromptTokens: promptTokens, CompletionTokens: maxCompletionTokens})
}
