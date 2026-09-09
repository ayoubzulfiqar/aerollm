package intelligence

import (
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestLookupExactMatch(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	cost, ok := m.Lookup("gpt-4o")
	if !ok {
		t.Fatal("expected to find gpt-4o")
	}
	if cost.InputCostPer1M != 5.0 {
		t.Fatalf("expected input cost 5.0, got %f", cost.InputCostPer1M)
	}
	if cost.MaxContextWindow != 128000 {
		t.Fatalf("expected context window 128000, got %d", cost.MaxContextWindow)
	}
}

func TestLookupPrefixMatch(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	// "gpt-4-turbo" exists as exact match, but "gpt-4-turbo-2024" should match prefix "gpt-4-"
	cost, ok := m.Lookup("gpt-4-turbo-2024-preview")
	if !ok {
		t.Fatal("expected prefix match for gpt-4-turbo-2024-preview")
	}
	if cost.InputCostPer1M != 10.0 {
		t.Fatalf("expected prefix input cost 10.0, got %f", cost.InputCostPer1M)
	}
}

func TestLookupDefaultFallback(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	// Unknown model should fall back to default pricing.
	cost, ok := m.Lookup("unknown-model-xyz")
	if !ok {
		t.Fatal("expected default fallback for unknown model")
	}
	if cost.InputCostPer1M != 0.01 {
		t.Fatalf("expected default input cost 0.01, got %f", cost.InputCostPer1M)
	}
}

func TestCalculateCost(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	usage := &models.Usage{
		PromptTokens:     1000,
		CompletionTokens: 500,
	}
	// gpt-4o: input 5.0/1M, output 15.0/1M
	// Cost = (1000/1M * 5.0) + (500/1M * 15.0) = 0.005 + 0.0075 = 0.0125
	cost := m.CalculateCost("gpt-4o", usage)
	expected := 0.0125
	if !approxEqual(cost, expected, 0.0001) {
		t.Fatalf("expected cost ~%.4f, got %.4f", expected, cost)
	}
}

func TestCalculateCostNilUsage(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	if m.CalculateCost("gpt-4o", nil) != 0.0 {
		t.Fatal("expected 0 cost for nil usage")
	}
}

func TestCalculateCostUnknownModel(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	usage := &models.Usage{
		PromptTokens:     1000,
		CompletionTokens: 500,
	}
	// Should use default pricing (0.01/1M input, 0.02/1M output).
	cost := m.CalculateCost("unknown-model", usage)
	expected := (1000.0/1_000_000.0 * 0.01) + (500.0/1_000_000.0 * 0.02)
	if !approxEqual(cost, expected, 0.0001) {
		t.Fatalf("expected cost ~%.6f, got %.6f", expected, cost)
	}
}

func TestMaxContextWindow(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	if m.MaxContextWindow("gpt-4-turbo") != 128000 {
		t.Fatalf("expected 128000, got %d", m.MaxContextWindow("gpt-4-turbo"))
	}
	if m.MaxContextWindow("unknown-model") == 0 {
		t.Fatal("expected default context window for unknown model")
	}
}

func TestHasModel(t *testing.T) {
	m := NewModelCostMap()
	m.LoadFromDefault()

	if !m.HasModel("gpt-4o") {
		t.Fatal("expected HasModel(gpt-4o) to be true")
	}
	if !m.HasModel("unknown-model") {
		t.Fatal("expected HasModel(unknown-model) to be true (default fallback)")
	}
}

func TestLoadFromBytes(t *testing.T) {
	m := NewModelCostMap()

	jsonData := `{
		"my-custom-model": {"input_cost_per_1m_tokens": 1.5, "output_cost_per_1m_tokens": 3.0, "max_context_window": 32000},
		"gpt-4-*": {"input_cost_per_1m_tokens": 20.0, "output_cost_per_1m_tokens": 40.0, "max_context_window": 8192}
	}`

	if err := m.LoadFromBytes([]byte(jsonData)); err != nil {
		t.Fatalf("failed to load from bytes: %v", err)
	}

	cost, ok := m.Lookup("my-custom-model")
	if !ok {
		t.Fatal("expected to find my-custom-model")
	}
	if cost.InputCostPer1M != 1.5 {
		t.Fatalf("expected input cost 1.5, got %f", cost.InputCostPer1M)
	}

	// Prefix match.
	cost, ok = m.Lookup("gpt-4-vision-preview")
	if !ok {
		t.Fatal("expected prefix match for gpt-4-vision-preview")
	}
	if cost.InputCostPer1M != 20.0 {
		t.Fatalf("expected prefix input cost 20.0, got %f", cost.InputCostPer1M)
	}
}

func TestSetStrategyStillWorks(t *testing.T) {
	// Ensure router still compiles with the new cost map integration.
	m := NewModelCostMap()
	m.LoadFromDefault()

	if m.CalculateCost("claude-3-opus", &models.Usage{
		PromptTokens:     1000000,
		CompletionTokens: 100000,
	}) <= 0 {
		t.Fatal("expected positive cost for claude-3-opus")
	}
}

func approxEqual(a, b, epsilon float64) bool {
	return a-b < epsilon && b-a < epsilon
}
