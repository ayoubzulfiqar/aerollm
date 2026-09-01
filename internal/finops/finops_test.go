package finops

import (
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestPricingMapDefaults(t *testing.T) {
	p := NewPricingMap()
	pr, ok := p.Get("gpt-4")
	if !ok || pr.PromptPrice != 0.03 {
		t.Fatalf("unexpected pricing: %+v", pr)
	}
}

func TestCalculateCost(t *testing.T) {
	p := NewPricingMap()
	c := NewCostTracker(nil, p)
	cost := c.CalculateCost("gpt-4", &models.Usage{PromptTokens: 10, CompletionTokens: 20})
	expected := 10*0.03 + 20*0.06
	if cost != expected {
		t.Fatalf("expected %.4f, got %.4f", expected, cost)
	}
}
