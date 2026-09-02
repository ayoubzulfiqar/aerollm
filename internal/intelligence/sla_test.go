package intelligence

import (
	"context"
	"testing"
)

func TestSLASelectorFiltersAndPicksCheapest(t *testing.T) {
	selector := NewSLASelector()
	opts := []ModelOptionWithSLA{
		{ModelOption: ModelOption{Provider: "a", Model: "slow", Cost: 0.01, Latency: 5000, Quality: 0.9}, Available: true},
		{ModelOption: ModelOption{Provider: "b", Model: "fast", Cost: 0.005, Latency: 1000, Quality: 0.7}, Available: true},
		{ModelOption: ModelOption{Provider: "c", Model: "mid", Cost: 0.002, Latency: 2000, Quality: 0.5}, Available: true},
	}

	selected, err := selector.Select(context.Background(), opts, SLA{MaxLatencyMs: 3000, PreferCheapest: true})
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if selected.Model != "mid" {
		t.Fatalf("expected mid, got %s", selected.Model)
	}
}
