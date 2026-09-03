package hardware

import (
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
)

func TestLocalDetectorReturnsCPUAlways(t *testing.T) {
	d := NewLocalDetector()
	caps := d.Detect()
	found := false
	for _, c := range caps {
		if c.Name == "cpu" && c.Available {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cpu capability, got %+v", caps)
	}
}

func TestHardwareAwareSelectorFallsBack(t *testing.T) {
	sel := NewHardwareAwareSelector(intelligence.NewHeuristicSelector(), NewLocalDetector())
	_, err := sel.Select(nil, []intelligence.ModelOption{{Provider: "a", Model: "m", Cost: 0, Latency: 0, Quality: 0.1}}, intelligence.Policy{MinQuality: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
