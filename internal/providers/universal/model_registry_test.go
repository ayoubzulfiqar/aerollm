package universal

import (
	"context"
	"testing"
	"time"
)

func TestModelRegistryLifecycle(t *testing.T) {
	reg := NewModelRegistry()
	now := time.Now()
	card := ModelCard{
		ID:           "m1",
		Provider:     "openai",
		Type:         "chat",
		Capabilities: []string{"chat", "tools"},
		Pricing:      map[string]float64{"1k": 0.01},
		CreatedAt:    now,
	}
	if err := reg.Register(context.Background(), card); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if got, ok := reg.Get("m1"); !ok || got.Provider != "openai" {
		t.Fatalf("get failed: %v %v", ok, got)
	}
	if len(reg.ByProvider("openai")) != 1 {
		t.Fatalf("by provider failed: %v", reg.ByProvider("openai"))
	}
	if len(reg.List()) != 1 {
		t.Fatalf("list failed: %v", reg.List())
	}
	if err := reg.Register(context.Background(), ModelCard{}); err == nil {
		t.Fatalf("expected empty id error")
	}
}
