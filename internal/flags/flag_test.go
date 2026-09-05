package flags

import (
	"testing"
)

func TestFlagStore(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "darkmode", Enabled: true, Strategy: RolloutGlobal})
	if got, ok := store.Get("darkmode"); !ok || !got.Enabled {
		t.Fatalf("expected darkmode enabled")
	}
	if !store.Enabled("darkmode", nil) {
		t.Fatalf("expected global flag to be enabled")
	}
}

func TestRolloutPercentage(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "beta", Enabled: true, Strategy: RolloutPercentage, Percentage: 50})
	enabled := 0
	for i := 0; i < 100; i++ {
		if store.Enabled("beta", map[string]string{"user": string(rune('a' + i))}) {
			enabled++
		}
	}
	if enabled == 0 || enabled == 100 {
		t.Fatalf("expected mixed rollout, got %d", enabled)
	}
}

func TestRolloutAllowList(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "admin", Enabled: true, Strategy: RolloutAllowList, AllowList: []string{"admin1"}})
	if !store.Enabled("admin", map[string]string{"id": "admin1"}) {
		t.Fatalf("expected allowed user to pass")
	}
	if store.Enabled("admin", map[string]string{"id": "other"}) {
		t.Fatalf("expected non-allowed user to fail")
	}
}
