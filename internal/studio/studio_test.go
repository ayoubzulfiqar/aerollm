package studio

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
)

func TestTopologyResponseIncludesMesh(t *testing.T) {
	pricing := finops.NewPricingMap()
	h := NewHandler(nil, nil, pricing, nil, nil)
	h.SetMeshStatus(MeshStatus{Enabled: true, LocalPeerID: "p1", PeerCount: 1, PeerIDs: []string{"p2"}})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/studio/topology", nil)
	h.Topology(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !contains(w.Body.String(), `"type":"mesh"`) {
		t.Fatalf("expected mesh node in topology, got: %s", w.Body.String())
	}
}

func TestAnalyticsCostAggregatesLedgerAndMarket(t *testing.T) {
	ledgerStore := &fakeLedger{records: []ledger.LedgerRecord{{Timestamp: time.Now()}}}
	marketRec := &fakeMarket{events: []marketplace.RoyaltyEvent{{PluginID: "plugin-1", CostUSD: 1.5}}}
	pricing := finops.NewPricingMap()
	h := NewHandler(nil, nil, pricing, ledgerStore, marketRec)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/studio/analytics/cost", nil)
	h.AnalyticsCost(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !contains(w.Body.String(), `"by_plugin":{"plugin-1":1.5}`) {
		t.Fatalf("expected plugin breakdown in analytics response, got: %s", w.Body.String())
	}
}

func TestSetMeshStatusIsThreadSafe(t *testing.T) {
	pricing := finops.NewPricingMap()
	h := NewHandler(nil, nil, pricing, nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			h.SetMeshStatus(MeshStatus{Enabled: true, LocalPeerID: "n", PeerCount: n})
		}(i)
	}
	wg.Wait()
}

type fakeLedger struct {
	records []ledger.LedgerRecord
}

func (f *fakeLedger) All(ctx context.Context) ([]ledger.LedgerRecord, error) {
	return f.records, nil
}

type fakeMarket struct {
	events []marketplace.RoyaltyEvent
}

func (f *fakeMarket) Snapshot() []marketplace.RoyaltyEvent {
	return f.events
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
