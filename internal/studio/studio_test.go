package studio

import (
	"context"
	"encoding/json"
	"math"
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

func TestNewHandlerNilPricingDoesNotPanic(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil)
	w := httptest.NewRecorder()
	h.AnalyticsCost(w, httptest.NewRequest(http.MethodGet, "/v1/studio/analytics/cost", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestTopologyAlwaysHasRouterNode(t *testing.T) {
	h := NewHandler(nil, &fakeSwarms{n: 2}, nil, nil, nil)
	w := httptest.NewRecorder()
	h.Topology(w, httptest.NewRequest(http.MethodGet, "/v1/studio/topology", nil))
	var resp TopologyResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ids := map[string]bool{}
	for _, n := range resp.Nodes {
		ids[n.ID] = true
	}
	for _, e := range resp.Edges {
		if !ids[e.Source] || !ids[e.Target] {
			t.Fatalf("edge references missing node: %+v (nodes %+v)", e, resp.Nodes)
		}
	}
	if !ids["router"] || !ids["swarm-default"] {
		t.Fatalf("expected router and swarm nodes, got %+v", resp.Nodes)
	}
}

func TestAnalyticsCostUsesLedgerUsage(t *testing.T) {
	pricing := finops.NewPricingMap()
	now := time.Now()
	ledgerStore := &fakeLedger{records: []ledger.LedgerRecord{
		{Timestamp: now, RequestPayload: `{"model":"gpt-4"}`, ResponsePayload: `{"model":"openai","usage":{"prompt_tokens":1000,"completion_tokens":1000,"total_tokens":2000}}`},
		{Timestamp: now.Add(-time.Minute), ResponsePayload: `not json`},
	}}
	h := NewHandler(nil, nil, pricing, ledgerStore, nil)
	w := httptest.NewRecorder()
	h.AnalyticsCost(w, httptest.NewRequest(http.MethodGet, "/v1/studio/analytics/cost", nil))
	var resp AnalyticsCostResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.TimeSeries) != 2 || !resp.TimeSeries[0].Timestamp.Before(resp.TimeSeries[1].Timestamp) {
		t.Fatalf("expected 2 sorted points, got %+v", resp.TimeSeries)
	}
	last := resp.TimeSeries[1]
	if last.Tokens != 2000 || math.Abs(last.CostUSD-0.09) > 1e-9 {
		t.Fatalf("unexpected point: %+v", last)
	}
	if math.Abs(resp.Breakdown.ByModel["gpt-4"]-0.09) > 1e-9 {
		t.Fatalf("unexpected model breakdown: %+v", resp.Breakdown.ByModel)
	}
}

func TestStudioHandlersRejectWrongMethod(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, nil)
	for _, fn := range []http.HandlerFunc{h.Topology, h.AnalyticsCost} {
		w := httptest.NewRecorder()
		fn(w, httptest.NewRequest(http.MethodPost, "/", nil))
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
			t.Fatalf("expected 405 with Allow, got %d", w.Code)
		}
	}
}

type fakeSwarms struct{ n int }

func (f *fakeSwarms) ActiveCount() int { return f.n }
