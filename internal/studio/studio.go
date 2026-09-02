package studio

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

// TopologyResponse represents the current system topology.
type TopologyResponse struct {
	Timestamp time.Time         `json:"timestamp"`
	Providers []ProviderStatus `json:"providers"`
	Swarms    []SwarmStatus    `json:"swarms"`
	Mesh      MeshStatus       `json:"mesh"`
}

// ProviderStatus represents the status of a provider.
type ProviderStatus struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	LatencyMs   float64 `json:"latency_ms"`
	CircuitOpen bool    `json:"circuit_open"`
}

// SwarmStatus represents the status of an agent swarm.
type SwarmStatus struct {
	ID         string `json:"id"`
	Task       string `json:"task"`
	Status     string `json:"status"`
	AgentCount int    `json:"agent_count"`
}

// MeshStatus represents the status of the mesh network.
type MeshStatus struct {
	Enabled      bool     `json:"enabled"`
	LocalPeerID  string   `json:"local_peer_id"`
	PeerCount    int      `json:"peer_count"`
	SyncInterval string   `json:"sync_interval"`
	PeerIDs      []string `json:"peer_ids"`
}

// AnalyticsCostResponse represents the cost analytics response.
type AnalyticsCostResponse struct {
	TimeSeries []CostTimeSeries `json:"time_series"`
	Breakdown  CostBreakdown    `json:"breakdown"`
}

// CostTimeSeries represents a time-series data point.
type CostTimeSeries struct {
	Timestamp time.Time `json:"timestamp"`
	CostUSD   float64   `json:"cost_usd"`
	Tokens    int64     `json:"tokens"`
}

// CostBreakdown represents cost breakdown by category.
type CostBreakdown struct {
	ByTenant map[string]float64 `json:"by_tenant"`
	ByModel  map[string]float64 `json:"by_model"`
	ByPlugin map[string]float64 `json:"by_plugin"`
}

// Handler handles studio API requests.
type Handler struct {
	mu         sync.RWMutex
	router     *router.Router
	swarms     SwarmProvider
	pricing    *finops.PricingMap
	meshStatus MeshStatus
	ledger     ledgerStore
	market     marketplaceRecorder
}

type ledgerStore interface {
	All(ctx context.Context) ([]ledger.LedgerRecord, error)
}

type marketplaceRecorder interface {
	Snapshot() []marketplace.RoyaltyEvent
}

// SwarmProvider provides swarm status information.
type SwarmProvider interface {
	ActiveCount() int
}

// NewHandler creates a new studio handler.
func NewHandler(
	router *router.Router,
	swarms SwarmProvider,
	pricing *finops.PricingMap,
	ledgerStore ledgerStore,
	marketRecorder marketplaceRecorder,
) *Handler {
	return &Handler{
		router:     router,
		swarms:     swarms,
		pricing:    pricing,
		ledger:     ledgerStore,
		market:     marketRecorder,
		meshStatus: MeshStatus{SyncInterval: "5s"},
	}
}

// SetMeshStatus updates mesh status shown in topology.
func (h *Handler) SetMeshStatus(status MeshStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.meshStatus = status
}

// Topology returns the current system topology.
func (h *Handler) Topology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := TopologyResponse{
		Timestamp: time.Now().UTC(),
	}

	if h.router != nil {
		for _, p := range h.router.Providers() {
			health := p.Health()
			resp.Providers = append(resp.Providers, ProviderStatus{
				Name:        p.Name(),
				Type:        string(p.Type()),
				LatencyMs:   health.LatencyMs,
				CircuitOpen: health.CircuitOpen,
			})
		}
	}

	if h.swarms != nil {
		resp.Swarms = append(resp.Swarms, SwarmStatus{
			ID:         "default",
			Status:     "active",
			AgentCount: h.swarms.ActiveCount(),
		})
	}

	h.mu.RLock()
	resp.Mesh = h.meshStatus
	h.mu.RUnlock()

	writeJSON(w, resp)
}

// AnalyticsCost returns cost analytics data aggregated from ledger/marketplace state.
func (h *Handler) AnalyticsCost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := AnalyticsCostResponse{
		TimeSeries: []CostTimeSeries{},
		Breakdown: CostBreakdown{
			ByTenant: make(map[string]float64),
			ByModel:  make(map[string]float64),
			ByPlugin: make(map[string]float64),
		},
	}

	if h.pricing != nil {
		for _, model := range h.pricing.Models() {
			resp.Breakdown.ByModel[model] = 0
		}
	}

	if h.ledger != nil {
		records, err := h.ledger.All(r.Context())
		if err == nil {
			for _, rec := range records {
				resp.TimeSeries = append(resp.TimeSeries, CostTimeSeries{
					Timestamp: rec.Timestamp,
					CostUSD:   0,
					Tokens:    0,
				})
			}
		}
	}

	if h.market != nil {
		for _, ev := range h.market.Snapshot() {
			resp.Breakdown.ByPlugin[ev.PluginID] += ev.CostUSD
		}
	}

	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
