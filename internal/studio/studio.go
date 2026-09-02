package studio

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
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
	Name     string  `json:"name"`
	Type     string `json:"type"`
	LatencyMs float64 `json:"latency_ms"`
	CircuitOpen bool `json:"circuit_open"`
}

// SwarmStatus represents the status of an agent swarm.
type SwarmStatus struct {
	ID       string `json:"id"`
	Task     string `json:"task"`
	Status   string `json:"status"`
	AgentCount int  `json:"agent_count"`
}

// MeshStatus represents the status of the mesh network.
type MeshStatus struct {
	Enabled      bool     `json:"enabled"`
	LocalPeerID  string `json:"local_peer_id"`
	PeerCount    int    `json:"peer_count"`
	SyncInterval string `json:"sync_interval"`
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
	router     *router.Router
	swarms     SwarmProvider
	pricing    *finops.PricingMap
}

// SwarmProvider provides swarm status information.
type SwarmProvider interface {
	ActiveCount() int
}

// NewHandler creates a new studio handler.
func NewHandler(router *router.Router, swarms SwarmProvider, pricing *finops.PricingMap) *Handler {
	return &Handler{
		router:  router,
		swarms:  swarms,
		pricing: pricing,
	}
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
		providers := h.router.Providers()
		for _, p := range providers {
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

	resp.Mesh = MeshStatus{
		Enabled:      false,
		LocalPeerID:  "",
		PeerCount:    0,
		SyncInterval: "5s",
	}

	respondJSON(w, resp)
}

// AnalyticsCost returns cost analytics data.
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
		models := h.pricing.Models()
		for _, model := range models {
			resp.Breakdown.ByModel[model] = 0
		}
	}

	respondJSON(w, resp)
}

func respondJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
