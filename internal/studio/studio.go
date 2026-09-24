package studio

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

// NodeType identifies the kind of topology node.
type NodeType string

const (
	NodeTypeProvider NodeType = "provider"
	NodeTypeSwarm    NodeType = "swarm"
	NodeTypeMesh     NodeType = "mesh"
)

// Node represents a graph node for frontend visualization.
type Node struct {
	ID    string                 `json:"id"`
	Label string                 `json:"label"`
	Type  NodeType               `json:"type"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
}

// Edge represents a graph relationship between nodes.
type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Label  string `json:"label,omitempty"`
}

// TopologyResponse represents the current system topology.
type TopologyResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Nodes     []Node    `json:"nodes"`
	Edges     []Edge    `json:"edges"`
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

type pricingProvider interface {
	Models() []string
}

// pricingLookup is optionally implemented by pricing providers (e.g.
// *finops.PricingMap) to price ledger usage.
type pricingLookup interface {
	Get(model string) (finops.Pricing, bool)
}

// MaxCostTimeSeriesPoints caps the number of points returned by AnalyticsCost
// (the most recent points are kept).
const MaxCostTimeSeriesPoints = 1000

// Handler handles studio API requests.
type Handler struct {
	mu         sync.RWMutex
	router     *router.Router
	swarms     SwarmProvider
	pricing    pricingProvider
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
	h := &Handler{
		router:     router,
		swarms:     swarms,
		ledger:     ledgerStore,
		market:     marketRecorder,
		meshStatus: MeshStatus{SyncInterval: "5s"},
	}
	// Never store a typed nil pointer in the interface: h.pricing != nil would
	// then be true and calling Models() would panic.
	if pricing != nil {
		h.pricing = pricing
	}
	return h
}

// SetMeshStatus updates mesh status shown in topology.
func (h *Handler) SetMeshStatus(status MeshStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.meshStatus = status
}

// Topology returns the current system topology as graph nodes/edges.
func (h *Handler) Topology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}

	resp := TopologyResponse{
		Timestamp: time.Now().UTC(),
		// Every edge below originates at the router, so it is always present.
		Nodes: []Node{{ID: "router", Label: "Router", Type: NodeTypeProvider}},
		Edges: []Edge{},
	}

	if h.router != nil {
		for _, p := range h.router.Providers() {
			health := p.Health()
			id := p.Name()
			resp.Nodes = append(resp.Nodes, Node{
				ID:    id,
				Label: p.Name(),
				Type:  NodeTypeProvider,
				Meta: map[string]interface{}{
					"type":         string(p.Type()),
					"latency_ms":   health.LatencyMs,
					"circuit_open": health.CircuitOpen,
					"healthy":      health.Healthy,
				},
			})
			resp.Edges = append(resp.Edges, Edge{
				Source: "router",
				Target: id,
				Label:  "routes",
			})
		}
	}

	if h.swarms != nil {
		swarmID := "swarm-default"
		resp.Nodes = append(resp.Nodes, Node{
			ID:    swarmID,
			Label: "Default Swarm",
			Type:  NodeTypeSwarm,
			Meta: map[string]interface{}{
				"agent_count": h.swarms.ActiveCount(),
			},
		})
		resp.Edges = append(resp.Edges, Edge{
			Source: "router",
			Target: swarmID,
			Label:  "orchestrates",
		})
	}

	h.mu.RLock()
	meshStatus := h.meshStatus
	h.mu.RUnlock()

	if meshStatus.Enabled {
		meshNodeID := "mesh-local"
		resp.Nodes = append(resp.Nodes, Node{
			ID:    meshNodeID,
			Label: "Mesh " + meshStatus.LocalPeerID,
			Type:  NodeTypeMesh,
			Meta: map[string]interface{}{
				"peer_count":    meshStatus.PeerCount,
				"sync_interval": meshStatus.SyncInterval,
				"peer_ids":      meshStatus.PeerIDs,
			},
		})
		resp.Edges = append(resp.Edges, Edge{
			Source: "router",
			Target: meshNodeID,
			Label:  "gossip",
		})
	}

	writeJSON(w, resp)
}

// AnalyticsCost returns cost analytics data aggregated from ledger/marketplace
// state. Token counts and models are read from the ledger request/response
// payloads; costs are computed from the pricing map when it can price the
// model (prices are interpreted as USD per 1K tokens).
func (h *Handler) AnalyticsCost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
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
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to read ledger")
			return
		}
		lookup, _ := h.pricing.(pricingLookup)
		for _, rec := range records {
			model, usage := ledgerUsage(rec)
			point := CostTimeSeries{Timestamp: rec.Timestamp}
			if usage != nil {
				point.Tokens = int64(usage.TotalTokens)
				if point.Tokens == 0 {
					point.Tokens = int64(usage.PromptTokens) + int64(usage.CompletionTokens)
				}
				if lookup != nil && model != "" {
					if p, ok := lookup.Get(model); ok {
						point.CostUSD = (float64(usage.PromptTokens)*p.PromptPrice + float64(usage.CompletionTokens)*p.CompletionPrice) / 1000
					}
				}
			}
			if model != "" {
				resp.Breakdown.ByModel[model] += point.CostUSD
			}
			if tenant, ok := rec.Metadata["tenant"].(string); ok && tenant != "" {
				resp.Breakdown.ByTenant[tenant] += point.CostUSD
			}
			resp.TimeSeries = append(resp.TimeSeries, point)
		}
		sort.SliceStable(resp.TimeSeries, func(i, j int) bool {
			return resp.TimeSeries[i].Timestamp.Before(resp.TimeSeries[j].Timestamp)
		})
		if over := len(resp.TimeSeries) - MaxCostTimeSeriesPoints; over > 0 {
			resp.TimeSeries = resp.TimeSeries[over:]
		}
	}

	if h.market != nil {
		for _, ev := range h.market.Snapshot() {
			resp.Breakdown.ByPlugin[ev.PluginID] += ev.CostUSD
		}
	}

	writeJSON(w, resp)
}

// ledgerUsage extracts the requested model and token usage from a ledger
// record's payloads. Malformed payloads yield ("", nil).
func ledgerUsage(rec ledger.LedgerRecord) (string, *models.Usage) {
	var req struct {
		Model string `json:"model"`
	}
	var resp struct {
		Model string        `json:"model"`
		Usage *models.Usage `json:"usage"`
	}
	if rec.RequestPayload != "" {
		_ = json.Unmarshal([]byte(rec.RequestPayload), &req)
	}
	if rec.ResponsePayload != "" {
		_ = json.Unmarshal([]byte(rec.ResponsePayload), &resp)
	}
	model := req.Model
	if model == "" {
		model = resp.Model
	}
	return model, resp.Usage
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
