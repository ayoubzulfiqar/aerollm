package federated

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// RegisterNodeRequest is the request payload for node registration.
type RegisterNodeRequest struct {
	NodeID     string   `json:"node_id"`
	Endpoint   string   `json:"endpoint"`
	PublicKey  string   `json:"public_key"`
	Algorithms []string `json:"algorithms"`
}

// RegisterNodeHandler returns an HTTP handler that registers a federated node.
func RegisterNodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req RegisterNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		if err := registry.Register(r.Context(), &NodeRegistration{
			NodeID:     req.NodeID,
			Endpoint:   req.Endpoint,
			PublicKey:  []byte(req.PublicKey),
			Algorithms: req.Algorithms,
		}); err != nil {
			http.Error(w, `{"error":"register failed"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
	}
}

// LatestNodeHandler returns an HTTP handler that serves the latest registered node.
func LatestNodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r
		node := registry.Latest()
		if node == nil {
			http.Error(w, `{"error":"no nodes"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(node)
	}
}

// NodeHistoryHandler returns an HTTP handler that serves registration history.
func NodeHistoryHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r
		history := registry.History()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(history)
	}
}

// NodeHandler returns an HTTP handler that serves a node by ID.
func NodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r
		nodeID := r.URL.Query().Get("id")
		if nodeID == "" {
			http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
			return
		}
		node, ok := registry.Node(nodeID)
		if !ok {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(node)
	}
}

// PrintLatestNode prints the latest node registration.
func PrintLatestNode(registry *GatewayRegistry) {
	node := registry.Latest()
	if node == nil {
		fmt.Println("no nodes registered")
		return
	}
	fmt.Printf("latest node=%s endpoint=%s algorithms=%v\n", node.NodeID, node.Endpoint, node.Algorithms)
}

// PrintNodeHistory prints registration history.
func PrintNodeHistory(registry *GatewayRegistry) {
	history := registry.History()
	if len(history) == 0 {
		fmt.Println("no history")
		return
	}
	for i, n := range history {
		fmt.Printf("%d: node=%s endpoint=%s\n", i, n.NodeID, n.Endpoint)
	}
}
