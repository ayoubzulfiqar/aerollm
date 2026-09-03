package federated

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegisterNodeHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/register", RegisterNodeHandler(registry))

	body := `{"node_id":"n1","endpoint":"http://n1","public_key":"pk","algorithms":["a"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/federated/nodes/register", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"registered"`) {
		t.Fatalf("expected registered status, got: %s", rec.Body.String())
	}
}

func TestLatestNodeHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	_ = registry.Register(nil, &NodeRegistration{NodeID: "n1", Endpoint: "http://n1"})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/latest", LatestNodeHandler(registry))
	req := httptest.NewRequest(http.MethodGet, "/v1/federated/nodes/latest", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"NodeID":"n1"`) {
		t.Fatalf("expected node id in response, got: %s", rec.Body.String())
	}
}

func TestNodeHistoryHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	_ = registry.Register(nil, &NodeRegistration{NodeID: "n1"})
	_ = registry.Register(nil, &NodeRegistration{NodeID: "n2"})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/history", NodeHistoryHandler(registry))
	req := httptest.NewRequest(http.MethodGet, "/v1/federated/nodes/history", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var history []*NodeRegistration
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(history))
	}
}
