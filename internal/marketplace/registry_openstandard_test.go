package marketplace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryServiceOpenStandardCapabilityManifest(t *testing.T) {
	s := NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"version":"1.0","hardware":{"has_local_gpu":true,"os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh"]}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/capability", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceOpenStandardBillingReceipt(t *testing.T) {
	s := NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"receipt_id":"r-1","customer_id":"c1","provider_id":"p1","event_name":"token","value":1,"currency":"USD"}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/receipt", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
}
