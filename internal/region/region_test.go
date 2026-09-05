package region

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegionStore(t *testing.T) {
	store := NewStore()
	store.UpsertRegion(Region{ID: "us-east-1", Name: "US East", Endpoint: "https://us.example.com", Primary: true})
	if len(store.ListRegions()) != 1 {
		t.Fatalf("expected 1 region, got %d", len(store.ListRegions()))
	}
	store.UpsertPolicy(ResidencyPolicy{ID: "p1", Region: "us-east-1", DataType: "pii", Required: true})
	if len(store.ListPolicies()) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(store.ListPolicies()))
	}
	store.UpsertRule(RouteRule{ID: "r1", Region: "us-east-1", Providers: []string{"openai"}, Priority: 1, Enabled: true})
	if len(store.ListRules()) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(store.ListRules()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/region/regions", WebhookHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/v1/region/regions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"name":"US East"`) {
		t.Fatalf("expected region name in body, got: %s", rec.Body.String())
	}
}
