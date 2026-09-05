package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/region"
)

func TestRegionRoutes(t *testing.T) {
	mux := http.NewServeMux()
	store := region.NewStore()
	mux.HandleFunc("/v1/region/regions", region.WebhookHandler(store))

	body := `{"id":"us-east-1","name":"US East","endpoint":"https://us.example.com","primary":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/region/regions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/region/regions", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"name":"US East"`) {
		t.Fatalf("expected region name in body, got: %s", listRec.Body.String())
	}
}
