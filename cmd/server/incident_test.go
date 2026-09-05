package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/incident"
)

func TestIncidentRoute(t *testing.T) {
	mux := http.NewServeMux()
	store := incident.NewStore()
	mux.HandleFunc("/v1/incidents", incident.WebhookHandler(store))

	req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(`{"title":"outage","severity":"high","status":"open"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/incidents", nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"severity":"high"`) {
		t.Fatalf("expected severity high in body, got: %s", getRec.Body.String())
	}
}
