package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/retention"
)

func TestRetentionRoute(t *testing.T) {
	mux := http.NewServeMux()
	store := retention.NewRetentionStore()
	mux.HandleFunc("/v1/retention", retention.WebhookHandler(store))

	req := httptest.NewRequest(http.MethodPost, "/v1/retention", strings.NewReader(`{"id":"logs","resource":"logs","ttl":24,"max_items":1000}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/retention?id=logs", nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"resource":"logs"`) {
		t.Fatalf("expected resource logs in body, got: %s", getRec.Body.String())
	}
}
