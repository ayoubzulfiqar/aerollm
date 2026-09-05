package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
)

func TestPolicyRoute(t *testing.T) {
	mux := http.NewServeMux()
	policyStore := compliance.NewHTTPPolicyStore()
	mux.HandleFunc("/v1/policy", compliance.HTTPPolicyHandler(policyStore))

	req := httptest.NewRequest(http.MethodPost, "/v1/policy", strings.NewReader(`{"id":"deny-post","name":"deny-post","expression":"deny-post","severity":"high"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/policy?id=deny-post", nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"severity":"high"`) {
		t.Fatalf("expected severity high in body, got: %s", getRec.Body.String())
	}
}
