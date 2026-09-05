package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/secrets"
)

func TestSecretsRoute(t *testing.T) {
	mux := http.NewServeMux()
	store := secrets.NewStore()
	mux.HandleFunc("/v1/secrets", secrets.WebhookHandler(store))

	body := `{"name":"api-key","value":"secret123","type":"token"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/secrets", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/secrets", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"type":"token"`) {
		t.Fatalf("expected token type in body, got: %s", listRec.Body.String())
	}
}
