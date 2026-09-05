package secrets

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSecretsStore(t *testing.T) {
	store := NewStore()
	store.Upsert(Secret{Name: "api-key", Value: "secret123", Type: "token"})
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 secret, got %d", len(store.List()))
	}
	if !store.Delete(store.List()[0].ID) {
		t.Fatalf("expected delete to succeed")
	}
}

func TestSecretsWebhook(t *testing.T) {
	mux := http.NewServeMux()
	store := NewStore()
	mux.HandleFunc("/v1/secrets", WebhookHandler(store))

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
