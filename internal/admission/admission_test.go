package admission

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type alwaysAllowValidator struct{}

func (alwaysAllowValidator) Validate(req AdmissionRequest) AdmissionResponse {
	return AdmissionResponse{Allowed: true, Reason: "allowed"}
}

type alwaysDenyValidator struct{}

func (alwaysDenyValidator) Validate(req AdmissionRequest) AdmissionResponse {
	return AdmissionResponse{Allowed: false, Reason: "denied"}
}

func TestWebhookHandlerAllows(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/validate", WebhookHandler(alwaysAllowValidator{}))

	req := httptest.NewRequest(http.MethodPost, "/validate", strings.NewReader(`{"resource":"models","path":"/v1/models"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"allowed":true`) {
		t.Fatalf("expected allowed response, got: %s", rec.Body.String())
	}
}

func TestWebhookHandlerDenies(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/validate", WebhookHandler(alwaysDenyValidator{}))

	req := httptest.NewRequest(http.MethodPost, "/validate", strings.NewReader(`{"resource":"models","path":"/v1/models"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"allowed":false`) {
		t.Fatalf("expected denied response, got: %s", rec.Body.String())
	}
}
