package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/admission"
)

func TestAdmissionValidateRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/admission/validate", admission.WebhookHandler(admission.ValidatorFunc(func(req admission.AdmissionRequest) admission.AdmissionResponse {
		return admission.AdmissionResponse{Allowed: true, Reason: "admission allow"}
	})))

	req := httptest.NewRequest(http.MethodPost, "/v1/admission/validate", strings.NewReader(`{"resource":"models","path":"/v1/models"}`))
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
