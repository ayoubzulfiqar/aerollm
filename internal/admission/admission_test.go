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

func TestWebhookHandlerHardening(t *testing.T) {
	var got AdmissionRequest
	h := WebhookHandler(ValidatorFunc(func(req AdmissionRequest) AdmissionResponse {
		got = req
		return AdmissionResponse{Allowed: true, Reason: "ok"}
	}))
	run := func(method, body string) *httptest.ResponseRecorder {
		var r *http.Request
		if body == "" {
			r = httptest.NewRequest(method, "/validate", nil)
		} else {
			r = httptest.NewRequest(method, "/validate", strings.NewReader(body))
		}
		w := httptest.NewRecorder()
		h(w, r)
		return w
	}
	if w := run(http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "POST" {
		t.Fatalf("expected 405 with Allow, got %d", w.Code)
	}
	if w := run(http.MethodPost, ""); w.Code != http.StatusBadRequest {
		t.Fatalf("missing body: %d", w.Code)
	}
	if w := run(http.MethodPost, `{"resource":"x"} {"extra":1}`); w.Code != http.StatusBadRequest {
		t.Fatalf("trailing data: %d", w.Code)
	}
	if w := run(http.MethodPost, `{"kind":"explode"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid kind: %d", w.Code)
	}
	if w := run(http.MethodPost, `{"body":"`+strings.Repeat("a", MaxBodyBytes)+`"}`); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized: %d", w.Code)
	}
	if w := run(http.MethodPost, `{"method":"delete","resource":"keys"}`); w.Code != http.StatusOK || got.Kind != RequestDelete || got.Method != "DELETE" {
		t.Fatalf("kind derivation: %d %+v", w.Code, got)
	}
}

func TestWebhookHandlerFailsClosed(t *testing.T) {
	for name, v := range map[string]Validator{
		"nil":   nil,
		"panic": ValidatorFunc(func(AdmissionRequest) AdmissionResponse { panic("boom") }),
	} {
		w := httptest.NewRecorder()
		WebhookHandler(v)(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)))
		if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), `"allowed":false`) {
			t.Fatalf("%s: expected fail-closed denial, got %d %s", name, w.Code, w.Body.String())
		}
	}
}

func TestChainAndDenyResources(t *testing.T) {
	v := Chain(alwaysAllowValidator{}, DenyResources("secrets"))
	if v.Validate(AdmissionRequest{Resource: "Secrets"}).Allowed {
		t.Fatal("denied resource admitted")
	}
	if !v.Validate(AdmissionRequest{Resource: "models"}).Allowed {
		t.Fatal("allowed resource denied")
	}
	if Chain().Validate(AdmissionRequest{}).Allowed {
		t.Fatal("empty chain must deny")
	}
	if Chain(nil).Validate(AdmissionRequest{}).Allowed {
		t.Fatal("nil validator must deny")
	}
}
