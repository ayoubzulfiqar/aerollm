package licensing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMiddlewareBlocksUnlicensed(t *testing.T) {
	t.Setenv(EnvLicenseKey, "")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, checker := range []LicenseChecker{NewEnvLicenseChecker(), nil} {
		handler := Middleware(checker, FeatureAdvancedCRDTMesh)(next)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", w.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
			t.Fatalf("error body must be valid JSON: %v %s", err, w.Body.String())
		}
		if w.Header().Get("Content-Type") != "application/json" {
			t.Fatal("expected JSON content type")
		}
	}
}

func TestMiddlewareAllowsLicensed(t *testing.T) {
	pub, priv := newKeys(t)
	setEnv(t, pub, issue(t, priv, []string{string(FeatureZeroKnowledge)}, time.Now().Add(time.Hour)))
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := Middleware(NewEnvLicenseChecker(), FeatureZeroKnowledge)(next)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
