package licensing

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMiddlewareBlocksUnlicensed(t *testing.T) {
	os.Setenv("AEROLLM_LICENSE_KEY", "")
	defer os.Unsetenv("AEROLLM_LICENSE_KEY")

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := Middleware(NewEnvLicenseChecker(), FeatureAdvancedCRDTMesh)
	handler := mw(next)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestMiddlewareAllowsLicensed(t *testing.T) {
	os.Setenv("AEROLLM_LICENSE_KEY", "enterprise")
	defer os.Unsetenv("AEROLLM_LICENSE_KEY")

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := Middleware(NewEnvLicenseChecker(), FeatureZeroKnowledge)
	handler := mw(next)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
