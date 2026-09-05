package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/flags"
)

func TestFeatureFlags(t *testing.T) {
	mux := http.NewServeMux()
	store := flags.NewStore()
	mux.HandleFunc("/v1/flags/", flags.WebhookHandler(store))
	mux.HandleFunc("/v1/flags", flags.WebhookHandler(store))

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode int
	}{
		{"create flag", http.MethodPost, "/v1/flags/darkmode", `{"key":"darkmode","enabled":true,"strategy":"global"}`, http.StatusOK},
		{"get flag", http.MethodGet, "/v1/flags/darkmode", "", http.StatusOK},
		{"list flags", http.MethodGet, "/v1/flags", "", http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.name == "get flag" && !strings.Contains(rec.Body.String(), `"enabled":true`) {
				t.Fatalf("expected enabled true in body, got: %s", rec.Body.String())
			}
		})
	}
}
