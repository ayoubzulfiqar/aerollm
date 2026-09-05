package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
)

func TestAuditEventsRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/audit/events", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"missing body"}`))
			return
		}
		defer r.Body.Close()
		var req struct{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad request"}`))
			return
		}
		logger := compliance.NewMemoryAuditLogger()
		logger.Log(&compliance.AuditEvent{Timestamp: time.Now(), Policy: "default", Decision: "allow", Reason: "audit endpoint"})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(logger.Events())
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/audit/events", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Decision") {
		t.Fatalf("expected Decision in response, got: %s", rec.Body.String())
	}
}
