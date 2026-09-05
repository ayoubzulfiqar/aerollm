package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
)

func TestQuotaRouteExposesMetrics(t *testing.T) {
	store := tenant.NewInMemoryQuotaStore()
	_ = store.Upsert(context.Background(), &tenant.Quota{ID: "q1", Scope: tenant.ScopeTenant, TargetID: "t1", Limit: 10})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/quota", func(w http.ResponseWriter, r *http.Request) {
		q, err := store.Get(r.Context(), "q1")
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"` + q.ID + `","limit":` + fmtInt(q.Limit) + `,"used":` + fmtInt(q.Used) + `}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/quota", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"limit":10`) {
		t.Fatalf("expected quota metrics, got: %s", rec.Body.String())
	}
}

func fmtInt(n int64) string {
	return strconv.FormatInt(n, 10)
}
