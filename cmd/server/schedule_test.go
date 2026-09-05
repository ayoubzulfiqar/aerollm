package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/schedule"
)

func TestScheduleRoute(t *testing.T) {
	mux := http.NewServeMux()
	store := schedule.NewStore()
	mux.HandleFunc("/v1/schedule", schedule.WebhookHandler(store))

	body := `{"name":"backup","type":"cron","schedule":"0 0 * * *","payload":"{}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/schedule", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/schedule", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"name":"backup"`) {
		t.Fatalf("expected task name in body, got: %s", listRec.Body.String())
	}
}
