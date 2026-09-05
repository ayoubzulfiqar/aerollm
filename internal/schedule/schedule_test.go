package schedule

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScheduleStore(t *testing.T) {
	store := NewStore()
	store.Upsert(ScheduledTask{Name: "backup", Type: TaskCron, Schedule: "0 0 * * *", Payload: "{}", Status: TaskPending})
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 task, got %d", len(store.List()))
	}
	store.UpdateStatus(store.List()[0].ID, TaskRunning)
	if store.List()[0].Status != TaskRunning {
		t.Fatalf("expected status running")
	}
}

func TestScheduleWebhook(t *testing.T) {
	mux := http.NewServeMux()
	store := NewStore()
	mux.HandleFunc("/v1/schedule", WebhookHandler(store))

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
