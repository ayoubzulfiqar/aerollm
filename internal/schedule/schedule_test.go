package schedule

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
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

// --- store ---

func fixedStore(now time.Time) *Store {
	s := NewStore()
	s.now = func() time.Time { return now }
	return s
}

func TestStoreGeneratesUniqueIDs(t *testing.T) {
	s := NewStore()
	idRe := regexp.MustCompile(`^task_[0-9a-f]{16}$`)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		task, err := s.Create(ScheduledTask{Name: "t", Schedule: "@hourly"})
		if err != nil {
			t.Fatal(err)
		}
		if !idRe.MatchString(task.ID) {
			t.Fatalf("unexpected id %q", task.ID)
		}
		if seen[task.ID] {
			t.Fatalf("duplicate id %q", task.ID)
		}
		seen[task.ID] = true
	}
	if len(s.List()) != 50 {
		t.Fatalf("tasks created in the same second must not overwrite each other, got %d", len(s.List()))
	}
}

func TestStoreComputesNextRun(t *testing.T) {
	now := utc(2026, 1, 1, 10, 7)
	s := fixedStore(now)
	cron, err := s.Create(ScheduledTask{Name: "c", Schedule: "*/15 * * * *"})
	if err != nil {
		t.Fatal(err)
	}
	if cron.Type != TaskCron || cron.Status != TaskPending || !cron.NextRun.Equal(utc(2026, 1, 1, 10, 15)) {
		t.Fatalf("unexpected cron task %+v", cron)
	}
	iv, err := s.Create(ScheduledTask{Name: "i", Type: TaskInterval, Schedule: "5m"})
	if err != nil {
		t.Fatal(err)
	}
	if !iv.NextRun.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("interval next run %s", iv.NextRun)
	}
	ny, err := s.Create(ScheduledTask{Name: "tz", Schedule: "0 9 * * *", Timezone: "America/New_York"})
	if err != nil {
		t.Fatal(err)
	}
	if want := utc(2026, 1, 1, 14, 0); !ny.NextRun.Equal(want) { // 09:00 EST
		t.Fatalf("tz next run %s, want %s", ny.NextRun, want)
	}
	at := now.Add(time.Hour)
	one, err := s.Create(ScheduledTask{Name: "o", Type: TaskOneTime, RunAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if !one.NextRun.Equal(at) || one.Schedule != at.Format(time.RFC3339) {
		t.Fatalf("unexpected one-time task %+v", one)
	}
	one2, err := s.Create(ScheduledTask{Name: "o2", Type: TaskOneTime, Schedule: "2026-01-02T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if !one2.RunAt.Equal(utc(2026, 1, 2, 0, 0)) {
		t.Fatalf("run_at not derived from schedule: %+v", one2)
	}
}

func TestStoreValidation(t *testing.T) {
	now := utc(2026, 1, 1, 0, 0)
	s := fixedStore(now)
	cases := []ScheduledTask{
		{Name: "", Schedule: "@daily"},
		{Name: strings.Repeat("n", 300), Schedule: "@daily"},
		{Name: "x", Schedule: "not cron"},
		{Name: "x", Schedule: ""},
		{Name: "x", Schedule: "0 0 30 2 *"},
		{Name: "x", Type: "weekly", Schedule: "@daily"},
		{Name: "x", Status: "bogus", Schedule: "@daily"},
		{Name: "x", Schedule: "@daily", Payload: strings.Repeat("p", maxPayloadBytes+1)},
		{Name: "x", Schedule: "@daily", Timezone: "Mars/Olympus"},
		{Name: "x", Schedule: "@daily", Timezone: "Local"},
		{Name: "x", Schedule: "@daily", Timezone: "../../etc/passwd"},
		{Name: "x", Type: TaskInterval, Schedule: "500ms"},
		{Name: "x", Type: TaskInterval, Schedule: "10000h"},
		{Name: "x", Type: TaskInterval, Schedule: "0 * * * *"},
		{Name: "x", Type: TaskOneTime},
		{Name: "x", Type: TaskOneTime, Schedule: "tomorrow"},
		{Name: "x", Type: TaskOneTime, RunAt: now.Add(-time.Minute)},
		{Name: "x", Type: TaskOneTime, RunAt: now},
		{Name: "x", Type: TaskOneTime, Schedule: "2026-02-01T00:00:00Z", RunAt: now.Add(time.Hour)},
		{ID: "../etc", Name: "x", Schedule: "@daily"},
		{ID: "-flag", Name: "x", Schedule: "@daily"},
	}
	for i, tc := range cases {
		_, err := s.Create(tc)
		var verr *ValidationError
		if !errors.As(err, &verr) {
			t.Errorf("case %d (%+v): expected validation error, got %v", i, tc.Name, err)
		}
	}
	if len(s.List()) != 0 {
		t.Fatalf("invalid tasks must not be stored")
	}
}

func TestStoreConflictUpsertAndLimit(t *testing.T) {
	s := fixedStore(utc(2026, 1, 1, 0, 0))
	s.maxTasks = 2
	first, err := s.Create(ScheduledTask{ID: "job-1", Name: "a", Schedule: "@daily"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ScheduledTask{ID: "job-1", Name: "b", Schedule: "@daily"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	s.now = func() time.Time { return utc(2026, 1, 1, 5, 0) }
	replaced, err := s.Upsert(ScheduledTask{ID: "job-1", Name: "b", Schedule: "@hourly"})
	if err != nil {
		t.Fatal(err)
	}
	if !replaced.CreatedAt.Equal(first.CreatedAt) || replaced.Name != "b" || !replaced.NextRun.Equal(utc(2026, 1, 1, 6, 0)) {
		t.Fatalf("upsert must keep created_at and recompute next_run: %+v", replaced)
	}
	if _, err := s.Create(ScheduledTask{Name: "c", Schedule: "@daily"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ScheduledTask{Name: "d", Schedule: "@daily"}); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected store full, got %v", err)
	}
	if !s.Delete("job-1") || s.Delete("job-1") {
		t.Fatal("delete should report existence")
	}
	if err := s.SetStatus("missing", TaskFailed); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestStoreUpdate(t *testing.T) {
	now := utc(2026, 1, 1, 0, 0)
	s := fixedStore(now)
	task, err := s.Create(ScheduledTask{Name: "a", Schedule: "@daily"})
	if err != nil {
		t.Fatal(err)
	}
	sched := "*/5 * * * *"
	got, err := s.Update(task.ID, TaskUpdate{Schedule: &sched})
	if err != nil {
		t.Fatal(err)
	}
	if !got.NextRun.Equal(utc(2026, 1, 1, 0, 5)) {
		t.Fatalf("next run not recomputed: %s", got.NextRun)
	}
	bad := "61 * * * *"
	if _, err := s.Update(task.ID, TaskUpdate{Schedule: &bad}); err == nil {
		t.Fatal("expected validation error")
	}
	if cur, _ := s.Get(task.ID); cur.Schedule != sched {
		t.Fatalf("failed update must not modify the task: %+v", cur)
	}
	typ := TaskOneTime
	at := now.Add(2 * time.Hour)
	got, err = s.Update(task.ID, TaskUpdate{Type: &typ, RunAt: &at})
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != TaskOneTime || !got.NextRun.Equal(at) {
		t.Fatalf("unexpected one-time conversion: %+v", got)
	}
	if _, err := s.Update("missing", TaskUpdate{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				task, err := s.Create(ScheduledTask{Name: "t", Schedule: "@hourly"})
				if err != nil {
					t.Error(err)
					return
				}
				s.UpdateStatus(task.ID, TaskFailed)
				_ = s.List()
				s.Get(task.ID)
			}
		}()
	}
	wg.Wait()
	if n := len(s.List()); n != 800 {
		t.Fatalf("expected 800 tasks, got %d", n)
	}
}

// --- HTTP ---

func newTestMux(s *Store) *http.ServeMux {
	mux := http.NewServeMux()
	h := WebhookHandler(s)
	mux.HandleFunc("/v1/schedule", h)
	mux.HandleFunc("/v1/schedule/", h)
	return mux
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeTask(t *testing.T, rec *httptest.ResponseRecorder) ScheduledTask {
	t.Helper()
	var task ScheduledTask
	if err := json.Unmarshal(rec.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return task
}

func TestHandlerCRUD(t *testing.T) {
	s := fixedStore(utc(2026, 1, 1, 0, 0))
	mux := newTestMux(s)

	rec := do(t, mux, http.MethodPost, "/v1/schedule", `{"name":"backup","type":"cron","schedule":"0 0 * * *","payload":"{}","created_at":"1999-01-01T00:00:00Z","runs":99}`)
	if rec.Code != http.StatusCreated || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	created := decodeTask(t, rec)
	if created.ID == "" || !created.NextRun.Equal(utc(2026, 1, 2, 0, 0)) || created.Runs != 0 || created.CreatedAt.Year() != 2026 {
		t.Fatalf("unexpected created task %+v", created)
	}

	for _, target := range []string{"/v1/schedule?id=" + created.ID, "/v1/schedule/" + created.ID} {
		rec = do(t, mux, http.MethodGet, target, "")
		if rec.Code != http.StatusOK || decodeTask(t, rec).ID != created.ID {
			t.Fatalf("get %s: %d %s", target, rec.Code, rec.Body.String())
		}
	}
	if rec = do(t, mux, http.MethodGet, "/v1/schedule?id=nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodGet, "/v1/schedule/a?id=b", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for conflicting ids, got %d", rec.Code)
	}

	// Legacy status-only PUT.
	rec = do(t, mux, http.MethodPut, "/v1/schedule?id="+created.ID, `{"status":"completed"}`)
	if rec.Code != http.StatusOK || decodeTask(t, rec).Status != TaskCompleted {
		t.Fatalf("put: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, mux, http.MethodPut, "/v1/schedule?id="+created.ID, `{"status":"exploded"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad status, got %d", rec.Code)
	}
	rec = do(t, mux, http.MethodPatch, "/v1/schedule/"+created.ID, `{"schedule":"*/10 * * * *","name":"b2"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	if p := decodeTask(t, rec); p.Name != "b2" || !p.NextRun.Equal(utc(2026, 1, 1, 0, 10)) {
		t.Fatalf("unexpected patched task %+v", p)
	}
	if rec = do(t, mux, http.MethodPatch, "/v1/schedule/"+created.ID, `{"schedule":"* * *"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid cron, got %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodPut, "/v1/schedule?id=nope", `{"status":"pending"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodPut, "/v1/schedule", `{"status":"pending"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 missing id, got %d", rec.Code)
	}

	if rec = do(t, mux, http.MethodDelete, "/v1/schedule?id="+created.ID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodDelete, "/v1/schedule?id="+created.ID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("second delete: %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodGet, "/v1/schedule", ""); strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected empty list, got %s", rec.Body.String())
	}
}

func TestHandlerRejectsBadInput(t *testing.T) {
	s := fixedStore(utc(2026, 1, 1, 0, 0))
	s.maxTasks = 3
	mux := newTestMux(s)
	cases := []struct {
		name string
		body string
		code int
	}{
		{"invalid cron", `{"name":"x","schedule":"99 * * * *"}`, http.StatusBadRequest},
		{"never fires", `{"name":"x","schedule":"0 0 30 2 *"}`, http.StatusBadRequest},
		{"missing name", `{"schedule":"@daily"}`, http.StatusBadRequest},
		{"bad type", `{"name":"x","type":"sometimes","schedule":"@daily"}`, http.StatusBadRequest},
		{"onetime in past", `{"name":"x","type":"onetime","schedule":"2025-01-01T00:00:00Z"}`, http.StatusBadRequest},
		{"interval too small", `{"name":"x","type":"interval","schedule":"10ms"}`, http.StatusBadRequest},
		{"non-pending status", `{"name":"x","schedule":"@daily","status":"running"}`, http.StatusBadRequest},
		{"malformed json", `{"name":`, http.StatusBadRequest},
		{"trailing data", `{"name":"x","schedule":"@daily"} {"x":1}`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
		{"ok onetime", `{"name":"x","type":"onetime","schedule":"2026-06-01T00:00:00Z"}`, http.StatusCreated},
		{"ok interval", `{"name":"x","type":"interval","schedule":"@every 5m"}`, http.StatusCreated},
		{"ok with id", `{"id":"nightly","name":"x","schedule":"@daily"}`, http.StatusCreated},
		{"duplicate id", `{"id":"nightly","name":"x","schedule":"@daily"}`, http.StatusConflict},
		{"store full", `{"name":"x","schedule":"@daily"}`, http.StatusInsufficientStorage},
	}
	for _, tc := range cases {
		rec := do(t, mux, http.MethodPost, "/v1/schedule", tc.body)
		if rec.Code != tc.code {
			t.Errorf("%s: expected %d, got %d: %s", tc.name, tc.code, rec.Code, rec.Body.String())
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s: content-type %q", tc.name, ct)
		}
	}

	big := `{"name":"x","schedule":"@daily","payload":"` + strings.Repeat("a", 2<<20) + `"}`
	if rec := do(t, mux, http.MethodPost, "/v1/schedule", big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}

	rec := do(t, mux, http.MethodOptions, "/v1/schedule", "")
	if rec.Code != http.StatusMethodNotAllowed || !strings.Contains(rec.Header().Get("Allow"), "POST") {
		t.Fatalf("expected 405 with Allow, got %d %v", rec.Code, rec.Header())
	}
	rec = do(t, mux, http.MethodPost, "/v1/schedule/some-id", `{"name":"x","schedule":"@daily"}`)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("expected 405 for POST on item, got %d", rec.Code)
	}
	if rec = do(t, mux, http.MethodGet, "/v1/schedule/a/b", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nested path, got %d", rec.Code)
	}
}

func TestHandlerListFilters(t *testing.T) {
	s := fixedStore(utc(2026, 1, 1, 0, 0))
	mux := newTestMux(s)
	do(t, mux, http.MethodPost, "/v1/schedule", `{"name":"a","schedule":"@daily"}`)
	do(t, mux, http.MethodPost, "/v1/schedule", `{"name":"b","type":"interval","schedule":"1m"}`)
	rec := do(t, mux, http.MethodGet, "/v1/schedule?type=interval", "")
	var tasks []ScheduledTask
	if err := json.Unmarshal(rec.Body.Bytes(), &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Name != "b" {
		t.Fatalf("unexpected filtered list %+v", tasks)
	}
}
