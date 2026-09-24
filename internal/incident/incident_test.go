package incident

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIncidentStore(t *testing.T) {
	store := NewStore()
	store.Create(Incident{Title: "outage", Severity: SeverityHigh, Status: StatusOpen})
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 incident, got %d", len(store.List()))
	}
	if !store.Resolve(store.List()[0].ID) {
		t.Fatalf("expected resolve to succeed")
	}
}

func TestCreateGeneratesUniqueIDs(t *testing.T) {
	store := NewStore()
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		inc, err := store.Create(Incident{Title: "same second"})
		if err != nil {
			t.Fatal(err)
		}
		if seen[inc.ID] || !strings.HasPrefix(inc.ID, "inc_") {
			t.Fatalf("bad or duplicate id %q", inc.ID)
		}
		seen[inc.ID] = true
	}
	// The old timestamp-based IDs collided within one second and silently
	// overwrote incidents.
	if n := len(store.List()); n != 100 {
		t.Fatalf("expected 100 incidents, got %d", n)
	}
}

func TestCreateValidation(t *testing.T) {
	store := NewStore()
	bad := []Incident{
		{},
		{Title: "   "},
		{Title: strings.Repeat("t", MaxTitleLength+1)},
		{Title: "x", Severity: "apocalyptic"},
		{Title: "x", Status: "done"},
		{Title: "x", ID: "../../x"},
		{Title: "x", Description: strings.Repeat("d", MaxDescription+1)},
	}
	for i, inc := range bad {
		if _, err := store.Create(inc); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: expected ErrInvalid, got %v", i, err)
		}
	}
	inc, err := store.Create(Incident{Title: "defaults"})
	if err != nil {
		t.Fatal(err)
	}
	if inc.Severity != SeverityMedium || inc.Status != StatusOpen || inc.CreatedAt.IsZero() {
		t.Fatalf("unexpected defaults: %+v", inc)
	}
	if _, err := store.Create(Incident{ID: inc.ID, Title: "dup"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestLifecycleTransitions(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{StatusOpen, StatusAcknowledged, true},
		{StatusOpen, StatusResolved, true},
		{StatusOpen, StatusClosed, true},
		{StatusAcknowledged, StatusResolved, true},
		{StatusAcknowledged, StatusOpen, false},
		{StatusInvestigating, StatusAcknowledged, false},
		{StatusResolved, StatusClosed, true},
		{StatusResolved, StatusOpen, true},
		{StatusResolved, StatusAcknowledged, false},
		{StatusClosed, StatusOpen, false},
		{StatusClosed, StatusResolved, false},
		{StatusClosed, StatusClosed, true},
		{StatusOpen, "bogus", false},
	}
	for _, tc := range cases {
		if got := CanTransition(tc.from, tc.to); got != tc.ok {
			t.Errorf("%s -> %s: got %v want %v", tc.from, tc.to, got, tc.ok)
		}
	}
}

func TestTransitionTimestamps(t *testing.T) {
	store := NewStore()
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { clock = clock.Add(time.Minute); return clock }
	inc, _ := store.Create(Incident{Title: "db down", Severity: SeverityCritical})
	if !inc.AcknowledgedAt.IsZero() || !inc.ResolvedAt.IsZero() {
		t.Fatalf("new incident must not have lifecycle timestamps: %+v", inc)
	}
	acked, err := store.Transition(inc.ID, StatusAcknowledged)
	if err != nil || acked.AcknowledgedAt.IsZero() {
		t.Fatalf("ack: %v %+v", err, acked)
	}
	resolved, err := store.Transition(inc.ID, StatusResolved)
	if err != nil || !resolved.ResolvedAt.After(acked.AcknowledgedAt) {
		t.Fatalf("resolve: %v %+v", err, resolved)
	}
	closed, err := store.Transition(inc.ID, StatusClosed)
	if err != nil || closed.ClosedAt.IsZero() || !closed.ResolvedAt.Equal(resolved.ResolvedAt) {
		t.Fatalf("close: %v %+v", err, closed)
	}
	if _, err := store.Transition(inc.ID, StatusOpen); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("closed incidents must not reopen: %v", err)
	}
	if store.Resolve(inc.ID) {
		t.Fatal("resolving a closed incident must fail")
	}
	if store.Resolve("missing") {
		t.Fatal("resolving a missing incident must fail")
	}

	// Reopen clears resolution timestamps.
	inc2, _ := store.Create(Incident{Title: "flaky"})
	store.Transition(inc2.ID, StatusResolved)
	reopened, err := store.Transition(inc2.ID, StatusOpen)
	if err != nil || !reopened.ResolvedAt.IsZero() {
		t.Fatalf("reopen: %v %+v", err, reopened)
	}
	// Update keeps created_at and ignores client timestamps.
	upd, err := store.Update(inc2.ID, Incident{Title: "renamed", CreatedAt: time.Unix(0, 0)})
	if err != nil || upd.Title != "renamed" || !upd.CreatedAt.Equal(inc2.CreatedAt) || upd.Status != StatusOpen {
		t.Fatalf("update: %v %+v", err, upd)
	}
}

func TestHookAndBoundedStore(t *testing.T) {
	store := NewStoreWithLimit(2)
	var mu sync.Mutex
	var events []string
	store.SetHook(func(e Event) {
		mu.Lock()
		events = append(events, e.Type)
		mu.Unlock()
	})
	a, _ := store.Create(Incident{Title: "a"})
	b, _ := store.Create(Incident{Title: "b"})
	if _, err := store.Create(Incident{Title: "c"}); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected ErrStoreFull with only open incidents, got %v", err)
	}
	store.Transition(a.ID, StatusResolved)
	c, err := store.Create(Incident{Title: "c"})
	if err != nil {
		t.Fatalf("expected eviction of resolved incident: %v", err)
	}
	if _, ok := store.Get(a.ID); ok {
		t.Fatal("resolved incident should have been evicted")
	}
	if _, ok := store.Get(b.ID); !ok {
		t.Fatal("open incident must never be evicted")
	}
	store.Delete(c.ID)
	mu.Lock()
	defer mu.Unlock()
	want := "created,created,transitioned,created,deleted"
	if got := strings.Join(events, ","); got != want {
		t.Fatalf("events %s, want %s", got, want)
	}
}

func TestConcurrentStore(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				inc, err := store.Create(Incident{Title: fmt.Sprintf("t%d-%d", i, j)})
				if err != nil {
					t.Error(err)
					return
				}
				store.Transition(inc.ID, StatusAcknowledged)
				store.List()
				store.Resolve(inc.ID)
			}
		}(i)
	}
	wg.Wait()
	if n := len(store.Filter(StatusResolved, "")); n != 400 {
		t.Fatalf("expected 400 resolved, got %d", n)
	}
}

func serve(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIncidentWebhook(t *testing.T) {
	mux := http.NewServeMux()
	store := NewStore()
	mux.HandleFunc("/v1/incidents", WebhookHandler(store))

	req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(`{"title":"outage","severity":"high","status":"open"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/incidents", nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"severity":"high"`) {
		t.Fatalf("expected severity high in body, got: %s", getRec.Body.String())
	}
}

func TestWebhookREST(t *testing.T) {
	store := NewStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/incidents", WebhookHandler(store))
	mux.HandleFunc("/v1/incidents/", WebhookHandler(store))

	rec := serve(t, mux, http.MethodPost, "/v1/incidents", `{"title":"api errors","severity":"critical"}`)
	var created Incident
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil || rec.Code != http.StatusCreated || created.ID == "" {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "resolved_at") {
		t.Fatalf("unset timestamps must be omitted: %s", rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPost, "/v1/incidents", `{"title":""}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid create: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodGet, "/v1/incidents?id="+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get by query id: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodGet, "/v1/incidents/"+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get by path id: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodGet, "/v1/incidents/nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get missing: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodPost, "/v1/incidents/"+created.ID+"/ack", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"acknowledged"`) {
		t.Fatalf("ack: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPatch, "/v1/incidents?id="+created.ID, `{"status":"open"}`); rec.Code != http.StatusConflict {
		t.Fatalf("invalid transition: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPatch, "/v1/incidents/"+created.ID, `{"severity":"low"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"severity":"low"`) {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPut, "/v1/incidents?id=missing", `{"title":"x"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("put missing: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodPut, "/v1/incidents?id="+created.ID, `{"title":"renamed","severity":"high"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"title":"renamed"`) {
		t.Fatalf("put: %d %s", rec.Code, rec.Body.String())
	}
	// Legacy resolve shape.
	rec = serve(t, mux, http.MethodPost, "/v1/incidents?resolve=true&id="+created.ID, "")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"resolved"}` {
		t.Fatalf("legacy resolve: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPost, "/v1/incidents?resolve=true&id=missing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("legacy resolve missing: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodGet, "/v1/incidents?status=resolved", ""); !strings.Contains(rec.Body.String(), created.ID) {
		t.Fatalf("filter: %s", rec.Body.String())
	}
	if rec := serve(t, mux, http.MethodPost, "/v1/incidents/"+created.ID+"/explode", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown action: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodDelete, "/v1/incidents/"+created.ID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if rec := serve(t, mux, http.MethodDelete, "/v1/incidents/"+created.ID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d", rec.Code)
	}
	rec = serve(t, mux, "TRACE", "/v1/incidents", "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("405: %d %q", rec.Code, rec.Header().Get("Allow"))
	}
	if rec := serve(t, mux, http.MethodPost, "/v1/incidents", `{"title":"`+strings.Repeat("a", maxBodyBytes)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("413: %d", rec.Code)
	}
}
