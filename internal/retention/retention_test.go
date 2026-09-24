package retention

import (
	"context"
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

func TestRetentionStore(t *testing.T) {
	store := NewRetentionStore()
	if _, err := store.Upsert(RetentionPolicy{ID: "r1", Resource: "logs", TTL: 24 * time.Hour, MaxItems: 1000}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("r1"); !ok {
		t.Fatalf("expected policy r1")
	}
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(store.List()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/retention", WebhookHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/v1/retention?id=r1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"resource":"logs"`) {
		t.Fatalf("expected resource logs in body, got: %s", rec.Body.String())
	}
}

func TestTTLJSONUnits(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{`{"ttl":24}`, 24 * time.Hour, true}, // legacy clients send hours
		{`{"ttl":0.5}`, 30 * time.Minute, true},
		{`{"ttl":"90m"}`, 90 * time.Minute, true},
		{`{"ttl":"7d"}`, 7 * 24 * time.Hour, true},
		{`{"ttl_duration":"36h"}`, 36 * time.Hour, true},
		{`{"ttl":-1}`, 0, false},
		{`{"ttl":1e30}`, 0, false},
		{`{"ttl":"forever"}`, 0, false},
		{`{"ttl":true}`, 0, false},
	}
	for _, tc := range cases {
		var p RetentionPolicy
		err := json.Unmarshal([]byte(tc.in), &p)
		if (err == nil) != tc.ok || (tc.ok && p.TTL != tc.want) {
			t.Errorf("%s: got ttl=%s err=%v", tc.in, p.TTL, err)
		}
	}
	out, _ := json.Marshal(RetentionPolicy{ID: "x", Resource: "logs", TTL: 36 * time.Hour})
	if !strings.Contains(string(out), `"ttl":36`) || !strings.Contains(string(out), `"ttl_duration":"36h0m0s"`) {
		t.Fatalf("unexpected encoding: %s", out)
	}
	var round RetentionPolicy
	if err := json.Unmarshal(out, &round); err != nil || round.TTL != 36*time.Hour {
		t.Fatalf("round trip: %v %s", err, round.TTL)
	}
}

func TestValidation(t *testing.T) {
	s := NewRetentionStore()
	bad := []RetentionPolicy{
		{Resource: ""},
		{Resource: "logs"}, // neither ttl nor max_items
		{Resource: "logs", TTL: -time.Hour},
		{Resource: "logs", MaxItems: -5},
		{Resource: "../etc", TTL: time.Hour},
		{ID: "bad id", Resource: "logs", TTL: time.Hour},
		{Resource: "logs", TTL: MaxTTL + time.Hour},
	}
	for i, p := range bad {
		if _, err := s.Upsert(p); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: expected ErrInvalid, got %v", i, err)
		}
	}
	p, err := s.Upsert(RetentionPolicy{Resource: "logs", MaxItems: 5})
	if err != nil || !strings.HasPrefix(p.ID, "ret_") || p.CreatedAt.IsZero() {
		t.Fatalf("generated policy: %v %+v", err, p)
	}
	again, _ := s.Upsert(RetentionPolicy{ID: p.ID, Resource: "logs", MaxItems: 6, CreatedAt: time.Unix(0, 0)})
	if !again.CreatedAt.Equal(p.CreatedAt) {
		t.Fatal("created_at must be preserved and server managed")
	}
}

func TestSweepEnforcesTTLAndMaxItems(t *testing.T) {
	s := NewRetentionStore()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	logs := NewMemoryTarget()
	for i := 0; i < 10; i++ {
		// log-0 is 10h old ... log-9 is 1h old
		logs.Put(fmt.Sprintf("log-%d", i), now.Add(-time.Duration(10-i)*time.Hour))
	}
	logs.Put("undated", time.Time{})
	s.RegisterTarget("logs", logs)
	s.Upsert(RetentionPolicy{ID: "ttl", Resource: "logs", TTL: 5*time.Hour + time.Minute})
	s.Upsert(RetentionPolicy{ID: "cap", Resource: "logs", MaxItems: 4})
	s.Upsert(RetentionPolicy{ID: "loose", Resource: "logs", TTL: 100 * time.Hour, MaxItems: 100})
	s.Upsert(RetentionPolicy{ID: "orphan", Resource: "traces", TTL: time.Hour})

	rep, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// TTL (strictest 5h1m) removes log-0..log-4; 6 remain (log-5..9 + undated),
	// cap 4 trims undated (unknown age sorts oldest) and log-5.
	if logs.Len() != 4 {
		t.Fatalf("expected 4 items, got %d", logs.Len())
	}
	for i := 6; i < 10; i++ {
		if !logs.Has(fmt.Sprintf("log-%d", i)) {
			t.Fatalf("newest item log-%d must be kept", i)
		}
	}
	if len(rep.Resources) != 2 {
		t.Fatalf("expected two resources in report: %+v", rep)
	}
	got := rep.Resources[0]
	if got.Resource != "logs" || got.Expired != 5 || got.Trimmed != 2 || got.Scanned != 11 || !got.Enforced {
		t.Fatalf("unexpected logs report: %+v", got)
	}
	if rep.Resources[1].Enforced || rep.Resources[1].Error == "" {
		t.Fatalf("unregistered resource must be reported unenforced: %+v", rep.Resources[1])
	}
	// Idempotent: a second sweep deletes nothing.
	rep, _ = s.Sweep(context.Background())
	if rep.Resources[0].Expired != 0 || rep.Resources[0].Trimmed != 0 {
		t.Fatalf("second sweep should be a no-op: %+v", rep.Resources[0])
	}
}

func TestSweepReportsTargetErrors(t *testing.T) {
	s := NewRetentionStore()
	s.Upsert(RetentionPolicy{ID: "p", Resource: "db", MaxItems: 1})
	s.RegisterTarget("db", TargetFuncs{
		ItemsFunc: func(context.Context) ([]Item, error) {
			return []Item{{Key: "a", CreatedAt: time.Now()}, {Key: "b", CreatedAt: time.Now()}}, nil
		},
		DeleteFunc: func(context.Context, []string) error { return errors.New("disk on fire") },
	})
	rep, err := s.Sweep(context.Background())
	if err == nil || !strings.Contains(err.Error(), "disk on fire") || rep.Resources[0].Error == "" {
		t.Fatalf("expected delete error, got %v %+v", err, rep)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Sweep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	s := NewRetentionStore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, time.Millisecond, nil)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestConcurrentSweepAndUpsert(t *testing.T) {
	s := NewRetentionStore()
	target := NewMemoryTarget()
	s.RegisterTarget("r", target)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				target.Put(fmt.Sprintf("%d-%d", i, j), time.Now())
				s.Upsert(RetentionPolicy{ID: fmt.Sprintf("p%d", i), Resource: "r", MaxItems: 10 + j})
				s.Sweep(context.Background())
				s.List()
			}
		}(i)
	}
	wg.Wait()
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandlerREST(t *testing.T) {
	s := NewRetentionStore()
	h := WebhookHandler(s)
	rec := do(t, h, http.MethodPost, "/v1/retention", `{"id":"logs","resource":"logs","ttl":24,"max_items":1000}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ttl":24`) {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if p, _ := s.Get("logs"); p.TTL != 24*time.Hour {
		t.Fatalf("ttl must be 24h, got %s", p.TTL)
	}
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"resource":"logs","ttl":-4}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "ttl") {
		t.Fatalf("negative ttl: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"resource":"logs"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("no bounds: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/v1/retention/logs", ""); rec.Code != http.StatusOK {
		t.Fatalf("get by path: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/v1/retention?id=missing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get missing: %d", rec.Code)
	}
	rec = do(t, h, http.MethodPatch, "/v1/retention?id=logs", `{"max_items":10}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"max_items":10`) || !strings.Contains(rec.Body.String(), `"ttl":24`) {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodPut, "/v1/retention/missing", `{"resource":"x","ttl":1}`); rec.Code != http.StatusNotFound {
		t.Fatalf("put missing: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPut, "/v1/retention/logs", `{"resource":"logs","ttl":"1h"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"max_items":0`) {
		t.Fatalf("put replaces: %d %s", rec.Code, rec.Body.String())
	}
	s.RegisterTarget("logs", NewMemoryTarget())
	if rec := do(t, h, http.MethodPost, "/v1/retention?sweep=true", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"enforced":true`) {
		t.Fatalf("sweep: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodDelete, "/v1/retention/logs", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodDelete, "/v1/retention/logs", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d", rec.Code)
	}
	rec = do(t, h, "OPTIONS", "/v1/retention", "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("405: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"resource":"`+strings.Repeat("a", maxBodyBytes)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("413: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/v1/retention", `{"resource":`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", rec.Code)
	}
}
