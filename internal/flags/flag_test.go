package flags

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestFlagStore(t *testing.T) {
	store := NewStore()
	if err := store.Upsert(FeatureFlag{Key: "darkmode", Enabled: true, Strategy: RolloutGlobal}); err != nil {
		t.Fatal(err)
	}
	if got, ok := store.Get("darkmode"); !ok || !got.Enabled {
		t.Fatalf("expected darkmode enabled")
	}
	if !store.Enabled("darkmode", nil) {
		t.Fatalf("expected global flag to be enabled")
	}
}

func TestRolloutPercentage(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "beta", Enabled: true, Strategy: RolloutPercentage, Percentage: 50})
	enabled := 0
	for i := 0; i < 100; i++ {
		if store.Enabled("beta", map[string]string{"user": string(rune('a' + i))}) {
			enabled++
		}
	}
	if enabled == 0 || enabled == 100 {
		t.Fatalf("expected mixed rollout, got %d", enabled)
	}
}

func TestRolloutPercentageDeterministicWithManyAttributes(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "beta", Enabled: true, Strategy: RolloutPercentage, Percentage: 50})
	for i := 0; i < 200; i++ {
		ctx := map[string]string{"id": fmt.Sprintf("user-%d", i), "country": "US", "plan": "pro", "region": "eu", "x": "y"}
		first := store.Enabled("beta", ctx)
		for j := 0; j < 20; j++ {
			// Map iteration order is randomized; the old hash depended on it.
			if store.Enabled("beta", ctx) != first {
				t.Fatalf("non-deterministic evaluation for %v", ctx)
			}
		}
		// Extra non-identity attributes must not change the bucket.
		if store.Enabled("beta", map[string]string{"id": ctx["id"]}) != first {
			t.Fatalf("bucket depends on non-identity attributes for %s", ctx["id"])
		}
	}
}

func TestRolloutPercentageDistributionAndBounds(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "p30", Enabled: true, Strategy: RolloutPercentage, Percentage: 30})
	store.Upsert(FeatureFlag{Key: "p0", Enabled: true, Strategy: RolloutPercentage, Percentage: 0})
	store.Upsert(FeatureFlag{Key: "p100", Enabled: true, Strategy: RolloutPercentage, Percentage: 100})
	const n = 20000
	on := 0
	for i := 0; i < n; i++ {
		ctx := map[string]string{"user_id": fmt.Sprintf("u%d", i)}
		if store.Enabled("p30", ctx) {
			on++
		}
		if store.Enabled("p0", ctx) {
			t.Fatalf("0%% rollout enabled for %v", ctx)
		}
		if !store.Enabled("p100", ctx) {
			t.Fatalf("100%% rollout disabled for %v", ctx)
		}
	}
	frac := float64(on) / n
	if frac < 0.28 || frac > 0.32 {
		t.Fatalf("expected ~30%% enabled, got %.3f", frac)
	}
}

func TestRolloutPercentageNoIdentity(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "half", Enabled: true, Strategy: RolloutPercentage, Percentage: 99})
	if store.Enabled("half", nil) {
		t.Fatal("partial rollout without identity must be off")
	}
	store.Upsert(FeatureFlag{Key: "full", Enabled: true, Strategy: RolloutPercentage, Percentage: 100})
	if !store.Enabled("full", map[string]string{}) {
		t.Fatal("full rollout without identity must be on")
	}
}

func TestBucketByAttribute(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "org", Enabled: true, Strategy: RolloutPercentage, Percentage: 50, BucketBy: "org"})
	// All users of one org get the same answer.
	want := store.Enabled("org", map[string]string{"org": "acme", "id": "u1"})
	for i := 0; i < 50; i++ {
		if store.Enabled("org", map[string]string{"org": "acme", "id": fmt.Sprintf("u%d", i)}) != want {
			t.Fatal("bucket_by must group by org")
		}
	}
}

func TestRolloutAllowList(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "admin", Enabled: true, Strategy: RolloutAllowList, AllowList: []string{"admin1"}})
	if !store.Enabled("admin", map[string]string{"id": "admin1"}) {
		t.Fatalf("expected allowed user to pass")
	}
	if store.Enabled("admin", map[string]string{"id": "other"}) {
		t.Fatalf("expected non-allowed user to fail")
	}
	if store.Enabled("admin", nil) {
		t.Fatalf("expected anonymous user to fail")
	}
}

func TestDenyListAndKillSwitch(t *testing.T) {
	store := NewStore()
	store.Upsert(FeatureFlag{Key: "f", Enabled: true, Strategy: RolloutDenyList, DenyList: []string{"bad"}})
	if store.Enabled("f", map[string]string{"id": "BAD"}) {
		t.Fatal("denied user must be off (case-insensitive)")
	}
	if !store.Enabled("f", map[string]string{"id": "good"}) {
		t.Fatal("other users must be on")
	}
	store.Upsert(FeatureFlag{Key: "f", Enabled: false, Strategy: RolloutGlobal})
	if store.Enabled("f", nil) {
		t.Fatal("disabled flag must be off")
	}
	if store.Enabled("missing", nil) {
		t.Fatal("missing flag must be off")
	}
}

func TestTargetingRules(t *testing.T) {
	store := NewStore()
	err := store.Upsert(FeatureFlag{
		Key: "rules", Enabled: true, Strategy: RolloutPercentage, Percentage: 0,
		Rules: []Rule{
			{Attribute: "country", Operator: OpEquals, Values: []string{"DE"}, Serve: false},
			{Attribute: "email", Operator: OpSuffix, Values: []string{"@corp.com"}, Serve: true},
			{Attribute: "beta", Operator: OpExists, Serve: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		ctx    map[string]string
		want   bool
		reason string
	}{
		{map[string]string{"email": "a@CORP.com"}, true, "rule:1"},
		{map[string]string{"email": "a@corp.com", "country": "de"}, false, "rule:0"},
		{map[string]string{"beta": "1"}, true, "rule:2"},
		{map[string]string{"beta": ""}, false, "percentage_no_identity"},
		{map[string]string{"id": "x"}, false, "percentage"},
	}
	for _, tc := range cases {
		ev := store.Explain("rules", tc.ctx)
		if ev.Enabled != tc.want || ev.Reason != tc.reason {
			t.Errorf("ctx %v: got %+v, want enabled=%v reason=%s", tc.ctx, ev, tc.want, tc.reason)
		}
	}
}

func TestValidation(t *testing.T) {
	store := NewStore()
	bad := []FeatureFlag{
		{Key: ""},
		{Key: "../etc"},
		{Key: strings.Repeat("a", 200)},
		{Key: "k", Strategy: "bogus"},
		{Key: "k", Percentage: 101},
		{Key: "k", Percentage: -1},
		{Key: "k", Rules: []Rule{{Attribute: "", Operator: OpExists}}},
		{Key: "k", Rules: []Rule{{Attribute: "a", Operator: "regex", Values: []string{".*"}}}},
		{Key: "k", Rules: []Rule{{Attribute: "a", Operator: OpEquals}}},
		{Key: "k", Description: strings.Repeat("d", MaxDescription+1)},
	}
	for i, f := range bad {
		if err := store.Upsert(f); !errors.Is(err, ErrInvalidFlag) {
			t.Errorf("case %d: expected ErrInvalidFlag, got %v", i, err)
		}
	}
	if len(store.List()) != 0 {
		t.Fatal("invalid flags must not be stored")
	}
	// Empty strategy normalizes to global.
	if err := store.Upsert(FeatureFlag{Key: "k", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if f, _ := store.Get("k"); f.Strategy != RolloutGlobal || !store.Enabled("k", nil) {
		t.Fatalf("expected normalized global strategy, got %q", f.Strategy)
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	store := NewStore()
	allow := []string{"a"}
	meta := map[string]interface{}{"nested": map[string]interface{}{"x": 1}}
	store.Upsert(FeatureFlag{Key: "c", Enabled: true, Strategy: RolloutAllowList, AllowList: allow, Metadata: meta})
	allow[0] = "mutated"
	meta["nested"].(map[string]interface{})["x"] = 2
	got, _ := store.Get("c")
	if got.AllowList[0] != "a" || got.Metadata["nested"].(map[string]interface{})["x"] != 1 {
		t.Fatalf("store aliased caller data: %+v", got)
	}
	got.AllowList[0] = "again"
	if again, _ := store.Get("c"); again.AllowList[0] != "a" {
		t.Fatal("Get must return a copy")
	}
}

func TestStoreBounded(t *testing.T) {
	store := NewStore()
	for i := 0; i < MaxFlags; i++ {
		store.flags[fmt.Sprintf("f%d", i)] = FeatureFlag{Key: fmt.Sprintf("f%d", i)}
	}
	if err := store.Upsert(FeatureFlag{Key: "overflow"}); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected ErrStoreFull, got %v", err)
	}
	if err := store.Upsert(FeatureFlag{Key: "f1", Enabled: true}); err != nil {
		t.Fatalf("updating an existing flag must work when full: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				key := fmt.Sprintf("k%d", j%10)
				store.Upsert(FeatureFlag{Key: key, Enabled: true, Strategy: RolloutPercentage, Percentage: j % 100})
				store.Enabled(key, map[string]string{"id": fmt.Sprint(i)})
				store.List()
				if j%50 == 0 {
					store.Delete(key)
				}
			}
		}(i)
	}
	wg.Wait()
}

func newMux(store *Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/flags", WebhookHandler(store))
	mux.HandleFunc("/v1/flags/", WebhookHandler(store))
	return mux
}

func do(t *testing.T, mux http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestHandlerCRUD(t *testing.T) {
	store := NewStore()
	mux := newMux(store)

	rec := do(t, mux, http.MethodPost, "/v1/flags/darkmode", `{"key":"darkmode","enabled":true,"strategy":"global"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q", ct)
	}
	// Key taken from path when absent in body.
	if rec := do(t, mux, http.MethodPost, "/v1/flags/beta", `{"enabled":true}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"key":"beta"`) {
		t.Fatalf("create from path: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodPost, "/v1/flags/beta", `{"key":"other"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("key mismatch: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/v1/flags", `{"key":"x","strategy":"nope"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid strategy: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodGet, "/v1/flags/?key=darkmode", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"enabled":true`) {
		t.Fatalf("get query: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/v1/flags/darkmode", ""); rec.Code != http.StatusOK {
		t.Fatalf("get path: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodGet, "/v1/flags/nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get missing: %d", rec.Code)
	}
	rec = do(t, mux, http.MethodGet, "/v1/flags", "")
	var list []FeatureFlag
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list) != 2 || list[0].Key != "beta" {
		t.Fatalf("list: %v %s", err, rec.Body.String())
	}

	if rec := do(t, mux, http.MethodPut, "/v1/flags/missing", `{"enabled":true}`); rec.Code != http.StatusNotFound {
		t.Fatalf("put missing: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPut, "/v1/flags/beta", `{"enabled":false}`); rec.Code != http.StatusOK {
		t.Fatalf("put: %d %s", rec.Code, rec.Body.String())
	}
	if store.Enabled("beta", nil) {
		t.Fatal("put should have disabled beta")
	}
	rec = do(t, mux, http.MethodPatch, "/v1/flags/beta", `{"enabled":true,"strategy":"percentage","percentage":100}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"percentage":100`) {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodPatch, "/v1/flags/beta", `{"percentage":500}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid patch: %d", rec.Code)
	}
	if f, _ := store.Get("beta"); f.Percentage != 100 {
		t.Fatal("invalid patch must not be applied")
	}
	if rec := do(t, mux, http.MethodPatch, "/v1/flags/none", `{"enabled":true}`); rec.Code != http.StatusNotFound {
		t.Fatalf("patch missing: %d", rec.Code)
	}

	if rec := do(t, mux, http.MethodDelete, "/v1/flags/beta", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodDelete, "/v1/flags/beta", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d", rec.Code)
	}
	rec = do(t, mux, "OPTIONS", "/v1/flags", "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("405: %d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestHandlerEvaluate(t *testing.T) {
	store := NewStore()
	mux := newMux(store)
	store.Upsert(FeatureFlag{Key: "vip", Enabled: true, Strategy: RolloutAllowList, AllowList: []string{"u1"}})

	rec := do(t, mux, http.MethodGet, "/v1/flags/vip/evaluate?id=u1", "")
	var ev Evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &ev); err != nil || rec.Code != http.StatusOK || !ev.Enabled || ev.Reason != "allowlist" {
		t.Fatalf("evaluate GET: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, mux, http.MethodPost, "/v1/flags/vip/evaluate", `{"context":{"id":"u2"}}`)
	if err := json.Unmarshal(rec.Body.Bytes(), &ev); err != nil || rec.Code != http.StatusOK || ev.Enabled {
		t.Fatalf("evaluate POST: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/v1/flags/none/evaluate", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("evaluate missing: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodGet, "/v1/flags/vip/bogus", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown action: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodDelete, "/v1/flags/vip/evaluate", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("evaluate DELETE: %d", rec.Code)
	}
}

func TestHandlerBodyLimits(t *testing.T) {
	mux := newMux(NewStore())
	big := `{"key":"big","description":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	if rec := do(t, mux, http.MethodPost, "/v1/flags", big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/v1/flags", `{"key":"a"}{"key":"b"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for trailing data, got %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/v1/flags", `not json`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("expected JSON 400, got %d %s", rec.Code, rec.Body.String())
	}
}
