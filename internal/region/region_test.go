package region

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

func TestRegionStore(t *testing.T) {
	store := NewStore()
	store.UpsertRegion(Region{ID: "us-east-1", Name: "US East", Endpoint: "https://us.example.com", Primary: true})
	if len(store.ListRegions()) != 1 {
		t.Fatalf("expected 1 region, got %d", len(store.ListRegions()))
	}
	store.UpsertPolicy(ResidencyPolicy{ID: "p1", Region: "us-east-1", DataType: "pii", Required: true})
	if len(store.ListPolicies()) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(store.ListPolicies()))
	}
	store.UpsertRule(RouteRule{ID: "r1", Region: "us-east-1", Providers: []string{"openai"}, Priority: 1, Enabled: true})
	if len(store.ListRules()) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(store.ListRules()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/region/regions", WebhookHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/v1/region/regions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"name":"US East"`) {
		t.Fatalf("expected region name in body, got: %s", rec.Body.String())
	}
}

func TestValidation(t *testing.T) {
	s := NewStore()
	badRegions := []Region{
		{ID: "../x"},
		{ID: "r", Endpoint: "ftp://x"},
		{ID: "r", Endpoint: "https://user:pw@x.example.com"},
		{ID: "r", Endpoint: "not a url"},
		{ID: "r", Name: strings.Repeat("n", 200)},
	}
	for i, r := range badRegions {
		if _, err := s.UpsertRegion(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("region case %d: expected ErrInvalid, got %v", i, err)
		}
	}
	gen, err := s.UpsertRegion(Region{Name: "generated"})
	if err != nil || !strings.HasPrefix(gen.ID, "rg_") {
		t.Fatalf("generated id: %v %+v", err, gen)
	}
	if _, err := s.UpsertPolicy(ResidencyPolicy{Region: "nope", DataType: "pii"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("policy with unknown region: %v", err)
	}
	if _, err := s.UpsertPolicy(ResidencyPolicy{Region: gen.ID}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("policy without data type: %v", err)
	}
	if _, err := s.UpsertRule(RouteRule{Region: gen.ID}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("rule without providers: %v", err)
	}
	if _, err := s.UpsertRule(RouteRule{Region: gen.ID, Providers: []string{"a"}, Priority: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative priority: %v", err)
	}
	rule, err := s.UpsertRule(RouteRule{Region: gen.ID, Providers: []string{" a ", "a", "b"}})
	if err != nil || len(rule.Providers) != 2 {
		t.Fatalf("providers not cleaned: %v %+v", err, rule)
	}
	if err := s.DeleteRegion(gen.ID); !errors.Is(err, ErrInUse) {
		t.Fatalf("deleting referenced region: %v", err)
	}
	s.DeleteRule(rule.ID)
	if err := s.DeleteRegion(gen.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestSinglePrimary(t *testing.T) {
	s := NewStore()
	s.UpsertRegion(Region{ID: "a", Primary: true})
	s.UpsertRegion(Region{ID: "b", Primary: true})
	primaries := 0
	for _, r := range s.ListRegions() {
		if r.Primary {
			primaries++
			if r.ID != "b" {
				t.Fatalf("expected b to be primary, got %s", r.ID)
			}
		}
	}
	if primaries != 1 {
		t.Fatalf("expected exactly one primary, got %d", primaries)
	}
}

func residencyFixture(t *testing.T) *Store {
	t.Helper()
	s := NewStore()
	must := func(_ interface{}, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.UpsertRegion(Region{ID: "us-east-1", Primary: true}))
	must(s.UpsertRegion(Region{ID: "eu-west-1"}))
	must(s.UpsertRegion(Region{ID: "eu-central-1"}))
	must(s.UpsertRegion(Region{ID: "ap-south-1"}))
	must(s.UpsertPolicy(ResidencyPolicy{ID: "gdpr-fr", Region: "eu-west-1", DataType: "PII", Required: true}))
	must(s.UpsertPolicy(ResidencyPolicy{ID: "gdpr-de", Region: "eu-central-1", DataType: "pii", Required: true}))
	must(s.UpsertPolicy(ResidencyPolicy{ID: "logs-pref", Region: "ap-south-1", DataType: "logs"}))
	must(s.UpsertRule(RouteRule{ID: "us-main", Region: "us-east-1", Providers: []string{"openai"}, Priority: 1, Enabled: true}))
	must(s.UpsertRule(RouteRule{ID: "eu-backup", Region: "eu-west-1", Providers: []string{"mistral", "azure-eu"}, Priority: 5, Enabled: true}))
	must(s.UpsertRule(RouteRule{ID: "eu-main", Region: "eu-west-1", Providers: []string{"azure-eu"}, Priority: 1, Enabled: true}))
	must(s.UpsertRule(RouteRule{ID: "eu-off", Region: "eu-west-1", Providers: []string{"disabled"}, Priority: 0, Enabled: false}))
	must(s.UpsertRule(RouteRule{ID: "ap", Region: "ap-south-1", Providers: []string{"local"}, Priority: 1, Enabled: true}))
	return s
}

func TestResolveResidency(t *testing.T) {
	s := residencyFixture(t)

	// PII must stay in the EU even if the caller prefers the US.
	d, err := s.Resolve("pii", "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if d.Region.ID != "eu-west-1" || !d.ResidencyEnforced || !d.Rerouted {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if strings.Join(d.Providers, ",") != "azure-eu,mistral" || strings.Join(d.Rules, ",") != "eu-main,eu-backup" {
		t.Fatalf("providers must follow rule priority, got %v rules %v", d.Providers, d.Rules)
	}

	// Allowed preferred region is honored when it has rules; eu-central-1
	// has no rules, so we fall through to another compliant region.
	d, err = s.Resolve("pii", "eu-central-1")
	if err != nil || d.Region.ID != "eu-west-1" || !d.Rerouted {
		t.Fatalf("expected fallthrough to compliant eu-west-1: %v %+v", err, d)
	}

	// Unregulated data follows the preference, then the primary.
	d, err = s.Resolve("chat", "")
	if err != nil || d.Region.ID != "us-east-1" || d.ResidencyEnforced || d.Rerouted {
		t.Fatalf("expected primary region: %v %+v", err, d)
	}
	d, err = s.Resolve("chat", "eu-west-1")
	if err != nil || d.Region.ID != "eu-west-1" || d.Rerouted {
		t.Fatalf("expected preferred region: %v %+v", err, d)
	}
	// Non-required policy acts as a preference.
	d, err = s.Resolve("logs", "")
	if err != nil || d.Region.ID != "ap-south-1" {
		t.Fatalf("expected preferred-by-policy region: %v %+v", err, d)
	}

	// Disabling the compliant rules must fail closed, never leak to the US.
	for _, id := range []string{"eu-main", "eu-backup"} {
		r, _ := s.GetRule(id)
		r.Enabled = false
		if _, err := s.UpsertRule(r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Resolve("PII", "us-east-1"); !errors.Is(err, ErrNoCompliantRoute) {
		t.Fatalf("expected ErrNoCompliantRoute, got %v", err)
	}
}

func TestResolveEmpty(t *testing.T) {
	if _, err := NewStore().Resolve("pii", ""); !errors.Is(err, ErrNoRegions) {
		t.Fatalf("expected ErrNoRegions, got %v", err)
	}
	s := NewStore()
	s.UpsertRegion(Region{ID: "a"})
	d, err := s.Resolve("", "")
	if err != nil || d.Region.ID != "a" || len(d.Providers) != 0 {
		t.Fatalf("expected region without rules: %v %+v", err, d)
	}
}

func TestConcurrentStore(t *testing.T) {
	s := residencyFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.UpsertRule(RouteRule{ID: fmt.Sprintf("r%d", i), Region: "us-east-1", Providers: []string{"p"}, Enabled: j%2 == 0})
				s.Resolve("pii", "us-east-1")
				s.ListRules()
			}
		}(i)
	}
	wg.Wait()
}

func req(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestHandlerREST(t *testing.T) {
	s := NewStore()
	h := WebhookHandler(s)

	if rec := req(t, h, http.MethodPost, "/v1/region/regions", `{"id":"eu","name":"EU","endpoint":"https://eu.example.com"}`); rec.Code != http.StatusOK {
		t.Fatalf("create region: %d %s", rec.Code, rec.Body.String())
	}
	if rec := req(t, h, http.MethodPost, "/v1/region/regions", `{"id":"bad","endpoint":"javascript:alert(1)"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid region: %d", rec.Code)
	}
	rec := req(t, h, http.MethodPost, "/v1/region/residency", `{"region":"eu","data_type":"pii","required":true}`)
	var pol ResidencyPolicy
	if err := json.Unmarshal(rec.Body.Bytes(), &pol); err != nil || rec.Code != http.StatusOK || pol.ID == "" {
		t.Fatalf("create policy: %d %s", rec.Code, rec.Body.String())
	}
	if rec := req(t, h, http.MethodPost, "/v1/region/residency", `{"region":"mars","data_type":"pii"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("policy unknown region: %d", rec.Code)
	}
	rec = req(t, h, http.MethodPost, "/v1/region/routes", `{"id":"r1","region":"eu","providers":["azure-eu"],"priority":1,"enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create rule: %d %s", rec.Code, rec.Body.String())
	}
	rec = req(t, h, http.MethodGet, "/v1/region/routes?resolve=true&data_type=pii&region=us", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"azure-eu"`) {
		t.Fatalf("resolve: %d %s", rec.Code, rec.Body.String())
	}
	if rec := req(t, h, http.MethodGet, "/v1/region/regions/eu", ""); rec.Code != http.StatusOK {
		t.Fatalf("get by path: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodGet, "/v1/region/residency?id="+pol.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get by query: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodGet, "/v1/region/routes/missing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get missing: %d", rec.Code)
	}
	// PATCH merges; PUT on missing is 404.
	rec = req(t, h, http.MethodPatch, "/v1/region/routes/r1", `{"enabled":false}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"providers":["azure-eu"]`) || !strings.Contains(rec.Body.String(), `"enabled":false`) {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	if rec := req(t, h, http.MethodGet, "/v1/region/routes?resolve=true&data_type=pii", ""); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("resolve without compliant route: %d %s", rec.Code, rec.Body.String())
	}
	if rec := req(t, h, http.MethodPut, "/v1/region/routes/nope", `{"region":"eu","providers":["x"]}`); rec.Code != http.StatusNotFound {
		t.Fatalf("put missing: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodPut, "/v1/region/routes/r1", `{"id":"other","region":"eu","providers":["x"]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("put id mismatch: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodDelete, "/v1/region/regions/eu", ""); rec.Code != http.StatusConflict {
		t.Fatalf("delete referenced region: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodDelete, "/v1/region/routes/r1", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete rule: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodDelete, "/v1/region/residency?id="+pol.ID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete policy: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodDelete, "/v1/region/regions/eu", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete region: %d", rec.Code)
	}
	rec = req(t, h, "CONNECT", "/v1/region/regions", "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("405: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodGet, "/v1/region/unknown", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown collection: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodPost, "/v1/region/regions", `{"id":"x"`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", rec.Code)
	}
	if rec := req(t, h, http.MethodPost, "/v1/region/regions", `{"name":"`+strings.Repeat("a", maxBodyBytes)+`"}`); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("413: %d", rec.Code)
	}
}
