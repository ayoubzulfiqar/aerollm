package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func getResults(t *testing.T, h http.Handler, query string) (ShadowResultsResponse, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/shadow/results"+query, nil))
	var body ShadowResultsResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v\n%s", err, rec.Body)
		}
	}
	return body, rec
}

func TestResultsHandlerEndToEnd(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	target := server.URL + "/tenant-secret-token/"
	s := mustTester(t, ShadowTesterConfig{TargetURL: target, APIKey: "sk-live-secret", MaxInFlight: 1})
	for i := 0; i < 3; i++ {
		if err := s.RunAsync(context.Background(), "", "", &models.LLMRequest{Model: fmt.Sprint("m", i)}); err != nil {
			t.Fatal(err)
		}
		s.Wait()
	}

	body, rec := getResults(t, s.ResultsHandler(), "")
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d headers %v", rec.Code, rec.Header())
	}
	raw := rec.Body.String()
	for _, secret := range []string{"sk-live-secret", "tenant-secret-token"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("secret %q leaked in results: %s", secret, raw)
		}
	}
	if body.Buffered != 3 || body.Summary.Count != 3 || body.Summary.Errors != 1 {
		t.Fatalf("unexpected summary: %+v buffered=%d", body.Summary, body.Buffered)
	}
	// Newest first: the third call succeeded, the second failed.
	if !body.Results[0].OK || body.Results[1].OK || body.Results[1].StatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected ordering/results: %+v", body.Results)
	}
	if body.Results[0].Provider != server.URL {
		t.Fatalf("provider = %q, want %q", body.Results[0].Provider, server.URL)
	}

	body, _ = getResults(t, s.ResultsHandler(), "?errors=true")
	if body.Summary.Count != 1 || body.Results[0].OK {
		t.Fatalf("errors filter: %+v", body)
	}
	body, _ = getResults(t, s.ResultsHandler(), "?limit=2")
	if len(body.Results) != 2 || body.Buffered != 3 {
		t.Fatalf("limit: %+v", body)
	}
}

func TestResultsHandlerRedactsErrorURLAndBoundsMessage(t *testing.T) {
	s := mustTester(t, ShadowTesterConfig{ResultBuffer: 5})
	provider := "https://shadow.example.com:8443/acct/abc123/"
	s.pool.results.add(ShadowResult{
		Provider:     provider,
		Timestamp:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Latency:      1500 * time.Millisecond,
		Error:        errors.New("boom"),
		ErrorMessage: `traffic: shadow request: Post "https://shadow.example.com:8443/acct/abc123/v1/chat/completions": dial tcp: i/o timeout` + strings.Repeat("é", 600),
	})
	s.pool.results.add(ShadowResult{Provider: "https://ok.example.com", Error: errors.New("only error set")})
	body, rec := getResults(t, s.ResultsHandler(), "")
	if strings.Contains(rec.Body.String(), "abc123") {
		t.Fatalf("path leaked: %s", rec.Body)
	}
	got := body.Results[1]
	if got.Provider != "https://shadow.example.com:8443" || got.LatencyMS != 1500 || got.OK {
		t.Fatalf("unexpected view: %+v", got)
	}
	if !strings.Contains(got.Error, `Post "https://shadow.example.com:8443/v1/chat/completions"`) {
		t.Fatalf("error not redacted as expected: %q", got.Error)
	}
	if len(got.Error) > maxErrorMessageLen+len("…") || !strings.HasSuffix(got.Error, "…") {
		t.Fatalf("error not bounded: %d bytes", len(got.Error))
	}
	if !utf8.ValidString(got.Error) {
		t.Fatal("truncation split a UTF-8 sequence")
	}
	if body.Results[0].OK || body.Results[0].Error != "only error set" {
		t.Fatalf("error-only result: %+v", body.Results[0])
	}
}

func TestResultsHandlerValidationAndMethods(t *testing.T) {
	h := mustTester(t, ShadowTesterConfig{}).ResultsHandler()
	for _, q := range []string{"?limit=0", "?limit=-3", "?limit=abc", "?errors=maybe"} {
		if _, rec := getResults(t, h, q); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d", q, rec.Code)
		}
	}
	body, rec := getResults(t, h, "?limit=999999")
	if rec.Code != http.StatusOK || body.Results == nil || len(body.Results) != 0 {
		t.Fatalf("empty buffer: %d %s", rec.Code, rec.Body)
	}
	for _, m := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(m, "/v1/shadow/results", nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("%s: %d allow=%q", m, rec.Code, rec.Header().Get("Allow"))
		}
	}
	var nilTester *ShadowTester
	if body, rec := getResults(t, nilTester.ResultsHandler(), ""); rec.Code != http.StatusOK || body.Summary.Count != 0 {
		t.Fatalf("nil tester: %d %s", rec.Code, rec.Body)
	}
}
