package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEvalJudgeRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/eval/judge", newEvalJudgeHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/eval/judge", strings.NewReader(`{"prompt":"hi","response":"hello","model":"m1","provider":"p1","prompt_version":"v1"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"score":85`) {
		t.Fatalf("expected score 85, got: %s", rec.Body.String())
	}
}

func TestEvalRegressionRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/eval/regression", newEvalRegressionHandler())
	req := httptest.NewRequest(http.MethodGet, "/v1/eval/regression", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvalBenchmarkRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/eval/benchmark", newEvalBenchmarkHandler())
	body := `{"dataset":"{\"prompt\":\"hello\"}\n","model":"m1","provider":"p1","rubric":"general"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/eval/benchmark", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"Total":1`) {
		t.Fatalf("expected total 1, got: %s", rec.Body.String())
	}
}
