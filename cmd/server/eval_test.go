package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/eval"
)

type fakeJudgeClient struct{ score string }

func (f *fakeJudgeClient) ChatCompletion(ctx context.Context, prompt string) (string, error) {
	return f.score, nil
}

func evalMux() *http.ServeMux {
	judge, regression, benchmark := newEvalHandlersWith(&fakeJudgeClient{score: "85"}, eval.NewInMemoryScoreStore())
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/eval/judge", judge)
	mux.HandleFunc("/v1/eval/regression", regression)
	mux.HandleFunc("/v1/eval/benchmark", benchmark)
	return mux
}

func TestEvalJudgeRoute(t *testing.T) {
	mux := evalMux()
	req := httptest.NewRequest(http.MethodPost, "/v1/eval/judge", strings.NewReader(`{"prompt":"hi","response":"hello","model":"m1","provider":"p1","prompt_version":"v1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"score":85`) {
		t.Fatalf("expected score 85, got: %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/eval/judge", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET judge: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/eval/judge", strings.NewReader(`{"prompt":""}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty prompt: %d", rec.Code)
	}
}

func TestEvalJudgeWithoutModelIsUnavailable(t *testing.T) {
	judge, _, _ := newEvalHandlersWith(&gatewayJudge{}, eval.NewInMemoryScoreStore())
	rec := httptest.NewRecorder()
	judge(rec, httptest.NewRequest(http.MethodPost, "/v1/eval/judge", strings.NewReader(`{"prompt":"hi","response":"hello"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without a judge model, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvalRegressionRoute(t *testing.T) {
	mux := evalMux()
	req := httptest.NewRequest(http.MethodGet, "/v1/eval/regression", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected 200 [], got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvalBenchmarkRoute(t *testing.T) {
	mux := evalMux()
	body := `{"dataset":"{\"prompt\":\"hello\"}\n","model":"m1","provider":"p1","rubric":"general"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/eval/benchmark", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"Total":1`) {
		t.Fatalf("expected total 1, got: %s", rec.Body.String())
	}
}
