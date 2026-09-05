package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/eval"
)

func newEvalJudgeHandler() http.HandlerFunc {
	store := eval.NewInMemoryScoreStore()
	pipeline := eval.NewJudgePipeline(&fakeJudgeClient{}, store, "general")
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req struct {
			Prompt       string `json:"prompt"`
			Response     string `json:"response"`
			Model        string `json:"model"`
			Provider     string `json:"provider"`
			PromptVersion string `json:"prompt_version"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		score, err := pipeline.ScoreRequest(r.Context(), req.Prompt, req.Response, req.Model, req.Provider, req.PromptVersion)
		if err != nil {
			http.Error(w, `{"error":"score failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]float64{"score": score})
	}
}

func newEvalRegressionHandler() http.HandlerFunc {
	store := eval.NewInMemoryScoreStore()
	detector := eval.NewRegressionDetector(store)
	return func(w http.ResponseWriter, r *http.Request) {
		regressions, err := detector.Detect(r.Context(), eval.ScoreFilter{})
		if err != nil {
			http.Error(w, `{"error":"detection failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(regressions)
	}
}

func newEvalBenchmarkHandler() http.HandlerFunc {
	store := eval.NewInMemoryScoreStore()
	runner := eval.NewBenchmarkRunner(&fakeJudgeClient{}, store)
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req struct {
			Dataset  string `json:"dataset"`
			Model    string `json:"model"`
			Provider string `json:"provider"`
			Rubric   string `json:"rubric"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		reader := strings.NewReader(req.Dataset)
		result, err := runner.Run(r.Context(), reader, req.Model, req.Provider, req.Rubric)
		if err != nil {
			http.Error(w, `{"error":"benchmark failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

type fakeJudgeClient struct{}

func (f *fakeJudgeClient) ChatCompletion(ctx context.Context, prompt string) (string, error) {
	return "85", nil
}
