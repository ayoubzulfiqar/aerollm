package main

import (
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/eval"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func chatRequest(model, prompt string) *models.LLMRequest {
	p := prompt
	return &models.LLMRequest{Model: model, Messages: []models.Message{{Role: models.RoleUser, Content: &p}}}
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		return false
	}
	return true
}

func writeEvalError(w http.ResponseWriter, what string, err error) {
	if errors.Is(err, errJudgeNotConfigured) {
		middleware.WriteJSONError(w, http.StatusServiceUnavailable, err.Error(), "")
		return
	}
	middleware.WriteJSONError(w, http.StatusBadGateway, what+" failed: "+err.Error(), "")
}

// newEvalHandlers returns the judge, regression and benchmark handlers. They
// share one score store (so regressions are detected over judged scores) and
// use a real judge model routed through the gateway.
func newEvalHandlers(h *api.Handler) (judge, regression, benchmark http.HandlerFunc) {
	return newEvalHandlersWith(&gatewayJudge{h: h, model: os.Getenv("AEROLLM_EVAL_JUDGE_MODEL")}, eval.NewInMemoryScoreStore())
}

func newEvalHandlersWith(client eval.JudgeClient, store eval.ScoreStore) (judge, regression, benchmark http.HandlerFunc) {
	pipeline := eval.NewJudgePipeline(client, store, "general")
	detector := eval.NewRegressionDetector(store)
	runner := eval.NewBenchmarkRunner(client, store)

	judge = func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		var req struct {
			Prompt        string `json:"prompt"`
			Response      string `json:"response"`
			Model         string `json:"model"`
			Provider      string `json:"provider"`
			PromptVersion string `json:"prompt_version"`
		}
		if !decodeBody(w, r, &req) {
			return
		}
		if strings.TrimSpace(req.Prompt) == "" || strings.TrimSpace(req.Response) == "" {
			middleware.WriteJSONError(w, http.StatusBadRequest, "prompt and response are required", "")
			return
		}
		score, err := pipeline.ScoreRequest(r.Context(), req.Prompt, req.Response, req.Model, req.Provider, req.PromptVersion)
		if err != nil {
			writeEvalError(w, "scoring", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]float64{"score": score})
	}

	regression = func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		filter := eval.ScoreFilter{}
		q := r.URL.Query()
		filter.Model = q.Get("model")
		filter.Provider = q.Get("provider")
		regressions, err := detector.Detect(r.Context(), filter)
		if err != nil {
			middleware.WriteJSONError(w, http.StatusInternalServerError, "regression detection failed", "")
			return
		}
		if regressions == nil {
			regressions = []eval.Regression{}
		}
		writeJSON(w, http.StatusOK, regressions)
	}

	benchmark = func(w http.ResponseWriter, r *http.Request) {
		if !requirePost(w, r) {
			return
		}
		var req struct {
			Dataset  string `json:"dataset"`
			Model    string `json:"model"`
			Provider string `json:"provider"`
			Rubric   string `json:"rubric"`
		}
		if !decodeBody(w, r, &req) {
			return
		}
		if strings.TrimSpace(req.Dataset) == "" {
			middleware.WriteJSONError(w, http.StatusBadRequest, "dataset (JSONL) is required", "")
			return
		}
		result, err := runner.Run(r.Context(), strings.NewReader(req.Dataset), req.Model, req.Provider, req.Rubric)
		if err != nil {
			writeEvalError(w, "benchmark", err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
	return judge, regression, benchmark
}
