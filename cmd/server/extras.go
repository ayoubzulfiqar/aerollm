package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/batch"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/meter"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/tools"
)

// newApprovalStore returns the human-in-the-loop approval store. Approvals
// need shared durable state, so they are only available with Redis.
func newApprovalStore(a *app) agent.ApprovalStore {
	if a.redis == nil {
		return nil
	}
	return agent.NewRedisApprovalStore(a.redis, "approval:", 24*time.Hour)
}

// newAdvancedAgent builds the HITL-capable agent engine; it calls LLMs
// through the router so resumed conversations can continue.
func newAdvancedAgent(a *app, store agent.ApprovalStore) *agent.AdvancedAgentEngine {
	return agent.NewAdvancedAgentEngine(tools.NewRouterAdapter(a.router), a.registry, store)
}

// mountBatches wires the OpenAI-compatible Batch API (client keys; each
// key sees its own batches, admins see all). Jobs resolve providers per
// request through the gateway, so config hot-reloads apply, and every
// executed line is billed to the batch owner.
func (a *app) mountBatches(mux *http.ServeMux, client func(http.Handler, ...middleware.Middleware) http.Handler) {
	workDir := os.Getenv("AEROLLM_BATCH_DIR")
	if workDir == "" {
		workDir = os.TempDir()
	}
	store := batch.NewInMemoryStore()
	proc := batch.NewBatchProcessor(store, a.batchResolver, batch.BatchProcessorConfig{
		WorkDir:     workDir,
		Concurrency: 4,
		Context:     a.ctx,
		OnResult: func(ctx context.Context, res batch.RequestResult) {
			if res.Err != nil || res.Usage == nil || res.Owner == "" {
				return
			}
			cost := a.costTracker.CalculateCost(res.Model, res.Usage)
			if a.cfg.Finops.Enabled {
				if _, err := a.costTracker.Record(ctx, finops.CostRequest{APIKey: res.Owner, Model: res.Model, Usage: res.Usage, CostUSD: cost, Timestamp: time.Now().UTC()}); err != nil {
					a.logger.Error("batch usage recording failed", "batch", res.BatchID, "error", err)
				}
			}
			a.analytics.RecordFromUsage(res.BatchID+":"+res.CustomID, res.Owner, "", "", res.Model, res.Provider, res.Usage, cost)
			a.meter.Record(meter.UsageRecord{Timestamp: time.Now().UTC(), APIKey: res.Owner, Provider: res.Provider, Model: res.Model,
				TokensIn: int64(res.Usage.PromptTokens), TokensOut: int64(res.Usage.CompletionTokens), LatencyMs: float64(res.Latency.Microseconds()) / 1000})
		},
	})
	a.onClose(func(ctx context.Context) { _ = proc.Shutdown(ctx) })
	bh := api.NewBatchHandler(proc, store)
	mux.Handle("/v1/batches", client(bh))
	mux.Handle("/v1/batches/", client(bh))
}

// batchResolver serves batch lines with the gateway's resolution: configured
// models first, then the router with fallback.
func (a *app) batchResolver(model string) (providers.Provider, bool) {
	if p, ok := a.gateway.ResolveModel(model); ok {
		return p, true
	}
	if len(a.router.Providers()) == 0 {
		return nil, false
	}
	return &gatewayProvider{h: a.gateway}, true
}

// gatewayProvider exposes the gateway's routed completion (fallback, tools)
// as a providers.Provider.
type gatewayProvider struct{ h *api.Handler }

func (g *gatewayProvider) Name() string                 { return "gateway" }
func (g *gatewayProvider) Type() providers.ProviderType { return "gateway" }
func (g *gatewayProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: "gateway", Healthy: true}
}
func (g *gatewayProvider) Close() error { return nil }
func (g *gatewayProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	resp, _, err := g.h.Complete(ctx, req)
	return resp, err
}

// gatewayJudge adapts the gateway to eval.JudgeClient so evaluations are
// scored by a real model (AEROLLM_EVAL_JUDGE_MODEL) through normal routing.
type gatewayJudge struct {
	h     *api.Handler
	model string
}

var errJudgeNotConfigured = errors.New("evaluation judge model not configured (set AEROLLM_EVAL_JUDGE_MODEL)")

func (j *gatewayJudge) ChatCompletion(ctx context.Context, prompt string) (string, error) {
	if j.model == "" {
		return "", errJudgeNotConfigured
	}
	req := chatRequest(j.model, prompt)
	resp, _, err := j.h.Complete(ctx, req)
	if err != nil {
		return "", err
	}
	if resp == nil || len(resp.Choices) == 0 {
		return "", errors.New("judge returned no choices")
	}
	return resp.Choices[0].Message.TextContent(), nil
}
