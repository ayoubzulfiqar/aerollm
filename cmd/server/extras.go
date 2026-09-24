package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/batch"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/meter"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/tools"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

// newApprovalStore returns the human-in-the-loop approval store: Redis when
// available (multi-instance), otherwise the durable local store, otherwise
// memory.
func newApprovalStore(a *app) (agent.ApprovalStore, error) {
	switch {
	case a.redis != nil:
		return agent.NewRedisApprovalStore(a.redis, "approval:", 24*time.Hour), nil
	case a.persist != nil:
		return agent.NewPersistentApprovalStore(a.persist, 24*time.Hour)
	default:
		return agent.NewMemoryApprovalStore(24 * time.Hour), nil
	}
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
func (a *app) mountBatches(mux *http.ServeMux, client func(http.Handler, ...middleware.Middleware) http.Handler) error {
	workDir := os.Getenv("AEROLLM_BATCH_DIR")
	var store batch.BatchStore = batch.NewInMemoryStore()
	durable := false
	if a.persist != nil {
		ps, err := batch.NewPersistentStore(a.persist)
		if ps == nil {
			return fmt.Errorf("batch store: %w", err)
		}
		if err != nil {
			a.logger.Warn("some persisted batches were unreadable", "error", err)
		}
		store, durable = ps, true
		if workDir == "" {
			workDir = filepath.Join(stateDir(), "batches")
		}
	}
	if workDir == "" {
		workDir = os.TempDir()
	}
	proc := batch.NewBatchProcessor(store, a.batchResolver, batch.BatchProcessorConfig{
		WorkDir:           workDir,
		PersistentWorkDir: durable,
		SuspendOnShutdown: durable,
		Concurrency:       4,
		Context:           a.ctx,
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
			if hash, ok := a.virtualByID.Load(res.Owner); ok && cost > 0 {
				if _, err := a.keyManager.RecordSpendByHash(ctx, hash.(string), cost); err != nil {
					a.logger.Error("batch virtual key spend failed", "batch", res.BatchID, "error", err)
				}
			}
			a.analytics.RecordFromUsage(res.BatchID+":"+res.CustomID, res.Owner, "", "", res.Model, res.Provider, res.Usage, cost)
			a.meter.Record(meter.UsageRecord{Timestamp: time.Now().UTC(), APIKey: res.Owner, Provider: res.Provider, Model: res.Model,
				TokensIn: int64(res.Usage.PromptTokens), TokensOut: int64(res.Usage.CompletionTokens), LatencyMs: float64(res.Latency.Microseconds()) / 1000})
		},
	})
	a.onClose(func(ctx context.Context) { _ = proc.Shutdown(ctx) })
	if durable {
		report, err := proc.Recover(a.ctx)
		if err != nil {
			a.logger.Error("batch recovery failed", "error", err)
		} else {
			a.logger.Info("batch recovery", "report", fmt.Sprintf("%+v", report))
		}
	}
	bh := api.NewBatchHandler(proc, store)
	// Remember which virtual key owns each key ID so asynchronous batch spend
	// is also charged to the virtual key's own budget.
	track := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if p, ok := middleware.PrincipalFromContext(r.Context()); ok && p.Virtual != nil {
				a.virtualByID.Store(p.KeyID, p.Virtual.KeyHash)
			}
			next.ServeHTTP(w, r)
		})
	}
	mux.Handle("/v1/batches", client(bh, track))
	mux.Handle("/v1/batches/", client(bh, track))
	return nil
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

// loadWasmTools registers the configured WebAssembly modules as server-side
// tools and request/response hooks. Modules run in the wasmrt sandbox (no
// filesystem, network or host environment; memory, time and output caps).
func (a *app) loadWasmTools() error {
	pc := a.cfg.Plugins
	if len(pc.Tools) == 0 && len(pc.Hooks) == 0 {
		return nil
	}
	rtCfg := wasmrt.Config{
		Timeout:       pc.Timeout,
		MaxConcurrent: pc.MaxConcurrent,
		AllowClock:    pc.AllowClock,
		AllowRandom:   pc.AllowRandom,
	}
	if pc.MemoryMiB > 0 {
		rtCfg.MaxMemoryPages = uint32(pc.MemoryMiB * 16) // 16 x 64 KiB pages per MiB
	}
	rt, err := wasmrt.New(rtCfg)
	if err != nil {
		return fmt.Errorf("wasm runtime: %w", err)
	}
	host, err := plugins.NewWasmHostWithOptions(nil, plugins.WasmHostOptions{Runtime: rt, Timeout: pc.Timeout})
	if err != nil {
		_ = rt.Close()
		return fmt.Errorf("wasm plugin host: %w", err)
	}
	a.onClose(func(ctx context.Context) {
		_ = host.Close(ctx)
		_ = rt.Close()
	})
	for _, t := range pc.Tools {
		if err := host.LoadPluginFile(a.ctx, t.Name, pc.Dir, t.File); err != nil {
			return fmt.Errorf("plugin tool %q: %w", t.Name, err)
		}
		tool, err := host.Tool(t.Name, t.Name, t.Description, t.Parameters)
		if err != nil {
			return fmt.Errorf("plugin tool %q: %w", t.Name, err)
		}
		if err := a.registry.Register(tool); err != nil {
			return fmt.Errorf("plugin tool %q: %w", t.Name, err)
		}
		a.logger.Info("wasm tool registered", "tool", t.Name)
	}
	if len(pc.Hooks) == 0 {
		return nil
	}
	// Hooks live in their own host so tool modules never receive hook calls.
	hooks, err := plugins.NewWasmHostWithOptions(nil, plugins.WasmHostOptions{Runtime: rt, Timeout: pc.Timeout})
	if err != nil {
		return fmt.Errorf("wasm hook host: %w", err)
	}
	a.onClose(func(ctx context.Context) { _ = hooks.Close(ctx) })
	ids := make([]string, 0, len(pc.Hooks))
	for _, hk := range pc.Hooks {
		if err := hooks.LoadPluginFile(a.ctx, hk.ID, pc.Dir, hk.File); err != nil {
			return fmt.Errorf("plugin hook %q: %w", hk.ID, err)
		}
		ids = append(ids, hk.ID)
		a.logger.Info("wasm hook registered", "hook", hk.ID)
	}
	a.hookRunner = &wasmHooks{host: hooks, ids: ids}
	return nil
}

// wasmHooks runs configured hook plugins in order over JSON payloads.
type wasmHooks struct {
	host *plugins.WasmHost
	ids  []string
}

// run passes v (as a JSON object) through every hook and decodes the final
// payload back into v.
func (w *wasmHooks) run(ctx context.Context, hook plugins.Hook, v interface{}) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	for _, id := range w.ids {
		out, err := w.host.RunHook(ctx, id, hook, payload)
		if errors.Is(err, plugins.ErrPluginFailed) {
			return fmt.Errorf("%w: %s: %v", api.ErrHookRejected, id, err)
		}
		if err != nil {
			return err
		}
		if out != nil {
			payload = out
		}
	}
	raw, err = json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

func (w *wasmHooks) onRequest(ctx context.Context, req *models.LLMRequest) error {
	next := &models.LLMRequest{}
	if err := func() error {
		raw, err := json.Marshal(req)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, next); err != nil {
			return err
		}
		return w.run(ctx, plugins.HookOnRequest, next)
	}(); err != nil {
		return err
	}
	*req = *next
	return nil
}

func (w *wasmHooks) onResponse(ctx context.Context, _ *models.LLMRequest, resp *models.LLMResponse) error {
	return w.run(ctx, plugins.HookOnResponse, resp)
}
