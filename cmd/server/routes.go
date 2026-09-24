package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/ayoubzulfiqar/aerollm/internal/admission"
	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/autoscale"
	"github.com/ayoubzulfiqar/aerollm/internal/backpressure"
	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/chaos"
	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
	"github.com/ayoubzulfiqar/aerollm/internal/contextmgr"
	"github.com/ayoubzulfiqar/aerollm/internal/flags"
	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/genui"
	"github.com/ayoubzulfiqar/aerollm/internal/graphrag"
	"github.com/ayoubzulfiqar/aerollm/internal/guardrails"
	"github.com/ayoubzulfiqar/aerollm/internal/incident"
	"github.com/ayoubzulfiqar/aerollm/internal/licensing"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/mcp"
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/notification"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/rag"
	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
	"github.com/ayoubzulfiqar/aerollm/internal/region"
	"github.com/ayoubzulfiqar/aerollm/internal/retention"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/internal/rsi"
	"github.com/ayoubzulfiqar/aerollm/internal/schedule"
	"github.com/ayoubzulfiqar/aerollm/internal/secrets"
	"github.com/ayoubzulfiqar/aerollm/internal/slo"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/ayoubzulfiqar/aerollm/internal/state"
	"github.com/ayoubzulfiqar/aerollm/internal/studio"
	"github.com/ayoubzulfiqar/aerollm/internal/swarm"
	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
	"github.com/ayoubzulfiqar/aerollm/internal/tools"
	"github.com/ayoubzulfiqar/aerollm/internal/traffic"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// adapt converts a func(http.HandlerFunc) http.HandlerFunc middleware.
func adapt(mw func(http.HandlerFunc) http.HandlerFunc) middleware.Middleware {
	return func(next http.Handler) http.Handler { return mw(next.ServeHTTP) }
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			middleware.WriteJSONError(w, http.StatusRequestEntityTooLarge, "request body too large", "")
		} else {
			middleware.WriteJSONError(w, http.StatusBadRequest, "invalid JSON body", "")
		}
		return false
	}
	return true
}

func envBool(key string) bool {
	v, _ := strconv.ParseBool(os.Getenv(key))
	return v
}

// routes builds the HTTP handler. Endpoints fall into three tiers:
//   - public: health, readiness, metrics (configurable) and API docs;
//   - client: inference APIs, any valid admin, static or virtual key;
//   - admin: key management, configuration and the whole control plane.
func (a *app) routes() (http.Handler, error) {
	cfg := a.cfg
	h := a.gateway
	mux := http.NewServeMux()

	bp := backpressure.NewBackpressureController(backpressure.Config{MaxInflight: 1000, Window: time.Minute})
	// SLO tracking is observational: it measures availability and burn rate
	// but never rejects traffic (an exhausted error budget must not turn a
	// brief upstream outage into a gateway-wide outage).
	sloTracker, err := slo.NewTracker(0.995, 24*time.Hour)
	if err != nil {
		return nil, err
	}
	chaosInj := chaos.NewInjector(chaos.Config{Enabled: envBool("AEROLLM_CHAOS_ENABLED")})

	client := func(hd http.Handler, extra ...middleware.Middleware) http.Handler {
		mws := []middleware.Middleware{
			middleware.BodyLimit(cfg.Server.MaxBodyBytes),
			a.auth.RequireKey(),
			middleware.RateLimit(middleware.RateLimitOptions{Limiter: h.RateLimiter, DefaultTPM: cfg.RateLimit.DefaultTPM}),
			bp.Middleware,
			sloTracker.Middleware,
			chaosInj.Middleware,
		}
		return middleware.Chain(hd, append(mws, extra...)...)
	}
	admin := func(hd http.Handler) http.Handler {
		return middleware.Chain(hd, middleware.BodyLimit(cfg.Server.MaxBodyBytes), a.auth.RequireAdmin())
	}
	adminFunc := func(f http.HandlerFunc) http.Handler { return admin(f) }
	// both mounts pattern and pattern/ so /{id} subpaths reach the handler.
	both := func(pattern string, hd http.Handler) {
		mux.Handle(pattern, hd)
		mux.Handle(pattern+"/", hd)
	}

	// ---- Public -------------------------------------------------------
	mux.HandleFunc("/health", h.HealthCheck)
	mux.HandleFunc("/healthz", h.HealthCheck)
	mux.Handle("/ready", a.readiness)
	mux.Handle("/readyz", a.readiness)
	metrics := http.Handler(telemetry.MetricsHandler())
	if !cfg.Security.PublicMetrics {
		metrics = admin(metrics)
	}
	mux.Handle("/metrics", metrics)
	mux.Handle("/swagger/", httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")))

	// ---- Inference (client keys) ---------------------------------------
	h.TrimMessages = func(model string, msgs []models.Message) []models.Message {
		return contextmgr.TrimToFit(msgs, model, 1024)
	}

	scoper := guardrails.NewAPIKeyScoper()
	vectorStore := rag.NewInMemoryVectorStore()
	keywordIndex := rag.NewInMemoryKeywordIndex()
	graphStore := graphrag.NewBboltGraphStore()
	if a.persist != nil {
		for name, enable := range map[string]func(persist.Store) error{
			"rag vectors":  vectorStore.EnablePersistence,
			"rag keywords": keywordIndex.EnablePersistence,
			"graph":        graphStore.EnablePersistence,
		} {
			if err := enable(a.persist); err != nil {
				if !errors.Is(err, rag.ErrPartialLoad) && !errors.Is(err, graphrag.ErrPartialLoad) {
					return nil, fmt.Errorf("%s persistence: %w", name, err)
				}
				a.logger.Warn("partially restored "+name, "error", err)
			}
		}
	}
	chatGuards := []middleware.Middleware{
		adapt(guardrails.APIKeyScopingMiddleware(scoper)),
		adapt(guardrails.PIIMiddleware),
		adapt(guardrails.InjectionShieldMiddleware),
		adapt(rag.RAGHTTPMiddleware(rag.NewHybridRetriever(vectorStore, keywordIndex))),
		func(next http.Handler) http.Handler {
			return graphrag.NewGraphRAGMiddleware(graphStore).Middleware(next)
		},
		adapt(genui.NewGenUIHandler),
	}
	mux.Handle("/v1/chat/completions", client(http.HandlerFunc(h.ChatCompletions), chatGuards...))
	mux.Handle("/chat/completions", client(http.HandlerFunc(h.ChatCompletions), chatGuards...))
	mux.Handle("/v1/messages", client(http.HandlerFunc(h.Messages),
		adapt(guardrails.APIKeyScopingMiddleware(scoper))))
	both("/v1/models", client(http.HandlerFunc(h.Models)))
	mux.Handle("/v1/embeddings", client(http.HandlerFunc(h.Embeddings)))
	mux.Handle("/v1/images/generations", client(http.HandlerFunc(h.ImageGenerations)))
	mux.Handle("/v1/audio/transcriptions", client(http.HandlerFunc(h.AudioTranscriptions)))
	mux.Handle("/v1/responses", client(http.HandlerFunc(h.Responses)))
	mux.Handle("/v1/spatial/parse", client(spatial.ParseHandler()))
	spatialStream := spatial.NewVideo3DStreamHandler()
	spatialStream.MaxDuration = 5 * time.Minute
	mux.Handle("/v1/spatial/stream", client(spatialStream))

	feedback := flywheel.NewFeedbackExporter(a.ledger)
	mux.Handle("/v1/feedback", client(http.HandlerFunc(feedback.FeedbackHandler)))

	// MCP exposes the server tool registry; tools execute server-side, so
	// it requires a valid key.
	var mcpOpts []mcp.Option
	if envBool("AEROLLM_MCP_STATELESS") {
		// Sessions live in one process; multi-replica deployments without
		// sticky routing must run stateless.
		mcpOpts = append(mcpOpts, mcp.WithoutSessions())
	}
	mux.Handle("/mcp", client(mcp.NewServerWithRegistry(a.registry, mcpOpts...)))

	hub := realtime.NewHub()
	a.onClose(func(context.Context) { hub.CancelAll() })
	rtProvider := realtime.NewStreamingProvider("gateway", func(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
		ch, _, err := h.OpenStream(ctx, req)
		return ch, err
	})
	mux.Handle("/ws", middleware.Chain(realtime.ServeWS(hub, rtProvider), a.auth.RequireKey()))

	// ---- Admin: keys, config, spend, cache ------------------------------
	kh := a.keyHandler
	for path, fn := range map[string]http.HandlerFunc{
		"/key/generate":   kh.GenerateKeys,
		"/key/update":     kh.UpdateKey,
		"/key/delete":     kh.DeleteKey,
		"/key/list":       kh.ListKeys,
		"/key/block":      kh.BlockKey,
		"/key/unblock":    kh.UnblockKey,
		"/key/regenerate": kh.RegenerateKey,
		"/user/new":       kh.CreateUser,
		"/team/new":       kh.TeamCreate,
		"/team/create":    kh.TeamCreate,
		"/team/update":    kh.TeamUpdate,
	} {
		mux.Handle(path, adminFunc(fn))
	}
	// Self-service lookups: the key handler scopes non-admin callers to
	// their own key and user.
	selfService := func(f http.HandlerFunc) http.Handler {
		return middleware.Chain(f, middleware.BodyLimit(cfg.Server.MaxBodyBytes), a.auth.RequireKey())
	}
	mux.Handle("/key/info", selfService(kh.InfoKey))
	mux.Handle("/user/info", selfService(kh.UserInfo))
	mux.Handle("/team/info", selfService(kh.TeamInfo))

	configHandler := api.NewConfigHandler(a.reloader, a.logger.Func())
	mux.Handle("/model/info", adminFunc(configHandler.ModelInfo))
	mux.Handle("/config/yaml", adminFunc(configHandler.ConfigYaml))
	mux.Handle("/config/update", adminFunc(configHandler.ConfigUpdate))

	spend := api.NewSpendHandler(a.analytics, a.logger.Func())
	mux.Handle("/global/spend/report", adminFunc(spend.SpendReport))
	mux.Handle("/global/spend/logs", adminFunc(spend.SpendLogs))
	mux.Handle("/v1/budgets", admin(api.NewBudgetHandler(a.costTracker, a.budgetPeriod)))

	cacheHandler := api.NewCacheHandler(h.Cache, h.SemanticCache)
	mux.Handle("/v1/cache/stats", adminFunc(cacheHandler.Stats))
	mux.Handle("/v1/cache", adminFunc(cacheHandler.Clear))
	mux.Handle("/v1/cache/inspect", adminFunc(cacheHandler.Inspect))

	if err := a.mountBatches(mux, client); err != nil {
		return nil, err
	}

	mux.Handle("/v1/rag/documents", admin(rag.NewDocumentsHandler(vectorStore, keywordIndex)))
	mux.Handle("/v1/graphrag/graph", admin(graphrag.NewGraphHandler(graphStore, nil)))

	// ---- Admin: human-in-the-loop approvals & traffic tools -------------
	advancedStore, err := newApprovalStore(a)
	if err != nil {
		return nil, fmt.Errorf("approvals: %w", err)
	}
	advanced := newAdvancedAgent(a, advancedStore)
	h.Advanced = advanced
	mux.Handle("/v1/agents/approvals/", adminFunc(h.ResumeApproval))

	shadow, err := traffic.NewShadowTesterWithConfig(traffic.ShadowTesterConfig{
		TargetURL: os.Getenv("AEROLLM_SHADOW_URL"),
		APIKey:    os.Getenv("AEROLLM_SHADOW_API_KEY"),
	})
	if err != nil {
		return nil, fmt.Errorf("shadow tester: %w", err)
	}
	a.onClose(func(context.Context) { shadow.Wait() })
	mux.Handle("/v1/shadow/results", admin(shadow.ResultsHandler()))
	mux.Handle("/v1/shadow", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		if os.Getenv("AEROLLM_SHADOW_URL") == "" {
			middleware.WriteJSONError(w, http.StatusNotImplemented, "shadow target not configured (AEROLLM_SHADOW_URL)", "")
			return
		}
		var req models.LLMRequest
		if !decodeBody(w, r, &req) {
			return
		}
		if err := shadow.RunAsync(r.Context(), "", "", &req); err != nil {
			middleware.WriteJSONError(w, http.StatusBadGateway, "shadow dispatch failed", "")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"shadow": "accepted"})
	}))

	// ---- Admin: RSI -----------------------------------------------------
	if err := a.mountRSI(mux, adminFunc); err != nil {
		return nil, err
	}

	// ---- Admin: control plane -------------------------------------------
	var secretStore *secrets.Store
	if a.persist != nil && os.Getenv(secrets.EnvKey) != "" {
		if secretStore, err = secrets.NewPersistentStore(a.persist); err != nil {
			return nil, fmt.Errorf("secrets: %w", err)
		}
	} else {
		secretStore = secrets.NewStore()
		if err := secretStore.KeyError(); err != nil {
			return nil, fmt.Errorf("secrets: %w", err)
		}
		a.logger.Warn(secrets.EnvKey + " not set: secrets are encrypted with an ephemeral key and not persisted")
	}
	both("/v1/secrets", admin(secrets.WebhookHandler(secretStore)))
	policyStore := compliance.NewHTTPPolicyStore()
	auditLog := compliance.NewMemoryAuditLogger()
	quotaStore := tenant.NewInMemoryQuotaStore()
	if a.persist != nil {
		if policyStore, err = compliance.NewPersistentHTTPPolicyStore(a.persist); err != nil {
			return nil, fmt.Errorf("policy store: %w", err)
		}
		if auditLog, err = compliance.NewPersistentAuditLogger(a.persist, compliance.DefaultAuditCapacity); err != nil {
			return nil, fmt.Errorf("audit log: %w", err)
		}
		if quotaStore, err = tenant.NewPersistentQuotaStore(a.persist); err != nil {
			return nil, fmt.Errorf("quota store: %w", err)
		}
	}
	mux.Handle("/v1/policy", admin(compliance.HTTPPolicyHandler(policyStore)))
	mux.Handle("/v1/policy/block", admin(compliance.HTTPBlockHandler(policyStore)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))))
	flagStore, err := openStore(a, "flags", flags.NewStore, flags.NewStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	both("/v1/flags", admin(flags.WebhookHandler(flagStore)))

	retentionStore, err := openStore(a, "retention", retention.NewRetentionStore, retention.NewRetentionStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	both("/v1/retention", admin(retention.WebhookHandler(retentionStore)))

	notificationStore, err := openStore(a, "notification", notification.NewStore, notification.NewStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	both("/v1/notification/channels", admin(notification.WebhookHandler(notificationStore)))
	both("/v1/notification/subscriptions", admin(notification.WebhookHandler(notificationStore)))
	emailSender, smsSender, err := notification.SendersFromEnv()
	if err != nil {
		return nil, fmt.Errorf("notification senders: %w", err)
	}
	notifier := notification.NewDispatcher(notificationStore, notification.DispatcherOptions{EmailSender: emailSender, SMSSender: smsSender})
	mux.Handle(notification.SendPath, admin(notification.SendHandler(notifier)))

	incidentStore, err := openStore(a, "incident", incident.NewStore, incident.NewStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	incidentStore.SetHook(func(ev incident.Event) {
		inc := ev.Incident
		msg := notification.Message{
			AlertID:   "incident." + ev.Type,
			Title:     fmt.Sprintf("Incident %s: %s", ev.Type, inc.Title),
			Text:      fmt.Sprintf("Incident %s is now %s", inc.ID, inc.Status),
			Severity:  string(inc.Severity),
			Timestamp: time.Now().UTC(),
		}
		a.goWorker("incident-notify", func(ctx context.Context) {
			sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := notifier.Notify(sendCtx, msg.AlertID, msg); err != nil && !errors.Is(err, notification.ErrChannelNotImplemented) {
				a.logger.Error("incident notification failed", "incident", inc.ID, "error", err)
			}
		})
	})
	both("/v1/incidents", admin(incident.WebhookHandler(incidentStore)))

	scheduleStore, err := openStore(a, "schedule", schedule.NewStore, schedule.NewStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	both("/v1/schedule", admin(schedule.WebhookHandler(scheduleStore)))
	if err := a.startScheduleRunner(scheduleStore); err != nil {
		return nil, err
	}
	regionStore, err := openStore(a, "region", region.NewStore, region.NewStoreWithPersistence)
	if err != nil {
		return nil, err
	}
	both("/v1/region/regions", admin(region.WebhookHandler(regionStore)))
	both("/v1/region/residency", admin(region.WebhookHandler(regionStore)))
	both("/v1/region/routes", admin(region.WebhookHandler(regionStore)))

	mux.Handle("/v1/chaos/fault", admin(chaos.Handler(chaosInj)))
	mux.Handle("/v1/trace/metrics", admin(a.trace.MetricsHandler()))
	mux.Handle("/v1/trace/spans", admin(a.trace.SpansHandler()))
	mux.Handle("/v1/slo/budget", admin(sloTracker.Handler("availability")))
	mux.Handle("/backpressure/status", admin(bp.Handler()))
	mux.Handle("/resilience/status", adminFunc(a.resilienceStatus))
	mux.Handle("/v1/admission/validate", admin(admission.WebhookHandler(admission.ValidatorFunc(func(req admission.AdmissionRequest) admission.AdmissionResponse {
		return admission.AdmissionResponse{Allowed: true, Reason: "no admission policy configured"}
	}))))

	mux.Handle("/v1/audit/events", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		writeJSON(w, http.StatusOK, auditLog.Events())
	}))

	mux.Handle("/v1/quota", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		var q tenant.Quota
		if !decodeBody(w, r, &q) {
			return
		}
		res, err := quotaStore.Enforce(r.Context(), &q, 0)
		if errors.Is(err, tenant.ErrPersistence) {
			middleware.WriteJSONError(w, http.StatusServiceUnavailable, "quota store unavailable", "")
			return
		}
		if err != nil {
			middleware.WriteJSONError(w, http.StatusBadRequest, err.Error(), "")
			return
		}
		writeJSON(w, http.StatusOK, res)
	}))

	// Metered usage (keys are redacted by the meter handler).
	mux.Handle("/v1/meter/usage", admin(a.meter.Handler()))
	mux.Handle("/v1/billing/preview", adminFunc(a.billingPreview))

	evalJudge, evalRegression, evalBenchmark := newEvalHandlers(h)
	mux.Handle("/v1/eval/judge", admin(evalJudge))
	mux.Handle("/v1/eval/regression", admin(evalRegression))
	mux.Handle("/v1/eval/benchmark", admin(evalBenchmark))

	clientPlain := func(hd http.Handler) http.Handler { return client(hd) }
	if err := a.mountFederated(mux, admin, clientPlain); err != nil {
		return nil, err
	}
	if err := a.configureAutoscaleBootstrap(); err != nil {
		return nil, err
	}
	if err := a.mountFineTuning(mux, adminFunc, feedback); err != nil {
		return nil, err
	}
	if err := a.startAIOps(mux, adminFunc, h.RateLimiter); err != nil {
		return nil, err
	}
	if err := a.mountRedTeam(mux, adminFunc); err != nil {
		return nil, err
	}

	infra := autoscale.NewServerMetaAgentLoop()
	mux.Handle("/v1/autoscale/evaluate", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		var req struct {
			Deficit float64 `json:"deficit"`
		}
		if !decodeBody(w, r, &req) {
			return
		}
		node, err := infra.Evaluate(r.Context(), req.Deficit)
		switch {
		case errors.Is(err, autoscale.ErrInvalidDeficit):
			middleware.WriteJSONError(w, http.StatusBadRequest, err.Error(), "")
		case errors.Is(err, autoscale.ErrCooldown):
			middleware.WriteJSONError(w, http.StatusTooManyRequests, err.Error(), "")
		case errors.Is(err, autoscale.ErrMaxNodes):
			middleware.WriteJSONError(w, http.StatusConflict, err.Error(), "")
		case err != nil:
			middleware.WriteJSONError(w, http.StatusInternalServerError, "provisioning failed", "")
		default:
			writeJSON(w, http.StatusOK, map[string]interface{}{"node": node, "deficit": req.Deficit})
		}
	}))

	mux.Handle("/v1/pqc/keys", admin(pqc.HandshakeHandler(pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519))))

	if err := a.mountStudioAndMarketplace(mux, admin, client); err != nil {
		return nil, err
	}
	a.startMesh()

	// Global middleware: panic recovery, request IDs, access logs, security
	// headers, CORS, in-flight gauge and request tracing.
	return middleware.Chain(mux,
		middleware.Recover(a.logger),
		middleware.RequestID(),
		middleware.AccessLog(a.logger),
		middleware.SecurityHeaders(),
		middleware.CORS(cfg.Security.CORSAllowedOrigins),
		middleware.Inflight(),
		a.trace.TraceMiddleware,
	), nil
}

// resilienceStatus summarises upstream circuit breaker state.
func (a *app) resilienceStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		return
	}
	provs := a.gateway.HealthProviders()
	open := 0
	for _, p := range provs {
		if p.CircuitOpen {
			open++
		}
	}
	state, status := "ok", http.StatusOK
	if len(provs) > 0 && open == len(provs) {
		state, status = "degraded", http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]interface{}{"state": state, "providers": provs, "open_circuits": open})
}

// billingPreview aggregates metered usage into invoice line items per key
// without contacting any billing provider.
func (a *app) billingPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		return
	}
	keyID := r.URL.Query().Get("key_id")
	var entries []billing.MeterEntry
	for _, rec := range a.meter.Records() {
		if keyID != "" && rec.APIKey != keyID {
			continue
		}
		tokens := float64(rec.TokensIn + rec.TokensOut)
		if tokens <= 0 || rec.APIKey == "" {
			continue
		}
		entries = append(entries, billing.MeterEntry{CustomerID: rec.APIKey, EventName: "token", Value: tokens, Timestamp: rec.Timestamp})
	}
	items := billing.NewAggregator().Aggregate(r.Context(), entries)
	writeJSON(w, http.StatusOK, map[string]interface{}{"line_items": items, "entries": len(entries)})
}

// mountRSI wires the recursive self-improvement orchestrator. Policies are
// evaluated and reported, but live deployment is opt-in
// (AEROLLM_RSI_DEPLOY=true); otherwise it runs in dry-run mode.
func (a *app) mountRSI(mux *http.ServeMux, adminFunc func(http.HandlerFunc) http.Handler) error {
	rsiCfg := rsi.DefaultRSIConfig()
	deploy := envBool("AEROLLM_RSI_DEPLOY")
	rsiCfg.DryRun = !deploy
	orch := rsi.NewRSIOrchestrator(a.ledger, a.trace, a.costTracker, &routerProviderLister{r: a.router}, rsiCfg)
	if a.persist != nil {
		if err := orch.EnablePersistence(a.persist); err != nil {
			if !errors.Is(err, rsi.ErrPartialRestore) {
				return fmt.Errorf("rsi persistence: %w", err)
			}
			a.logger.Warn("rsi state partially restored", "error", err)
		}
	}
	if deploy {
		orch.SetHooks(
			func(ctx context.Context, policy rsi.Policy, cycle rsi.RSICycle) error {
				a.logger.Info("RSI policy deployed", "cycle", cycle.ID, "policy", fmt.Sprintf("%T", policy), "improvement_pct", cycle.ImprovementPct)
				return nil
			},
			func(ctx context.Context, policy rsi.Policy, cycle rsi.RSICycle) error {
				a.logger.Info("RSI policy rolled back", "cycle", cycle.ID)
				return nil
			},
			nil,
		)
	}
	a.goWorker("rsi", orch.Run)
	rh := api.NewRSIHandler(orch)
	mux.Handle("/v1/rsi/headroom", adminFunc(rh.Headroom()))
	mux.Handle("/v1/rsi/cycles", adminFunc(rh.Cycles()))
	mux.Handle("/v1/rsi/cycle", adminFunc(rh.TriggerCycle()))
	mux.Handle("/v1/rsi/current", adminFunc(rh.CurrentCycle()))
	mux.Handle("/v1/rsi/config", adminFunc(rh.Config()))
	mux.Handle("/v1/rsi/stats", adminFunc(rh.Stats()))
	mux.Handle("/v1/rsi/rollback", adminFunc(rh.Rollback()))
	return nil
}

// mountStudioAndMarketplace wires the studio (licensed) and plugin registry.
func (a *app) mountStudioAndMarketplace(mux *http.ServeMux, admin func(http.Handler) http.Handler, client func(http.Handler, ...middleware.Middleware) http.Handler) error {
	dir := stateDir()
	stateStore, err := state.OpenBboltStateStore(dir)
	if err != nil {
		a.logger.Warn("state store unavailable; swarm features disabled", "dir", dir, "error", err)
	} else {
		a.onClose(func(context.Context) { _ = stateStore.Close() })
	}

	marketClient := marketplace.NewClient(os.Getenv("AEROLLM_MARKETPLACE_URL"))
	var registryStore marketplace.Store = marketplace.NewInMemoryStore()
	if a.redis != nil {
		registryStore = marketplace.NewRedisStore(marketplace.RedisOptions{Client: a.redis})
	}
	registryService := marketplace.NewRegistryService(marketClient, registryStore)
	if spec := os.Getenv("AEROLLM_MARKETPLACE_TRUSTED_KEYS"); spec != "" {
		trust, err := marketplace.ParseTrustedKeys(spec)
		if err != nil {
			return fmt.Errorf("AEROLLM_MARKETPLACE_TRUSTED_KEYS: %w", err)
		}
		registryService.SetTrustStore(trust)
	}
	mux.Handle("/v1/marketplace/plugins", admin(registryService.PluginsHandler()))
	mux.Handle("/v1/marketplace/plugins/", admin(registryService.PluginByIDHandler()))
	registryService.SetReceiptSink(newReceiptSink(a.persist))
	mux.Handle("/v1/marketplace/openstandard/capability", client(registryService.CapabilityManifestHandler()))
	mux.Handle("/v1/marketplace/openstandard/receipt", client(registryService.BillingReceiptHandler()))

	royaltyCfg := webhooks.BudgetWebhookConfig{URL: os.Getenv("AEROLLM_ROYALTY_WEBHOOK_URL"), Timeout: 2 * time.Second}
	royalties := marketplace.NewRoyaltyRecorder(a.webhooks, royaltyCfg)

	var swarmOrch *swarm.SwarmOrchestrator
	if stateStore != nil {
		swarmOrch = swarm.NewSwarmOrchestrator(stateStore, a.registry)
		swarmOrch.Provider = tools.NewRouterAdapter(a.router)
	}
	studioHandler := studio.NewHandler(a.router, swarmOrch, a.pricing, a.ledger, royalties)
	dagHandler := studio.NewDAGHandler(studio.NewInMemoryDAGStore())
	lic := licensing.NewEnvLicenseChecker()
	if err := lic.Err(); err != nil {
		a.logger.Info("no valid enterprise license; studio endpoints disabled (community mode)", "reason", err.Error())
	}
	licensed := func(hd http.Handler) http.Handler {
		return admin(licensing.Middleware(lic, licensing.FeatureAdvancedCRDTMesh)(hd))
	}
	mux.Handle("/v1/studio/topology", licensed(http.HandlerFunc(studioHandler.Topology)))
	mux.Handle("/v1/studio/analytics/cost", licensed(http.HandlerFunc(studioHandler.AnalyticsCost)))
	mux.Handle("/v1/studio/dags", licensed(http.HandlerFunc(dagHandler.ServeDAGs)))
	return nil
}

// startMesh starts peer sync when AEROLLM_MESH_ENABLED=true. The transport
// is in-process only (see internal/mesh).
func (a *app) startMesh() {
	if !envBool("AEROLLM_MESH_ENABLED") {
		return
	}
	meshCfg := mesh.DefaultMeshConfig()
	meshCfg.Enabled = true
	if v := os.Getenv("AEROLLM_MESH_BIND"); v != "" {
		meshCfg.BindAddress = v
	}
	meshCfg.LocalPeerID = mesh.PeerID(os.Getenv("AEROLLM_MESH_NODE_ID"))
	if meshCfg.LocalPeerID == "" {
		meshCfg.LocalPeerID = mesh.PeerID(randomToken("node-", 6))
	}
	meshCfg.PeerAddresses = mesh.ParsePeerAddresses(os.Getenv("AEROLLM_MESH_PEERS"))
	// Real peers talk over mutual TLS (AEROLLM_MESH_TLS_CERT/KEY/CA or
	// AEROLLM_MESH_TLS_PINS); without it the mesh is in-process only.
	var transport mesh.SecureTransport
	tlsTr, ok, err := mesh.NewTLSTransportFromEnv(mesh.PeerID(os.Getenv("AEROLLM_MESH_NODE_ID")), os.Getenv)
	switch {
	case err != nil:
		a.logger.Error("mesh TLS transport unavailable; mesh disabled", "error", err)
		return
	case ok:
		transport = tlsTr
		meshCfg.LocalPeerID = tlsTr.LocalID()
	default:
		a.logger.Warn("mesh running without TLS: in-process transport only (set AEROLLM_MESH_TLS_* to join remote peers)")
		transport = mesh.NewInMemoryTransport(meshCfg.LocalPeerID)
	}
	dcfg := mesh.DiscoveryConfig{
		LocalID:     meshCfg.LocalPeerID,
		BindAddress: meshCfg.BindAddress,
		Peers:       meshCfg.PeerDescriptors(),
		Transport:   transport,
	}
	if adv := os.Getenv("AEROLLM_MESH_ADVERTISE"); adv != "" {
		dcfg.Advertise = mesh.PeerDescriptor{ID: meshCfg.LocalPeerID, Address: adv}
	}
	discovery := mesh.NewDiscovery(dcfg)
	worker := mesh.NewSyncWorker(mesh.SyncWorkerConfig{
		State:     mesh.NewPluginRegistrySync(),
		Discovery: discovery,
		Interval:  meshCfg.GossipInterval,
	})
	a.goWorker("mesh-sync", worker.Start)
	a.onClose(func(context.Context) {
		worker.Stop()
		if c, ok := transport.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	a.logger.Info("mesh sync started", "node", string(meshCfg.LocalPeerID), "peers", len(meshCfg.PeerAddresses))
}

// routerProviderLister exposes router provider names to the RSI engine.
type routerProviderLister struct {
	r *router.Router
}

func (l *routerProviderLister) ProviderNames() []string {
	cbs := l.r.Providers()
	names := make([]string, len(cbs))
	for i, p := range cbs {
		names[i] = p.Name()
	}
	return names
}

// openStore returns a durable store when persistence is enabled, else the
// in-memory one. A nil store from the persistent constructor is fatal; an
// error alongside a store means some saved documents were skipped.
func openStore[T any](a *app, name string, mem func() *T, durable func(persist.Store) (*T, error)) (*T, error) {
	if a.persist == nil {
		return mem(), nil
	}
	s, err := durable(a.persist)
	if s == nil {
		if err == nil {
			err = errors.New("no store returned")
		}
		return nil, fmt.Errorf("%s persistence: %w", name, err)
	}
	if err != nil {
		a.logger.Warn("some persisted "+name+" documents were skipped", "error", err)
	}
	return s, nil
}

// startScheduleRunner executes scheduled webhook tasks (SSRF-safe,
// optionally HMAC-signed via AEROLLM_SCHEDULE_WEBHOOK_SECRET).
func (a *app) startScheduleRunner(store *schedule.Store) error {
	opts, err := schedule.WebhookExecutorOptionsFromEnv(os.Getenv)
	if err != nil {
		return fmt.Errorf("schedule executor: %w", err)
	}
	exec, err := schedule.NewWebhookExecutor(opts)
	if err != nil {
		return fmt.Errorf("schedule executor: %w", err)
	}
	runner, err := schedule.NewRunner(store, exec, schedule.RunnerOptions{})
	if err != nil {
		return fmt.Errorf("schedule runner: %w", err)
	}
	a.goWorker("schedule-runner", func(ctx context.Context) {
		if err := runner.Run(ctx); err != nil {
			a.logger.Error("schedule runner stopped", "error", err)
		}
	})
	return nil
}
