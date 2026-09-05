package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/aiops"
	"github.com/ayoubzulfiqar/aerollm/internal/autoscale"
	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/genui"
	"github.com/ayoubzulfiqar/aerollm/internal/graphrag"
	"github.com/ayoubzulfiqar/aerollm/internal/guardrails"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/licensing"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/mcp"
	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/redteam"
	"github.com/ayoubzulfiqar/aerollm/internal/realtime"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/ayoubzulfiqar/aerollm/internal/evolution"
	"github.com/ayoubzulfiqar/aerollm/internal/learning"
	"github.com/ayoubzulfiqar/aerollm/internal/swarm"
	"github.com/ayoubzulfiqar/aerollm/internal/rag"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/internal/trace"
	"github.com/ayoubzulfiqar/aerollm/internal/state"
	"github.com/ayoubzulfiqar/aerollm/internal/studio"
	"github.com/ayoubzulfiqar/aerollm/internal/synthesis"
	"github.com/ayoubzulfiqar/aerollm/internal/chaos"
	"github.com/ayoubzulfiqar/aerollm/internal/slo"
	"github.com/ayoubzulfiqar/aerollm/internal/traffic"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/ayoubzulfiqar/aerollm/internal/zk"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

// LoggerAdapter implements the LoggerInterface.
type LoggerAdapter struct{}

func (l *LoggerAdapter) Info(msg string, keysAndValues ...interface{}) {
	fmt.Printf("INFO: %s\n", msg)
}

func (l *LoggerAdapter) Error(msg string, keysAndValues ...interface{}) {
	fmt.Printf("ERROR: %s\n", msg)
}

// NewRedisClient creates a new Redis client.
func NewRedisClient(ctx context.Context, addr string) (cache.RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return client, nil
}

// NewTelemetryProvider creates a new telemetry provider.
func NewTelemetryProvider(ctx context.Context, cfg telemetry.Config) (*telemetry.Provider, error) {
	return telemetry.NewProvider(ctx, cfg)
}

// NewRouter creates a new router.
func NewRouter(cfg router.Config) *router.Router {
	return router.New(cfg)
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter() *ratelimit.TokenBucketLimiter {
	return ratelimit.NewTokenBucketLimiter(100, 10)
}

// NewAdvancedAgent creates a new advanced agent engine with HITL support.
func NewAdvancedAgent(registry *agent.ToolRegistry, redisClient cache.RedisClient) *agent.AdvancedAgentEngine {
	store := agent.NewRedisApprovalStore(redisClient, "approval:", 24*time.Hour)
	return agent.NewAdvancedAgentEngine(nil, registry, store)
}

// NewAgent creates a new base agent engine.
func NewAgent(registry *agent.ToolRegistry) *agent.AgentEngine {
	return agent.NewAgentEngine(nil, registry)
}

// realtimeProvider adapts the router/provider flow for WebSocket streaming.
type realtimeProvider struct{}

func (r *realtimeProvider) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan realtime.StreamChunk, error) {
	_ = ctx
	_ = req
	ch := make(chan realtime.StreamChunk)
	go func() {
		defer close(ch)
		ch <- realtime.StreamChunk{Delta: "ok", Finish: true, Provider: "realtime-adapter"}
	}()
	return ch, nil
}

func (r *realtimeProvider) Name() string { return "realtime-adapter" }

func getenvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	appName := "aerollm"
	appVersion := "v1.0.0"

	logger := &LoggerAdapter{}
	ctx := context.Background()

	fmt.Printf("starting %s %s\n", appName, appVersion)

	tp, err := NewTelemetryProvider(ctx, telemetry.Config{ServiceName: "aerollm"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "telemetry init error: %v\n", err)
		os.Exit(1)
	}
	tp.Start()

	traceProvider := trace.NewProvider(trace.Config{ServiceName: "aerollm"})

	redisClient, err := NewRedisClient(ctx, "localhost:6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis init error: %v\n", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	appCfg, err := config.LoadConfig("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}

	r := NewRouter(router.Config{Strategy: appCfg.Router.Strategy})
	rl := NewRateLimiter()
	registry := agent.NewToolRegistry()
	a := NewAgent(registry)
	cacheInst := cache.NewRedisCache(redisClient, time.Hour)
	handler := api.NewHandler(r, a, cacheInst, rl, tp, logger)

	prices := finops.NewPricingMap()
	costTracker := finops.NewCostTracker(redisClient.(*redis.Client), prices)
	scoper := guardrails.NewAPIKeyScoper()
	scoper.AddScope(guardrails.APIKeyScope{
		APIKey:       getenvOrDefault("AEROLLM_API_KEY", "sk-demo"),
		AllowedModels: []string{"gpt-3.5-turbo", "gpt-4", "claude-3-sonnet"},
		MaxBudgetUSD: 100,
		IPAllowlist:  []string{"127.0.0.1"},
	})

	handler.UsageRecorder = costTracker
	handler.BudgetChecker = costTracker

	ledgerStore := ledger.NewInMemoryLedgerStore()
	handler.Ledger = ledgerStore

	webhookDispatcher := webhooks.NewWebhookDispatcher()
	webhookDispatcher.Register(webhooks.EventBudgetExceeded, webhooks.WebhookConfig{
		URL:        getenvOrDefault("AEROLLM_WEBHOOK_URL", "http://localhost:8080/webhooks"),
		Secret:     getenvOrDefault("AEROLLM_WEBHOOK_SECRET", ""),
		Timeout:    2 * time.Second,
		Retries:    3,
		RetryDelay: 200 * time.Millisecond,
	})

	queue := webhooks.NewRedisWebhookQueue(redisClient.(*redis.Client), "webhook:queue")
	var webhookWg sync.WaitGroup
	webhookDispatcher.StartWorkerWithWaitGroup(ctx, queue, &webhookWg)

	costTracker.SetBudgetWebhookConfig(webhookDispatcher, webhooks.BudgetWebhookConfig{
		URL:        getenvOrDefault("AEROLLM_BUDGET_WEBHOOK_URL", "http://localhost:8080/webhooks/budget"),
		Secret:     getenvOrDefault("AEROLLM_BUDGET_WEBHOOK_SECRET", ""),
		Timeout:    2 * time.Second,
		Retries:    3,
		RetryDelay: 200 * time.Millisecond,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handler.HealthCheck)
	mux.HandleFunc("/ready", handler.ReadyCheck)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/readyz", readiness)
	mux.HandleFunc("/resilience/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":"ok"}`))
	})
	mux.HandleFunc("/v1/shadow", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req models.LLMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		shadow := traffic.NewShadowTester()
		err := shadow.RunAsync(r.Context(), getenvOrDefault("AEROLLM_SHADOW_URL", ""), getenvOrDefault("AEROLLM_API_KEY", ""), &req)
		if err != nil {
			http.Error(w, `{"error":"shadow dispatch failed"}`, http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"shadow":"accepted"}`))
	})
	mux.HandleFunc("/v1/slo/budget", slo.Handler(slo.NewErrorBudget(100), "latency"))
	mux.HandleFunc("/v1/chaos/fault", chaos.Handler(chaos.NewInjector(chaos.Config{})))
	mux.HandleFunc("/v1/trace/metrics", traceProvider.MetricsHandler())

	graphStore := graphrag.NewBboltGraphStore()
	_ = graphStore
	graphRAGMiddleware := graphrag.NewGraphRAGMiddleware(graphStore)
	deficitDetector := synthesis.NewDeficitDetector()
	_ = deficitDetector

	chat := handler.ChatCompletions
	chat = rag.RAGHTTPMiddleware(rag.NewHybridRetriever(rag.NewInMemoryVectorStore(), rag.NewInMemoryKeywordIndex()))(chat)
	chat = guardrails.InjectionShieldMiddleware(chat)
	chat = guardrails.PIIMiddleware(chat)
	chat = guardrails.APIKeyScopingMiddleware(scoper)(chat)
	chat = middleware.NewRateLimitMiddleware(chat, rl).Next
	chat = middleware.NewAuthMiddleware(chat).Next
	chat = middleware.NewLoggingMiddleware(chat, logger).Next
	chat = middleware.NewRecoveryMiddleware(chat).Next
	chat = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if signals, ok := deficitDetector.Analyze(ctx, getenvOrDefault("AEROLLM_REQUEST_ID", "req"), req.URL.Path, nil); ok {
			logger.Info("synthesis deficit detected", "tool", signals.MissingTool)
		}
		chat(w, req)
	})
	chat = genui.NewGenUIHandler(chat)
	var chatHandler http.Handler = http.HandlerFunc(chat)
	chatHandler = graphRAGMiddleware.Middleware(chatHandler)
	chatHandler = zk.Middleware(nil)(chatHandler)
	mux.HandleFunc("/v1/chat/completions", chatHandler.ServeHTTP)

	advanced := NewAdvancedAgent(registry, redisClient)
	handler.Advanced = advanced
	mux.HandleFunc("/v1/agents/approvals/", handler.ResumeApproval)

	mcpServer := mcp.NewServer()
	mux.Handle("/mcp", mcpServer)

	mux.HandleFunc("/ws", realtime.ServeWS(realtime.NewHub(), &realtimeProvider{}))

	_ = pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	_ = spatial.NewVideo3DStreamHandler()
	pqcKM := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	mux.HandleFunc("/v1/pqc/keys", pqc.HandshakeHandler(pqcKM))

	mux.HandleFunc("/v1/spatial/parse", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"read failed"}`, http.StatusBadRequest)
			return
		}
		anchors := spatial.ParseSpatialAnchors(string(b))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(anchors)
	})

	mux.HandleFunc("/v1/spatial/stream", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		spatial.NewVideo3DStreamHandler().StreamResponse(w, r, r.Body)
	})

	awsProvisioner := autoscale.NewAWSProvisioner()
	_ = awsProvisioner
	gcpProvisioner := autoscale.NewGCPProvisioner()
	_ = gcpProvisioner
	infraLoop := autoscale.NewServerMetaAgentLoop()

	mux.HandleFunc("/v1/autoscale/evaluate", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req struct {
			Deficit float64 `json:"deficit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		node, err := infraLoop.Evaluate(r.Context(), req.Deficit)
		if err != nil {
			http.Error(w, `{"error":"provision failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"node": node, "deficit": req.Deficit})
	})

	fedAgg := federated.NewFedAvgAggregator()

	mux.HandleFunc("/v1/federated/aggregate", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var updates []*federated.LoRAMatrix
		if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		out, err := fedAgg.Aggregate(r.Context(), updates)
		if err != nil {
			http.Error(w, `{"error":"aggregate failed"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	stateStore, _ := state.OpenBboltStateStore(getenvOrDefault("AEROLLM_STATE_DIR", "./aerollm-state"))
	_ = stateStore
	go func() {
		redteam.NewWorker(redteam.DefaultConfig(), ledgerStore).Start(ctx)
	}()

	_ = swarm.NewSwarmOrchestrator(stateStore, registry)
	_ = learning.NewTrainerWithAggregator(&flywheel.DatasetExporter{Ledger: ledgerStore}, ledgerStore, getenvOrDefault("AEROLLM_LEARNING_DIR", "./fine-tune-jobs"), fedAgg)
	engine := evolution.NewEngine(evolution.DefaultConfig())
	go engine.Start(ctx)

	_ = graphrag.NewAutoOntologyWorker(graphStore, nil)
	go func() {
		_ = synthesis.NewToolPromoter(nil)
	}()

	marketClient := marketplace.NewClient(getenvOrDefault("AEROLLM_MARKETPLACE_URL", "https://registry.aerollm.io"))
	_ = marketClient
	var registryStore marketplace.Store = marketplace.NewInMemoryStore()
	if redisClient != nil {
		registryStore = marketplace.NewRedisStore(marketplace.RedisOptions{Client: redisClient.(*redis.Client)})
	}
	_ = registryStore
	registryService := marketplace.NewRegistryService(marketClient, registryStore)
	_ = registryService

	marketPlugins := http.HandlerFunc(registryService.PluginsHandler())
	marketPlugin := http.HandlerFunc(registryService.PluginByIDHandler())
	marketPlugins = middleware.NewAuthMiddleware(marketPlugins).Next
	marketPlugin = middleware.NewAuthMiddleware(marketPlugin).Next
	mux.Handle("/v1/marketplace/plugins", marketPlugins)
	mux.Handle("/v1/marketplace/plugins/", marketPlugin)
	royaltyRecorder := marketplace.NewRoyaltyRecorder(webhookDispatcher, webhooks.BudgetWebhookConfig{
		URL:     getenvOrDefault("AEROLLM_ROYALTY_WEBHOOK_URL", "http://localhost:8080/webhooks/royalty"),
		Timeout: 2 * time.Second,
	})
	_ = royaltyRecorder

	var invoiceProvider billing.Provider = billing.NewInMemoryProvider()
	if k := os.Getenv("AEROLLM_STRIPE_SECRET_KEY"); k != "" {
		invoiceProvider = billing.NewStripeProvider(k)
	}
	invoiceGen := billing.NewInvoiceGenerator(invoiceProvider)
	_ = invoiceGen
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = invoiceGen.Generate(ctx, []billing.MeterEntry{
				{CustomerID: "acme", EventName: "token", Value: 100},
			})
		}
	}()

	tuner := aiops.NewMetaAgentTuner(aiops.NewDefaultMetricsSource(telemetry.RequestCount, telemetry.ErrorCount, func() float64 { return telemetry.AvgLatency() }), 30*time.Second, 5*time.Minute)
	go tuner.Run(ctx)

	feedbackExporter := flywheel.NewFeedbackExporter(ledgerStore)
	mux.HandleFunc("/v1/feedback", feedbackExporter.FeedbackHandler)
	go func() {
		_ = &flywheel.BackgroundExportWorker{
			Exporter:  feedbackExporter,
			Dataset:   &flywheel.DatasetExporter{Ledger: ledgerStore},
			Interval:  5 * time.Minute,
			UploadFunc: func(ctx context.Context, payload string) error {
				_ = ctx
				_ = payload
				return nil
			},
		}
	}()

	studioOrchestrator := swarm.NewSwarmOrchestrator(stateStore, registry)
	_ = studioOrchestrator
	studioHandler := studio.NewHandler(r, studioOrchestrator, prices, ledgerStore, royaltyRecorder)
	dagStore := studio.NewInMemoryDAGStore()
	_ = dagStore
	dagHandler := studio.NewDAGHandler(dagStore)
	_ = dagHandler

	var studioTopology http.Handler = http.HandlerFunc(studioHandler.Topology)
	var studioAnalytics http.Handler = http.HandlerFunc(studioHandler.AnalyticsCost)
	var studioDAGs http.Handler = http.HandlerFunc(dagHandler.ServeDAGs)
	licenseChecker := licensing.NewEnvLicenseChecker()
	studioTopology = licensing.Middleware(licenseChecker, licensing.FeatureAdvancedCRDTMesh)(studioTopology)
	studioAnalytics = licensing.Middleware(licenseChecker, licensing.FeatureAdvancedCRDTMesh)(studioAnalytics)
	studioDAGs = licensing.Middleware(licenseChecker, licensing.FeatureAdvancedCRDTMesh)(studioDAGs)
	studioTopology = middleware.NewAuthMiddleware(studioTopology.ServeHTTP).Next
	studioAnalytics = middleware.NewAuthMiddleware(studioAnalytics.ServeHTTP).Next
	studioDAGs = middleware.NewAuthMiddleware(studioDAGs.ServeHTTP).Next
	mux.Handle("/v1/studio/topology", studioTopology)
	mux.Handle("/v1/studio/analytics/cost", studioAnalytics)
	mux.Handle("/v1/studio/dags", studioDAGs)

	meshCfg := mesh.DefaultMeshConfig()
	meshCfg.Enabled = os.Getenv("AEROLLM_MESH_ENABLED") == "true"
	meshCfg.BindAddress = getenvOrDefault("AEROLLM_MESH_BIND", "/ip4/127.0.0.1/tcp/0")
	meshCfg.LocalPeerID = mesh.PeerID(getenvOrDefault("AEROLLM_MESH_NODE_ID", ""))
	if meshCfg.LocalPeerID == "" {
		meshCfg.LocalPeerID = mesh.PeerID(fmt.Sprintf("node-%d", time.Now().UnixNano()))
	}
	meshCfg.PeerAddresses = append(meshCfg.PeerAddresses, getenvOrDefault("AEROLLM_MESH_PEERS", ""))

	if meshCfg.Enabled {
		meshTransport := mesh.NewInMemoryTransport(meshCfg.LocalPeerID)
		pluginState := mesh.NewPluginRegistrySync()
		discovery := mesh.NewDiscovery(mesh.DiscoveryConfig{
			LocalID:     meshCfg.LocalPeerID,
			BindAddress: meshCfg.BindAddress,
			Peers: []mesh.PeerDescriptor{{
				ID:      meshCfg.LocalPeerID,
				Address: meshCfg.BindAddress,
			}},
			Transport: meshTransport,
		})
		go mesh.NewSyncWorker(mesh.SyncWorkerConfig{
			State:     pluginState,
			Discovery: discovery,
			Interval:  meshCfg.GossipInterval,
		}).Start(ctx)
		_ = meshTransport
		_ = discovery
		_ = pluginState
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", appCfg.Server.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		fmt.Println("server starting on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("shutting down server...")
	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "server forced to shutdown: %v\n", err)
	}
	webhookWg.Wait()
	if stateStore != nil {
		_ = stateStore.Close()
	}
	fmt.Println("server gracefully stopped")
}
