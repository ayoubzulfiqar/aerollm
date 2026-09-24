package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/analytics"
	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/callbacks"
	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/health"
	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/meter"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/providers/universal"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/internal/tools"
	"github.com/ayoubzulfiqar/aerollm/internal/trace"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

// appOptions customise newApp (used by tests).
type appOptions struct {
	// redis, when set, is used instead of dialing cfg.Redis.
	redis *redis.Client
	// skipRedis runs without Redis (degraded mode) without dialing.
	skipRedis bool
	// notice receives the one-time generated admin key (default stderr).
	notice io.Writer
}

// app holds every long-lived component of the gateway.
type app struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    *config.Config
	logger *LoggerAdapter

	handler http.Handler

	redis       *redis.Client
	telemetry   *telemetry.Provider
	trace       *trace.Provider
	auth        *middleware.Authenticator
	reloader    *config.ConfigReloader
	router      *router.Router
	gateway     *api.Handler
	registry    *agent.ToolRegistry
	keyManager  *keymanager.Manager
	keyHandler  *keymanager.KeyHandler
	costTracker *finops.CostTracker
	costMap     *intelligence.ModelCostMap
	pricing     *finops.PricingMap
	ledger      *ledger.InMemoryLedgerStore
	analytics   *analytics.AnalyticsEngine
	meter       *meter.Recorder
	callbacks   *callbacks.CallbackManager
	webhooks    *webhooks.WebhookDispatcher
	readiness   *health.Registry

	generatedAdminKey string

	workers sync.WaitGroup
	closeMu sync.Mutex
	closers []func(ctx context.Context)
	closed  bool
}

// goWorker runs fn in a tracked goroutine that recovers from panics.
func (a *app) goWorker(name string, fn func(ctx context.Context)) {
	a.workers.Add(1)
	go func() {
		defer a.workers.Done()
		defer func() {
			if p := recover(); p != nil {
				a.logger.Error("background worker panicked", "worker", name, "panic", fmt.Sprint(p))
			}
		}()
		fn(a.ctx)
	}()
}

// onClose registers cleanup to run (in reverse order) during close.
func (a *app) onClose(fn func(ctx context.Context)) {
	a.closeMu.Lock()
	a.closers = append(a.closers, fn)
	a.closeMu.Unlock()
}

// close cancels background work, waits (bounded) for workers, then releases
// resources in reverse order of acquisition.
func (a *app) close(timeout time.Duration) {
	a.closeMu.Lock()
	if a.closed {
		a.closeMu.Unlock()
		return
	}
	a.closed = true
	closers := a.closers
	a.closeMu.Unlock()

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	a.cancel()
	done := make(chan struct{})
	go func() { a.workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		a.logger.Error("background workers did not stop in time", "timeout", timeout.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i](ctx)
	}
}

func randomToken(prefix string, n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return prefix + hex.EncodeToString(b)
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }) {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// resolveAdminKeys collects admin keys from config and the environment
// (AEROLLM_MASTER_KEY, legacy AEROLLM_API_KEY). With none configured, a
// random key is generated so the admin surface is never left open or
// protected by a well-known default.
func resolveAdminKeys(cfg *config.Config) (keys []string, generated string) {
	add := func(k string) {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	add(cfg.Auth.MasterKey)
	add(os.Getenv("AEROLLM_MASTER_KEY"))
	add(os.Getenv("AEROLLM_API_KEY"))
	for _, k := range cfg.Auth.AdminKeys {
		add(k)
	}
	if len(keys) == 0 {
		generated = randomToken("sk-aero-admin-", 24)
		keys = append(keys, generated)
	}
	return keys, generated
}

func (a *app) connectRedis(ctx context.Context, opts appOptions) error {
	if opts.redis != nil {
		a.redis = opts.redis
		return nil
	}
	if opts.skipRedis || a.cfg.Redis.Addr == "" {
		return nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:         a.cfg.Redis.Addr,
		Password:     a.cfg.Redis.Password,
		DB:           a.cfg.Redis.DB,
		PoolSize:     a.cfg.Redis.PoolSize,
		MinIdleConns: a.cfg.Redis.MinIdleConns,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		if a.cfg.Redis.Required {
			return fmt.Errorf("redis %s unreachable: %w", a.cfg.Redis.Addr, err)
		}
		a.logger.Warn("redis unreachable; running in degraded mode (in-memory rate limits, no exact cache, no webhook queue)", "addr", a.cfg.Redis.Addr, "error", err)
		return nil
	}
	a.redis = client
	a.onClose(func(context.Context) { _ = client.Close() })
	a.logger.Info("redis connected", "addr", a.cfg.Redis.Addr)
	return nil
}

// newApp wires every component and builds the HTTP handler.
func newApp(parent context.Context, cfg *config.Config, logger *LoggerAdapter, opts appOptions) (_ *app, err error) {
	ctx, cancel := context.WithCancel(parent)
	a := &app{ctx: ctx, cancel: cancel, cfg: cfg, logger: logger}
	defer func() {
		if err != nil {
			a.close(5 * time.Second)
		}
	}()

	// Telemetry.
	tcfg := telemetry.Config{ServiceName: cfg.Telemetry.ServiceName}
	if cfg.Telemetry.Enabled {
		tcfg.Exporter = cfg.Telemetry.Exporter
		tcfg.OTLPAddr = cfg.Telemetry.OTLPAddr
		tcfg.SampleRate = cfg.Telemetry.SampleRate
	}
	if a.telemetry, err = telemetry.NewProvider(ctx, tcfg); err != nil {
		return nil, fmt.Errorf("telemetry: %w", err)
	}
	a.onClose(func(ctx context.Context) { a.telemetry.Stop(ctx) })
	a.trace = trace.NewProvider(trace.Config{ServiceName: cfg.Telemetry.ServiceName})

	if err = a.connectRedis(ctx, opts); err != nil {
		return nil, err
	}

	if err = a.buildAuth(opts); err != nil {
		return nil, err
	}
	if err = a.buildGateway(); err != nil {
		return nil, err
	}
	a.handler, err = a.routes()
	if err != nil {
		return nil, err
	}
	return a, nil
}

// buildAuth sets up admin/client keys and the virtual key manager.
func (a *app) buildAuth(opts appOptions) error {
	adminKeys, generated := resolveAdminKeys(a.cfg)
	a.generatedAdminKey = generated
	if generated != "" {
		w := opts.notice
		if w == nil {
			w = os.Stderr
		}
		fmt.Fprintf(w, "\nNo admin key configured (set auth.master_key or AEROLLM_AUTH_MASTER_KEY).\nGenerated an ephemeral admin key for this process:\n\n    %s\n\n", generated)
		a.logger.Warn("using an ephemeral generated admin key; configure auth.master_key for production")
	}
	for _, k := range adminKeys {
		if k == "sk-demo" || len(k) < 16 {
			a.logger.Warn("weak admin key configured; use at least 32 random characters")
			break
		}
	}

	// The key manager's master key is an admin credential for the key API
	// (virtual keys are stored as SHA-256 hashes, not derived from it).
	a.keyManager = keymanager.NewManager(keymanager.NewInMemoryKeyStore(), adminKeys[0])
	a.keyHandler = keymanager.NewKeyHandler(a.keyManager, nil, nil, a.logger.Func())
	for _, k := range adminKeys {
		a.keyHandler.AddAdminKey(k)
	}

	clientKeys := append([]string{}, a.cfg.Auth.APIKeys...)
	clientKeys = append(clientKeys, splitList(os.Getenv("AEROLLM_CLIENT_KEYS"))...)
	a.auth = middleware.NewAuthenticator(adminKeys, clientKeys, a.keyManager)
	return nil
}

// registerEnvProviders registers providers configured through environment
// variables (OPENAI_API_KEY, ANTHROPIC_API_KEY, AEROLLM_LOCAL_URL) with the
// router. They serve any model not claimed by a config.yaml provider.
func (a *app) registerEnvProviders() {
	env := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		p := providers.NewOpenAIProvider("openai", key, env("OPENAI_BASE_URL", "https://api.openai.com"))
		a.router.RegisterProvider(p)
		a.logger.Info("provider registered", "provider", p.Name(), "source", "env")
	}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		p := providers.NewAnthropicProvider(env("ANTHROPIC_BASE_URL", "https://api.anthropic.com"), key, env("ANTHROPIC_DEFAULT_MODEL", "claude-3-5-sonnet-latest"))
		a.router.RegisterProvider(p)
		a.logger.Info("provider registered", "provider", p.Name(), "source", "env")
	}
	if url := os.Getenv("AEROLLM_LOCAL_URL"); url != "" {
		p := providers.NewLocalProvider(url, env("AEROLLM_LOCAL_MODEL", "llama-3-8b"))
		a.router.RegisterProvider(p)
		a.logger.Info("provider registered", "provider", p.Name(), "source", "env")
	}
}

// buildResolver creates a model resolver over the usable providers in cfgs.
// Providers without credentials (e.g. an unset ${OPENAI_API_KEY}) are
// skipped so they cannot capture models and fail every request.
func buildResolver(cfgs []config.ProviderConfig) (api.ModelResolverFunc, []string, error) {
	var usable []config.ProviderConfig
	var skipped []string
	for _, c := range cfgs {
		if c.Usable() {
			usable = append(usable, c)
		} else {
			skipped = append(skipped, c.ResolvedName())
		}
	}
	reg := universal.NewProviderRegistry()
	if err := reg.RegisterFromConfig(usable); err != nil {
		return nil, nil, err
	}
	return func(model string) (providers.Provider, bool) {
		adapter, err := reg.ResolveProviderByModel(model)
		if err != nil || adapter == nil {
			return nil, false
		}
		return &universalAdapterBridge{inner: adapter}, true
	}, skipped, nil
}

// buildGateway wires routing, caching, accounting and the API handler.
func (a *app) buildGateway() error {
	cfg := a.cfg
	a.reloader = config.NewConfigReloader(cfg)

	a.costMap = intelligence.NewModelCostMap()
	a.costMap.LoadFromDefault()
	a.pricing = finops.NewPricingMap()

	a.router = router.New(router.Config{
		Strategy: cfg.Router.Strategy,
		BreakerConfig: router.CircuitBreakerConfig{
			MaxFailures:      cfg.Router.CircuitBreak.MaxFailures,
			ResetTimeout:     cfg.Router.CircuitBreak.ResetTimeout,
			HalfOpenMaxCalls: cfg.Router.CircuitBreak.HalfOpenMaxCalls,
		},
		CostMap:     a.costMap,
		MaxAttempts: cfg.Router.MaxAttempts,
	})
	a.registerEnvProviders()

	// Tools available to the server-side agent loop and MCP.
	a.registry = agent.NewToolRegistry()
	for _, tool := range tools.All() {
		if err := a.registry.Register(tool); err != nil {
			a.logger.Error("tool registration failed", "tool", tool.Name(), "error", err)
		}
	}
	engine := agent.NewAgentEngine(nil, a.registry)
	if cfg.Agent.MaxIterations > 0 {
		engine.MaxIterations = cfg.Agent.MaxIterations
	}
	if cfg.Agent.ToolTimeout > 0 {
		engine.ToolTimeout = cfg.Agent.ToolTimeout
	}
	if cfg.Agent.MaxConcurrent > 0 {
		engine.MaxConcurrent = cfg.Agent.MaxConcurrent
	}
	if !cfg.Agent.Enabled {
		engine = nil
	}

	var limiter ratelimit.RateLimiter
	if a.redis != nil {
		limiter = ratelimit.NewRedisLimiter(a.redis, cfg.RateLimit.DefaultRPS, cfg.RateLimit.BurstMultiplier)
	} else {
		limiter = ratelimit.NewTokenBucketLimiter(cfg.RateLimit.DefaultRPS, cfg.RateLimit.BurstMultiplier)
	}

	var exactCache *cache.RedisCache
	if cfg.Cache.Enabled && a.redis != nil {
		exactCache = cache.NewRedisCache(a.redis, cfg.Cache.TTL)
	}

	h := api.NewHandler(a.router, engine, exactCache, limiter, a.telemetry, a.logger)
	h.CacheSharedAcrossKeys = cfg.Cache.SharedAcrossKeys
	h.CacheTTL = cfg.Cache.TTL
	if cfg.Cache.Enabled && cfg.Cache.SemanticEnabled {
		// Local deterministic embedder (lexical similarity); plug a real
		// embedding provider in with SetEmbedder for semantic matching.
		sc := cache.NewVectorSemanticCache(cfg.Cache.SemanticPrefix, cfg.Cache.TTL, cfg.Cache.SemanticThreshold, cache.NewHashingEmbedder(0))
		h.SemanticCache = sc
		a.goWorker("semantic-cache-janitor", func(ctx context.Context) { sc.StartJanitor(ctx, time.Minute) })
	}

	resolver, skipped, err := buildResolver(cfg.Providers)
	if err != nil {
		return fmt.Errorf("providers: %w", err)
	}
	if len(skipped) > 0 {
		a.logger.Warn("providers skipped: missing API key", "providers", strings.Join(skipped, ","))
	}
	h.SetModelResolver(resolver)
	a.reloader.SetReloadCallback(func(ctx context.Context, next *config.Config) error {
		res, _, err := buildResolver(next.Providers)
		if err != nil {
			return err
		}
		h.SetModelResolver(res)
		a.logger.Info("provider registry hot-reloaded", "providers", len(next.Providers))
		return nil
	})
	h.ModelLister = func() []api.ModelEntry {
		infos := a.reloader.GetModels()
		out := make([]api.ModelEntry, 0, len(infos))
		for _, m := range infos {
			out = append(out, api.ModelEntry{ID: m.Model, Object: "model", OwnedBy: m.Provider})
		}
		return out
	}

	// Spend accounting.
	a.costTracker = finops.NewCostTracker(a.redis, a.pricing, a.costMap)
	if cfg.Finops.Enabled {
		h.UsageRecorder = a.costTracker
		h.BudgetChecker = a.costTracker
	}
	h.CostFunc = a.costTracker.CalculateCost
	a.analytics = analytics.NewAnalyticsEngine()
	h.Analytics = a.analytics
	a.meter = meter.NewRecorder()
	a.ledger = ledger.NewInMemoryLedgerStore()
	h.Ledger = a.ledger
	h.Authorize = a.authorizeKey
	h.OnUsage = a.recordUsage

	a.buildCallbacks()
	h.CallbackMgr = a.callbacks
	a.buildWebhooks()

	a.gateway = h
	a.registerReadiness()
	return nil
}

// authorizeKey enforces virtual key budgets and model allow-lists.
func (a *app) authorizeKey(ctx context.Context, p *middleware.Principal, model string) error {
	if p == nil || p.Virtual == nil {
		return nil
	}
	if !p.Virtual.AllowsModel(model) {
		return fmt.Errorf("key is not allowed to use model %q", model)
	}
	if p.Virtual.BudgetExceeded() {
		return errors.New("key budget exceeded")
	}
	return nil
}

// recordUsage fans a completed request out to per-key spend and metering.
func (a *app) recordUsage(ctx context.Context, ev api.UsageEvent) {
	if ev.Principal != nil && ev.Principal.Virtual != nil && ev.CostUSD > 0 {
		if err := a.keyManager.RecordSpend(ctx, ev.Principal.Key, ev.CostUSD); err != nil {
			a.logger.Error("virtual key spend update failed", "key_id", ev.KeyID, "error", err)
		}
	}
	rec := meter.UsageRecord{
		Timestamp: time.Now().UTC(),
		APIKey:    ev.KeyID,
		Provider:  ev.Provider,
		Model:     ev.Model,
		LatencyMs: float64(ev.Latency.Microseconds()) / 1000,
	}
	if ev.Usage != nil {
		rec.TokensIn = int64(ev.Usage.PromptTokens)
		rec.TokensOut = int64(ev.Usage.CompletionTokens)
	}
	a.meter.Record(rec)
}

func (a *app) buildCallbacks() {
	cfg := a.cfg.Callbacks
	a.callbacks = callbacks.NewCallbackManager(5 * time.Second)
	if cfg.Webhook.Enabled && cfg.Webhook.URL != "" {
		a.callbacks.Register(callbacks.NewWebhookCallback(callbacks.WebhookConfig{
			URL:        cfg.Webhook.URL,
			Secret:     cfg.Webhook.Secret,
			Timeout:    cfg.Webhook.Timeout,
			Retries:    cfg.Webhook.Retries,
			RetryDelay: cfg.Webhook.RetryDelay,
		}))
	}
	if cfg.Langfuse.Enabled {
		switch {
		case cfg.Langfuse.PublicKey != "" && cfg.Langfuse.SecretKey != "":
			a.callbacks.Register(callbacks.NewLangfuseCallbackWithKeys(cfg.Langfuse.PublicKey, cfg.Langfuse.SecretKey, cfg.Langfuse.BaseURL))
		case cfg.Langfuse.APIKey != "":
			a.callbacks.Register(callbacks.NewLangfuseCallback(cfg.Langfuse.APIKey, cfg.Langfuse.BaseURL, cfg.Langfuse.ProjectID))
		default:
			a.logger.Warn("langfuse callback enabled without keys; skipping")
		}
	}
	if cfg.Datadog.Enabled && cfg.Datadog.APIKey != "" {
		a.callbacks.Register(callbacks.NewDatadogCallback(cfg.Datadog.APIKey, cfg.Datadog.BaseURL, cfg.Datadog.Site))
	}
	a.onClose(func(ctx context.Context) { _ = a.callbacks.Shutdown(ctx) })
}

// buildWebhooks registers operator-configured webhooks. Nothing is sent
// unless a URL is explicitly configured.
func (a *app) buildWebhooks() {
	a.webhooks = webhooks.NewWebhookDispatcher()
	a.onClose(func(ctx context.Context) { _ = a.webhooks.Shutdown(ctx) })
	if !a.cfg.Webhooks.Enabled {
		return
	}
	if url := os.Getenv("AEROLLM_WEBHOOK_URL"); url != "" {
		a.webhooks.Register(webhooks.EventBudgetExceeded, webhooks.WebhookConfig{
			URL:        url,
			Secret:     os.Getenv("AEROLLM_WEBHOOK_SECRET"),
			Timeout:    2 * time.Second,
			Retries:    3,
			RetryDelay: 200 * time.Millisecond,
		})
	}
	if url := os.Getenv("AEROLLM_BUDGET_WEBHOOK_URL"); url != "" {
		a.costTracker.SetBudgetWebhookConfig(a.webhooks, webhooks.BudgetWebhookConfig{
			URL:        url,
			Secret:     os.Getenv("AEROLLM_BUDGET_WEBHOOK_SECRET"),
			Timeout:    2 * time.Second,
			Retries:    3,
			RetryDelay: 200 * time.Millisecond,
		})
	}
	if a.redis != nil {
		queue := webhooks.NewRedisWebhookQueue(a.redis, "webhook:queue")
		a.goWorker("webhook-queue", func(ctx context.Context) {
			var wg sync.WaitGroup
			a.webhooks.StartWorkerWithWaitGroup(ctx, queue, &wg)
			<-ctx.Done()
			wg.Wait()
		})
	}
}

// redisChecker reports Redis connectivity for readiness.
type redisChecker struct{ client *redis.Client }

func (c redisChecker) Name() string { return "redis" }
func (c redisChecker) Check(ctx context.Context) health.Check {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := c.client.Ping(ctx).Err()
	chk := health.Check{Name: "redis", Healthy: err == nil, Latency: time.Since(start), CheckedAt: time.Now()}
	if err != nil {
		chk.Error = "unreachable"
	}
	return chk
}

// providerChecker reports whether any upstream provider is usable.
type providerChecker struct{ a *app }

func (c providerChecker) Name() string { return "providers" }
func (c providerChecker) Check(ctx context.Context) health.Check {
	routed := c.a.gateway.HealthProviders()
	open := 0
	for _, p := range routed {
		if p.CircuitOpen {
			open++
		}
	}
	configured := len(c.a.reloader.GetModels())
	healthy := configured > 0 || (len(routed) > 0 && open < len(routed))
	chk := health.Check{Name: "providers", Healthy: healthy, CheckedAt: time.Now()}
	if !healthy {
		chk.Error = "no usable upstream provider configured"
	}
	return chk
}

func (a *app) registerReadiness() {
	a.readiness = health.NewRegistry()
	a.readiness.Register(providerChecker{a: a})
	if a.redis != nil {
		a.readiness.Register(redisChecker{client: a.redis})
	}
}

// modelNames lists configured model IDs (sorted), for diagnostics.
func (a *app) modelNames() []string {
	var out []string
	for _, m := range a.reloader.GetModels() {
		out = append(out, m.Model)
	}
	sort.Strings(out)
	return out
}
