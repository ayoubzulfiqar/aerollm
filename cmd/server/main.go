package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/api"
	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/config"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/guardrails"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
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
		APIKey:       "sk-dev-1",
		AllowedModels: []string{"gpt-3.5-turbo", "gpt-4", "claude-3-sonnet"},
		MaxBudgetUSD: 100,
		IPAllowlist:  []string{"127.0.0.1"},
	})

	handler.UsageRecorder = costTracker

	webhookDispatcher := webhooks.NewWebhookDispatcher()
	webhookDispatcher.Register(webhooks.EventBudgetExceeded, webhooks.WebhookConfig{
		URL:        getenvOrDefault("AEROLLM_WEBHOOK_URL", "http://localhost:8080/webhooks"),
		Secret:     getenvOrDefault("AEROLLM_WEBHOOK_SECRET", ""),
		Timeout:    2 * time.Second,
		Retries:    3,
		RetryDelay: 200 * time.Millisecond,
	})

	queue := webhooks.NewRedisWebhookQueue(redisClient.(*redis.Client), "webhook:queue")
	webhookDispatcher.StartWorker(ctx, queue)

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
	mux.HandleFunc("/v1/chat/completions", handler.ChatCompletions)

	chat := handler.ChatCompletions
	chat = guardrails.InjectionShieldMiddleware(chat)
	chat = guardrails.PIIMiddleware(chat)
	chat = guardrails.APIKeyScopingMiddleware(scoper)(chat)
	chat = middleware.NewRateLimitMiddleware(chat, rl).Next
	chat = middleware.NewAuthMiddleware(chat).Next
	chat = middleware.NewLoggingMiddleware(chat, logger).Next
	chat = middleware.NewRecoveryMiddleware(chat).Next

	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, req *http.Request) {
		chat(w, req)
	})

	advanced := NewAdvancedAgent(registry, redisClient)
	handler.Advanced = advanced

	mux.HandleFunc("/v1/agents/approvals/", handler.ResumeApproval)

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
	fmt.Println("server gracefully stopped")
}

func getenvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
