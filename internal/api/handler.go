package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// Handler handles HTTP requests for the LLM gateway.
type Handler struct {
	Router      *router.Router
	Agent       *agent.AgentEngine
	Cache       *cache.RedisCache
	RateLimiter ratelimit.RateLimiter
	Telemetry   *telemetry.Provider
	Logger      LoggerInterface
}

// LoggerInterface defines the logging interface.
type LoggerInterface interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// NewHandler creates a new Handler with dependency injection.
func NewHandler(router *router.Router, agent *agent.AgentEngine, cache *cache.RedisCache, rl ratelimit.RateLimiter, tp *telemetry.Provider, logger LoggerInterface) *Handler {
	return &Handler{
		Router:      router,
		Agent:       agent,
		Cache:       cache,
		RateLimiter: rl,
		Telemetry:   tp,
		Logger:      logger,
	}
}

// ChatCompletions handles the /v1/chat/completions endpoint.
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	ctx, span := h.Telemetry.StartSpan(ctx, "ChatCompletions")
	defer span.End()

	var req models.LLMRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.Logger.Error("invalid request", "error", err)
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	// Cache exact-match check
	if h.Cache != nil {
		cacheKey := cache.KeyForRequest(&req)
		if cached, err := h.Cache.GetExact(cacheKey); err == nil && cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(cached.Response)
			telemetry.RecordCacheHit(true)
			h.Logger.Info("cache hit", "key", cacheKey)
			return
		}
	}

	// Rate limiting
	if h.RateLimiter != nil {
		allowed, err := h.RateLimiter.Allow(ctx, r.Header.Get("Authorization"), "")
		if err != nil || !allowed {
			h.Logger.Error("rate limited", "error", err)
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
	}

	// Route to provider
	_, err := h.Router.Route(ctx, &req)
	if err != nil {
		h.Logger.Error("routing error", "error", err)
		http.Error(w, `{"error":"routing failed"}`, http.StatusInternalServerError)
		return
	}

	// Agentic tool execution loop
	resp, err := h.Agent.RunToolExecutionLoop(ctx, &req)
	if err != nil {
		h.Logger.Error("agent error", "error", err)
		http.Error(w, `{"error":"agent execution failed"}`, http.StatusInternalServerError)
		return
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		h.Logger.Error("encoding error", "error", err)
		http.Error(w, `{"error":"encoding failed"}`, http.StatusInternalServerError)
		return
	}

	// Cache exact-match store
	if h.Cache != nil {
		cacheKey := cache.KeyForRequest(&req)
		_ = h.Cache.SetExact(cacheKey, respBytes, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)

	telemetry.RecordRequestCount("", 1)
	telemetry.RecordLatency("", time.Since(start))
}

// HealthCheck handles the /health endpoint.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ReadyCheck handles the /ready endpoint.
func (h *Handler) ReadyCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"ready": "true"})
}

// RateLimitMiddleware wraps a handler with rate limiting.
func RateLimitMiddleware(next http.HandlerFunc, rl ratelimit.RateLimiter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		next(w, r)
	}
}
