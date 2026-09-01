package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
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
	Advanced   interface {
		ResumeApproval(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, error)
	}
	UsageRecorder *finops.CostTracker
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
	if req.Model == "" {
		h.Logger.Error("missing model")
		http.Error(w, `{"error":"missing model"}`, http.StatusBadRequest)
		return
	}

	apiKey := r.Header.Get("Authorization")
	if h.UsageRecorder != nil && apiKey != "" {
		_ = h.UsageRecorder.RecordUsage(ctx, finops.CostRequest{APIKey: apiKey, Model: req.Model, Usage: &models.Usage{}})
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
		allowed, err := h.RateLimiter.Allow(ctx, apiKey, req.Model)
		if err != nil || !allowed {
			h.Logger.Error("rate limited", "error", err)
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
	}

	// Route to provider
	selectedProvider, err := h.Router.Route(ctx, &req)
	if err != nil {
		h.Logger.Error("routing error", "error", err)
		http.Error(w, `{"error":"routing failed"}`, http.StatusInternalServerError)
		telemetry.RecordError()
		return
	}

	// Provider-aware agentic tool execution loop
	agentWithProvider := &agent.AgentEngine{
		Provider:      h.Agent.Provider,
		MaxIterations: h.Agent.MaxIterations,
		ToolTimeout:   h.Agent.ToolTimeout,
		MaxConcurrent: h.Agent.MaxConcurrent,
		ToolCache:     h.Agent.ToolCache,
		Registry:      h.Agent.Registry,
	}
	resp, err := agentWithProvider.RunToolExecutionLoop(ctx, &req)
	if err != nil {
		h.Logger.Error("agent error", "error", err)
		http.Error(w, `{"error":"agent execution failed"}`, http.StatusInternalServerError)
		telemetry.RecordError()
		return
	}
	if resp == nil {
		h.Logger.Error("empty agent response")
		http.Error(w, `{"error":"empty agent response"}`, http.StatusInternalServerError)
		return
	}
	if selectedProvider != nil {
		resp.Model = selectedProvider.Name()
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

	telemetry.RecordRequestCount(selectedProvider.Name(), 1)
	telemetry.RecordLatencyMs(float64(time.Since(start).Milliseconds()))
}

// ResumeApproval handles the /v1/agents/approvals/{id} endpoint.
func (h *Handler) ResumeApproval(w http.ResponseWriter, r *http.Request) {
	if h.Advanced == nil {
		http.Error(w, `{"error":"advanced agent not enabled"}`, http.StatusNotImplemented)
		return
	}

	approvalID := strings.TrimPrefix(r.URL.Path, "/v1/agents/approvals/")
	if approvalID == "" {
		http.Error(w, `{"error":"missing approval id"}`, http.StatusBadRequest)
		return
	}

	var req struct {
		Approved bool `json:"approved"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	resp, err := h.Advanced.ResumeApproval(r.Context(), approvalID, req.Approved, &models.LLMRequest{})
	if err != nil {
		h.Logger.Error("resume approval failed", "error", err)
		http.Error(w, `{"error":"approval failed"}`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ProviderHealth represents provider health status for HTTP responses.
type ProviderHealth struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Healthy     bool   `json:"healthy"`
	CircuitOpen bool   `json:"circuit_open"`
}

// HealthProviders returns the health status of all registered providers.
func (h *Handler) HealthProviders() []ProviderHealth {
	var out []ProviderHealth
	if h.Router == nil {
		return out
	}
	for _, cb := range h.Router.Providers() {
		health := cb.Health()
		out = append(out, ProviderHealth{
			Name:        health.Name,
			Type:        string(health.Type),
			Healthy:     health.Healthy,
			CircuitOpen: health.CircuitOpen,
		})
	}
	return out
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
