package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/analytics"
	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/callbacks"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// LoggerInterface defines the logging interface.
type LoggerInterface interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

// ModelResolverFunc resolves the provider configured for a model alias.
type ModelResolverFunc func(model string) (providers.Provider, bool)

// UsageEvent describes one completed (or cache-served) request. It is passed
// to Handler.OnUsage so spend can be recorded by any number of sinks.
type UsageEvent struct {
	RequestID string
	Endpoint  string
	Principal *middleware.Principal
	// KeyID is the non-reversible caller identifier (empty if anonymous).
	KeyID    string
	TeamID   string
	Model    string
	Provider string
	Usage    *models.Usage
	// UsageEstimated is true when the upstream did not report usage and the
	// gateway estimated token counts from text length.
	UsageEstimated bool
	CostUSD        float64
	Latency        time.Duration
	Stream         bool
	CacheHit       bool
	Request        *models.LLMRequest
	Response       *models.LLMResponse
}

// Handler handles HTTP requests for the LLM gateway.
type Handler struct {
	Router      *router.Router
	Agent       *agent.AgentEngine
	Cache       *cache.RedisCache
	RateLimiter ratelimit.RateLimiter
	Telemetry   *telemetry.Provider
	Logger      LoggerInterface

	// CallbackMgr dispatches observability callbacks asynchronously.
	CallbackMgr *callbacks.CallbackManager
	// SemanticCache provides similarity-based caching via embeddings.
	SemanticCache *cache.VectorSemanticCache
	// Analytics records spend data for /global/spend/report and /global/spend/logs.
	Analytics *analytics.AnalyticsEngine

	Advanced interface {
		ResumeApproval(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, error)
	}
	UsageRecorder *finops.CostTracker
	BudgetChecker interface {
		CheckBudget(ctx context.Context, apiKey string, estimatedCost float64) (float64, error)
	}
	Ledger interface {
		Append(ctx context.Context, record ledger.LedgerRecord) error
		Latest(ctx context.Context) (*ledger.LedgerRecord, error)
	}
	UsageSink chan<- billing.MeterEntry

	// ModelResolver resolves a provider for a given model alias. Optional.
	// Prefer SetModelResolver when replacing it while serving traffic.
	ModelResolver func(model string) (providers.Provider, bool)

	// CostFunc prices a completion. When nil, UsageRecorder's pricing is used.
	CostFunc func(model string, usage *models.Usage) float64
	// OnUsage is called after every successful completion (including cache
	// hits, with zero cost) to record spend in external systems.
	OnUsage func(ctx context.Context, ev UsageEvent)
	// Authorize is an optional per-request policy check (e.g. virtual key
	// model allow-lists, budgets). Returning an error rejects with 403.
	Authorize func(ctx context.Context, p *middleware.Principal, model string) error
	// TrimMessages optionally fits the conversation into the model's context
	// window before it is sent upstream.
	TrimMessages func(model string, msgs []models.Message) []models.Message
	// ModelLister returns extra models for GET /v1/models.
	ModelLister func() []ModelEntry

	// CacheTTL is the exact-cache TTL (0 = cache default).
	CacheTTL time.Duration
	// CacheSharedAcrossKeys lets different API keys share cache entries.
	CacheSharedAcrossKeys bool

	resolver atomic.Pointer[ModelResolverFunc]
}

// NewHandler creates a new Handler with dependency injection.
func NewHandler(router *router.Router, agent *agent.AgentEngine, cache *cache.RedisCache, rl ratelimit.RateLimiter, tp *telemetry.Provider, logger LoggerInterface) *Handler {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Handler{
		Router:      router,
		Agent:       agent,
		Cache:       cache,
		RateLimiter: rl,
		Telemetry:   tp,
		Logger:      logger,
	}
}

type nopLogger struct{}

func (nopLogger) Info(string, ...interface{})  {}
func (nopLogger) Error(string, ...interface{}) {}

// SetModelResolver atomically replaces the model resolver; safe to call while
// requests are being served (used by config hot-reload).
func (h *Handler) SetModelResolver(fn ModelResolverFunc) {
	if fn == nil {
		h.resolver.Store(nil)
		return
	}
	h.resolver.Store(&fn)
}

// ResolveModel resolves a configured provider for model without falling
// back to the router.
func (h *Handler) ResolveModel(model string) (providers.Provider, bool) {
	if fn := h.resolver.Load(); fn != nil {
		if p, ok := (*fn)(model); ok && p != nil {
			return p, true
		}
		return nil, false
	}
	if h.ModelResolver != nil {
		if p, ok := h.ModelResolver(model); ok && p != nil {
			return p, true
		}
	}
	return nil, false
}

// requestMeta carries per-request context through the chat pipeline.
type requestMeta struct {
	start     time.Time
	requestID string
	principal *middleware.Principal
	keyID     string
	teamID    string
	cacheNS   string
	cacheKey  string
	noCache   bool
	noStore   bool
	endpoint  string
}

func (h *Handler) newMeta(r *http.Request, endpoint string) *requestMeta {
	m := &requestMeta{start: time.Now(), requestID: middleware.RequestIDFromContext(r.Context()), endpoint: endpoint}
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok {
		m.principal = p
		m.keyID = p.KeyID
		if p.Virtual != nil {
			m.teamID = p.Virtual.TeamID
		}
	} else if key := middleware.APIKeyFromRequest(r); key != "" {
		m.keyID = middleware.KeyID(key)
	}
	if m.requestID == "" {
		m.requestID = middleware.NewRequestID()
	}
	if !h.CacheSharedAcrossKeys {
		m.cacheNS = m.keyID
	}
	cc := strings.ToLower(r.Header.Get("Cache-Control"))
	m.noCache = strings.Contains(cc, "no-cache") || strings.Contains(cc, "no-store")
	m.noStore = strings.Contains(cc, "no-store")
	return m
}

// validateChatRequest rejects requests the upstreams would reject anyway.
func validateChatRequest(req *models.LLMRequest) error {
	if strings.TrimSpace(req.Model) == "" {
		return errors.New("model is required")
	}
	if len(req.Messages) == 0 {
		return errors.New("messages must contain at least one message")
	}
	for i, m := range req.Messages {
		switch m.Role {
		case models.RoleSystem, models.RoleUser, models.RoleAssistant, models.RoleTool, models.RoleDeveloper, "function":
		default:
			return fmt.Errorf("messages[%d].role %q is invalid", i, m.Role)
		}
	}
	if t := req.Temperature; t != nil && (*t < 0 || *t > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if p := req.TopP; p != nil && (*p < 0 || *p > 1) {
		return errors.New("top_p must be between 0 and 1")
	}
	if mt := req.EffectiveMaxTokens(); mt != nil && *mt < 1 {
		return errors.New("max_tokens must be positive")
	}
	if req.N != nil && (*req.N < 1 || *req.N > 16) {
		return errors.New("n must be between 1 and 16")
	}
	return nil
}

// authorize applies the configured policy plus the virtual key model list.
func (h *Handler) authorize(ctx context.Context, m *requestMeta, model string) error {
	if m.principal != nil && m.principal.Virtual != nil && !m.principal.Virtual.AllowsModel(model) {
		return fmt.Errorf("key is not allowed to use model %q", model)
	}
	if h.Authorize != nil {
		return h.Authorize(ctx, m.principal, model)
	}
	return nil
}

// ChatCompletions handles the /v1/chat/completions endpoint.
// @Summary Chat completion
// @Description Send a chat completion request to an LLM provider. Set stream=true for Server-Sent Events.
// @Tags chat
// @Accept json
// @Produce json
// @Param req body models.LLMRequest true "Chat completion request"
// @Success 200 {object} models.LLMResponse
// @Failure 400 {object} map[string]string
// @Router /v1/chat/completions [post]
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var req models.LLMRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	h.serveChat(w, r, &req, "/v1/chat/completions")
}

// serveChat runs the full gateway pipeline for a decoded chat request.
func (h *Handler) serveChat(w http.ResponseWriter, r *http.Request, req *models.LLMRequest, endpoint string) {
	ctx, span := h.Telemetry.StartSpan(r.Context(), "ChatCompletions")
	defer span.End()
	m := h.newMeta(r, endpoint)

	if err := validateChatRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.authorize(ctx, m, req.Model); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}

	// Handler-level rate limiting only when no RateLimit middleware ran, so a
	// request is never charged twice.
	if h.RateLimiter != nil && !middleware.RateLimitApplied(ctx) {
		allowed, err := h.RateLimiter.Allow(ctx, m.keyID, req.Model)
		if err != nil || !allowed {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	if h.BudgetChecker != nil && m.keyID != "" {
		if _, err := h.BudgetChecker.CheckBudget(ctx, m.keyID, h.estimateCost(req)); err != nil {
			if isBudgetExceeded(err) {
				h.Logger.Info("budget exceeded", "key_id", m.keyID)
				writeErrorType(w, http.StatusPaymentRequired, "budget exceeded for this API key", "insufficient_quota")
				return
			}
			// Budget store unavailable: fail open so a cache/Redis outage
			// does not take inference down; spend is still recorded later.
			h.Logger.Error("budget check failed; allowing request", "key_id", m.keyID, "error", err)
		}
	}

	if h.serveFromCache(ctx, w, req, m) {
		return
	}

	if h.TrimMessages != nil {
		req.Messages = h.TrimMessages(req.Model, req.Messages)
	}

	if req.Stream {
		h.streamChat(ctx, w, req, m)
		return
	}

	resp, provider, err := h.complete(ctx, req)
	if err != nil {
		h.recordFailure(ctx, req, m, provider, err)
		writeUpstreamError(w, err)
		return
	}
	if resp.Model == "" {
		resp.Model = req.Model
	}
	if resp.Object == "" {
		resp.Object = "chat.completion"
	}
	if resp.Created == 0 {
		resp.Created = time.Now().Unix()
	}
	if resp.ID == "" {
		resp.ID = "chatcmpl-" + m.requestID
	}
	estimated := false
	if resp.Usage == nil {
		resp.Usage = estimateUsage(req, resp)
		estimated = true
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode response")
		return
	}
	providerName := providerNameOf(provider)
	if providerName != "" {
		w.Header().Set("X-AeroLLM-Provider", providerName)
	}
	w.Header().Set("X-AeroLLM-Cache", "MISS")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)

	h.afterCompletion(ctx, req, resp, respBytes, providerName, m, estimated, false)
}

// isBudgetExceeded recognises budget errors from the cost tracker (and
// legacy checkers that only say "budget exceeded").
func isBudgetExceeded(err error) bool {
	if errors.Is(err, finops.ErrBudgetExceeded) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "budget exceeded")
}

// estimateCost prices the worst case of req (prompt estimate + max output)
// for the budget pre-check.
func (h *Handler) estimateCost(req *models.LLMRequest) float64 {
	est, ok := h.BudgetChecker.(interface {
		EstimateRequestCost(model string, promptTokens, maxCompletionTokens int) float64
	})
	if !ok {
		return 0
	}
	prompt := 0
	for _, msg := range req.Messages {
		prompt += 4 + estimateTokens(msg.TextContent())
	}
	maxOut := 1024
	if mt := req.EffectiveMaxTokens(); mt != nil && *mt > 0 {
		maxOut = *mt
	}
	return est.EstimateRequestCost(req.Model, prompt, maxOut)
}

func providerNameOf(p providers.Provider) string {
	if p == nil {
		return ""
	}
	return p.Name()
}

// complete executes a non-streaming completion, running the server-side
// tool loop when the request offers tools the gateway implements.
func (h *Handler) complete(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, providers.Provider, error) {
	if !h.usesServerTools(req) {
		return h.callOnce(ctx, req)
	}
	var last providers.Provider
	caller := toolCaller(func(ctx context.Context, r *models.LLMRequest) (*models.LLMResponse, error) {
		resp, p, err := h.callOnce(ctx, r)
		if p != nil {
			last = p
		}
		return resp, err
	})
	engine := h.agentWith(caller)
	resp, err := engine.RunToolExecutionLoop(ctx, req)
	if err == nil && resp == nil {
		err = errors.New("agent returned no response")
	}
	return resp, last, err
}

// callOnce sends req to the provider configured for its model, or routes it
// across the router's providers with fallback.
func (h *Handler) callOnce(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, providers.Provider, error) {
	start := time.Now()
	if p, ok := h.ResolveModel(req.Model); ok {
		resp, err := p.ChatCompletions(ctx, req)
		h.recordProvider(p.Name(), start, err)
		if err == nil && resp == nil {
			err = providers.BadResponseError(p.Name(), errors.New("provider returned no response"))
		}
		return resp, p, err
	}
	if h.Router == nil {
		return nil, nil, &router.NoProviderError{Model: req.Model}
	}
	resp, p, err := h.Router.ChatCompletionsWithFallback(ctx, req)
	h.recordProvider(providerNameOf(p), start, err)
	return resp, p, err
}

func (h *Handler) recordProvider(name string, start time.Time, err error) {
	if name == "" {
		return
	}
	if err != nil {
		telemetry.RecordProviderError(name)
		return
	}
	telemetry.RecordProviderMetrics(name, float64(time.Since(start).Milliseconds()))
}

// toolCaller adapts a function to agent.ToolProvider.
type toolCaller func(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)

func (f toolCaller) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return f(ctx, req)
}

// usesServerTools reports whether any tool offered in the request is
// implemented by the gateway's tool registry.
func (h *Handler) usesServerTools(req *models.LLMRequest) bool {
	if h.Agent == nil || h.Agent.Registry == nil || len(req.Tools) == 0 {
		return false
	}
	for _, t := range req.Tools {
		if _, ok := h.Agent.Registry.Get(t.Name); ok {
			return true
		}
	}
	return false
}

// agentWith returns an engine configured like h.Agent that calls LLMs
// through p for this request only.
func (h *Handler) agentWith(p agent.ToolProvider) *agent.AgentEngine {
	return &agent.AgentEngine{
		Provider:      p,
		MaxIterations: h.Agent.MaxIterations,
		ToolTimeout:   h.Agent.ToolTimeout,
		MaxConcurrent: h.Agent.MaxConcurrent,
		ToolCache:     h.Agent.ToolCache,
		Registry:      h.Agent.Registry,
		ToolBilling:   h.Agent.ToolBilling,
	}
}

// cacheable reports whether req may be served from / stored in the cache.
func (h *Handler) cacheable(req *models.LLMRequest, m *requestMeta) bool {
	return h.Cache != nil && !m.noCache
}

// serveFromCache answers from the exact or semantic cache when possible.
func (h *Handler) serveFromCache(ctx context.Context, w http.ResponseWriter, req *models.LLMRequest, m *requestMeta) bool {
	if h.Cache != nil && !m.noStore {
		m.cacheKey = cache.KeyForRequestNS(m.cacheNS, req)
	}
	var body []byte
	if h.cacheable(req, m) {
		if entry, err := h.Cache.GetExactCtx(ctx, m.cacheKey); err == nil && entry != nil && len(entry.Response) > 0 {
			body = entry.Response
		}
	}
	if body == nil && h.SemanticCache != nil && !m.noCache && len(req.Tools) == 0 {
		if hit, err := h.SemanticCache.SearchNS(ctx, cache.ScopedNamespace(m.cacheNS, req.Model), semanticText(req)); err == nil && hit != nil && len(hit.Response) > 0 {
			body = hit.Response
		}
	}
	if body == nil {
		if h.Cache != nil || h.SemanticCache != nil {
			telemetry.RecordCacheHit(false)
		}
		return false
	}
	var resp models.LLMResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return false // corrupt entry: treat as a miss
	}
	telemetry.RecordCacheHit(true)
	w.Header().Set("X-AeroLLM-Cache", "HIT")
	if req.Stream {
		h.writeStreamFromResponse(ctx, w, req, &resp)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
	h.emitUsage(ctx, UsageEvent{
		RequestID: m.requestID, Endpoint: m.endpoint, Principal: m.principal, KeyID: m.keyID, TeamID: m.teamID,
		Model: req.Model, Usage: resp.Usage, Latency: time.Since(m.start), Stream: req.Stream, CacheHit: true,
		Request: req, Response: &resp,
	})
	return true
}

// semanticText is the text embedded for similarity caching. The model and
// sampling-relevant settings are included so different models never match.
func semanticText(req *models.LLMRequest) string {
	var b strings.Builder
	b.WriteString(req.Model)
	if req.ResponseFormat != nil {
		b.WriteString(" format=")
		b.WriteString(req.ResponseFormat.Type)
	}
	for _, msg := range req.Messages {
		b.WriteByte('\n')
		b.WriteString(string(msg.Role))
		b.WriteString(": ")
		b.WriteString(msg.TextContent())
	}
	return b.String()
}

// hitlResumer is implemented by approval engines that can report a further
// approval required by the resumed conversation.
type hitlResumer interface {
	ResumeApprovalWithHITL(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, string, error)
}

// ResumeApproval handles POST /v1/agents/approvals/{id} with body
// {"approved": bool}. The paused conversation is stored with the approval,
// so the caller only decides; approvals are bound to the key that created
// them.
func (h *Handler) ResumeApproval(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if h.Advanced == nil {
		writeError(w, http.StatusNotImplemented, "advanced agent not enabled")
		return
	}

	approvalID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/agents/approvals/"), "/")
	if approvalID == "" || strings.Contains(approvalID, "/") || len(approvalID) > 128 {
		writeError(w, http.StatusBadRequest, "missing or invalid approval id")
		return
	}

	var body struct {
		Approved *bool `json:"approved"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Approved == nil {
		writeError(w, http.StatusBadRequest, "approved (boolean) is required")
		return
	}

	ctx := r.Context()
	if p, ok := middleware.PrincipalFromContext(ctx); ok && p.KeyID != "" {
		ctx = agent.WithPrincipal(ctx, p.KeyID)
	}
	var (
		resp *models.LLMResponse
		next string
		err  error
	)
	if hr, ok := h.Advanced.(hitlResumer); ok {
		resp, next, err = hr.ResumeApprovalWithHITL(ctx, approvalID, *body.Approved, nil)
	} else {
		resp, err = h.Advanced.ResumeApproval(ctx, approvalID, *body.Approved, nil)
	}
	var more *agent.ApprovalRequiredError
	switch {
	case err == nil && next != "":
		writeJSON(w, http.StatusAccepted, map[string]interface{}{"status": "approval_required", "approval_id": next, "response": resp})
		return
	case errors.As(err, &more):
		writeJSON(w, http.StatusAccepted, map[string]interface{}{"status": "approval_required", "approval_id": more.ApprovalID})
		return
	case err != nil:
		h.Logger.Error("resume approval failed", "error", err)
		writeApprovalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeApprovalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, agent.ErrApprovalNotFound):
		writeError(w, http.StatusNotFound, "approval not found")
	case errors.Is(err, agent.ErrApprovalProcessed), errors.Is(err, agent.ErrApprovalInProgress):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, agent.ErrApprovalExpired):
		writeError(w, http.StatusGone, "approval expired")
	case errors.Is(err, agent.ErrApprovalForbidden):
		writeError(w, http.StatusForbidden, "approval belongs to a different key")
	case errors.Is(err, agent.ErrNoProvider), errors.Is(err, agent.ErrNoApprovalStore):
		writeError(w, http.StatusServiceUnavailable, "approval engine not configured")
	default:
		writeUpstreamError(w, err)
	}
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
	out := []ProviderHealth{}
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

// HealthCheck handles the /health endpoint (liveness).
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ReadyCheck handles the /ready endpoint. The gateway is ready when at least
// one upstream provider is configured.
func (h *Handler) ReadyCheck(w http.ResponseWriter, r *http.Request) {
	routerProviders := 0
	if h.Router != nil {
		routerProviders = len(h.Router.Providers())
	}
	configured := len(h.listModels()) > 0
	ready := routerProviders > 0 || configured
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]interface{}{
		"ready":     ready,
		"providers": h.HealthProviders(),
	})
}

// AdvancedLoopHandler wires the advanced execution loop into the HTTP handler.
type AdvancedLoopHandler struct {
	Agent  *agent.AgentEngine
	Logger LoggerInterface
	Hooks  []agent.LoopHook
}

// NewAdvancedLoopHandler creates a new handler wrapper.
func NewAdvancedLoopHandler(a *agent.AgentEngine, logger LoggerInterface, hooks []agent.LoopHook) *AdvancedLoopHandler {
	return &AdvancedLoopHandler{Agent: a, Logger: logger, Hooks: hooks}
}

// Run executes the advanced agent loop for a request.
func (h *AdvancedLoopHandler) Run(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if h.Agent == nil {
		return nil, errors.New("agent not configured")
	}
	return h.Agent.RunAdvancedExecutionLoop(ctx, req, agent.AdvancedLoopOptions{Hooks: h.Hooks})
}

// RateLimitMiddleware wraps a handler with rate limiting.
func RateLimitMiddleware(next http.HandlerFunc, rl ratelimit.RateLimiter) http.HandlerFunc {
	return middleware.RateLimit(middleware.RateLimitOptions{Limiter: rl})(next).ServeHTTP
}

// extractAPIKey extracts the API key from the Authorization header.
func extractAPIKey(auth string) string {
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return strings.TrimSpace(auth)
}
