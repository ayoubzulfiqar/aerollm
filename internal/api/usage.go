package api

import (
	"context"
	"encoding/json"
	"math"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/callbacks"
	"github.com/ayoubzulfiqar/aerollm/internal/finops"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// estimateTokens approximates a token count (~4 characters per token).
func estimateTokens(s string) int {
	n := utf8.RuneCountInString(s)
	if n == 0 {
		return 0
	}
	return int(math.Ceil(float64(n) / 4))
}

// estimateUsage approximates usage when the upstream did not report it.
func estimateUsage(req *models.LLMRequest, resp *models.LLMResponse) *models.Usage {
	u := &models.Usage{}
	for _, m := range req.Messages {
		u.PromptTokens += 4 + estimateTokens(m.TextContent())
	}
	if resp != nil {
		for _, c := range resp.Choices {
			u.CompletionTokens += estimateTokens(c.Message.TextContent())
			for _, tc := range c.Message.ToolCalls {
				u.CompletionTokens += estimateTokens(tc.Function.Name) + estimateTokens(tc.Function.Arguments)
			}
		}
	}
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	return u
}

// costFor prices a completion using CostFunc, falling back to the cost
// tracker's pricing table.
func (h *Handler) costFor(model string, usage *models.Usage) float64 {
	if usage == nil {
		return 0
	}
	var c float64
	switch {
	case h.CostFunc != nil:
		c = h.CostFunc(model, usage)
	case h.UsageRecorder != nil:
		c = h.UsageRecorder.CalculateCost(model, usage)
	}
	if c < 0 || math.IsNaN(c) || math.IsInf(c, 0) {
		return 0
	}
	return c
}

// afterCompletion performs all bookkeeping for a successful upstream
// completion: cache store, spend accounting, analytics, callbacks, ledger.
// It runs after the response has been written, so failures here are logged
// and never affect the client.
func (h *Handler) afterCompletion(ctx context.Context, req *models.LLMRequest, resp *models.LLMResponse, respBytes []byte, providerName string, m *requestMeta, estimated, stream bool) {
	// The client may already be gone; bookkeeping must still complete.
	ctx = context.WithoutCancel(ctx)
	latency := time.Since(m.start)

	cost := h.costFor(resp.Model, resp.Usage)
	if cost == 0 && resp.Model != req.Model {
		cost = h.costFor(req.Model, resp.Usage)
	}

	telemetry.RecordRequestCount(providerName, 1)
	telemetry.RecordLatencyMs(float64(latency.Microseconds()) / 1000)
	if resp.Usage != nil {
		telemetry.RecordTokens(resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
	}
	telemetry.RecordCostUSD(cost)

	if h.Cache != nil && m.cacheKey != "" && !m.noStore && cacheableResponse(resp) {
		tokens := 0
		if resp.Usage != nil {
			tokens = resp.Usage.TotalTokens
		}
		if err := h.Cache.SetExactCtx(ctx, m.cacheKey, respBytes, tokens); err != nil {
			h.Logger.Error("cache store failed", "error", err)
		}
	}
	if h.SemanticCache != nil && !m.noStore && len(req.Tools) == 0 && cacheableResponse(resp) {
		key := m.cacheKey
		if key == "" {
			key = cache.KeyForRequestNS(m.cacheNS, req)
		}
		if err := h.SemanticCache.UpsertNS(ctx, cache.ScopedNamespace(m.cacheNS, req.Model), key, semanticText(req), respBytes, map[string]interface{}{"model": req.Model}); err != nil {
			h.Logger.Error("semantic cache store failed", "error", err)
		}
	}

	if h.UsageRecorder != nil && m.keyID != "" {
		res, err := h.UsageRecorder.Record(ctx, finops.CostRequest{
			RequestID: m.requestID,
			APIKey:    m.keyID,
			Model:     resp.Model,
			Usage:     resp.Usage,
			Timestamp: time.Now().UTC(),
			CostUSD:   cost,
		})
		if err != nil {
			h.Logger.Error("usage recording failed", "error", err)
		} else if cost == 0 && res.CostUSD > 0 {
			cost = res.CostUSD
		}
	}
	if h.Analytics != nil {
		h.Analytics.RecordFromUsage(resp.ID, m.keyID, "", m.teamID, resp.Model, providerName, resp.Usage, cost)
	}
	if h.UsageSink != nil && m.keyID != "" && resp.Usage != nil {
		select {
		case h.UsageSink <- billing.MeterEntry{CustomerID: m.keyID, EventName: "tokens", Value: float64(resp.Usage.TotalTokens), Timestamp: time.Now().UTC()}:
		default: // never block the request path on a full sink
		}
	}

	h.emitUsage(ctx, UsageEvent{
		RequestID: m.requestID, Endpoint: m.endpoint, Principal: m.principal, KeyID: m.keyID, TeamID: m.teamID,
		Model: resp.Model, Provider: providerName, Usage: resp.Usage, UsageEstimated: estimated, CostUSD: cost,
		Latency: latency, Stream: stream, Request: req, Response: resp,
	})

	if h.CallbackMgr != nil {
		h.CallbackMgr.FireSuccess(callbackRequest(req, m, providerName, cost), callbackResponse(resp, providerName, latency, stream))
	}

	if h.Ledger != nil {
		reqBytes, _ := json.Marshal(req)
		if err := h.appendLedger(ctx, string(reqBytes), string(respBytes)); err != nil {
			h.Logger.Error("ledger append failed", "error", err)
		}
	}
}

// cacheableResponse reports whether a response is worth caching: complete
// answers only, never truncated or tool-call turns.
func cacheableResponse(resp *models.LLMResponse) bool {
	if resp == nil || len(resp.Choices) == 0 {
		return false
	}
	for _, c := range resp.Choices {
		if c.FinishReason == "length" || c.FinishReason == "tool_calls" || len(c.Message.ToolCalls) > 0 {
			return false
		}
	}
	return true
}

// chainedAppender is implemented by ledgers that can link records atomically.
type chainedAppender interface {
	AppendChained(ctx context.Context, requestPayload, responsePayload string) (ledger.LedgerRecord, error)
}

func (h *Handler) appendLedger(ctx context.Context, reqPayload, respPayload string) error {
	if ca, ok := h.Ledger.(chainedAppender); ok {
		_, err := ca.AppendChained(ctx, reqPayload, respPayload)
		return err
	}
	latest, _ := h.Ledger.Latest(ctx)
	prevHash := ""
	if latest != nil {
		prevHash = latest.ChainHash
	}
	return h.Ledger.Append(ctx, ledger.LedgerRecord{
		Timestamp:       time.Now().UTC(),
		PrevHash:        prevHash,
		RequestPayload:  reqPayload,
		ResponsePayload: respPayload,
		ChainHash:       ledger.ComputeChainHash(prevHash, reqPayload, respPayload),
	})
}

func (h *Handler) emitUsage(ctx context.Context, ev UsageEvent) {
	if h.OnUsage == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			h.Logger.Error("usage hook panicked", "panic", p)
		}
	}()
	h.OnUsage(ctx, ev)
}

// recordFailure reports a failed completion to telemetry and callbacks.
func (h *Handler) recordFailure(ctx context.Context, req *models.LLMRequest, m *requestMeta, provider providers.Provider, err error) {
	telemetry.RecordError()
	status, _, _ := upstreamStatus(err)
	if status == 499 {
		return
	}
	h.Logger.Error("completion failed", "model", req.Model, "provider", providerNameOf(provider), "request_id", m.requestID, "error", err)
	if h.CallbackMgr != nil {
		h.CallbackMgr.FireError(callbackRequest(req, m, providerNameOf(provider), 0), err)
	}
}

func callbackRequest(req *models.LLMRequest, m *requestMeta, provider string, cost float64) *callbacks.CallbackRequestData {
	msgs := make([]map[string]interface{}, 0, len(req.Messages))
	for _, msg := range req.Messages {
		msgs = append(msgs, map[string]interface{}{"role": string(msg.Role), "content": msg.TextContent()})
	}
	return &callbacks.CallbackRequestData{
		RequestID: m.requestID,
		Model:     req.Model,
		Provider:  provider,
		Messages:  msgs,
		CostUSD:   cost,
		Timestamp: m.start,
	}
}

func callbackResponse(resp *models.LLMResponse, provider string, latency time.Duration, stream bool) *callbacks.CallbackResponseData {
	usage := map[string]interface{}{}
	tokens := map[string]int{}
	if resp.Usage != nil {
		usage["prompt_tokens"] = resp.Usage.PromptTokens
		usage["completion_tokens"] = resp.Usage.CompletionTokens
		usage["total_tokens"] = resp.Usage.TotalTokens
		tokens["input"] = resp.Usage.PromptTokens
		tokens["output"] = resp.Usage.CompletionTokens
	}
	return &callbacks.CallbackResponseData{
		ResponseID: resp.ID,
		Usage:      usage,
		Model:      resp.Model,
		Provider:   provider,
		LatencyMs:  latency.Milliseconds(),
		TokenCount: tokens,
		Metadata:   map[string]interface{}{"stream": stream},
		Timestamp:  time.Now(),
	}
}
