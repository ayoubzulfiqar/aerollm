// ToolProvider adapters bridge the [agent.ToolProvider] interface
// with the [providers.Provider] and [router.Router] interfaces.
//
// The [agent.ToolProvider] interface requires a `CallLLM` method,
// but [providers.Provider] exposes `ChatCompletions`. This file provides
// adapters so that existing provider implementations can be used directly
// in the [agent.AgentEngine] tool execution loop.

package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

// RouterAdapter wraps a [router.Router] to satisfy the
// [agent.ToolProvider] interface (which requires `CallLLM`).
//
// When the agent loop calls CallLLM, this adapter routes the request
// through the router's strategy with fallback: retryable failures
// (429/5xx/timeouts/open circuits) move on to the next provider.
//
// If no providers are registered, it returns an error so the agent
// loop fails gracefully rather than hanging.
type RouterAdapter struct {
	router *router.Router
}

// NewRouterAdapter creates a new [RouterAdapter] wrapping the given router.
func NewRouterAdapter(r *router.Router) *RouterAdapter {
	return &RouterAdapter{router: r}
}

// CallLLM routes the request through the router (with provider fallback)
// and returns the first successful response. The upstream-reported model
// is preserved; it is only filled from the request when empty.
//
// This satisfies the [agent.ToolProvider] interface.
func (a *RouterAdapter) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if a == nil || a.router == nil {
		return nil, errors.New("router is not initialized")
	}
	if req == nil {
		return nil, errors.New("nil LLM request")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	resp, provider, err := a.router.ChatCompletionsWithFallback(ctx, req)
	if err != nil {
		if provider != nil {
			return nil, fmt.Errorf("provider %s error: %w", provider.Name(), err)
		}
		return nil, fmt.Errorf("routing failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("provider %s returned nil response", provider.Name())
	}
	if resp.Model == "" {
		resp.Model = req.Model
	}
	return resp, nil
}
