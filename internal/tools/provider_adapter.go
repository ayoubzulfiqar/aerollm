/// ToolProvider adapters bridge the [agent.ToolProvider] interface
/// with the [providers.Provider] and [router.Router] interfaces.
///
/// The [agent.ToolProvider] interface requires a `CallLLM` method,
/// but [providers.Provider] exposes `ChatCompletions`. This file provides
/// adapters so that existing provider implementations can be used directly
/// in the [agent.AgentEngine] tool execution loop.
package tools

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

/// RouterAdapter wraps a [router.Router] to satisfy the
/// [agent.ToolProvider] interface (which requires `CallLLM`).
///
/// When the agent loop calls CallLLM, this adapter routes the request
/// to the best available provider via the router's routing strategy
/// (round-robin, latency-based, cost-based, or fallback).
///
/// If no providers are registered, it returns an error so the agent
/// loop fails gracefully rather than hanging.
type RouterAdapter struct {
	router *router.Router
}

/// NewRouterAdapter creates a new [RouterAdapter] wrapping the given router.
func NewRouterAdapter(r *router.Router) *RouterAdapter {
	return &RouterAdapter{router: r}
}

/// CallLLM routes the request through the router to select an appropriate
/// provider, then delegates to that provider's ChatCompletions method.
///
/// This satisfies the [agent.ToolProvider] interface.
func (a *RouterAdapter) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if a.router == nil {
		return nil, fmt.Errorf("router is not initialized")
	}

	// Route to the best available provider.
	provider, err := a.router.Route(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("routing failed: %w", err)
	}

	if provider == nil {
		return nil, fmt.Errorf("no available provider for routing")
	}

	// Delegate to the selected provider's ChatCompletions.
	resp, err := provider.ChatCompletions(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("provider %s error: %w", provider.Name(), err)
	}

	if resp == nil {
		return nil, fmt.Errorf("provider %s returned nil response", provider.Name())
	}

	// Ensure the provider name is set on the response.
	resp.Model = provider.Name()

	return resp, nil
}
