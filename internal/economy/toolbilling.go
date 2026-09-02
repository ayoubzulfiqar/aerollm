package economy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// PluginPricing captures commercial metadata for a tool/plugin.
type PluginPricing struct {
	PluginID     string
	PricePerCall float64
	CreatorID    string
}

// PricingStore retrieves pricing metadata for plugins/tools.
type PricingStore interface {
	PluginPricing(ctx context.Context, pluginID string) (PluginPricing, bool)
}

// InMemoryPricingStore is a development pricing store.
type InMemoryPricingStore struct {
	mu      sync.RWMutex
	pricing map[string]PluginPricing
}

// NewInMemoryPricingStore creates a new in-memory pricing store.
func NewInMemoryPricingStore() *InMemoryPricingStore {
	return &InMemoryPricingStore{pricing: make(map[string]PluginPricing)}
}

// Set stores or updates plugin pricing.
func (s *InMemoryPricingStore) Set(p PluginPricing) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pricing[strings.ToLower(p.PluginID)] = p
}

// PluginPricing returns pricing if available.
func (s *InMemoryPricingStore) PluginPricing(_ context.Context, pluginID string) (PluginPricing, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pricing[strings.ToLower(pluginID)]
	return p, ok
}

// ToolCallBilling records micro-transactions for tool execution.
type ToolCallBilling struct {
	wallets  WalletStore
	pricing  PricingStore
	currency string
}

// WalletStore resolves wallets by identifier.
type WalletStore interface {
	Wallet(ctx context.Context, id WalletID) (Wallet, error)
}

// NewToolCallBilling creates a new tool billing interceptor.
func NewToolCallBilling(wallets WalletStore, pricing PricingStore) *ToolCallBilling {
	return &ToolCallBilling{wallets: wallets, pricing: pricing, currency: "USD"}
}

// BillToolCall charges the caller wallet and credits the creator wallet.
func (b *ToolCallBilling) BillToolCall(ctx context.Context, callerID WalletID, toolName string) error {
	if b.wallets == nil || b.pricing == nil {
		return nil
	}
	pricing, ok := b.pricing.PluginPricing(ctx, toolName)
	if !ok || pricing.PricePerCall <= 0 {
		return nil
	}

	callerWallet, err := b.wallets.Wallet(ctx, callerID)
	if err != nil {
		return fmt.Errorf("economy: resolve caller wallet: %w", err)
	}

	_, err = callerWallet.Debit(ctx, pricing.PricePerCall, fmt.Sprintf("tool_call:%s", toolName))
	if err != nil {
		return err
	}

	if pricing.CreatorID != "" {
		creatorWallet, err := b.wallets.Wallet(ctx, WalletID(pricing.CreatorID))
		if err == nil {
			_, _ = creatorWallet.Credit(ctx, pricing.PricePerCall, fmt.Sprintf("tool_call:%s", toolName))
		}
	}

	return nil
}

// ToolCallEvent represents an intercepted tool call for economy evaluation.
type ToolCallEvent struct {
	TenantID WalletID
	ToolName string
}

// ToolCallInterceptor returns a plugins.Hook-compatible interceptor payload helper.
//
// It decodes a tool-call payload, bills the caller, and returns the mutated payload
// map so plugin hosts can mark the event as billed.
func ToolCallInterceptor(billing *ToolCallBilling) func(context.Context, map[string]interface{}) (map[string]interface{}, error) {
	return func(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
		if billing == nil {
			return payload, nil
		}
		event, err := decodeToolCallEvent(payload)
		if err != nil {
			return payload, nil
		}
		if err := billing.BillToolCall(ctx, event.TenantID, event.ToolName); err != nil {
			return payload, err
		}
		payload["economy_billed"] = true
		return payload, nil
	}
}

func decodeToolCallEvent(payload map[string]interface{}) (ToolCallEvent, error) {
	var event ToolCallEvent
	b, err := json.Marshal(payload)
	if err != nil {
		return event, err
	}
	if err := json.Unmarshal(b, &event); err != nil {
		return event, err
	}
	return event, nil
}
