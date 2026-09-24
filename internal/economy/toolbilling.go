package economy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

// Set stores or updates plugin pricing. Prices that are NaN, infinite or
// negative are rejected (returns false).
func (s *InMemoryPricingStore) Set(p PluginPricing) bool {
	if math.IsNaN(p.PricePerCall) || math.IsInf(p.PricePerCall, 0) || p.PricePerCall < 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pricing[strings.ToLower(p.PluginID)] = p
	return true
}

// PluginPricing returns pricing if available.
func (s *InMemoryPricingStore) PluginPricing(_ context.Context, pluginID string) (PluginPricing, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pricing[strings.ToLower(pluginID)]
	return p, ok
}

// WalletStore resolves wallets by identifier.
type WalletStore interface {
	Wallet(ctx context.Context, id WalletID) (Wallet, error)
}

// maxBilledCallIDs bounds the de-duplication memory of ToolCallBilling.
const maxBilledCallIDs = 10_000

// ToolCallBilling records micro-transactions for tool execution.
type ToolCallBilling struct {
	wallets  WalletStore
	pricing  PricingStore
	currency string

	mu     sync.Mutex
	billed map[string]struct{}
	order  []string
}

// NewToolCallBilling creates a new tool billing interceptor.
func NewToolCallBilling(wallets WalletStore, pricing PricingStore) *ToolCallBilling {
	return &ToolCallBilling{wallets: wallets, pricing: pricing, currency: "USD", billed: map[string]struct{}{}}
}

// BillToolCall charges the caller wallet and credits the creator wallet.
func (b *ToolCallBilling) BillToolCall(ctx context.Context, callerID WalletID, toolName string) error {
	return b.BillToolCallOnce(ctx, callerID, toolName, "")
}

// BillToolCallOnce is BillToolCall with an idempotency key: a non-empty
// callID that was already billed successfully is not charged again.
//
// The caller is charged the tool's price and the creator (if any) is
// credited. With an AtomicLedgerStore-backed WalletStore (such as
// InMemoryWalletStore) the move is one atomic transfer; otherwise the
// caller is debited first and refunded if crediting the creator fails, so
// money is never created or destroyed.
func (b *ToolCallBilling) BillToolCallOnce(ctx context.Context, callerID WalletID, toolName, callID string) error {
	if b == nil || b.wallets == nil || b.pricing == nil || toolName == "" {
		return nil
	}
	pricing, ok := b.pricing.PluginPricing(ctx, toolName)
	if !ok {
		return nil
	}
	price := pricing.PricePerCall
	if math.IsNaN(price) || math.IsInf(price, 0) || price < 0 {
		return fmt.Errorf("%w: price for tool %q", ErrInvalidAmount, toolName)
	}
	if price == 0 {
		return nil
	}
	if callerID == "" {
		return fmt.Errorf("economy: missing caller wallet for priced tool %q", toolName)
	}
	if callID != "" && !b.reserve(callID) {
		return nil // already billed
	}
	err := b.charge(ctx, callerID, WalletID(pricing.CreatorID), toolName, price)
	if err != nil && callID != "" {
		b.release(callID)
	}
	return err
}

func (b *ToolCallBilling) charge(ctx context.Context, callerID, creatorID WalletID, toolName string, price float64) error {
	reason := fmt.Sprintf("tool_call:%s", toolName)
	if creatorID == callerID {
		return nil // a creator using their own tool would pay themselves
	}
	callerWallet, err := b.wallets.Wallet(ctx, callerID)
	if err != nil {
		return fmt.Errorf("economy: resolve caller wallet: %w", err)
	}
	if as, ok := b.wallets.(AtomicLedgerStore); ok && creatorID != "" {
		_, err := Transfer(ctx, as, callerID, creatorID, price, reason)
		return err
	}
	if _, err := callerWallet.Debit(ctx, price, reason); err != nil {
		return err
	}
	if creatorID == "" {
		return nil
	}
	creatorWallet, err := b.wallets.Wallet(ctx, creatorID)
	if err == nil {
		_, err = creatorWallet.Credit(ctx, price, reason)
	}
	if err != nil {
		if _, rerr := callerWallet.Credit(ctx, price, "refund:"+reason); rerr != nil {
			return errors.Join(fmt.Errorf("economy: credit creator: %w", err), fmt.Errorf("economy: refund caller: %w", rerr))
		}
		return fmt.Errorf("economy: credit creator (caller refunded): %w", err)
	}
	return nil
}

func (b *ToolCallBilling) reserve(callID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.billed == nil {
		b.billed = map[string]struct{}{}
	}
	if _, dup := b.billed[callID]; dup {
		return false
	}
	b.billed[callID] = struct{}{}
	b.order = append(b.order, callID)
	if len(b.order) > maxBilledCallIDs {
		delete(b.billed, b.order[0])
		b.order = b.order[1:]
	}
	return true
}

func (b *ToolCallBilling) release(callID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.billed, callID)
}

// ToolCallEvent represents an intercepted tool call for economy evaluation.
type ToolCallEvent struct {
	TenantID WalletID `json:"tenant_id"`
	ToolName string   `json:"tool_name"`
	// CallID optionally identifies the tool call for idempotent billing.
	CallID string `json:"call_id,omitempty"`
}

// ToolCallInterceptor returns a plugins.Hook-compatible interceptor payload helper.
//
// It decodes a tool-call payload ({"tenant_id", "tool_name", "call_id"}),
// bills the caller, and returns a copy of the payload with
// "economy_billed": true. Payloads without a tool name pass through
// unchanged; billing failures (e.g. insufficient balance) are returned.
func ToolCallInterceptor(billing *ToolCallBilling) func(context.Context, map[string]interface{}) (map[string]interface{}, error) {
	return func(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
		if billing == nil {
			return payload, nil
		}
		event, err := decodeToolCallEvent(payload)
		if err != nil {
			return payload, fmt.Errorf("economy: decode tool call: %w", err)
		}
		if event.ToolName == "" {
			return payload, nil
		}
		if err := billing.BillToolCallOnce(ctx, event.TenantID, event.ToolName, event.CallID); err != nil {
			return payload, err
		}
		out := make(map[string]interface{}, len(payload)+1)
		for k, v := range payload {
			out[k] = v
		}
		out["economy_billed"] = true
		return out, nil
	}
}

func decodeToolCallEvent(payload map[string]interface{}) (ToolCallEvent, error) {
	var event ToolCallEvent
	if payload == nil {
		return event, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return event, err
	}
	if err := json.Unmarshal(b, &event); err != nil {
		return event, err
	}
	return event, nil
}
