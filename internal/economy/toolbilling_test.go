package economy

import (
	"context"
	"errors"
	"testing"
)

func TestBillToolCallChargesCallerAndCreditsCreator(t *testing.T) {
	store := NewInMemoryWalletStore()
	_ = store.SetBalance(context.Background(), "caller", 10)
	_ = store.SetBalance(context.Background(), "creator", 0)

	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 2, CreatorID: "creator"})

	billing := NewToolCallBilling(store, pricing)
	if err := billing.BillToolCall(context.Background(), "caller", "weather"); err != nil {
		t.Fatalf("bill failed: %v", err)
	}

	callerBal, _ := store.Balance(context.Background(), "caller")
	creatorBal, _ := store.Balance(context.Background(), "creator")
	if callerBal != 8 || creatorBal != 2 {
		t.Fatalf("unexpected balances: caller=%f creator=%f", callerBal, creatorBal)
	}
}

func TestBillToolCallSkipsMissingPricing(t *testing.T) {
	store := NewInMemoryWalletStore()
	billing := NewToolCallBilling(store, NewInMemoryPricingStore())
	if err := billing.BillToolCall(context.Background(), "caller", "weather"); err != nil {
		t.Fatalf("expected nil for missing pricing, got %v", err)
	}
}

func TestBillToolCallSkipsFreeTool(t *testing.T) {
	store := NewInMemoryWalletStore()
	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 0, CreatorID: "creator"})
	billing := NewToolCallBilling(store, pricing)
	if err := billing.BillToolCall(context.Background(), "caller", "weather"); err != nil {
		t.Fatalf("expected nil for zero price, got %v", err)
	}
}

func TestToolCallInterceptorMarksBilled(t *testing.T) {
	store := NewInMemoryWalletStore()
	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 1, CreatorID: "creator"})
	interceptor := ToolCallInterceptor(NewToolCallBilling(store, pricing))

	payload := map[string]interface{}{
		"tenant_id": "caller",
		"tool_name": "weather",
	}
	out, err := interceptor(context.Background(), payload)
	if err != nil {
		t.Fatalf("interceptor failed: %v", err)
	}
	if billed, _ := out["economy_billed"].(bool); !billed {
		t.Fatalf("expected economy_billed flag")
	}
}

func TestBillToolCallReturnsWalletError(t *testing.T) {
	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 1, CreatorID: "creator"})
	billing := NewToolCallBilling(&failingWalletStore{}, pricing)
	err := billing.BillToolCall(context.Background(), "caller", "weather")
	if err == nil {
		t.Fatalf("expected wallet error")
	}
}

type failingWalletStore struct{}

func (f *failingWalletStore) Wallet(ctx context.Context, id WalletID) (Wallet, error) {
	return nil, errors.New("wallet error")
}

func (f *failingWalletStore) Balance(ctx context.Context, id WalletID) (float64, error) {
	return 0, nil
}

func (f *failingWalletStore) SetBalance(ctx context.Context, id WalletID, balance float64) error {
	return nil
}

func (f *failingWalletStore) AppendTransaction(ctx context.Context, tx Transaction) error {
	return nil
}

func (f *failingWalletStore) Transactions(ctx context.Context, id WalletID, limit int) ([]Transaction, error) {
	return nil, nil
}
