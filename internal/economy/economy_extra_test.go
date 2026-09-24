package economy

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

// plainStore is a LedgerStore without TransferNanos (exercises the
// in-process locked fallback used by e.g. cmd/edge-node's bbolt store).
type plainStore struct {
	mu       sync.Mutex
	balances map[WalletID]float64
	txs      []Transaction
	failTx   bool
}

func newPlainStore() *plainStore { return &plainStore{balances: map[WalletID]float64{}} }

func (p *plainStore) Balance(_ context.Context, id WalletID) (float64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.balances[id], nil
}
func (p *plainStore) SetBalance(_ context.Context, id WalletID, b float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.balances[id] = b
	return nil
}
func (p *plainStore) AppendTransaction(_ context.Context, tx Transaction) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failTx {
		return errors.New("disk full")
	}
	p.txs = append(p.txs, tx)
	return nil
}
func (p *plainStore) Transactions(_ context.Context, id WalletID, limit int) ([]Transaction, error) {
	return nil, nil
}

func concurrentDebits(t *testing.T, store LedgerStore) {
	t.Helper()
	ctx := context.Background()
	w := NewDefaultWallet("t", store)
	if _, err := w.Credit(ctx, 100, "seed"); err != nil {
		t.Fatal(err)
	}
	var ok, insufficient atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine uses its own wallet handle, like callers of
			// WalletStore.Wallet do.
			_, err := NewDefaultWallet("t", store).Debit(ctx, 1, "spend")
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrInsufficientBalance):
				insufficient.Add(1)
			default:
				t.Errorf("unexpected error %v", err)
			}
		}()
	}
	wg.Wait()
	bal, _ := w.Balance(ctx)
	if ok.Load() != 100 || insufficient.Load() != 100 || bal != 0 {
		t.Fatalf("double spend: ok=%d insufficient=%d balance=%v", ok.Load(), insufficient.Load(), bal)
	}
}

func TestConcurrentDebitsNoOverdraftAtomicStore(t *testing.T) {
	concurrentDebits(t, NewInMemoryWalletStore())
}

func TestConcurrentDebitsNoOverdraftPlainStore(t *testing.T) {
	concurrentDebits(t, newPlainStore())
}

func TestConcurrentCreditsNoLostUpdates(t *testing.T) {
	for name, store := range map[string]LedgerStore{"atomic": NewInMemoryWalletStore(), "plain": newPlainStore()} {
		ctx := context.Background()
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = NewDefaultWallet("c", store).Credit(ctx, 0.1, "tip")
			}()
		}
		wg.Wait()
		bal, _ := store.Balance(ctx, "c")
		if bal != 10 {
			t.Fatalf("%s: lost updates or float drift: balance=%v", name, bal)
		}
	}
}

func TestInvalidAmountsRejected(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryWalletStore()
	w := NewDefaultWallet("x", store)
	_, _ = w.Credit(ctx, 10, "seed")
	for _, amt := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1), 1e-12, 1e300} {
		if _, err := w.Credit(ctx, amt, "bad"); !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("credit %v: expected ErrInvalidAmount, got %v", amt, err)
		}
		if _, err := w.Debit(ctx, amt, "bad"); !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("debit %v: expected ErrInvalidAmount, got %v", amt, err)
		}
	}
	if err := store.SetBalance(ctx, "x", math.NaN()); !errors.Is(err, ErrInvalidAmount) {
		t.Fatal("NaN balance accepted")
	}
	if err := store.SetBalance(ctx, "x", -5); !errors.Is(err, ErrInvalidAmount) {
		t.Fatal("negative balance accepted")
	}
	if bal, _ := w.Balance(ctx); bal != 10 {
		t.Fatalf("balance corrupted: %v", bal)
	}
	tx, err := w.Debit(ctx, 0.000000001, "tiny")
	if err != nil || tx.AmountNanos != 1 {
		t.Fatalf("smallest unit debit failed: %+v %v", tx, err)
	}
}

func TestPlainStoreRollsBackWhenTransactionAppendFails(t *testing.T) {
	ctx := context.Background()
	store := newPlainStore()
	w := NewDefaultWallet("r", store)
	_, _ = w.Credit(ctx, 5, "seed")
	store.failTx = true
	if _, err := w.Debit(ctx, 2, "x"); err == nil {
		t.Fatal("expected error")
	}
	if bal, _ := w.Balance(ctx); bal != 5 {
		t.Fatalf("balance changed without a ledger entry: %v", bal)
	}
}

func TestTransfer(t *testing.T) {
	ctx := context.Background()
	for name, store := range map[string]LedgerStore{"atomic": NewInMemoryWalletStore(), "plain": newPlainStore()} {
		_, _ = NewDefaultWallet("a", store).Credit(ctx, 3, "seed")
		if _, err := Transfer(ctx, store, "a", "b", 2, "pay"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := Transfer(ctx, store, "a", "b", 2, "pay"); !errors.Is(err, ErrInsufficientBalance) {
			t.Fatalf("%s: expected insufficient balance, got %v", name, err)
		}
		a, _ := store.Balance(ctx, "a")
		b, _ := store.Balance(ctx, "b")
		if a != 1 || b != 2 {
			t.Fatalf("%s: a=%v b=%v", name, a, b)
		}
	}
}

// creatorFailingStore wraps a plain store and fails to resolve the creator.
type creatorFailingStore struct{ *plainStore }

func (c creatorFailingStore) Wallet(_ context.Context, id WalletID) (Wallet, error) {
	if id == "creator" {
		return nil, errors.New("creator unknown")
	}
	return NewDefaultWallet(id, c.plainStore), nil
}

func TestBillToolCallRefundsCallerWhenCreatorCreditFails(t *testing.T) {
	ctx := context.Background()
	store := creatorFailingStore{newPlainStore()}
	_ = store.SetBalance(ctx, "caller", 10)
	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 2, CreatorID: "creator"})
	err := NewToolCallBilling(store, pricing).BillToolCall(ctx, "caller", "weather")
	if err == nil {
		t.Fatal("expected creator credit failure")
	}
	if bal, _ := store.Balance(ctx, "caller"); bal != 10 {
		t.Fatalf("caller not refunded: %v", bal)
	}
}

func TestBillToolCallIdempotentAndValidated(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryWalletStore()
	_ = store.SetBalance(ctx, "caller", 10)
	pricing := NewInMemoryPricingStore()
	if pricing.Set(PluginPricing{PluginID: "bad", PricePerCall: math.NaN()}) {
		t.Fatal("NaN price accepted")
	}
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 1, CreatorID: "creator"})
	billing := NewToolCallBilling(store, pricing)
	for i := 0; i < 3; i++ {
		if err := billing.BillToolCallOnce(ctx, "caller", "weather", "call-1"); err != nil {
			t.Fatal(err)
		}
	}
	if bal, _ := store.Balance(ctx, "caller"); bal != 9 {
		t.Fatalf("same call billed more than once: %v", bal)
	}
	if err := billing.BillToolCall(ctx, "", "weather"); err == nil {
		t.Fatal("priced tool without caller must fail")
	}
	nanPricing := pricingFunc(func(string) (PluginPricing, bool) { return PluginPricing{PricePerCall: math.NaN()}, true })
	if err := NewToolCallBilling(store, nanPricing).BillToolCall(ctx, "caller", "x"); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("NaN price must be rejected, got %v", err)
	}
	if bal, _ := store.Balance(ctx, "caller"); bal != 9 || math.IsNaN(bal) {
		t.Fatalf("balance corrupted: %v", bal)
	}
}

type pricingFunc func(string) (PluginPricing, bool)

func (f pricingFunc) PluginPricing(_ context.Context, id string) (PluginPricing, bool) { return f(id) }

func TestInterceptorInsufficientBalanceAndPassthrough(t *testing.T) {
	store := NewInMemoryWalletStore()
	pricing := NewInMemoryPricingStore()
	pricing.Set(PluginPricing{PluginID: "weather", PricePerCall: 1})
	icpt := ToolCallInterceptor(NewToolCallBilling(store, pricing))
	if _, err := icpt(context.Background(), map[string]interface{}{"tenant_id": "poor", "tool_name": "weather"}); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("expected insufficient balance, got %v", err)
	}
	out, err := icpt(context.Background(), map[string]interface{}{"other": 1})
	if err != nil || out["economy_billed"] != nil {
		t.Fatalf("payload without tool must pass through: %v %v", out, err)
	}
}
