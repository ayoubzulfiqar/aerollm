package billing

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v80"
	"github.com/stripe/stripe-go/v80/form"
)

func TestInMemoryProviderRecordsMeterEntries(t *testing.T) {
	p := NewInMemoryProvider()
	if err := p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c1", EventName: "token", Value: 1}}); err != nil {
		t.Fatalf("sync meter failed: %v", err)
	}
	items := p.Snapshot()
	if len(items) != 1 || items[0].CustomerID != "c1" || items[0].Value != 1 {
		t.Fatalf("unexpected snapshot: %+v", items)
	}
}

func TestInMemoryProviderRejectsInvalidAndDedupes(t *testing.T) {
	p := NewInMemoryProvider()
	ctx := context.Background()
	bad := [][]MeterEntry{
		{{CustomerID: "c", EventName: "e", Value: -1}},
		{{CustomerID: "c", EventName: "e", Value: math.NaN()}},
		{{CustomerID: "c", EventName: "e", Value: math.Inf(1)}},
		{{CustomerID: "", EventName: "e", Value: 1}},
		{{CustomerID: "c", EventName: "", Value: 1}},
		{{CustomerID: "c", EventName: "e", Value: 1}, {CustomerID: "c", EventName: "e", Value: -2}},
	}
	for i, b := range bad {
		if err := p.SyncMeter(ctx, b); !errors.Is(err, ErrInvalidMeterEntry) {
			t.Fatalf("case %d: expected ErrInvalidMeterEntry, got %v", i, err)
		}
	}
	if len(p.Snapshot()) != 0 {
		t.Fatal("invalid batches must be rejected atomically")
	}
	e := MeterEntry{CustomerID: "c", EventName: "e", Value: 1, ID: "evt-1"}
	_ = p.SyncMeter(ctx, []MeterEntry{e})
	_ = p.SyncMeter(ctx, []MeterEntry{e})
	if len(p.Snapshot()) != 1 {
		t.Fatal("entries with the same ID must be de-duplicated")
	}
}

func TestInMemoryProviderConcurrent(t *testing.T) {
	p := NewInMemoryProvider()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = p.AppendMeter(context.Background(), MeterEntry{CustomerID: "c", EventName: "e", Value: 1})
				_ = p.Snapshot()
			}
		}()
	}
	wg.Wait()
	if len(p.Snapshot()) != 800 {
		t.Fatalf("expected 800 entries, got %d", len(p.Snapshot()))
	}
}

func TestInvoiceGeneratorAggregatesUsage(t *testing.T) {
	prov := NewInMemoryProvider()
	g := NewInvoiceGenerator(prov)
	inv, err := g.Generate(context.Background(), []MeterEntry{
		{CustomerID: "c1", EventName: "token", Value: 2},
		{CustomerID: "c1", EventName: "token", Value: 3},
		{CustomerID: "c1", EventName: "image", Value: 1},
	})
	if err != nil {
		t.Fatalf("generate invoice failed: %v", err)
	}
	if inv.TotalUSD != 6 || len(inv.Lines) != 2 || inv.CustomerID != "c1" {
		t.Fatalf("unexpected invoice: %+v", inv)
	}
	if inv.Lines[0].EventName != "token" || inv.Lines[0].Quantity != 5 {
		t.Fatalf("lines not aggregated per event: %+v", inv.Lines)
	}
	if got := prov.Snapshot(); len(got) != 3 {
		t.Fatalf("usage must be synced to the provider, got %d entries", len(got))
	}
}

func TestInvoiceGeneratorUnitPricesAndPeriod(t *testing.T) {
	g := NewInvoiceGenerator(NewInMemoryProvider())
	g.UnitPrices = map[string]float64{"token": 0.002}
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	inv, err := g.Generate(context.Background(), []MeterEntry{
		{CustomerID: "c1", EventName: "token", Value: 1000, Timestamp: t0.Add(time.Hour)},
		{CustomerID: "c1", EventName: "token", Value: 500, Timestamp: t0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.TotalUSD != 3 {
		t.Fatalf("expected 3 USD, got %v", inv.TotalUSD)
	}
	if !inv.PeriodStart.Equal(t0) || !inv.PeriodEnd.Equal(t0.Add(time.Hour)) {
		t.Fatalf("period not derived from entries: %v - %v", inv.PeriodStart, inv.PeriodEnd)
	}
}

func TestInvoiceGeneratorRejectsBadInput(t *testing.T) {
	g := NewInvoiceGenerator(NewInMemoryProvider())
	ctx := context.Background()
	if _, err := g.Generate(ctx, nil); !errors.Is(err, ErrNoUsage) {
		t.Fatalf("expected ErrNoUsage for empty meters, got %v", err)
	}
	if _, err := g.Generate(ctx, []MeterEntry{{CustomerID: "c", EventName: "e", Value: 0}}); !errors.Is(err, ErrNoUsage) {
		t.Fatalf("expected ErrNoUsage for zero usage, got %v", err)
	}
	if _, err := g.Generate(ctx, []MeterEntry{{CustomerID: "c", EventName: "e", Value: -5}}); !errors.Is(err, ErrInvalidMeterEntry) {
		t.Fatalf("expected ErrInvalidMeterEntry, got %v", err)
	}
	if _, err := g.Generate(ctx, []MeterEntry{{CustomerID: "a", EventName: "e", Value: 1}, {CustomerID: "b", EventName: "e", Value: 1}}); !errors.Is(err, ErrMixedCustomers) {
		t.Fatalf("expected ErrMixedCustomers, got %v", err)
	}
	if _, err := NewInvoiceGenerator(nil).Generate(ctx, []MeterEntry{{CustomerID: "a", EventName: "e", Value: 1}}); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestInvoiceGeneratorIdempotentRetry(t *testing.T) {
	prov := NewInMemoryProvider()
	g := NewInvoiceGenerator(prov)
	ts := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	entries := []MeterEntry{{CustomerID: "c", EventName: "e", Value: 2, Timestamp: ts}}
	a, err := g.Generate(context.Background(), entries)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Generate(context.Background(), entries)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatal("same input must yield the same invoice ID")
	}
	if len(prov.Snapshot()) != 1 {
		t.Fatalf("retry double-billed: %d entries", len(prov.Snapshot()))
	}
}

func TestGenerateAllPerCustomer(t *testing.T) {
	g := NewInvoiceGenerator(NewInMemoryProvider())
	invs, err := g.GenerateAll(context.Background(), []MeterEntry{
		{CustomerID: "b", EventName: "e", Value: 1},
		{CustomerID: "a", EventName: "e", Value: 2},
		{CustomerID: "c", EventName: "e", Value: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 2 || invs[0].CustomerID != "a" || invs[1].CustomerID != "b" {
		t.Fatalf("unexpected invoices %+v", invs)
	}
}

type failingProvider struct{}

func (failingProvider) SyncMeter(context.Context, []MeterEntry) error { return errors.New("down") }

func TestInvoiceGeneratorPropagatesSyncFailure(t *testing.T) {
	g := NewInvoiceGenerator(failingProvider{})
	if _, err := g.Generate(context.Background(), []MeterEntry{{CustomerID: "a", EventName: "e", Value: 1}}); err == nil {
		t.Fatal("expected sync failure to be returned")
	}
}

// fakeStripeBackend captures meter event calls.
type fakeStripeBackend struct {
	mu    sync.Mutex
	calls []capturedCall
	fail  bool
}

type capturedCall struct {
	path, key, idem string
	form            url.Values
}

func (f *fakeStripeBackend) Call(method, path, key string, params stripe.ParamsContainer, v stripe.LastResponseSetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	values := &form.Values{}
	form.AppendTo(values, params)
	c := capturedCall{path: path, key: key, form: values.ToValues()}
	if p := params.GetParams(); p != nil && p.IdempotencyKey != nil {
		c.idem = *p.IdempotencyKey
	}
	f.calls = append(f.calls, c)
	if f.fail {
		return &stripe.Error{Msg: "boom"}
	}
	return nil
}
func (f *fakeStripeBackend) CallStreaming(string, string, string, stripe.ParamsContainer, stripe.StreamingLastResponseSetter) error {
	return nil
}
func (f *fakeStripeBackend) CallRaw(string, string, string, *form.Values, *stripe.Params, stripe.LastResponseSetter) error {
	return nil
}
func (f *fakeStripeBackend) CallMultipart(string, string, string, string, *bytes.Buffer, *stripe.Params, stripe.LastResponseSetter) error {
	return nil
}
func (f *fakeStripeBackend) SetMaxNetworkRetries(int64) {}

func TestStripeProviderSendsIdempotentMeterEvents(t *testing.T) {
	fb := &fakeStripeBackend{}
	p := NewStripeProvider("sk_test_123").WithBackend(fb)
	ts := time.Unix(1_790_000_000, 0)
	err := p.SyncMeter(context.Background(), []MeterEntry{
		{CustomerID: "cus_1", EventName: "tokens", Value: 1.5, Timestamp: ts},
		{CustomerID: "cus_1", EventName: "tokens", Value: 0, Timestamp: ts},
		{CustomerID: "cus_1", EventName: "tokens", Value: 7, ID: "fixed-id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fb.calls) != 2 {
		t.Fatalf("expected 2 calls (zero value skipped), got %d", len(fb.calls))
	}
	c := fb.calls[0]
	if c.path != "/v1/billing/meter_events" || c.key != "sk_test_123" {
		t.Fatalf("unexpected call %+v", c)
	}
	if c.form.Get("payload[value]") != "1.5" || c.form.Get("payload[stripe_customer_id]") != "cus_1" || c.form.Get("timestamp") != "1790000000" {
		t.Fatalf("unexpected form %v", c.form)
	}
	if c.form.Get("identifier") == "" || c.idem == "" {
		t.Fatal("identifier and idempotency key are required")
	}
	if fb.calls[1].form.Get("identifier") != "fixed-id" {
		t.Fatalf("explicit ID must be used as identifier: %v", fb.calls[1].form)
	}
	if stripe.Key == "sk_test_123" {
		t.Fatal("provider must not mutate the global stripe.Key")
	}

	// Retry of the same timestamped entry reuses the identifier.
	fb2 := &fakeStripeBackend{}
	p2 := NewStripeProvider("sk_test_123").WithBackend(fb2)
	e := []MeterEntry{{CustomerID: "cus_1", EventName: "tokens", Value: 1, Timestamp: ts}}
	_ = p2.SyncMeter(context.Background(), e)
	_ = p2.SyncMeter(context.Background(), e)
	if fb2.calls[0].form.Get("identifier") != fb2.calls[1].form.Get("identifier") {
		t.Fatal("identifier must be stable across retries")
	}
}

func TestStripeProviderValidation(t *testing.T) {
	fb := &fakeStripeBackend{}
	if err := NewStripeProvider("").WithBackend(fb).SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1}}); err == nil {
		t.Fatal("missing key must fail")
	}
	p := NewStripeProvider("sk").WithBackend(fb)
	err := p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1}, {CustomerID: "c", EventName: "e", Value: math.NaN()}})
	if !errors.Is(err, ErrInvalidMeterEntry) || len(fb.calls) != 0 {
		t.Fatalf("invalid batch must be rejected before sending: %v calls=%d", err, len(fb.calls))
	}
	fb.fail = true
	if err := p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1}}); err == nil {
		t.Fatal("stripe error must propagate")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.SyncMeter(ctx, []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
