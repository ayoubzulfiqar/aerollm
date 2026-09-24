package billing

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
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

func TestStripeProviderAggregatesIntegerMeterEvents(t *testing.T) {
	fb := &fakeStripeBackend{}
	p := NewStripeProvider("sk_test_123").WithBackend(fb)
	ts := time.Unix(1_790_000_000, 0)
	err := p.SyncMeter(context.Background(), []MeterEntry{
		{CustomerID: "cus_1", EventName: "tokens", Value: 1.5, Timestamp: ts},
		{CustomerID: "cus_1", EventName: "tokens", Value: 0, Timestamp: ts},
		{CustomerID: "cus_2", EventName: "tokens", Value: 3, Timestamp: ts},
		{CustomerID: "cus_1", EventName: "tokens", Value: 7, ID: "fixed-id", Timestamp: ts.Add(time.Second)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fb.calls) != 2 {
		t.Fatalf("expected one event per customer/event pair, got %d", len(fb.calls))
	}
	c := fb.calls[0]
	if c.path != "/v1/billing/meter_events" || c.key != "sk_test_123" {
		t.Fatalf("unexpected call %+v", c)
	}
	// 1.5 + 7 = 8.5 -> 8 whole units now, 0.5 carried.
	if c.form.Get("payload[value]") != "8" || c.form.Get("payload[stripe_customer_id]") != "cus_1" || c.form.Get("timestamp") != "1790000001" {
		t.Fatalf("unexpected form %v", c.form)
	}
	if c.form.Get("identifier") == "" || c.idem != "meter-"+c.form.Get("identifier") {
		t.Fatal("identifier and matching idempotency key are required")
	}
	if fb.calls[1].form.Get("payload[value]") != "3" || fb.calls[1].form.Get("payload[stripe_customer_id]") != "cus_2" {
		t.Fatalf("second pair: %v", fb.calls[1].form)
	}
	if rem := p.CarriedRemainders(); len(rem) != 1 || rem["cus_1/tokens"] != 0.5 {
		t.Fatalf("fraction must be carried: %v", rem)
	}
	if stripe.Key == "sk_test_123" {
		t.Fatal("provider must not mutate the global stripe.Key")
	}

	// The carried half unit completes with the next half.
	if err := p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus_1", EventName: "tokens", Value: 0.5, Timestamp: ts.Add(time.Minute)}}); err != nil {
		t.Fatal(err)
	}
	if len(fb.calls) != 3 || fb.calls[2].form.Get("payload[value]") != "1" {
		t.Fatalf("carried remainder must be billed: %+v", fb.calls[2:])
	}
	if rem := p.CarriedRemainders(); len(rem) != 0 {
		t.Fatalf("no remainder expected, got %v", rem)
	}
}

func TestStripeProviderRetriesAreIdempotent(t *testing.T) {
	fb := &fakeStripeBackend{}
	p := NewStripeProvider("sk_test_123").WithBackend(fb)
	ts := time.Unix(1_790_000_000, 0)
	batch := []MeterEntry{
		{CustomerID: "cus_1", EventName: "tokens", Value: 1, Timestamp: ts},
		{CustomerID: "cus_1", EventName: "tokens", Value: 2, Timestamp: ts},
	}
	_ = p.SyncMeter(context.Background(), batch)
	_ = p.SyncMeter(context.Background(), batch)
	if len(fb.calls) != 1 {
		t.Fatalf("a retried batch must not be billed twice, got %d calls", len(fb.calls))
	}
	// Explicit IDs are billed at most once, even in another batch.
	_ = p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus_1", EventName: "tokens", Value: 5, ID: "evt-1"}, {CustomerID: "cus_1", EventName: "tokens", Value: 5, ID: "evt-1"}})
	_ = p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus_1", EventName: "tokens", Value: 5, ID: "evt-1"}})
	if len(fb.calls) != 2 || fb.calls[1].form.Get("payload[value]") != "5" || fb.calls[1].form.Get("identifier") != "evt-1" {
		t.Fatalf("explicit IDs must be de-duplicated: %+v", fb.calls)
	}

	// A failed send is retried with identical parameters even when other
	// usage of the same pair was synced in between.
	fb2 := &fakeStripeBackend{fail: true}
	p2 := NewStripeProvider("sk").WithBackend(fb2)
	first := []MeterEntry{{CustomerID: "c", EventName: "e", Value: 2.5, Timestamp: ts}}
	if err := p2.SyncMeter(context.Background(), first); err == nil {
		t.Fatal("stripe error must propagate")
	}
	fb2.fail = false
	if err := p2.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1.75, Timestamp: ts.Add(time.Second)}}); err != nil {
		t.Fatal(err)
	}
	if err := p2.SyncMeter(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if len(fb2.calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(fb2.calls))
	}
	if a, b := fb2.calls[0], fb2.calls[2]; a.idem != b.idem || a.form.Get("payload[value]") != b.form.Get("payload[value]") || a.form.Get("payload[value]") != "2" {
		t.Fatalf("retry must reuse the value and idempotency key: %v / %v", a.form, b.form)
	}
	// Units sent plus the carried remainder always equal the usage
	// (2.5 + 1.75): the retry sent 2, the other sync 1, 1.25 is carried.
	var sent int64
	for _, c := range fb2.calls[1:] {
		v, _ := strconv.ParseInt(c.form.Get("payload[value]"), 10, 64)
		sent += v
	}
	if rem := p2.CarriedRemainders()["c/e"]; float64(sent)+rem != 4.25 {
		t.Fatalf("units sent (%d) + carried (%v) must equal usage 4.25", sent, rem)
	}
}

func TestStripeProviderUnitScales(t *testing.T) {
	fb := &fakeStripeBackend{}
	p := NewStripeProvider("sk").WithBackend(fb).WithUnitScale("cost_usd", MicroUSDPerUSD)
	ts := time.Unix(1_790_000_000, 0)
	var entries []MeterEntry
	for i := 0; i < 3; i++ {
		entries = append(entries, MeterEntry{CustomerID: "cus", EventName: "cost_usd", Value: 0.0000015, Timestamp: ts.Add(time.Duration(i) * time.Second)})
	}
	if err := p.SyncMeter(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	// 3 x $0.0000015 = 4.5 micro-dollars -> 4 sent, 0.5 carried.
	if len(fb.calls) != 1 || fb.calls[0].form.Get("payload[value]") != "4" {
		t.Fatalf("unexpected calls %+v", fb.calls)
	}
	if rem := p.CarriedRemainders()["cus/cost_usd"]; rem < 0.4999 || rem > 0.5001 {
		t.Fatalf("expected 0.5 micro-dollar carried, got %v", rem)
	}
	// Below one unit nothing is sent but the fraction accumulates.
	_ = p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus", EventName: "cost_usd", Value: 0.0000002, Timestamp: ts.Add(time.Hour)}})
	if len(fb.calls) != 1 {
		t.Fatal("sub-unit usage must be carried, not sent")
	}
	if rem := p.CarriedRemainders()["cus/cost_usd"]; rem < 0.6999 || rem > 0.7001 {
		t.Fatalf("expected 0.7 carried, got %v", rem)
	}
	if err := NewStripeProvider("sk").WithBackend(fb).WithUnitScale("e", -1).SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1}}); !errors.Is(err, ErrInvalidMeterEntry) {
		t.Fatalf("invalid scale must be rejected: %v", err)
	}
	if err := NewStripeProvider("sk").WithBackend(fb).WithUnitScale("e", 1e9).SyncMeter(context.Background(), []MeterEntry{{CustomerID: "c", EventName: "e", Value: 1e9}}); !errors.Is(err, ErrInvalidMeterEntry) {
		t.Fatalf("values beyond exact integer range must be rejected: %v", err)
	}
}

func TestStripeProviderPersistsRemaindersAndSentIDs(t *testing.T) {
	ps := persist.NewMemory()
	fb := &fakeStripeBackend{}
	p := NewStripeProvider("sk").WithBackend(fb)
	ts := time.Unix(1_790_000_000, 0)
	// State accumulated before persistence is enabled is kept.
	_ = p.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus", EventName: "tokens", Value: 0.25, Timestamp: ts}})
	if err := p.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	batch := []MeterEntry{{CustomerID: "cus", EventName: "tokens", Value: 1.5, ID: "u-1", Timestamp: ts}}
	if err := p.SyncMeter(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(fb.calls) != 1 || fb.calls[0].form.Get("payload[value]") != "1" {
		t.Fatalf("unexpected calls %+v", fb.calls)
	}

	// Restart: a new provider over the same store keeps the 0.75 carried
	// and does not bill the retried batch again.
	fb2 := &fakeStripeBackend{}
	p2 := NewStripeProvider("sk").WithBackend(fb2)
	if err := p2.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if rem := p2.CarriedRemainders()["cus/tokens"]; rem != 0.75 {
		t.Fatalf("remainder not restored: %v", rem)
	}
	_ = p2.SyncMeter(context.Background(), batch)
	if len(fb2.calls) != 0 {
		t.Fatalf("retry after restart must be de-duplicated, got %d calls", len(fb2.calls))
	}
	_ = p2.SyncMeter(context.Background(), []MeterEntry{{CustomerID: "cus", EventName: "tokens", Value: 0.25, Timestamp: ts.Add(time.Hour)}})
	if len(fb2.calls) != 1 || fb2.calls[0].form.Get("payload[value]") != "1" {
		t.Fatalf("carried 0.75 + 0.25 must bill one unit: %+v", fb2.calls)
	}
	if err := p2.EnablePersistence(nil); err == nil {
		t.Fatal("nil store must be rejected")
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
