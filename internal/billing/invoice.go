package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// InvoiceLineItem represents one usage line on an invoice.
type InvoiceLineItem struct {
	CustomerID string
	EventName  string
	Quantity   float64
	UnitAmount float64
}

// Invoice represents a finalized billing invoice.
type Invoice struct {
	ID          string
	CustomerID  string
	PeriodStart time.Time
	PeriodEnd   time.Time
	TotalUSD    float64
	Lines       []InvoiceLineItem
	CreatedAt   time.Time
}

// Aggregator summarizes usage into invoice lines.
type Aggregator struct {
	// UnitPrices maps event name to USD per unit. Events without a price
	// are billed at 1 USD per unit (the value is treated as an amount).
	UnitPrices map[string]float64
}

// NewAggregator creates a billing invoice aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{}
}

func (a *Aggregator) unitPrice(event string) float64 {
	if a != nil && a.UnitPrices != nil {
		if p, ok := a.UnitPrices[event]; ok && p >= 0 && !math.IsNaN(p) && !math.IsInf(p, 0) {
			return p
		}
	}
	return 1
}

// Aggregate merges meter entries into one line per (customer, event),
// preserving first-seen order. Entries with zero value are skipped.
func (a *Aggregator) Aggregate(ctx context.Context, entries []MeterEntry) []InvoiceLineItem {
	_ = ctx
	type lineKey struct{ customer, event string }
	idx := make(map[lineKey]int)
	out := make([]InvoiceLineItem, 0, len(entries))
	for _, e := range entries {
		if e.Value == 0 {
			continue
		}
		k := lineKey{e.CustomerID, e.EventName}
		if i, ok := idx[k]; ok {
			out[i].Quantity += e.Value
			continue
		}
		idx[k] = len(out)
		out = append(out, InvoiceLineItem{
			CustomerID: e.CustomerID,
			EventName:  e.EventName,
			Quantity:   e.Value,
			UnitAmount: a.unitPrice(e.EventName),
		})
	}
	return out
}

// InvoiceGenerator creates finalized invoices from aggregated usage and
// syncs the usage to the billing Provider.
type InvoiceGenerator struct {
	Provider Provider
	// UnitPrices maps event name to USD per unit (default 1).
	UnitPrices map[string]float64
	now        func() time.Time
}

// NewInvoiceGenerator creates an invoice generator.
func NewInvoiceGenerator(provider Provider) *InvoiceGenerator {
	return &InvoiceGenerator{Provider: provider}
}

func (g *InvoiceGenerator) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}

// Generate validates entries (one customer; finite, non-negative values),
// builds an invoice and syncs the usage to the provider. The invoice ID and
// the per-entry idempotency IDs are derived from the entries, so retrying
// Generate with the same timestamped (or ID-carrying) input never
// double-bills. It returns ErrNoUsage when there is nothing billable (no
// invoice is created and nothing is synced).
func (g *InvoiceGenerator) Generate(ctx context.Context, entries []MeterEntry) (*Invoice, error) {
	if g == nil || g.Provider == nil {
		return nil, fmt.Errorf("billing: missing provider")
	}
	now := g.clock().UTC()
	billable := make([]MeterEntry, 0, len(entries))
	customer := ""
	for _, e := range entries {
		// Entries without a timestamp or ID cannot be told apart from a
		// retry of identical usage; stamp them so they are billed (and
		// de-duplicated) per call. Set Timestamp or ID for retry safety.
		if e.Timestamp.IsZero() && e.ID == "" {
			e.Timestamp = now
		}
		if err := ValidateMeterEntry(e); err != nil {
			return nil, err
		}
		if customer == "" {
			customer = e.CustomerID
		} else if e.CustomerID != customer {
			return nil, ErrMixedCustomers
		}
		if e.Value > 0 {
			billable = append(billable, e)
		}
	}
	if len(billable) == 0 {
		return nil, ErrNoUsage
	}

	lines := (&Aggregator{UnitPrices: g.UnitPrices}).Aggregate(ctx, billable)
	total := 0.0
	for _, l := range lines {
		total += l.Quantity * l.UnitAmount
	}
	total = math.Round(total*1e6) / 1e6
	if math.IsNaN(total) || math.IsInf(total, 0) {
		return nil, fmt.Errorf("%w: invoice total overflow", ErrInvalidMeterEntry)
	}

	start, end := periodOf(billable, now)
	id := invoiceID(customer, billable)

	synced := make([]MeterEntry, len(billable))
	for i, e := range billable {
		if e.ID == "" {
			e.ID = id + "-" + strconv.Itoa(i)
		}
		synced[i] = e
	}
	if err := g.Provider.SyncMeter(ctx, synced); err != nil {
		return nil, fmt.Errorf("billing: sync usage for invoice %s: %w", id, err)
	}

	return &Invoice{
		ID:          id,
		CustomerID:  customer,
		PeriodStart: start,
		PeriodEnd:   end,
		TotalUSD:    total,
		Lines:       lines,
		CreatedAt:   now,
	}, nil
}

// GenerateAll groups entries by customer and generates one invoice per
// customer (sorted by customer ID). Customers without billable usage are
// skipped. The first error aborts and is returned with the invoices made so far.
func (g *InvoiceGenerator) GenerateAll(ctx context.Context, entries []MeterEntry) ([]*Invoice, error) {
	byCustomer := map[string][]MeterEntry{}
	for _, e := range entries {
		if err := ValidateMeterEntry(e); err != nil {
			return nil, err
		}
		byCustomer[e.CustomerID] = append(byCustomer[e.CustomerID], e)
	}
	customers := make([]string, 0, len(byCustomer))
	for c := range byCustomer {
		customers = append(customers, c)
	}
	sort.Strings(customers)
	var out []*Invoice
	for _, c := range customers {
		inv, err := g.Generate(ctx, byCustomer[c])
		if errors.Is(err, ErrNoUsage) {
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, inv)
	}
	return out, nil
}

func periodOf(entries []MeterEntry, now time.Time) (time.Time, time.Time) {
	var start, end time.Time
	for _, e := range entries {
		if e.Timestamp.IsZero() {
			continue
		}
		ts := e.Timestamp.UTC()
		if start.IsZero() || ts.Before(start) {
			start = ts
		}
		if end.IsZero() || ts.After(end) {
			end = ts
		}
	}
	if start.IsZero() {
		return now, now
	}
	return start, end
}

// invoiceID derives a deterministic invoice identifier from its contents.
func invoiceID(customer string, entries []MeterEntry) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00", customer)
	for _, e := range entries {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%s\x00", e.CustomerID, e.EventName,
			strconv.FormatFloat(e.Value, 'g', -1, 64), e.Timestamp.UnixNano(), e.ID)
	}
	return "inv_" + hex.EncodeToString(h.Sum(nil)[:12])
}
