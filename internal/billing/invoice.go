package billing

import (
	"context"
	"fmt"
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
	ID         string
	CustomerID string
	PeriodStart time.Time
	PeriodEnd   time.Time
	TotalUSD   float64
	Lines      []InvoiceLineItem
	CreatedAt  time.Time
}

// Aggregator summarizes usage into invoice lines.
type Aggregator struct{}

// NewAggregator creates a billing invoice aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{}
}

// Aggregate converts raw meter entries into invoice line items.
func (a *Aggregator) Aggregate(ctx context.Context, entries []MeterEntry) []InvoiceLineItem {
	_ = ctx
	out := make([]InvoiceLineItem, 0, len(entries))
	for _, e := range entries {
		out = append(out, InvoiceLineItem{
			CustomerID: e.CustomerID,
			EventName:  e.EventName,
			Quantity:   e.Value,
			UnitAmount: 1,
		})
	}
	return out
}

// InvoiceGenerator creates finalized invoices from aggregated usage.
type InvoiceGenerator struct {
	Provider Provider
}

// NewInvoiceGenerator creates an invoice generator.
func NewInvoiceGenerator(provider Provider) *InvoiceGenerator {
	return &InvoiceGenerator{Provider: provider}
}

// Generate builds an invoice from usage entries.
func (g *InvoiceGenerator) Generate(ctx context.Context, entries []MeterEntry) (*Invoice, error) {
	if g.Provider == nil {
		return nil, fmt.Errorf("billing: missing provider")
	}
	lines := NewAggregator().Aggregate(ctx, entries)
	total := 0.0
	for _, l := range lines {
		total += l.Quantity * l.UnitAmount
	}
	return &Invoice{
		ID:         fmt.Sprintf("inv-%d", time.Now().UnixNano()),
		CustomerID: safeCustomer(entries),
		PeriodStart: time.Now().Add(-24 * time.Hour),
		PeriodEnd:   time.Now(),
		TotalUSD:    total,
		Lines:      lines,
		CreatedAt:  time.Now(),
	}, nil
}

func safeCustomer(entries []MeterEntry) string {
	if len(entries) > 0 && entries[0].CustomerID != "" {
		return entries[0].CustomerID
	}
	return "unknown"
}
