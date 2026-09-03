package billing

import (
	"context"
	"testing"
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

func TestInvoiceGeneratorAggregatesUsage(t *testing.T) {
	g := NewInvoiceGenerator(NewInMemoryProvider())
	inv, err := g.Generate(context.Background(), []MeterEntry{
		{CustomerID: "c1", EventName: "token", Value: 2},
		{CustomerID: "c1", EventName: "token", Value: 3},
	})
	if err != nil {
		t.Fatalf("generate invoice failed: %v", err)
	}
	if inv.TotalUSD != 5 || len(inv.Lines) != 2 || inv.CustomerID != "c1" {
		t.Fatalf("unexpected invoice: %+v", inv)
	}
}
