package billing

import (
	"context"
	"sync"
	"time"
)

// LedgerStore is a minimal interface so billing can read usage without
// depending on the full economy package.
type LedgerStore interface {
	Transactions(ctx context.Context, id string, limit int) ([]Transaction, error)
}

// Transaction is a lightweight view of an economy transaction.
type Transaction struct {
	ID        string
	Amount    float64
	Timestamp time.Time
}

// MeterEntry represents one line of usage to sync to Stripe.
type MeterEntry struct {
	CustomerID string
	EventName  string
	Value      float64
	Timestamp  time.Time
}

// Provider defines the contract for a billing backend.
type Provider interface {
	SyncMeter(ctx context.Context, entries []MeterEntry) error
}

// InMemoryProvider records metering calls for tests and offline mode.
type InMemoryProvider struct {
	mu      sync.RWMutex
	entries []MeterEntry
}

// NewInMemoryProvider creates a no-op billing provider.
func NewInMemoryProvider() *InMemoryProvider {
	return &InMemoryProvider{entries: make([]MeterEntry, 0)}
}

// SyncMeter appends metering entries in memory.
func (p *InMemoryProvider) SyncMeter(_ context.Context, entries []MeterEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries = append(p.entries, entries...)
	return nil
}

// Snapshot returns a copy of recorded meter entries.
func (p *InMemoryProvider) Snapshot() []MeterEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]MeterEntry, len(p.entries))
	copy(out, p.entries)
	return out
}
