package billing

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// MeterEntry represents one line of usage to sync to a billing backend.
type MeterEntry struct {
	CustomerID string
	EventName  string
	Value      float64
	Timestamp  time.Time
	// ID is an optional idempotency identifier. Backends de-duplicate
	// entries with the same ID, so retries never double-bill.
	ID string
}

// Limits applied by ValidateMeterEntry.
const (
	maxCustomerIDLen = 255
	maxEventNameLen  = 100
	maxMeterValue    = 1e15
)

var (
	// ErrInvalidMeterEntry is returned for malformed usage entries.
	ErrInvalidMeterEntry = errors.New("billing: invalid meter entry")
	// ErrNoUsage is returned when there is nothing billable.
	ErrNoUsage = errors.New("billing: no billable usage")
	// ErrMixedCustomers is returned by Generate when entries span customers.
	ErrMixedCustomers = errors.New("billing: entries belong to multiple customers")
)

// ValidateMeterEntry checks that an entry is billable: it needs a customer,
// an event name and a finite, non-negative value.
func ValidateMeterEntry(e MeterEntry) error {
	switch {
	case e.CustomerID == "" || len(e.CustomerID) > maxCustomerIDLen:
		return fmt.Errorf("%w: customer id must be 1-%d bytes", ErrInvalidMeterEntry, maxCustomerIDLen)
	case e.EventName == "" || len(e.EventName) > maxEventNameLen:
		return fmt.Errorf("%w: event name must be 1-%d bytes", ErrInvalidMeterEntry, maxEventNameLen)
	case math.IsNaN(e.Value) || math.IsInf(e.Value, 0):
		return fmt.Errorf("%w: value must be finite", ErrInvalidMeterEntry)
	case e.Value < 0:
		return fmt.Errorf("%w: value must not be negative", ErrInvalidMeterEntry)
	case e.Value > maxMeterValue:
		return fmt.Errorf("%w: value too large", ErrInvalidMeterEntry)
	}
	return nil
}

// Provider defines the contract for a billing backend.
type Provider interface {
	SyncMeter(ctx context.Context, entries []MeterEntry) error
}

// InMemoryProvider records metering calls for tests and offline mode. It is
// safe for concurrent use and de-duplicates entries carrying an ID.
type InMemoryProvider struct {
	mu      sync.RWMutex
	entries []MeterEntry
	seen    map[string]struct{}
}

// NewInMemoryProvider creates an in-memory billing provider.
func NewInMemoryProvider() *InMemoryProvider {
	return &InMemoryProvider{entries: make([]MeterEntry, 0), seen: make(map[string]struct{})}
}

// SyncMeter validates and appends metering entries in memory. The call is
// all-or-nothing: an invalid entry rejects the whole batch.
func (p *InMemoryProvider) SyncMeter(_ context.Context, entries []MeterEntry) error {
	for _, e := range entries {
		if err := ValidateMeterEntry(e); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen == nil {
		p.seen = make(map[string]struct{})
	}
	for _, e := range entries {
		if e.ID != "" {
			if _, dup := p.seen[e.ID]; dup {
				continue
			}
			p.seen[e.ID] = struct{}{}
		}
		p.entries = append(p.entries, e)
	}
	return nil
}

// AppendMeter adds metering entries to the in-memory provider.
func (p *InMemoryProvider) AppendMeter(ctx context.Context, entries ...MeterEntry) error {
	return p.SyncMeter(ctx, entries)
}

// Snapshot returns a copy of recorded meter entries.
func (p *InMemoryProvider) Snapshot() []MeterEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]MeterEntry, len(p.entries))
	copy(out, p.entries)
	return out
}
