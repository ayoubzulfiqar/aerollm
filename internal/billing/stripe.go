package billing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/stripe/stripe-go/v80"
	"github.com/stripe/stripe-go/v80/billing/meterevent"
)

// StripeProvider records usage through Stripe Meter Events.
//
// It never touches the global stripe.Key: every call uses a per-provider
// client, so several providers (or tests) can coexist safely.
type StripeProvider struct {
	secretKey string
	backend   stripe.Backend

	// CustomerPayloadKey / ValuePayloadKey match the meter's
	// customer_mapping and value_settings keys (Stripe defaults below).
	CustomerPayloadKey string
	ValuePayloadKey    string
}

// NewStripeProvider creates a Stripe-backed billing provider.
func NewStripeProvider(secretKey string) *StripeProvider {
	return &StripeProvider{secretKey: secretKey}
}

// WithBackend overrides the Stripe API backend (used by tests).
func (p *StripeProvider) WithBackend(b stripe.Backend) *StripeProvider {
	p.backend = b
	return p
}

// meterIdentifier returns the entry's idempotency identifier: its ID when
// set, otherwise a hash of its contents, timestamp and batch position. An
// entry with neither ID nor timestamp gets a random identifier, because it
// cannot be distinguished from new identical usage.
func meterIdentifier(e MeterEntry, index int) string {
	if e.ID != "" {
		return e.ID
	}
	if e.Timestamp.IsZero() {
		var b [16]byte
		_, _ = rand.Read(b[:])
		return "aerollm-" + hex.EncodeToString(b[:])
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d",
		e.CustomerID, e.EventName, strconv.FormatFloat(e.Value, 'g', -1, 64), e.Timestamp.UnixNano(), index)))
	return "aerollm-" + hex.EncodeToString(sum[:16])
}

// SyncMeter sends one meter event per non-zero entry. All entries are
// validated before anything is sent. Each event carries a stable
// identifier and idempotency key, so retrying a failed batch does not
// double-bill events that already succeeded.
func (p *StripeProvider) SyncMeter(ctx context.Context, entries []MeterEntry) error {
	if p.secretKey == "" {
		return fmt.Errorf("billing: missing stripe secret key")
	}
	for _, e := range entries {
		if err := ValidateMeterEntry(e); err != nil {
			return err
		}
	}
	backend := p.backend
	if backend == nil {
		backend = stripe.GetBackend(stripe.APIBackend)
	}
	client := meterevent.Client{B: backend, Key: p.secretKey}
	custKey := p.CustomerPayloadKey
	if custKey == "" {
		custKey = "stripe_customer_id"
	}
	valKey := p.ValuePayloadKey
	if valKey == "" {
		valKey = "value"
	}

	for i, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.Value == 0 {
			continue
		}
		id := meterIdentifier(e, i)
		params := &stripe.BillingMeterEventParams{
			EventName:  stripe.String(e.EventName),
			Identifier: stripe.String(id),
			Payload: map[string]string{
				custKey: e.CustomerID,
				valKey:  strconv.FormatFloat(e.Value, 'f', -1, 64),
			},
		}
		if !e.Timestamp.IsZero() {
			params.Timestamp = stripe.Int64(e.Timestamp.Unix())
		}
		params.Context = ctx
		params.SetIdempotencyKey("meter-" + id)
		if _, err := client.New(params); err != nil {
			return fmt.Errorf("billing: stripe meter event %d/%d (%s): %w", i+1, len(entries), id, err)
		}
	}
	return nil
}
