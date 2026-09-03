package billing

import (
	"context"
	"fmt"

	"github.com/stripe/stripe-go/v80"
	"github.com/stripe/stripe-go/v80/billing/meterevent"
)

// StripeProvider records usage through Stripe Meter Events.
type StripeProvider struct {
	secretKey string
}

// NewStripeProvider creates a Stripe-backed billing provider.
func NewStripeProvider(secretKey string) *StripeProvider {
	return &StripeProvider{secretKey: secretKey}
}

// SyncMeter sends one or more meter events to Stripe.
func (p *StripeProvider) SyncMeter(ctx context.Context, entries []MeterEntry) error {
	if p.secretKey == "" {
		return fmt.Errorf("billing: missing stripe secret key")
	}
	stripe.Key = p.secretKey
	for _, e := range entries {
		params := &stripe.BillingMeterEventParams{
			EventName: stripe.String(e.EventName),
			Payload: map[string]string{
				"stripe_customer_id": e.CustomerID,
				"value":              fmt.Sprintf("%f", e.Value),
			},
		}
		if _, err := meterevent.New(params); err != nil {
			return err
		}
	}
	return nil
}
