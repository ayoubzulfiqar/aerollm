package billing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/stripe/stripe-go/v80"
	"github.com/stripe/stripe-go/v80/billing/meterevent"
)

// Stripe meter events carry integer values. MeterEntry values are converted
// with a per-event unit scale (StripeProvider.UnitScales, default 1):
//
//   - token events ("tokens", "token", ...) are already whole numbers and
//     use the default scale 1 (one Stripe unit per token);
//   - USD amounts should use MicroUSDPerUSD: one Stripe unit is one
//     micro-dollar ($0.000001), fine enough for single cheap requests.
//     Price such a meter per unit with unit_amount_decimal "0.0001" (cents)
//     or with transform_quantity divide_by 1,000,000 on a per-dollar price.
//
// Fractions of a unit are never dropped: they are carried per (customer,
// event) and added to that pair's next sync.
const (
	// MicroUSDPerUSD converts USD amounts to integer micro-dollars.
	MicroUSDPerUSD = 1e6
	// CentsPerUSD converts USD amounts to integer cents.
	CentsPerUSD = 100
)

// Persistence buckets of the Stripe provider.
const (
	StripeRemainderBucket = "billing_stripe_remainders"
	StripeSentBucket      = "billing_stripe_sent"
)

const (
	nanoUnits = int64(1_000_000_000) // fixed-point scale of carried fractions
	// maxScaledValue keeps scaled values exactly representable (2^53).
	maxScaledValue = 1 << 53
	// sentRetention bounds how long committed aggregate identifiers are
	// remembered (Stripe de-duplicates identifiers for 24h).
	sentRetention = 48 * time.Hour
	// maxStripeIdentifier is Stripe's limit for meter event identifiers.
	maxStripeIdentifier = 100
)

// StripeProvider records usage through Stripe Meter Events.
//
// SyncMeter aggregates the entries of one call per (customer, event) and
// sends one meter event per pair with an integer value (see UnitScales);
// the fractional remainder is carried to the pair's next sync. Each event
// has a stable identifier and idempotency key derived from its entries, so
// retrying a batch never double-bills: pairs already accepted by an
// earlier attempt are skipped locally and Stripe de-duplicates the rest.
// Calls are serialised. Carried remainders live in memory unless
// EnablePersistence is used.
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
	// UnitScales maps an event name to the factor converting
	// MeterEntry.Value into Stripe units (default 1). For example
	// {"cost_usd": MicroUSDPerUSD}. Set it before the first SyncMeter.
	UnitScales map[string]float64

	mu         sync.Mutex
	remainders map[string]int64     // pairKey -> carried nano-units
	sent       map[string]time.Time // committed aggregate identifiers
	sentIDs    map[string]time.Time // explicit MeterEntry.IDs already billed
	pending    map[string]pendingEvent
	ps         persist.Store
	now        func() time.Time
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

// WithUnitScale sets the unit scale of one event (see UnitScales).
func (p *StripeProvider) WithUnitScale(event string, scale float64) *StripeProvider {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.UnitScales == nil {
		p.UnitScales = map[string]float64{}
	}
	p.UnitScales[event] = scale
	return p
}

func (p *StripeProvider) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *StripeProvider) scale(event string) (float64, error) {
	s, ok := p.UnitScales[event]
	if !ok {
		return 1, nil
	}
	if math.IsNaN(s) || math.IsInf(s, 0) || s <= 0 {
		return 0, fmt.Errorf("%w: invalid unit scale %v for event %q", ErrInvalidMeterEntry, s, event)
	}
	return s, nil
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

// meterGroup is the aggregate of one (customer, event) pair in a sync.
type meterGroup struct {
	customer, event string
	whole           int64 // whole units
	frac            int64 // nano-units, < nanoUnits after normalisation
	members         []string
	explicitIDs     []string
	latest          time.Time
}

func pairKey(customer, event string) string { return customer + "\x1f" + event }

// add accumulates value (already scaled) exactly enough: the whole part is
// integral and the fraction is kept in nano-units.
func (g *meterGroup) add(scaled float64) error {
	w := math.Floor(scaled)
	f := int64(math.Round((scaled - w) * float64(nanoUnits)))
	if int64(w) > math.MaxInt64/2-g.whole {
		return fmt.Errorf("%w: aggregated value too large", ErrInvalidMeterEntry)
	}
	g.whole += int64(w)
	g.frac += f
	g.whole += g.frac / nanoUnits
	g.frac %= nanoUnits
	return nil
}

// identifier is stable for the same set of entries, in any order.
func (g *meterGroup) identifier(single MeterEntry) string {
	if len(g.members) == 1 && single.ID != "" && len(single.ID) <= maxStripeIdentifier {
		return single.ID
	}
	ids := append([]string(nil), g.members...)
	sort.Strings(ids)
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00", g.customer, g.event)
	for _, id := range ids {
		fmt.Fprintf(h, "%s\x00", id)
	}
	return "aerollm-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// SyncMeter aggregates entries per (customer, event), adds the carried
// fractional remainder and sends one integer meter event per pair whose
// total reaches at least one unit. All entries are validated before
// anything is sent. On error, pairs sent before the failure are committed;
// retry the same batch: committed pairs are skipped and the failed one is
// re-sent with exactly the same value and idempotency key. (A failed batch
// that is never retried keeps the remainder it reserved.)
func (p *StripeProvider) SyncMeter(ctx context.Context, entries []MeterEntry) error {
	if p.secretKey == "" {
		return fmt.Errorf("billing: missing stripe secret key")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initLocked()

	now := p.clock()
	p.pruneSentLocked(now)
	var groups []*meterGroup
	firstOf := map[string]MeterEntry{}
	byKey := map[string]*meterGroup{}
	seenIDs := map[string]bool{}
	for i, e := range entries {
		if err := ValidateMeterEntry(e); err != nil {
			return err
		}
		scale, err := p.scale(e.EventName)
		if err != nil {
			return err
		}
		scaled := e.Value * scale
		if scaled > maxScaledValue {
			return fmt.Errorf("%w: value %v of event %q exceeds %d units after scaling", ErrInvalidMeterEntry, e.Value, e.EventName, int64(maxScaledValue))
		}
		if e.Value == 0 {
			continue
		}
		if e.ID != "" {
			// IDs are idempotency keys: bill each at most once, within
			// this batch and across recent batches.
			if _, billed := p.sentIDs[e.ID]; billed || seenIDs[e.ID] {
				continue
			}
			seenIDs[e.ID] = true
		}
		k := pairKey(e.CustomerID, e.EventName)
		g := byKey[k]
		if g == nil {
			g = &meterGroup{customer: e.CustomerID, event: e.EventName}
			byKey[k] = g
			firstOf[k] = e
			groups = append(groups, g)
		}
		if err := g.add(scaled); err != nil {
			return err
		}
		g.members = append(g.members, meterIdentifier(e, i))
		if e.ID != "" {
			g.explicitIDs = append(g.explicitIDs, e.ID)
		}
		if e.Timestamp.After(g.latest) {
			g.latest = e.Timestamp
		}
	}
	if len(groups) == 0 {
		return nil
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
	var persistErrs []error
	for i, g := range groups {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(persistErrs, err)...)
		}
		k := pairKey(g.customer, g.event)
		id := g.identifier(firstOf[k])
		if _, done := p.sent[id]; done {
			continue // retry of a pair that was already committed
		}
		// The first attempt of an event reserves the pair's carried
		// remainder and fixes its value, so a retry after an ambiguous
		// failure (Stripe may have accepted it) sends identical parameters
		// under the same idempotency key, whatever was synced meanwhile.
		pe, retry := p.pending[id]
		if !retry {
			carried := p.remainders[k] + g.frac
			pe = pendingEvent{units: g.whole + carried/nanoUnits, rem: carried % nanoUnits}
			delete(p.remainders, k)
			p.pending[id] = pe
		}
		units, rem := pe.units, pe.rem
		if units > 0 {
			params := &stripe.BillingMeterEventParams{
				EventName:  stripe.String(g.event),
				Identifier: stripe.String(id),
				Payload: map[string]string{
					custKey: g.customer,
					valKey:  strconv.FormatInt(units, 10),
				},
			}
			if !g.latest.IsZero() {
				params.Timestamp = stripe.Int64(g.latest.Unix())
			}
			params.Context = ctx
			params.SetIdempotencyKey("meter-" + id)
			if _, err := client.New(params); err != nil {
				return errors.Join(append(persistErrs, fmt.Errorf("billing: stripe meter event %d/%d (%s): %w", i+1, len(groups), id, err))...)
			}
		}
		if err := p.commitLocked(k, g, id, rem, now); err != nil {
			persistErrs = append(persistErrs, err)
		}
	}
	return errors.Join(persistErrs...)
}

// remainderDoc is the persisted carried fraction of one pair.
type remainderDoc struct {
	Customer string `json:"customer"`
	Event    string `json:"event"`
	Nanos    int64  `json:"nano_units"`
}

// pendingEvent is an event whose delivery has not been confirmed yet.
type pendingEvent struct {
	units, rem int64
}

type sentDoc struct {
	At time.Time `json:"at"`
	// IDs are the explicit MeterEntry IDs billed by this event.
	IDs []string `json:"ids,omitempty"`
}

func (p *StripeProvider) initLocked() {
	if p.remainders == nil {
		p.remainders = map[string]int64{}
	}
	if p.sent == nil {
		p.sent = map[string]time.Time{}
	}
	if p.sentIDs == nil {
		p.sentIDs = map[string]time.Time{}
	}
	if p.pending == nil {
		p.pending = map[string]pendingEvent{}
	}
}

// commitLocked records a synced pair: its new remainder and identifier.
func (p *StripeProvider) commitLocked(k string, g *meterGroup, id string, rem int64, now time.Time) error {
	delete(p.pending, id)
	rem += p.remainders[k] // carried by events synced while this one was pending
	if rem == 0 {
		delete(p.remainders, k)
	} else {
		p.remainders[k] = rem
	}
	p.sent[id] = now
	for _, eid := range g.explicitIDs {
		p.sentIDs[eid] = now
	}
	if p.ps == nil {
		return nil
	}
	var errs []error
	errs = append(errs, persistRemainder(p.ps, g.customer, g.event, rem))
	errs = append(errs, p.ps.Put(StripeSentBucket, id, sentDoc{At: now, IDs: g.explicitIDs}))
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("billing: persist stripe meter state: %w", err)
	}
	return nil
}

func persistRemainder(ps persist.Store, customer, event string, rem int64) error {
	docKey := hex.EncodeToString([]byte(pairKey(customer, event)))
	if rem == 0 {
		return ps.Delete(StripeRemainderBucket, docKey)
	}
	return ps.Put(StripeRemainderBucket, docKey, remainderDoc{Customer: customer, Event: event, Nanos: rem})
}

func (p *StripeProvider) pruneSentLocked(now time.Time) {
	for id, at := range p.sent {
		if now.Sub(at) > sentRetention {
			delete(p.sent, id)
			if p.ps != nil {
				_ = p.ps.Delete(StripeSentBucket, id)
			}
		}
	}
	for id, at := range p.sentIDs {
		if now.Sub(at) > sentRetention {
			delete(p.sentIDs, id)
		}
	}
}

// CarriedRemainders returns the fractional units carried per
// "customer/event" pair (each in [0, 1)).
func (p *StripeProvider) CarriedRemainders() map[string]float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]float64, len(p.remainders))
	for k, n := range p.remainders {
		out[strings.Replace(k, "\x1f", "/", 1)] = float64(n) / float64(nanoUnits)
	}
	return out
}

// EnablePersistence stores carried remainders and recently committed
// event identifiers in ps (buckets StripeRemainderBucket and
// StripeSentBucket) and loads what a previous process left there, so
// neither fractions nor retry de-duplication are lost on restart.
func (p *StripeProvider) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("billing: nil persist store")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initLocked()
	rems := map[string]int64{}
	err := ps.ForEach(StripeRemainderBucket, func(_ string, raw json.RawMessage) error {
		var d remainderDoc
		if err := json.Unmarshal(raw, &d); err != nil {
			return fmt.Errorf("billing: undecodable stripe remainder: %w", err)
		}
		if d.Nanos > 0 {
			rems[pairKey(d.Customer, d.Event)] = d.Nanos
		}
		return nil
	})
	if err != nil {
		return err
	}
	sent := map[string]time.Time{}
	sentIDs := map[string]time.Time{}
	now := p.clock()
	var stale []string
	err = ps.ForEach(StripeSentBucket, func(id string, raw json.RawMessage) error {
		var d sentDoc
		if json.Unmarshal(raw, &d) != nil || now.Sub(d.At) > sentRetention {
			stale = append(stale, id)
			return nil
		}
		sent[id] = d.At
		for _, eid := range d.IDs {
			sentIDs[eid] = d.At
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range stale {
		_ = ps.Delete(StripeSentBucket, id)
	}
	// Merge state accumulated in memory before persistence was enabled and
	// write the merged remainders, so nothing carried so far is lost.
	var errs []error
	for k, n := range p.remainders {
		rems[k] += n
		customer, event, _ := strings.Cut(k, "\x1f")
		errs = append(errs, persistRemainder(ps, customer, event, rems[k]))
	}
	for id, at := range p.sent {
		sent[id] = at
	}
	for id, at := range p.sentIDs {
		sentIDs[id] = at
	}
	p.remainders, p.sent, p.sentIDs, p.ps = rems, sent, sentIDs, ps
	return errors.Join(errs...)
}
