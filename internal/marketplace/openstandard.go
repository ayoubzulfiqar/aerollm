package marketplace

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"sort"
	"time"
	"unicode"
	"unicode/utf8"
)

// Open-standard document limits.
const (
	// MaxOpenStandardBytes caps a capability manifest or billing receipt document.
	MaxOpenStandardBytes = 64 << 10

	maxCapabilities    = 64
	maxShortFieldBytes = 128
	maxURLBytes        = 2048
	maxMemoryGB        = 1 << 20 // 1 PiB; anything larger is garbage
	// MaxReceiptValue bounds BillingReceipt.Value so it survives conversion to
	// integer micro-units without overflow.
	MaxReceiptValue = 1e12
)

var (
	currencyPattern   = regexp.MustCompile(`^[A-Z]{3}$`)
	capabilityPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	docVersionPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+){0,2}$`)
)

// CapabilityManifest describes an edge capability advertised under the Open Standard.
type CapabilityManifest struct {
	Version      string    `json:"version"`
	Hardware     Hardware  `json:"hardware"`
	Billing      Billing   `json:"billing"`
	Capabilities []string  `json:"capabilities"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Hardware describes local compute available to the edge.
type Hardware struct {
	HasLocalGPU bool   `json:"has_local_gpu"`
	GPUName     string `json:"gpu_name,omitempty"`
	OS          string `json:"os,omitempty"`
	MemoryGB    int    `json:"memory_gb,omitempty"`
}

// Billing describes billing exposure for this edge.
type Billing struct {
	SupportsMetered bool   `json:"supports_metered"`
	Currency        string `json:"currency"`
	InvoiceURL      string `json:"invoice_url,omitempty"`
}

func validateShortText(field, s string) error {
	if len(s) > maxShortFieldBytes || !utf8.ValidString(s) {
		return fmt.Errorf("invalid %s: must be valid UTF-8 of at most %d bytes", field, maxShortFieldBytes)
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return fmt.Errorf("invalid %s: contains control characters", field)
		}
	}
	return nil
}

// Validate returns an error if the manifest is malformed.
func (m CapabilityManifest) Validate() error {
	if m.Version == "" {
		return errors.New("missing version")
	}
	if len(m.Version) > 32 || !docVersionPattern.MatchString(m.Version) {
		return errors.New("invalid version: expected MAJOR[.MINOR[.PATCH]]")
	}
	if m.Hardware.MemoryGB < 0 || m.Hardware.MemoryGB > maxMemoryGB {
		return errors.New("invalid memory_gb")
	}
	if err := validateShortText("gpu_name", m.Hardware.GPUName); err != nil {
		return err
	}
	if err := validateShortText("os", m.Hardware.OS); err != nil {
		return err
	}
	if m.Hardware.GPUName != "" && !m.Hardware.HasLocalGPU {
		return errors.New("gpu_name set but has_local_gpu is false")
	}
	if m.Billing.Currency == "" {
		return errors.New("missing currency")
	}
	if !currencyPattern.MatchString(m.Billing.Currency) {
		return errors.New("invalid currency: expected ISO 4217 code such as USD")
	}
	if m.Billing.InvoiceURL != "" {
		if err := validateHTTPURL(m.Billing.InvoiceURL); err != nil {
			return fmt.Errorf("invalid invoice_url: %w", err)
		}
	}
	if len(m.Capabilities) > maxCapabilities {
		return fmt.Errorf("too many capabilities (max %d)", maxCapabilities)
	}
	for _, c := range m.Capabilities {
		if !capabilityPattern.MatchString(c) {
			return fmt.Errorf("invalid capability %q", truncate(c, 32))
		}
	}
	return nil
}

func validateHTTPURL(raw string) error {
	if len(raw) > maxURLBytes {
		return fmt.Errorf("longer than %d bytes", maxURLBytes)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("not a URL")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("must be an absolute http(s) URL")
	}
	if u.User != nil {
		return errors.New("must not embed credentials")
	}
	return nil
}

// CanonicalJSON returns stable JSON for signing or hashing: capabilities are
// sorted and de-duplicated and the timestamp is normalised to UTC, so two
// logically identical manifests always encode to the same bytes.
func (m CapabilityManifest) CanonicalJSON() ([]byte, error) {
	type alias CapabilityManifest
	tmp := alias(m)
	if m.Capabilities != nil {
		caps := append([]string(nil), m.Capabilities...)
		sort.Strings(caps)
		out := caps[:0]
		for i, c := range caps {
			if i == 0 || c != caps[i-1] {
				out = append(out, c)
			}
		}
		tmp.Capabilities = out
	}
	tmp.UpdatedAt = m.UpdatedAt.UTC()
	return json.Marshal(tmp)
}

// ParseCapabilityManifest strictly decodes (size-capped, no unknown fields,
// no trailing data) and validates a capability manifest.
func ParseCapabilityManifest(r io.Reader) (CapabilityManifest, error) {
	var m CapabilityManifest
	if err := decodeStrictJSON(r, MaxOpenStandardBytes, &m); err != nil {
		return CapabilityManifest{}, err
	}
	if err := m.Validate(); err != nil {
		return CapabilityManifest{}, err
	}
	return m, nil
}

// BillingReceipt is a standardized usage receipt between edge and provider.
// Value is the metered quantity for EventName (for example tokens or seconds)
// and must be finite, non-negative and at most MaxReceiptValue.
type BillingReceipt struct {
	ReceiptID  string    `json:"receipt_id"`
	CustomerID string    `json:"customer_id"`
	ProviderID string    `json:"provider_id"`
	EventName  string    `json:"event_name"`
	Value      float64   `json:"value"`
	Currency   string    `json:"currency"`
	RecordedAt time.Time `json:"recorded_at"`
}

// Validate checks the receipt fields.
func (r BillingReceipt) Validate() error {
	if r.ReceiptID == "" {
		return errors.New("missing receipt_id")
	}
	if !idPattern.MatchString(r.ReceiptID) {
		return errors.New("invalid receipt_id: use 1-128 characters [A-Za-z0-9._-]")
	}
	if r.EventName == "" {
		return errors.New("missing event_name")
	}
	if !idPattern.MatchString(r.EventName) {
		return errors.New("invalid event_name")
	}
	for field, v := range map[string]string{"customer_id": r.CustomerID, "provider_id": r.ProviderID} {
		if v != "" && !idPattern.MatchString(v) {
			return fmt.Errorf("invalid %s", field)
		}
	}
	if r.Currency == "" {
		return errors.New("missing currency")
	}
	if !currencyPattern.MatchString(r.Currency) {
		return errors.New("invalid currency: expected ISO 4217 code such as USD")
	}
	if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) || r.Value < 0 || r.Value > MaxReceiptValue {
		return fmt.Errorf("invalid value: must be between 0 and %g", MaxReceiptValue)
	}
	return nil
}

// ValueMicros returns Value in integer micro-units (rounded half away from zero).
func (r BillingReceipt) ValueMicros() (int64, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	return int64(math.Round(r.Value * 1e6)), nil
}

// ParseBillingReceipt strictly decodes (size-capped, no unknown fields, no
// trailing data) and validates a billing receipt.
func ParseBillingReceipt(r io.Reader) (BillingReceipt, error) {
	var rec BillingReceipt
	if err := decodeStrictJSON(r, MaxOpenStandardBytes, &rec); err != nil {
		return BillingReceipt{}, err
	}
	if err := rec.Validate(); err != nil {
		return BillingReceipt{}, err
	}
	return rec, nil
}
