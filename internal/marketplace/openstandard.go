package marketplace

import (
	"encoding/json"
	"errors"
	"time"
)

// CapabilityManifest describes an edge capability advertised under the Open Standard.
type CapabilityManifest struct {
	Version      string    `json:"version"`
	Hardware     Hardware `json:"hardware"`
	Billing      Billing  `json:"billing"`
	Capabilities []string `json:"capabilities"`
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

// Validate returns an error if the manifest is malformed.
func (m CapabilityManifest) Validate() error {
	if m.Version == "" {
		return errors.New("missing version")
	}
	if m.Hardware.MemoryGB < 0 {
		return errors.New("invalid memory_gb")
	}
	if m.Billing.Currency == "" {
		return errors.New("missing currency")
	}
	return nil
}

// CanonicalJSON returns stable JSON for signing or hashing.
func (m CapabilityManifest) CanonicalJSON() ([]byte, error) {
	type alias CapabilityManifest
	tmp := alias(m)
	return json.Marshal(tmp)
}

// BillingReceipt is a standardized usage receipt between edge and provider.
type BillingReceipt struct {
	ReceiptID    string    `json:"receipt_id"`
	CustomerID   string    `json:"customer_id"`
	ProviderID   string    `json:"provider_id"`
	EventName    string    `json:"event_name"`
	Value        float64   `json:"value"`
	Currency     string    `json:"currency"`
	RecordedAt   time.Time `json:"recorded_at"`
}

// Validate checks the receipt fields.
func (r BillingReceipt) Validate() error {
	if r.ReceiptID == "" {
		return errors.New("missing receipt_id")
	}
	if r.EventName == "" {
		return errors.New("missing event_name")
	}
	if r.Currency == "" {
		return errors.New("missing currency")
	}
	return nil
}
