package marketplace

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCapabilityManifestValidation(t *testing.T) {
	m := CapabilityManifest{Version: "1.0", Hardware: Hardware{MemoryGB: 8}, Billing: Billing{Currency: "USD"}}
	require.NoError(t, m.Validate())

	bad := CapabilityManifest{Version: "", Hardware: Hardware{MemoryGB: 8}, Billing: Billing{Currency: "USD"}}
	require.Error(t, bad.Validate())

	bad2 := CapabilityManifest{Version: "1.0", Hardware: Hardware{MemoryGB: -1}, Billing: Billing{Currency: "USD"}}
	require.Error(t, bad2.Validate())

	bad3 := CapabilityManifest{Version: "1.0", Hardware: Hardware{MemoryGB: 8}, Billing: Billing{Currency: ""}}
	require.Error(t, bad3.Validate())

	base := func() CapabilityManifest {
		return CapabilityManifest{Version: "1.0", Billing: Billing{Currency: "USD"}}
	}
	for name, mutate := range map[string]func(*CapabilityManifest){
		"lowercase currency": func(m *CapabilityManifest) { m.Billing.Currency = "usd" },
		"bad version":        func(m *CapabilityManifest) { m.Version = "one" },
		"huge memory":        func(m *CapabilityManifest) { m.Hardware.MemoryGB = math.MaxInt32 },
		"javascript invoice": func(m *CapabilityManifest) { m.Billing.InvoiceURL = "javascript:alert(1)" },
		"relative invoice":   func(m *CapabilityManifest) { m.Billing.InvoiceURL = "/receipt" },
		"credential invoice": func(m *CapabilityManifest) { m.Billing.InvoiceURL = "https://u:p@x/r" },
		"bad capability":     func(m *CapabilityManifest) { m.Capabilities = []string{"Mesh Net"} },
		"too many caps": func(m *CapabilityManifest) {
			for i := 0; i < 100; i++ {
				m.Capabilities = append(m.Capabilities, "c")
			}
		},
		"control chars os": func(m *CapabilityManifest) { m.Hardware.OS = "linux\x00" },
		"gpu name no gpu":  func(m *CapabilityManifest) { m.Hardware.GPUName = "cuda" },
		"long gpu name": func(m *CapabilityManifest) {
			m.Hardware.HasLocalGPU = true
			m.Hardware.GPUName = strings.Repeat("x", 500)
		},
	} {
		m := base()
		mutate(&m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	ok := base()
	ok.Billing.InvoiceURL = "http://localhost:7910/v1/marketplace/openstandard/receipt/self"
	ok.Capabilities = []string{"mesh", "wasm"}
	require.NoError(t, ok.Validate())
}

func TestCapabilityManifestCanonicalJSON(t *testing.T) {
	m := CapabilityManifest{
		Version:      "1.0",
		Hardware:     Hardware{HasLocalGPU: true, OS: "linux", MemoryGB: 16},
		Billing:      Billing{SupportsMetered: true, Currency: "USD"},
		Capabilities: []string{"wasm", "mesh", "wasm"},
		UpdatedAt:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.FixedZone("X", 3600)),
	}
	b, err := m.CanonicalJSON()
	require.NoError(t, err)
	require.Contains(t, string(b), `"has_local_gpu":true`)
	require.Contains(t, string(b), `"capabilities":["mesh","wasm"]`)
	require.Contains(t, string(b), `"updated_at":"2026-01-01T11:00:00Z"`)
	// The caller's slice must not be reordered.
	require.Equal(t, []string{"wasm", "mesh", "wasm"}, m.Capabilities)

	m2 := m
	m2.Capabilities = []string{"mesh", "wasm"}
	m2.UpdatedAt = m.UpdatedAt.UTC()
	b2, err := m2.CanonicalJSON()
	require.NoError(t, err)
	require.Equal(t, string(b), string(b2))
}

func TestBillingReceiptValidation(t *testing.T) {
	r := BillingReceipt{ReceiptID: "r-1", EventName: "token", Currency: "USD"}
	require.NoError(t, r.Validate())

	bad := BillingReceipt{ReceiptID: "", EventName: "token", Currency: "USD"}
	require.Error(t, bad.Validate())

	for name, rec := range map[string]BillingReceipt{
		"negative":      {ReceiptID: "r", EventName: "token", Currency: "USD", Value: -1},
		"nan":           {ReceiptID: "r", EventName: "token", Currency: "USD", Value: math.NaN()},
		"inf":           {ReceiptID: "r", EventName: "token", Currency: "USD", Value: math.Inf(1)},
		"huge":          {ReceiptID: "r", EventName: "token", Currency: "USD", Value: 1e300},
		"slash id":      {ReceiptID: "../r", EventName: "token", Currency: "USD"},
		"bad currency":  {ReceiptID: "r", EventName: "token", Currency: "US"},
		"bad customer":  {ReceiptID: "r", EventName: "token", Currency: "USD", CustomerID: "a b"},
		"missing event": {ReceiptID: "r", Currency: "USD"},
	} {
		if err := rec.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	micros, err := BillingReceipt{ReceiptID: "r", EventName: "token", Currency: "USD", Value: 1.25}.ValueMicros()
	require.NoError(t, err)
	require.Equal(t, int64(1_250_000), micros)
}

func TestParseOpenStandardDocuments(t *testing.T) {
	m, err := ParseCapabilityManifest(strings.NewReader(`{"version":"1.0","billing":{"currency":"USD"}}`))
	require.NoError(t, err)
	require.Equal(t, "1.0", m.Version)

	for name, body := range map[string]string{
		"unknown field": `{"version":"1.0","billing":{"currency":"USD"},"memroy_gb":1}`,
		"trailing":      `{"version":"1.0","billing":{"currency":"USD"}} {}`,
		"invalid":       `{"version":"1.0"}`,
		"not json":      `nope`,
		"empty":         ``,
	} {
		if _, err := ParseCapabilityManifest(strings.NewReader(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	huge := `{"version":"1.0","billing":{"currency":"USD"},"hardware":{"os":"` + strings.Repeat("a", MaxOpenStandardBytes) + `"}}`
	_, err = ParseCapabilityManifest(strings.NewReader(huge))
	require.True(t, errors.Is(err, ErrTooLarge), "expected ErrTooLarge, got %v", err)

	rec, err := ParseBillingReceipt(strings.NewReader(`{"receipt_id":"r-1","event_name":"token","value":1,"currency":"USD"}`))
	require.NoError(t, err)
	require.Equal(t, "r-1", rec.ReceiptID)
	_, err = ParseBillingReceipt(strings.NewReader(`{"receipt_id":"r-1","event_name":"token","value":-1,"currency":"USD"}`))
	require.Error(t, err)
}
