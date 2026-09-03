package marketplace

import (
	"testing"

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
}

func TestCapabilityManifestCanonicalJSON(t *testing.T) {
	m := CapabilityManifest{
		Version:  "1.0",
		Hardware: Hardware{HasLocalGPU: true, OS: "linux", MemoryGB: 16},
		Billing:  Billing{SupportsMetered: true, Currency: "USD"},
		Capabilities: []string{"mesh", "wasm"},
	}
	b, err := m.CanonicalJSON()
	require.NoError(t, err)
	require.Contains(t, string(b), `"has_local_gpu":true`)
}

func TestBillingReceiptValidation(t *testing.T) {
	r := BillingReceipt{ReceiptID: "r-1", EventName: "token", Currency: "USD"}
	require.NoError(t, r.Validate())

	bad := BillingReceipt{ReceiptID: "", EventName: "token", Currency: "USD"}
	require.Error(t, bad.Validate())
}
