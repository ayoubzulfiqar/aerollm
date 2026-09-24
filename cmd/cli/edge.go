package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/spf13/cobra"
)

func newEdgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edge",
		Short: "Interact with a local edge node (status, capabilities, receipts, PQC)",
		Long: `Talk to an AeroLLM edge node. The node address comes from --edge-url,
then $EDGE_LISTEN (the node's listen address, e.g. ":7910"), then
` + defaultEdgeURL + `. The gateway API key is never sent to edge nodes.`,
	}
	cmd.PersistentFlags().String("edge-url", "", "edge node base URL (env EDGE_LISTEN, default "+defaultEdgeURL+")")
	cmd.AddCommand(newEdgeStatusCmd())
	cmd.AddCommand(newEdgeCapabilityCmd())
	cmd.AddCommand(newEdgeReceiptCmd())
	cmd.AddCommand(newEdgePqcCmd())
	cmd.AddCommand(newEdgeSpatialCmd())
	cmd.AddCommand(newEdgeFederatedCmd())
	return cmd
}

// edgeBaseURL resolves the edge node URL. EDGE_LISTEN is a listen address
// (":7910", "0.0.0.0:7910"), so wildcard/empty hosts map to localhost.
func edgeBaseURL(cmd *cobra.Command) string {
	if v := strings.TrimSpace(flagValue(cmd, "edge-url")); v != "" {
		return v
	}
	v := strings.TrimSpace(os.Getenv("EDGE_LISTEN"))
	if v == "" {
		return defaultEdgeURL
	}
	if strings.Contains(v, "://") {
		return v
	}
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return "http://" + v
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// newEdgeClient returns an API client for the edge node. It deliberately
// carries no API key: gateway credentials must not leak to edge nodes.
func newEdgeClient(cmd *cobra.Command) (*apiClient, error) {
	return newAPIClient(edgeBaseURL(cmd), "", resolveTimeout(cmd), cmd.ErrOrStderr())
}

func edgeGet(cmd *cobra.Command, path string) error {
	client, err := newEdgeClient(cmd)
	if err != nil {
		return err
	}
	data, err := client.call(cmd.Context(), http.MethodGet, path, nil, nil)
	if err != nil {
		return fmt.Errorf("edge node %s: %w", client.base.Redacted(), err)
	}
	return renderResult(cmd, data, formatJSON, nil, nil)
}

func newEdgeStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show edge node identity, hardware and wallet (/v1/edge/capabilities)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return edgeGet(cmd, "/v1/edge/capabilities")
		},
	}
}

func newEdgeCapabilityCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "capability",
		Short: "Show the edge node's Open Standard capability manifest",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return edgeGet(cmd, "/v1/marketplace/openstandard/capability/self")
		},
	}
}

// receiptFlags holds the flags shared by the edge and openstandard receipt
// commands.
type receiptFlags struct {
	receiptID, customerID, eventName, currency string
	value                                      float64
}

func (f *receiptFlags) register(cmd *cobra.Command) {
	fl := cmd.Flags()
	fl.StringVar(&f.receiptID, "receipt-id", "", "receipt id (default: random cli-<hex>)")
	fl.StringVarP(&f.customerID, "customer", "c", "cli", "customer id")
	fl.StringVarP(&f.eventName, "event", "e", "token", "event name")
	fl.Float64VarP(&f.value, "value", "v", 1, "receipt value")
	fl.StringVarP(&f.currency, "currency", "u", "USD", "ISO 4217 currency code")
}

// build returns a validated receipt.
func (f *receiptFlags) build(providerID string) (marketplace.BillingReceipt, error) {
	id := f.receiptID
	if id == "" {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return marketplace.BillingReceipt{}, err
		}
		id = "cli-" + hex.EncodeToString(b[:])
	}
	rec := marketplace.BillingReceipt{
		ReceiptID:  id,
		CustomerID: f.customerID,
		ProviderID: providerID,
		EventName:  f.eventName,
		Value:      f.value,
		Currency:   strings.ToUpper(f.currency),
		RecordedAt: time.Now().UTC(),
	}
	if err := rec.Validate(); err != nil {
		return rec, fmt.Errorf("invalid receipt: %w", err)
	}
	return rec, nil
}

func newEdgeReceiptCmd() *cobra.Command {
	var rf receiptFlags
	cmd := &cobra.Command{
		Use:     "receipt",
		Short:   "Create an Open Standard billing receipt on the edge node",
		Example: "  aerollm edge receipt --customer acme --event token --value 42",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			rec, err := rf.build("edge")
			if err != nil {
				return err
			}
			client, err := newEdgeClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/v1/marketplace/openstandard/receipt", nil, rec)
			if err != nil {
				return fmt.Errorf("edge node %s: %w", client.base.Redacted(), err)
			}
			return renderResult(cmd, data, formatJSON, nil, nil)
		},
	}
	rf.register(cmd)
	return cmd
}
