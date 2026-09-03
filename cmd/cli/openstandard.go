package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newOpenStandardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "openstandard",
		Short: "Interact with Open Standard registry endpoints",
	}
	cmd.AddCommand(newOpenStandardCapabilityCmd())
	cmd.AddCommand(newOpenStandardReceiptCmd())
	return cmd
}

func openStandardBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("AEROLLM_SERVER_URL")); v != "" { return v }
	return "http://localhost:8080"
}

func newOpenStandardCapabilityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "capability",
		Short: "Post Open Standard capability manifest to server registry",
		Run: func(cmd *cobra.Command, args []string) {
			body := `{"version":"1.0","hardware":{"has_local_gpu":true,"os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh","wasm"]}`
			resp, err := http.Post(openStandardBaseURL()+"/v1/marketplace/openstandard/capability", "application/json", strings.NewReader(body))
			if err != nil { fmt.Fprintf(os.Stderr, "capability publish failed: %v\n", err); os.Exit(1) }
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			fmt.Println(string(b))
		},
	}
	return cmd
}

func newOpenStandardReceiptCmd() *cobra.Command {
	var customerID, eventName, currency string
	var value float64
	cmd := &cobra.Command{
		Use:   "receipt",
		Short: "Post Open Standard billing receipt to server registry",
		Run: func(cmd *cobra.Command, args []string) {
			body := fmt.Sprintf(`{"receipt_id":"cli-%d","customer_id":"%s","provider_id":"server","event_name":"%s","value":%f,"currency":"%s"}`,
				os.Getpid(), customerID, eventName, value, currency)
			resp, err := http.Post(openStandardBaseURL()+"/v1/marketplace/openstandard/receipt", "application/json", strings.NewReader(body))
			if err != nil { fmt.Fprintf(os.Stderr, "receipt publish failed: %v\n", err); os.Exit(1) }
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			fmt.Println(string(b))
		},
	}
	cmd.Flags().StringVarP(&customerID, "customer", "c", "cli", "customer id")
	cmd.Flags().StringVarP(&eventName, "event", "e", "token", "event name")
	cmd.Flags().Float64VarP(&value, "value", "v", 1, "receipt value")
	cmd.Flags().StringVarP(&currency, "currency", "u", "USD", "currency")
	return cmd
}
