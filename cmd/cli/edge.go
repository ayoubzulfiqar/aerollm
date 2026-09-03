package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newEdgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edge",
		Short: "Interact with local edge-node Open Standard endpoints",
	}
	cmd.AddCommand(newEdgeStatusCmd())
	cmd.AddCommand(newEdgeCapabilityCmd())
	cmd.AddCommand(newEdgeReceiptCmd())
	return cmd
}

func edgeBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("EDGE_LISTEN")); v != "" { return "http://" + v }
	return "http://localhost:7910"
}

func newEdgeStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Check edge-node openstandard status",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(edgeBaseURL() + "/v1/marketplace/openstandard/capability/self")
			if err != nil { fmt.Fprintf(os.Stderr, "edge status failed: %v\n", err); os.Exit(1) }
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			fmt.Println(string(b))
		},
	}
	return cmd
}

func newEdgeCapabilityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "capability",
		Short: "Show current Open Standard capability manifest",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(edgeBaseURL() + "/v1/marketplace/openstandard/capability/self")
			if err != nil { fmt.Fprintf(os.Stderr, "capability fetch failed: %v\n", err); os.Exit(1) }
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			fmt.Println(string(b))
		},
	}
	return cmd
}

func newEdgeReceiptCmd() *cobra.Command {
	var customerID string
	var eventName string
	var value float64
	var currency string
	cmd := &cobra.Command{
		Use:   "receipt",
		Short: "Create an Open Standard billing receipt on edge-node",
		Run: func(cmd *cobra.Command, args []string) {
			body := fmt.Sprintf(`{"receipt_id":"cli-%d","customer_id":"%s","provider_id":"edge","event_name":"%s","value":%f,"currency":"%s"}`,
				os.Getpid(), customerID, eventName, value, currency)
			resp, err := http.Post(edgeBaseURL()+"/v1/marketplace/openstandard/receipt", "application/json", strings.NewReader(body))
			if err != nil { fmt.Fprintf(os.Stderr, "receipt create failed: %v\n", err); os.Exit(1) }
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
