package main

import (
	"fmt"
	"os"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/spf13/cobra"
)

func newBillingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "billing",
		Short: "Billing utilities",
	}
	cmd.AddCommand(newBillingGenerateCmd())
	return cmd
}

func newBillingGenerateCmd() *cobra.Command {
	var outputPath string
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate an invoice from usage meter entries",
		Run: func(cmd *cobra.Command, args []string) {
			provider := billing.NewInMemoryProvider()
			generator := billing.NewInvoiceGenerator(provider)
			entries := []billing.MeterEntry{
				{CustomerID: "c1", EventName: "token", Value: 10},
				{CustomerID: "c1", EventName: "token", Value: 5},
			}
			inv, err := generator.Generate(cmd.Context(), entries)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invoice generation failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("generated invoice %s for %s: %.2f USD\n", inv.ID, inv.CustomerID, inv.TotalUSD)
			if outputPath != "" {
				fmt.Fprintf(os.Stderr, "output path not implemented yet: %s\n", outputPath)
			}
		},
	}
	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "optional output file path")
	return cmd
}
