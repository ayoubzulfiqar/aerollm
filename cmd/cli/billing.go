package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

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
				if err := writeInvoiceOutput(inv, outputPath); err != nil {
					fmt.Fprintf(os.Stderr, "output write failed: %v\n", err)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "wrote invoice to %s\n", outputPath)
			}
		},
	}
	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "optional output file path (.json or .csv)")
	return cmd
}

func writeInvoiceOutput(inv *billing.Invoice, path string) error {
	switch filepath.Ext(path) {
	case ".json":
		b, err := json.MarshalIndent(inv, "", "  ")
		if err != nil { return err }
		return os.WriteFile(path, b, 0o644)
	case ".csv":
		f, err := os.Create(path)
		if err != nil { return err }
		defer f.Close()
		_, _ = f.WriteString("customer_id,line_item,quantity,unit_amount_usd\n")
		for _, l := range inv.Lines {
			_, _ = f.WriteString(fmt.Sprintf("%s,%s,%g,%g\n", l.CustomerID, l.EventName, l.Quantity, l.UnitAmount))
		}
		return nil
	default:
		return fmt.Errorf("unsupported output format: %s", path)
	}
}
