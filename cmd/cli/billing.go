package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/spf13/cobra"
)

func newBillingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "billing",
		Short: "Billing utilities (generate invoices from usage entries)",
	}
	cmd.AddCommand(newBillingGenerateCmd())
	return cmd
}

// meterEntryInput is the JSON input format for billing generate.
type meterEntryInput struct {
	CustomerID string    `json:"customer_id"`
	EventName  string    `json:"event_name"`
	Value      float64   `json:"value"`
	Timestamp  time.Time `json:"timestamp"`
}

func newBillingGenerateCmd() *cobra.Command {
	var outputPath, inputPath string
	var force bool
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate an invoice from usage meter entries",
		Long: `Generate an invoice from meter entries read from --input: a JSON array of
{"customer_id","event_name","value","timestamp"} objects. Without --input a
small built-in sample is used. --output writes the invoice as .json or .csv
(mode 0600; existing files are only replaced with --force).`,
		Example: "  aerollm billing generate --input usage.json --output invoice.csv",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var entries []billing.MeterEntry
			if inputPath != "" {
				raw, err := readValueArg(cmd, "@"+inputPath)
				if err != nil {
					return err
				}
				var in []meterEntryInput
				if err := jsonUnmarshalStrict([]byte(raw), &in); err != nil {
					return fmt.Errorf("--input: %w", err)
				}
				for i, e := range in {
					if e.CustomerID == "" || e.EventName == "" {
						return fmt.Errorf("--input entry %d: customer_id and event_name are required", i)
					}
					if e.Value < 0 {
						return fmt.Errorf("--input entry %d: value must not be negative", i)
					}
					entries = append(entries, billing.MeterEntry{CustomerID: e.CustomerID, EventName: e.EventName, Value: e.Value, Timestamp: e.Timestamp})
				}
				if len(entries) == 0 {
					return errors.New("--input contains no entries")
				}
			} else {
				fmt.Fprintln(cmd.ErrOrStderr(), "note: no --input given; using built-in sample usage entries")
				entries = []billing.MeterEntry{
					{CustomerID: "c1", EventName: "token", Value: 10},
					{CustomerID: "c1", EventName: "token", Value: 5},
				}
			}
			if outputPath != "" {
				if err := checkInvoiceExt(outputPath); err != nil {
					return err
				}
			}
			generator := billing.NewInvoiceGenerator(billing.NewInMemoryProvider())
			inv, err := generator.Generate(cmd.Context(), entries)
			if err != nil {
				return fmt.Errorf("invoice generation failed: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "generated invoice %s for %s: %.2f USD\n", inv.ID, inv.CustomerID, inv.TotalUSD)
			if outputPath != "" {
				if err := writeInvoiceOutput(inv, outputPath, force); err != nil {
					return fmt.Errorf("output write failed: %w", err)
				}
				fmt.Fprintf(cmd.ErrOrStderr(), "wrote invoice to %s\n", outputPath)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "optional output file path (.json or .csv)")
	cmd.Flags().StringVarP(&inputPath, "input", "i", "", "JSON file of meter entries")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite an existing output file")
	return cmd
}

func checkInvoiceExt(path string) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json", ".csv":
		return nil
	default:
		return fmt.Errorf("unsupported output format %q: use .json or .csv", path)
	}
}

func writeInvoiceOutput(inv *billing.Invoice, path string, force bool) error {
	if err := checkInvoiceExt(path); err != nil {
		return err
	}
	var data []byte
	if strings.ToLower(filepath.Ext(path)) == ".json" {
		b, err := json.MarshalIndent(inv, "", "  ")
		if err != nil {
			return err
		}
		data = append(b, '\n')
	} else {
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		_ = w.Write([]string{"customer_id", "line_item", "quantity", "unit_amount_usd"})
		for _, l := range inv.Lines {
			_ = w.Write([]string{
				csvSafe(l.CustomerID),
				csvSafe(l.EventName),
				strconv.FormatFloat(l.Quantity, 'g', -1, 64),
				strconv.FormatFloat(l.UnitAmount, 'g', -1, 64),
			})
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
		data = buf.Bytes()
	}
	return writeFileSafely(path, data, 0o600, force)
}

// csvSafe neutralises spreadsheet formula injection in text cells.
func csvSafe(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}
