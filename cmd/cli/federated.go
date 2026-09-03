package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/spf13/cobra"
)

func newFederatedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "federated",
		Short: "Federated learning utilities",
		Long:  "Aggregate LoRA updates, inspect matrices, and verify signatures.",
	}

	cmd.AddCommand(newFederatedAggregateCmd())
	cmd.AddCommand(newFederatedListCmd())
	cmd.AddCommand(newFederatedVerifyCmd())
	return cmd
}

func newFederatedAggregateCmd() *cobra.Command {
	var input string
	cmd := &cobra.Command{
		Use:   "aggregate",
		Short: "Aggregate federated LoRA updates",
		Run: func(_ *cobra.Command, _ []string) {
			if input == "" {
				fmt.Println("error: --input is required")
				return
			}
			var updates []*federated.LoRAMatrix
			if err := json.Unmarshal([]byte(input), &updates); err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			agg := federated.NewFedAvgAggregator()
			out, err := agg.Aggregate(nil, updates)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			fmt.Printf("aggregated rows=%d cols=%d checksum=%s\n", out.Rows, out.Cols, out.Checksum())
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON array of LoRAMatrix updates")
	return cmd
}

func newFederatedListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List supported federation features",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("fedavg")
			fmt.Println("secure-verify")
			fmt.Println("lora")
		},
	}
	return cmd
}

func newFederatedVerifyCmd() *cobra.Command {
	var matrixJSON string
	var signature string
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify a federated update signature",
		Run: func(_ *cobra.Command, _ []string) {
			if matrixJSON == "" || signature == "" {
				fmt.Println("error: --matrix and --signature are required")
				return
			}
			var m federated.LoRAMatrix
			if err := json.Unmarshal([]byte(matrixJSON), &m); err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			agg := federated.NewFedAvgAggregator()
			if err := agg.Verify(nil, &m, []byte(signature)); err != nil {
				fmt.Println("error: " + err.Error())
				os.Exit(1)
			}
			fmt.Println("ok")
		},
	}
	cmd.Flags().StringVarP(&matrixJSON, "matrix", "m", "", "JSON LoRAMatrix")
	cmd.Flags().StringVarP(&signature, "signature", "s", "", "signature bytes")
	return cmd
}
