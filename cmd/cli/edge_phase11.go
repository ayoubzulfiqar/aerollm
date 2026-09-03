package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/spf13/cobra"
)

func newEdgePqcCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pqc",
		Short: "Post-quantum crypto operations",
		Long:  "Generate hybrid keys and perform PQC handshakes.",
	}

	cmd.AddCommand(newEdgePqcHandshakeCmd())
	return cmd
}

func newEdgePqcHandshakeCmd() *cobra.Command {
	var edgeURL string
	cmd := &cobra.Command{
		Use:   "handshake",
		Short: "Perform a PQC handshake with the edge node",
		Run: func(_ *cobra.Command, _ []string) {
			url := edgeURL
			if url == "" {
				url = "http://localhost:7910/v1/edge/pqc/handshake"
			}
			req, _ := http.NewRequest(http.MethodPost, url, nil)
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error: " + err.Error())
				os.Exit(1)
			}
			defer resp.Body.Close()

			var result pqc.KeyResponse
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				fmt.Println("error: " + err.Error())
				os.Exit(1)
			}
			fmt.Printf("algorithm=%s public_key_len=%d\n", result.Algorithm, len(result.PublicKey))
		},
	}
	cmd.Flags().StringVarP(&edgeURL, "url", "u", "", "Edge node PQC handshake URL")
	return cmd
}

func newEdgeSpatialCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spatial",
		Short: "Spatial streaming operations",
		Long:  "Parse spatial anchors and manage 3D video streams.",
	}

	cmd.AddCommand(newEdgeSpatialStreamCmd())
	return cmd
}

func newEdgeSpatialStreamCmd() *cobra.Command {
	var anchor string
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream spatial anchor data",
		Run: func(_ *cobra.Command, _ []string) {
			if anchor == "" {
				fmt.Println("error: --anchor is required")
				return
			}
			parsed := spatial.ParseSpatialAnchors(anchor)
			fmt.Printf("parsed anchors=%d\n", len(parsed))
			for i, a := range parsed {
				fmt.Printf("anchor[%d]=%s x=%.2f y=%.2f z=%.2f\n", i, a.Type, a.X, a.Y, a.Z)
			}
		},
	}
	cmd.Flags().StringVarP(&anchor, "anchor", "a", "", "JSON spatial anchor text")
	return cmd
}

func newEdgeFederatedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "federated",
		Short: "Federated learning operations",
		Long:  "Aggregate LoRA updates from edge participants.",
	}

	cmd.AddCommand(newEdgeFederatedAggregateCmd())
	return cmd
}

func newEdgeFederatedAggregateCmd() *cobra.Command {
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
			fmt.Printf("received %d updates\n", len(updates))
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON array of LoRAMatrix updates")
	return cmd
}
