package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/spf13/cobra"
)

func newEdgePqcCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pqc",
		Short: "Post-quantum crypto operations against the edge node",
		Long:  "Perform PQC handshakes with the edge node.",
	}

	cmd.AddCommand(newEdgePqcHandshakeCmd())
	return cmd
}

func newEdgePqcHandshakeCmd() *cobra.Command {
	var handshakeURL string
	cmd := &cobra.Command{
		Use:   "handshake",
		Short: "Perform a PQC handshake with the edge node (/v1/edge/pqc/handshake)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			base, path := edgeBaseURL(cmd), "/v1/edge/pqc/handshake"
			if handshakeURL != "" {
				u, err := url.Parse(handshakeURL)
				if err != nil || u.Host == "" {
					return fmt.Errorf("invalid --url %q", handshakeURL)
				}
				path = u.Path
				u.Path, u.RawQuery, u.Fragment = "", "", ""
				base = u.String()
			}
			client, err := newAPIClient(base, "", resolveTimeout(cmd), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			var result pqc.KeyResponse
			if err := client.callJSON(cmd.Context(), http.MethodPost, path, nil, nil, &result); err != nil {
				return err
			}
			if len(result.PublicKey) == 0 {
				return errors.New("handshake response did not contain a public key")
			}
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			if format == formatJSON {
				return writeJSON(cmd.OutOrStdout(), map[string]any{
					"algorithm":      result.Algorithm,
					"public_key":     base64.StdEncoding.EncodeToString(result.PublicKey),
					"public_key_len": len(result.PublicKey),
				})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "algorithm=%s public_key_len=%d\n", result.Algorithm, len(result.PublicKey))
			return err
		},
	}
	cmd.Flags().StringVarP(&handshakeURL, "url", "u", "", "full handshake URL (overrides --edge-url)")
	return cmd
}

func newEdgeSpatialCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spatial",
		Short: "Spatial anchor utilities",
		Long:  "Parse spatial anchors used by edge 3D video streams.",
	}

	cmd.AddCommand(newEdgeSpatialStreamCmd())
	return cmd
}

func newEdgeSpatialStreamCmd() *cobra.Command {
	var anchor string
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Parse spatial anchor JSON locally and list the anchors found",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if anchor == "" {
				return errors.New("--anchor is required")
			}
			text, err := readValueArg(cmd, anchor)
			if err != nil {
				return err
			}
			parsed := spatial.ParseSpatialAnchors(text)
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "parsed anchors=%d\n", len(parsed))
			for i, a := range parsed {
				fmt.Fprintf(w, "anchor[%d]=%s x=%.2f y=%.2f z=%.2f\n", i, a.Type, a.X, a.Y, a.Z)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&anchor, "anchor", "a", "", "JSON spatial anchor text (literal, @file, or - for stdin)")
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
		Short: "Aggregate federated LoRA updates locally (FedAvg)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updates, err := parseLoRAUpdates(cmd, input)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "received %d updates\n", len(updates))
			return aggregateAndPrint(cmd, updates)
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON array of LoRAMatrix updates (literal, @file, or - for stdin)")
	return cmd
}

// parseLoRAUpdates decodes a JSON array of LoRA matrices from a flag value.
func parseLoRAUpdates(cmd *cobra.Command, input string) ([]*federated.LoRAMatrix, error) {
	if strings.TrimSpace(input) == "" {
		return nil, errors.New("--input is required")
	}
	raw, err := readValueArg(cmd, input)
	if err != nil {
		return nil, err
	}
	var updates []*federated.LoRAMatrix
	if err := json.Unmarshal([]byte(raw), &updates); err != nil {
		return nil, fmt.Errorf("--input must be a JSON array of LoRA matrices: %w", err)
	}
	return updates, nil
}

func aggregateAndPrint(cmd *cobra.Command, updates []*federated.LoRAMatrix) error {
	out, err := federated.NewFedAvgAggregator().Aggregate(cmd.Context(), updates)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "aggregated rows=%d cols=%d checksum=%s\n", out.Rows, out.Cols, out.Checksum())
	return err
}
