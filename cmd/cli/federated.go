package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/spf13/cobra"
)

func newFederatedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "federated",
		Short: "Federated learning utilities (aggregate and verify LoRA updates)",
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
		Short: "Aggregate federated LoRA updates locally (FedAvg)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updates, err := parseLoRAUpdates(cmd, input)
			if err != nil {
				return err
			}
			return aggregateAndPrint(cmd, updates)
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "JSON array of LoRAMatrix updates (literal, @file, or - for stdin)")
	return cmd
}

func newFederatedListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List supported federation features",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			for _, f := range []string{"fedavg", "secure-verify", "lora"} {
				if _, err := fmt.Fprintln(w, f); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return cmd
}

func newFederatedVerifyCmd() *cobra.Command {
	var matrixJSON, signature, publicKey string
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify an update's ed25519 signature against its owner's public key",
		Long: `Verify that --signature is a valid ed25519 signature by --public-key over
the canonical payload of the LoRA update (owner:rows:checksum). The signature
and public key may be hex or base64 encoded. Exits 1 if verification fails.`,
		Example: `  aerollm federated verify -m @update.json -s <base64-sig> -k <hex-pubkey>`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if matrixJSON == "" || signature == "" || publicKey == "" {
				return errors.New("--matrix, --signature and --public-key are required")
			}
			raw, err := readValueArg(cmd, matrixJSON)
			if err != nil {
				return err
			}
			var m federated.LoRAMatrix
			if err := json.Unmarshal([]byte(raw), &m); err != nil {
				return fmt.Errorf("--matrix: %w", err)
			}
			if m.Owner == "" {
				return errors.New("--matrix: update has no Owner")
			}
			pub, err := decodeKeyBytes(publicKey)
			if err != nil {
				return fmt.Errorf("--public-key: %w", err)
			}
			if len(pub) != ed25519.PublicKeySize {
				return fmt.Errorf("--public-key: expected %d bytes, got %d", ed25519.PublicKeySize, len(pub))
			}
			sig, err := decodeKeyBytes(signature)
			if err != nil {
				return fmt.Errorf("--signature: %w", err)
			}
			agg, err := federated.NewFedAvgAggregatorWithPublicKeys(map[string]ed25519.PublicKey{m.Owner: pub})
			if err != nil {
				return err
			}
			if err := agg.Verify(cmd.Context(), &m, sig); err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "ok")
			return err
		},
	}
	cmd.Flags().StringVarP(&matrixJSON, "matrix", "m", "", "JSON LoRAMatrix (literal, @file, or - for stdin)")
	cmd.Flags().StringVarP(&signature, "signature", "s", "", "signature (hex or base64)")
	cmd.Flags().StringVarP(&publicKey, "public-key", "k", "", "owner's ed25519 public key (hex or base64)")
	return cmd
}

// decodeKeyBytes decodes hex, standard base64 or URL-safe base64.
func decodeKeyBytes(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("empty value")
	}
	if b, err := hex.DecodeString(s); err == nil {
		return b, nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, errors.New("not valid hex or base64")
}
