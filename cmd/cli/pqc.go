package main

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/spf13/cobra"
)

func newPqcCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pqc",
		Short: "Post-quantum cryptography utilities (list algorithms, generate keys)",
		Long:  "Inspect supported algorithms and generate hybrid key material.",
	}

	cmd.AddCommand(newPqcKeysCmd())
	return cmd
}

func newPqcKeysCmd() *cobra.Command {
	var algorithm, privOut, pubOut string
	var force bool
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "List supported algorithms, or generate a key pair with --algorithm",
		Long: `Without --algorithm, list supported algorithms. With --algorithm, generate
a key pair: the private key is written (hex, mode 0600) to --private-key-out
and never printed; the public key is printed and optionally written to
--public-key-out.`,
		Example: `  aerollm pqc keys
  aerollm pqc keys -a hybrid-ed25519+mldsa-65 --private-key-out node.key --public-key-out node.pub`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			if algorithm == "" {
				fmt.Fprintln(w, "supported:")
				fmt.Fprintln(w, "- "+pqc.AlgorithmHybridEd25519MLDSA65)
				fmt.Fprintln(w, "- "+pqc.AlgorithmPQCMLKEM768)
				fmt.Fprintln(w, "- "+pqc.AlgorithmPQCMLDSA65)
				return nil
			}
			if privOut == "" {
				return errors.New("--private-key-out is required when generating keys (private keys are never printed)")
			}
			km := pqc.NewQuantumSafeKeyManager(algorithm)
			pub, priv, err := km.GenerateKeyPair(cmd.Context())
			if err != nil {
				return err
			}
			if err := writeFileSafely(privOut, []byte(hex.EncodeToString(priv)+"\n"), 0o600, force); err != nil {
				return fmt.Errorf("writing private key: %w", err)
			}
			if pubOut != "" {
				if err := writeFileSafely(pubOut, []byte(hex.EncodeToString(pub)+"\n"), 0o644, force); err != nil {
					return fmt.Errorf("writing public key: %w", err)
				}
			}
			fmt.Fprintf(w, "algorithm=%s\n", algorithm)
			fmt.Fprintf(w, "public=%x\n", []byte(pub))
			fmt.Fprintf(w, "private key written to %s\n", privOut)
			return nil
		},
	}
	cmd.Flags().StringVarP(&algorithm, "algorithm", "a", "", "algorithm id, empty to list")
	cmd.Flags().StringVar(&privOut, "private-key-out", "", "file to write the private key to (mode 0600)")
	cmd.Flags().StringVar(&pubOut, "public-key-out", "", "file to write the public key to")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing key files")
	return cmd
}
