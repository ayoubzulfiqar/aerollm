package main

import (
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
	"github.com/spf13/cobra"
)

func newPqcCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pqc",
		Short: "Post-quantum cryptography utilities",
		Long:  "Inspect supported algorithms and generate hybrid key material.",
	}

	cmd.AddCommand(newPqcKeysCmd())
	return cmd
}

func newPqcKeysCmd() *cobra.Command {
	var algorithm string
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "List or generate PQC keys",
		Run: func(_ *cobra.Command, _ []string) {
			if algorithm == "" {
				fmt.Println("supported:")
				fmt.Println("- " + pqc.AlgorithmHybridEd25519MLDSA65)
				fmt.Println("- " + pqc.AlgorithmPQCMLKEM768)
				fmt.Println("- " + pqc.AlgorithmPQCMLDSA65)
				return
			}
			km := pqc.NewQuantumSafeKeyManager(algorithm)
			pub, priv, err := km.GenerateKeyPair(nil)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			fmt.Printf("algorithm=%s\n", algorithm)
			fmt.Printf("public=%x\n", pub)
			fmt.Printf("private=%x\n", priv)
		},
	}
	cmd.Flags().StringVarP(&algorithm, "algorithm", "a", "", "algorithm id, empty to list")
	return cmd
}
