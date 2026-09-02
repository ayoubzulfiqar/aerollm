package main

import (
	"os"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "aerollm",
		Short: "AeroLLM CLI",
		Long:  "AeroLLM command line interface for scaffolding, plugins, and GitOps sync.",
	}

	root.AddCommand(newInitCmd())
	root.AddCommand(newPluginCmd())
	root.AddCommand(newGitOpsCmd())

	return root
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
