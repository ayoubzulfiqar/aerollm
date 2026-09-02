package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "aerollm",
		Short: "AeroLLM CLI",
		Long:  "AeroLLM command line interface for scaffolding, plugins, and GitOps sync.",
	}

	root.AddCommand(newInitCmd())
	root.AddCommand(newPluginCmd())
	root.AddCommand(newGitOpsCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
