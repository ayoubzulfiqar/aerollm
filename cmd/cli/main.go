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
	root.AddCommand(newBillingCmd())
	root.AddCommand(newEdgeCmd())
	root.AddCommand(newOpenStandardCmd())
	root.AddCommand(newPqcCmd())
	root.AddCommand(newSpatialCmd())
	root.AddCommand(newFederatedCmd())
	root.AddCommand(newTraceCmd())
	root.AddCommand(newHealthCmd())
	root.AddCommand(newResilienceCmd())
	root.AddCommand(newTrafficCmd())
	root.AddCommand(newSloCmd())
	root.AddCommand(newChaosCmd())
	root.AddCommand(newBackpressureCmd())
	root.AddCommand(newQuotaCmd())
	root.AddCommand(newAuditCmd())
	root.AddCommand(newAdmissionCmd())

	return root
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
