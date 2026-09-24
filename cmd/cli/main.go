package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "aerollm",
		Short: "AeroLLM gateway command line interface",
		Long: `AeroLLM command line interface.

Talk to an AeroLLM gateway (chat, models, keys, metrics, health and the
admin APIs), migrate LiteLLM configs, scaffold projects and build plugins.

Connection settings:
  --server   / $AEROLLM_URL      gateway base URL (default ` + defaultServerURL + `)
  --api-key  / $AEROLLM_API_KEY  sent as "Authorization: Bearer <key>"`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	addGlobalFlags(root)

	root.AddCommand(newChatCmd())
	root.AddCommand(newModelsCmd())
	root.AddCommand(newKeysCmd())
	root.AddCommand(newMetricsCmd())
	root.AddCommand(newHealthCmd())
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
	root.AddCommand(newResilienceCmd())
	root.AddCommand(newTrafficCmd())
	root.AddCommand(newSloCmd())
	root.AddCommand(newChaosCmd())
	root.AddCommand(newBackpressureCmd())
	root.AddCommand(newQuotaCmd())
	root.AddCommand(newAuditCmd())
	root.AddCommand(newAdmissionCmd())
	root.AddCommand(newMeterCmd())
	root.AddCommand(newFlagsCmd())
	root.AddCommand(newEvalCmd())
	root.AddCommand(newPolicyCmd())
	root.AddCommand(newRetentionCmd())
	root.AddCommand(newIncidentCmd())
	root.AddCommand(newNotificationCmd())
	root.AddCommand(newScheduleCmd())
	root.AddCommand(newSecretsCmd())
	root.AddCommand(newRegionCmd())
	root.AddCommand(newMigrateCmd())
	root.AddCommand(newRSICmd())

	return root
}

// run executes the CLI with the given arguments and streams and returns the
// process exit code: 0 on success, 1 on any error.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	root := newRootCmd()
	root.SetArgs(args)
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
