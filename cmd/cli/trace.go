package main

import (
	"net/http"

	"github.com/spf13/cobra"
)

func newTraceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trace",
		Short: "Trace and metrics utilities",
		Long:  "Inspect the gateway's trace provider metrics.",
	}

	cmd.AddCommand(newTraceMetricsCmd())
	return cmd
}

func newTraceMetricsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Show the gateway's trace metrics (/v1/trace/metrics)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serverRequest(cmd, http.MethodGet, "/v1/trace/metrics", nil, nil, formatTable, nil, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	return cmd
}
