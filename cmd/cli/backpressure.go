package main

import (
	"net/http"

	"github.com/spf13/cobra"
)

func newBackpressureCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "backpressure",
		Short: "Show the gateway's backpressure controller state (/backpressure/status)",
		Long:  "Show current inflight, dropped, total, drop rate, and window start.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serverRequest(cmd, http.MethodGet, "/backpressure/status", nil, nil, formatTable, nil, nil)
		},
	}
}
