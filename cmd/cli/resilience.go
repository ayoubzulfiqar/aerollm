package main

import (
	"net/http"

	"github.com/spf13/cobra"
)

func newResilienceCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resilience",
		Short: "Show the gateway's resilience status (/resilience/status)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serverRequest(cmd, http.MethodGet, "/resilience/status", nil, nil, formatJSON, nil, nil)
		},
	}
}
