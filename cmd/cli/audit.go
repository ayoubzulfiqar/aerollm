package main

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

func newAuditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Audit logging utilities",
		Long:  "Inspect audit events and compliance pipeline state.",
	}
	cmd.AddCommand(newAuditEventsCmd())
	return cmd
}

func newAuditEventsCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show recent audit events (/v1/audit/events)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if limit <= 0 {
				return fmt.Errorf("--limit must be positive, got %d", limit)
			}
			// The endpoint requires a JSON body; send the limit so servers that
			// support it can apply it, and truncate client-side as well.
			body := map[string]int{"limit": limit}
			truncate := func(v any) any {
				if list, ok := v.([]any); ok && len(list) > limit {
					return list[:limit]
				}
				return v
			}
			return serverRequest(cmd, http.MethodPost, "/v1/audit/events", nil, body, formatTable,
				[]string{"Timestamp", "Policy", "Decision", "Reason"}, truncate)
		},
	}
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "max events to return")
	return cmd
}
