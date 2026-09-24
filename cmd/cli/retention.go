package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/retention"
	"github.com/spf13/cobra"
)

func newRetentionCmd() *cobra.Command {
	var id, resource string
	var ttl int
	var maxItems int
	cmd := &cobra.Command{
		Use:   "retention",
		Short: "List, get or upsert data retention policies (/v1/retention)",
		Example: `  aerollm retention
  aerollm retention --id logs-30d --resource request_logs --ttl 720 --max-items 100000`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if resource != "" {
				if id == "" {
					return errors.New("--id is required when creating a policy")
				}
				if ttl <= 0 {
					return fmt.Errorf("--ttl must be a positive number of hours, got %d", ttl)
				}
				if maxItems < 0 {
					return fmt.Errorf("--max-items must not be negative, got %d", maxItems)
				}
				// RetentionPolicy.TTL is a time.Duration, which JSON encodes as
				// nanoseconds: convert the hours given on the command line.
				p := retention.RetentionPolicy{
					ID:       id,
					Resource: resource,
					TTL:      time.Duration(ttl) * time.Hour,
					MaxItems: maxItems,
				}
				return serverRequest(cmd, http.MethodPost, "/v1/retention", nil, p, formatJSON, nil, nil)
			}
			if id != "" {
				return serverRequest(cmd, http.MethodGet, "/v1/retention", idQuery(id), nil, formatJSON, nil, nil)
			}
			return serverRequest(cmd, http.MethodGet, "/v1/retention", nil, nil, formatTable,
				[]string{"id", "resource", "ttl", "max_items", "created_at"}, nil)
		},
	}
	cmd.Flags().StringVarP(&id, "id", "i", "", "policy id")
	cmd.Flags().StringVarP(&resource, "resource", "r", "", "resource name (creates/updates the policy)")
	cmd.Flags().IntVarP(&ttl, "ttl", "t", 24, "time-to-live in hours")
	cmd.Flags().IntVarP(&maxItems, "max-items", "m", 1000, "maximum items to retain")

	return cmd
}
