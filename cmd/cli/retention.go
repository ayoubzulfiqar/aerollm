package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/retention"
	"github.com/spf13/cobra"
)

func newRetentionCmd() *cobra.Command {
	var id, resource string
	var ttl int
	var maxItems int
	cmd := &cobra.Command{
		Use:   "retention",
		Short: "Manage data retention policies",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := retention.NewRetentionStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/retention", retention.WebhookHandler(store))

			if id != "" && resource != "" {
				body := fmt.Sprintf(`{"id":"%s","resource":"%s","ttl":%d,"max_items":%d}`, id, resource, ttl, maxItems)
				req := httptest.NewRequest(http.MethodPost, "/v1/retention", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/retention", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&id, "id", "i", "", "policy id")
	cmd.Flags().StringVarP(&resource, "resource", "r", "", "resource name")
	cmd.Flags().IntVarP(&ttl, "ttl", "t", 24, "time-to-live in hours")
	cmd.Flags().IntVarP(&maxItems, "max-items", "m", 1000, "maximum items to retain")

	return cmd
}
