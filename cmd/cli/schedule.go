package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/schedule"
	"github.com/spf13/cobra"
)

func newScheduleCmd() *cobra.Command {
	var name, taskType, cron, payload string
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage scheduled automation tasks",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := schedule.NewStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/schedule", schedule.WebhookHandler(store))

			if name != "" && cron != "" {
				body := fmt.Sprintf(`{"name":"%s","type":"%s","schedule":"%s","payload":"%s"}`, name, taskType, cron, payload)
				req := httptest.NewRequest(http.MethodPost, "/v1/schedule", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/schedule", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "", "task name")
	cmd.Flags().StringVarP(&taskType, "type", "t", "cron", "task type: cron|interval|onetime")
	cmd.Flags().StringVarP(&cron, "schedule", "s", "", "cron expression or interval")
	cmd.Flags().StringVarP(&payload, "payload", "p", "{}", "task payload")

	return cmd
}
