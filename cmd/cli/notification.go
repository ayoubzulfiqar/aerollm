package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/ayoubzulfiqar/aerollm/internal/notification"
	"github.com/spf13/cobra"
)

func newNotificationCmd() *cobra.Command {
	var resource string
	cmd := &cobra.Command{
		Use:   "notification",
		Short: "Manage notification channels and subscriptions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := notification.NewStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/notification/channels", notification.WebhookHandler(store))
			mux.HandleFunc("/v1/notification/subscriptions", notification.WebhookHandler(store))

			if resource == "channel" {
				req := httptest.NewRequest(http.MethodGet, "/v1/notification/channels", nil)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/notification/subscriptions", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&resource, "resource", "r", "subscription", "resource: channel|subscription")

	return cmd
}
