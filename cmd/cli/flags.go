package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/flags"
	"github.com/spf13/cobra"
)

func newFlagsCmd() *cobra.Command {
	var key string
	var setPayload string
	cmd := &cobra.Command{
		Use:   "flags",
		Short: "Manage feature flags",
		RunE: func(cmd *cobra.Command, _ []string) error {
			mux := http.NewServeMux()
			store := flags.NewStore()
			mux.HandleFunc("/v1/flags", flags.WebhookHandler(store))
			mux.HandleFunc("/v1/flags/", flags.WebhookHandler(store))
			if setPayload != "" {
				req := httptest.NewRequest(http.MethodPost, "/v1/flags/"+key, strings.NewReader(setPayload))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}
			if key != "" {
				req := httptest.NewRequest(http.MethodGet, "/v1/flags/"+key, nil)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}
			req := httptest.NewRequest(http.MethodGet, "/v1/flags", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}

	cmd.Flags().StringVarP(&key, "key", "k", "", "Feature flag key")
	cmd.Flags().StringVarP(&setPayload, "set", "s", "", "Set feature flag as JSON")

	return cmd
}
