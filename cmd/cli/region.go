package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/region"
	"github.com/spf13/cobra"
)

func newRegionCmd() *cobra.Command {
	var resource, name, endpoint string
	var primary bool
	cmd := &cobra.Command{
		Use:   "region",
		Short: "Manage regions, residency policies, and route rules",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := region.NewStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/region/regions", region.WebhookHandler(store))
			mux.HandleFunc("/v1/region/residency", region.WebhookHandler(store))
			mux.HandleFunc("/v1/region/routes", region.WebhookHandler(store))

			if resource == "region" && name != "" {
				body := fmt.Sprintf(`{"id":"%s","name":"%s","endpoint":"%s","primary":%v}`, name, name, endpoint, primary)
				req := httptest.NewRequest(http.MethodPost, "/v1/region/regions", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/region/regions", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&resource, "resource", "r", "region", "resource: region|residency|route")
	cmd.Flags().StringVarP(&name, "name", "n", "", "region name")
	cmd.Flags().StringVarP(&endpoint, "endpoint", "e", "", "region endpoint")
	cmd.Flags().BoolVarP(&primary, "primary", "p", false, "is primary region")

	return cmd
}
