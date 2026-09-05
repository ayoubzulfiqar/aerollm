package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/secrets"
	"github.com/spf13/cobra"
)

func newSecretsCmd() *cobra.Command {
	var name, secretType, value string
	cmd := &cobra.Command{
		Use:   "secrets",
		Short: "Manage secrets",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := secrets.NewStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/secrets", secrets.WebhookHandler(store))

			if name != "" && value != "" {
				body := fmt.Sprintf(`{"name":"%s","value":"%s","type":"%s"}`, name, value, secretType)
				req := httptest.NewRequest(http.MethodPost, "/v1/secrets", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/secrets", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "", "secret name")
	cmd.Flags().StringVarP(&value, "value", "v", "", "secret value")
	cmd.Flags().StringVarP(&secretType, "type", "t", "token", "secret type")

	return cmd
}
