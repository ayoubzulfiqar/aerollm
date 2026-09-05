package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/incident"
	"github.com/spf13/cobra"
)

func newIncidentCmd() *cobra.Command {
	var title, description, severity, status string
	cmd := &cobra.Command{
		Use:   "incident",
		Short: "Manage incidents",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := incident.NewStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/incidents", incident.WebhookHandler(store))

			if title != "" {
				body := fmt.Sprintf(`{"title":"%s","description":"%s","severity":"%s","status":"%s"}`, title, description, severity, status)
				req := httptest.NewRequest(http.MethodPost, "/v1/incidents", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/incidents", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&title, "title", "t", "", "incident title")
	cmd.Flags().StringVarP(&description, "desc", "d", "", "incident description")
	cmd.Flags().StringVarP(&severity, "severity", "s", "medium", "severity: low|medium|high|critical")
	cmd.Flags().StringVarP(&status, "status", "", "open", "status: open|investigating|resolved|closed")

	return cmd
}
