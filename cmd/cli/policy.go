package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
	"github.com/spf13/cobra"
)

func newPolicyCmd() *cobra.Command {
	var id, expr, severity string
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Manage compliance policies",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := compliance.NewHTTPPolicyStore()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/policy", compliance.HTTPPolicyHandler(store))
			mux.HandleFunc("/v1/policy/block", func(w http.ResponseWriter, r *http.Request) {
				compliance.HTTPBlockHandler(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"status":"ok"})
				})).ServeHTTP(w, r)
			})

			if id != "" && expr != "" {
				req := httptest.NewRequest(http.MethodPost, "/v1/policy", strings.NewReader(fmt.Sprintf(`{"id":"%s","name":"%s","expression":"%s","severity":"%s"}`, id, id, expr, severity)))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				fmt.Println(rec.Body.String())
				return nil
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/policy", nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			fmt.Println(rec.Body.String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&id, "id", "i", "", "rule id")
	cmd.Flags().StringVarP(&expr, "expr", "e", "", "expression")
	cmd.Flags().StringVarP(&severity, "severity", "s", "low", "severity")

	return cmd
}
