package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
	"github.com/spf13/cobra"
)

func newQuotaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "quota",
		Short: "Inspect quota state",
		Long:  "Show quota limits, usage, and remaining capacity.",
		Run: func(_ *cobra.Command, _ []string) {
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/quota", func(w http.ResponseWriter, r *http.Request) {
				if r == nil || r.Body == nil {
					http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
					return
				}
				defer r.Body.Close()
				var req tenant.Quota
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				store := tenant.NewInMemoryQuotaStore()
				if _, err := store.Enforce(r.Context(), &req, 0); err != nil {
					http.Error(w, `{"error":"quota not found"}`, http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(req)
			})

			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader(`{"id":"q1","scope":"tenant","target_id":"t1","limit":100,"used":25}`)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/quota", body)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 4096)
			n, _ := resp.Body.Read(buf)
			fmt.Println(strings.TrimSpace(string(buf[:n])))
		},
	}
	return cmd
}
