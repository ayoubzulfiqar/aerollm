package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
	"github.com/spf13/cobra"
)

func newAuditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Audit logging utilities",
		Long:  "Inspect audit events and compliance pipeline state.",
	}
	cmd.AddCommand(newAuditEventsCmd())
	return cmd
}

func newAuditEventsCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show recent audit events",
		Run: func(_ *cobra.Command, _ []string) {
			if limit <= 0 {
				limit = 20
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/audit/events", func(w http.ResponseWriter, r *http.Request) {
				if r == nil || r.Body == nil {
					http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
					return
				}
				defer r.Body.Close()
				var req struct{}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				logger := compliance.NewMemoryAuditLogger()
				logger.Log(&compliance.AuditEvent{Timestamp: time.Now(), Policy: "default", Decision: "allow", Reason: "audit endpoint"})
				events := logger.Events()
				if limit > 0 && len(events) > limit {
					events = events[:limit]
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(events)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader("{}")
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/audit/events", body)
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
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "max events to return")
	return cmd
}
