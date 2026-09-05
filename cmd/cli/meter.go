package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/meter"
	"github.com/spf13/cobra"
)

func newMeterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "meter",
		Short: "Usage metering utilities",
		Long:  "Record and inspect usage metrics for providers.",
	}
	cmd.AddCommand(newMeterUsageCmd())
	return cmd
}

func newMeterUsageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "usage",
		Short: "Record a usage event",
		Run: func(_ *cobra.Command, _ []string) {
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/meter/usage", func(w http.ResponseWriter, r *http.Request) {
				if r == nil || r.Body == nil {
					http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
					return
				}
				defer r.Body.Close()
				var req meter.UsageRecord
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				recorder := meter.NewRecorder()
				recorder.Record(req)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(recorder.Records())
			})

			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader(`{"api_key":"k1","provider":"p1","model":"m1","tokens_in":10,"tokens_out":20,"latency_ms":100}`)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/meter/usage", body)
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
