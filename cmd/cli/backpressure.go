package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/ayoubzulfiqar/aerollm/internal/backpressure"
	"github.com/spf13/cobra"
)

func newBackpressureCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "backpressure",
		Short: "Inspect backpressure controller state",
		Long:  "Show current inflight, dropped, total, drop rate, and window start.",
		Run: func(_ *cobra.Command, _ []string) {
			bp := backpressure.NewBackpressureController(backpressure.DefaultConfig())
			mux := http.NewServeMux()
			mux.HandleFunc("/backpressure/status", bp.Handler())
			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			req, _ := http.NewRequest(http.MethodGet, server.URL+"/backpressure/status", nil)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 4096)
			n, _ := resp.Body.Read(buf)
			fmt.Println(string(buf[:n]))
		},
	}
}
