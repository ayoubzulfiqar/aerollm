package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/trace"
	"github.com/spf13/cobra"
)

func newTraceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trace",
		Short: "Trace and metrics utilities",
		Long:  "Inspect trace provider metrics and generate sample span data.",
	}

	cmd.AddCommand(newTraceMetricsCmd())
	return cmd
}

func newTraceMetricsCmd() *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Show local trace metrics snapshot",
		Run: func(_ *cobra.Command, _ []string) {
			if addr == "" {
				addr = "http://localhost:8080"
			}
			p := trace.NewProvider(trace.Config{ServiceName: "aerollm"})
			_, span := p.StartSpan(nil, "sample")
			p.End(nil, span, 10*time.Millisecond, false)

			mux := http.NewServeMux()
			mux.HandleFunc("/v1/trace/metrics", p.MetricsHandler())
			server := httptest.NewServer(mux)
			defer server.Close()

			req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/trace/metrics", nil)
			resp, err := http.DefaultClient.Do(req)
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
	cmd.Flags().StringVarP(&addr, "addr", "a", "", "base address for trace metrics endpoint")
	return cmd
}
