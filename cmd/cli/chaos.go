package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/chaos"
	"github.com/spf13/cobra"
)

func newChaosCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chaos",
		Short: "Chaos engineering utilities",
		Long:  "Inspect fault injection status, simulate latency, and validate resilience.",
	}

	cmd.AddCommand(newChaosFaultCmd())
	return cmd
}

func newChaosFaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fault",
		Short: "Run a local chaos fault test against /v1/chaos/fault",
		Run: func(_ *cobra.Command, _ []string) {
			mux := http.NewServeMux()
			injector := chaos.NewInjector(chaos.Config{Type: chaos.FaultError, Percent: 100, StatusCode: http.StatusBadGateway, Message: "boom"})
			mux.HandleFunc("/v1/chaos/fault", chaos.Handler(injector))

			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader(`{"type":"error","percent":100}`)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/chaos/fault", body)
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
