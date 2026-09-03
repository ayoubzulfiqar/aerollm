package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/slo"
	"github.com/spf13/cobra"
)

func newSloCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "slo",
		Short: "Service-level objective utilities",
		Long:  "Inspect error budget, budget consumption, and SLO windows.",
	}
	cmd.AddCommand(newSloBudgetCmd())
	return cmd
}

func newSloBudgetCmd() *cobra.Command {
	var target string
	cmd := &cobra.Command{
		Use:   "budget",
		Short: "Show current SLO budget snapshot",
		Run: func(_ *cobra.Command, _ []string) {
			if target == "" {
				target = "latency"
			}
			budget := slo.NewErrorBudget(100)
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/slo/budget", slo.Handler(budget, target))

			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/slo/budget", nil)
			req.Header.Set("x-slo-target", target)
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
	cmd.Flags().StringVarP(&target, "target", "t", "latency", "SLO target name")
	return cmd
}
