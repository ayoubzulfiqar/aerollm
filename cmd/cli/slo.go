package main

import (
	"net/http"

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
		Short: "Show the gateway's SLO error budget (/v1/slo/budget)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			req, err := client.newRequest(cmd.Context(), http.MethodGet, "/v1/slo/budget", nil, nil)
			if err != nil {
				return err
			}
			if target != "" {
				req.Header.Set("x-slo-target", target)
			}
			resp, err := client.do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			data, err := readAllLimited(resp.Body, "response")
			if err != nil {
				return err
			}
			return renderResult(cmd, []byte(data), formatTable, nil, nil)
		},
	}
	cmd.Flags().StringVarP(&target, "target", "t", "latency", "SLO target name")
	return cmd
}
