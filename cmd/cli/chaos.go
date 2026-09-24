package main

import (
	"fmt"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/chaos"
	"github.com/spf13/cobra"
)

func newChaosCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chaos",
		Short: "Chaos engineering utilities",
		Long:  "Configure fault injection on the gateway to validate resilience.",
	}

	cmd.AddCommand(newChaosFaultCmd())
	return cmd
}

func newChaosFaultCmd() *cobra.Command {
	var (
		faultType  string
		percent    float64
		duration   string
		statusCode int
		message    string
	)
	cmd := &cobra.Command{
		Use:   "fault",
		Short: "Configure the gateway fault injector (POST /v1/chaos/fault)",
		Example: `  aerollm chaos fault --type latency --percent 10 --duration 500ms
  aerollm chaos fault --type error --percent 5 --status-code 503
  aerollm chaos fault --percent 0    # disable`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			typ, err := requireOneOf("type", faultType, string(chaos.FaultLatency), string(chaos.FaultError), string(chaos.FaultPanic))
			if err != nil {
				return err
			}
			if percent < 0 || percent > 100 {
				return fmt.Errorf("--percent must be between 0 and 100, got %g", percent)
			}
			if statusCode != 0 && (statusCode < 400 || statusCode > 599) {
				return fmt.Errorf("--status-code must be a 4xx/5xx code, got %d", statusCode)
			}
			cfg := chaos.Config{Type: chaos.FaultType(typ), Percent: percent, StatusCode: statusCode, Message: message}
			if duration != "" {
				d, err := parsePositiveDuration("duration", duration)
				if err != nil {
					return err
				}
				cfg.Duration = d
			}
			return serverRequest(cmd, http.MethodPost, "/v1/chaos/fault", nil, cfg, formatJSON, nil, nil)
		},
	}
	f := cmd.Flags()
	f.StringVar(&faultType, "type", "error", "fault type: latency|error|panic")
	f.Float64Var(&percent, "percent", 100, "percentage of requests affected (0 disables)")
	f.StringVar(&duration, "duration", "", "injected latency, e.g. 250ms (latency faults)")
	f.IntVar(&statusCode, "status-code", http.StatusBadGateway, "HTTP status returned by error faults")
	f.StringVar(&message, "message", "chaos fault injected", "error message returned by error faults")
	return cmd
}
