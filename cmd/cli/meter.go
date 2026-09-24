package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

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
	var (
		keyID, provider, model string
		tokensIn, tokensOut    int64
		latencyMs              float64
	)
	cmd := &cobra.Command{
		Use:     "usage",
		Short:   "Record a usage event (POST /v1/meter/usage)",
		Example: "  aerollm meter usage --key-id team-a --provider openai --model gpt-4o --tokens-in 120 --tokens-out 480 --latency-ms 950",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if provider == "" || model == "" {
				return errors.New("--provider and --model are required")
			}
			if tokensIn < 0 || tokensOut < 0 || latencyMs < 0 {
				return fmt.Errorf("token counts and latency must not be negative")
			}
			rec := meter.UsageRecord{
				Timestamp: time.Now().UTC(),
				APIKey:    keyID,
				Provider:  provider,
				Model:     model,
				TokensIn:  tokensIn,
				TokensOut: tokensOut,
				LatencyMs: latencyMs,
			}
			return serverRequest(cmd, http.MethodPost, "/v1/meter/usage", nil, rec, formatJSON, nil, nil)
		},
	}
	f := cmd.Flags()
	f.StringVar(&keyID, "key-id", "", "identifier of the API key / tenant the usage is attributed to (not a secret key)")
	f.StringVar(&provider, "provider", "", "provider name")
	f.StringVar(&model, "model", "", "model name")
	f.Int64Var(&tokensIn, "tokens-in", 0, "prompt tokens")
	f.Int64Var(&tokensOut, "tokens-out", 0, "completion tokens")
	f.Float64Var(&latencyMs, "latency-ms", 0, "request latency in milliseconds")
	return cmd
}
