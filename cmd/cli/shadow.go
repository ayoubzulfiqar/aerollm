package main

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

func newShadowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shadow",
		Short: "Inspect shadow-traffic results (admin)",
		Long: `Inspect the results of shadow traffic mirrored to AEROLLM_SHADOW_URL.
Dispatch shadow requests with "aerollm traffic shadow".`,
	}
	cmd.AddCommand(&cobra.Command{
		Use:     "results",
		Short:   "List recent shadow results (GET /v1/shadow/results)",
		Example: "  aerollm shadow results\n  aerollm shadow results -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/v1/shadow/results", nil, nil)
			if err != nil {
				return err
			}
			return renderList(cmd, data, listView{
				fields:  []string{"results", "data"},
				columns: []string{"timestamp", "provider", "status_code", "latency", "auth_dropped", "error"},
				format:  formatNanosField("latency"),
			})
		},
	})
	return cmd
}

// formatNanosField renders integer nanosecond durations (Go time.Duration
// JSON) as human-readable durations.
func formatNanosField(fields ...string) func(map[string]any) {
	return func(m map[string]any) {
		for _, f := range fields {
			if n, ok := m[f].(interface{ Int64() (int64, error) }); ok {
				if ns, err := n.Int64(); err == nil {
					m[f] = time.Duration(ns).String()
				}
			}
		}
	}
}
