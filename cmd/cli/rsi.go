package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// newRSICmd creates the RSI (Recursive Self-Improvement) CLI command group.
func newRSICmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rsi",
		Short: "Inspect and trigger RSI (recursive self-improvement) cycles",
		Long:  "Inspect headroom assessments, list cycle history, trigger RSI cycles and manage RSI configuration.",
	}
	cmd.AddCommand(newRSIHeadroomCmd())
	cmd.AddCommand(newRSICyclesCmd())
	cmd.AddCommand(newRSICurrentCmd())
	cmd.AddCommand(newRSITriggerCmd())
	cmd.AddCommand(newRSIConfigCmd())
	return cmd
}

// addDeprecatedAddrFlag keeps the historical --addr/-a flag working as an
// alias for the global --server flag.
func addDeprecatedAddrFlag(cmd *cobra.Command) {
	cmd.Flags().StringP("addr", "a", "", "base address of the aerollm server")
	_ = cmd.Flags().MarkDeprecated("addr", "use --server instead")
}

// rsiGet fetches an RSI endpoint and renders it.
func rsiGet(cmd *cobra.Command, path string, def string, columns []string) error {
	client, err := newServerClient(cmd)
	if err != nil {
		return err
	}
	data, err := client.call(cmd.Context(), http.MethodGet, path, nil, nil)
	if err != nil {
		return err
	}
	return renderResult(cmd, data, def, columns, nil)
}

func newRSIHeadroomCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "headroom",
		Short: "Print the HCI headroom assessment for all dimensions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rsiGet(cmd, "/v1/rsi/headroom", formatJSON, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	return cmd
}

func newRSICyclesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cycles",
		Short: "List RSI cycle history",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rsiGet(cmd, "/v1/rsi/cycles", formatTable,
				[]string{"id", "dimension", "improvement_pct", "deployed", "timestamp"})
		},
	}
	addDeprecatedAddrFlag(cmd)
	return cmd
}

func newRSICurrentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current",
		Short: "Show the most recent RSI cycle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rsiGet(cmd, "/v1/rsi/current", formatJSON, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	return cmd
}

func newRSITriggerCmd() *cobra.Command {
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Run one RSI cycle on the server and print the result",
		Long: `Trigger an RSI cycle. The server runs the cycle synchronously, so the
command waits for it to finish; --wait bounds how long to wait (it overrides
--timeout for this request).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			if wait > 0 {
				client.timeout = wait
				client.http = &http.Client{Timeout: wait}
			}
			req, err := client.newRequest(cmd.Context(), http.MethodPost, "/v1/rsi/cycle", nil, nil)
			if err != nil {
				return err
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
			if resp.StatusCode == http.StatusPartialContent {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: the cycle completed with errors (HTTP 206); result is partial")
			}
			return renderResult(cmd, []byte(data), formatJSON, nil, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	cmd.Flags().DurationVarP(&wait, "wait", "w", 0, "maximum time to wait for the cycle to complete (default: --timeout)")
	return cmd
}

func newRSIConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Get or set RSI configuration",
	}
	cmd.AddCommand(newRSIConfigGetCmd())
	cmd.AddCommand(newRSIConfigSetCmd())
	return cmd
}

func newRSIConfigGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get current RSI configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rsiGet(cmd, "/v1/rsi/config", formatJSON, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	return cmd
}

func newRSIConfigSetCmd() *cobra.Command {
	var cfgJSON string
	cmd := &cobra.Command{
		Use:     "set",
		Short:   "Update RSI configuration (JSON via --json, @file or - for stdin)",
		Example: "  aerollm rsi config set --json @rsi.json\n  aerollm rsi config set --json '{\"enabled\":true}'",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(cfgJSON) == "" {
				return errors.New("--json is required")
			}
			raw, err := readValueArg(cmd, cfgJSON)
			if err != nil {
				return err
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(raw), &obj); err != nil {
				return fmt.Errorf("--json must be a JSON object: %w", err)
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPut, "/v1/rsi/config", nil, json.RawMessage(raw))
			if err != nil {
				return err
			}
			return renderResult(cmd, data, formatJSON, nil, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	cmd.Flags().StringVarP(&cfgJSON, "json", "j", "", "JSON config to set (literal, @file, or - for stdin)")
	return cmd
}
