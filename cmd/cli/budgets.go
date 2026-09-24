package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/spf13/cobra"
)

func newBudgetsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "budgets",
		Aliases: []string{"budget"},
		Short:   "Get, set and remove per-key spend limits (/v1/budgets, admin)",
		Long: `Manage per-key spend limits enforced by the gateway.

Budgets are keyed by the gateway key ID ("key_" + 16 hex characters, as
shown in logs and spend reports). --key accepts a raw API key instead ("-"
reads it from stdin); it is converted to its key ID locally, so the raw key
is never sent to the server, put in a URL or printed.`,
	}
	cmd.AddCommand(newBudgetsGetCmd(), newBudgetsSetCmd(), newBudgetsDeleteCmd())
	return cmd
}

// budgetTarget selects a budget by key ID or raw key.
type budgetTarget struct{ keyID, key string }

func (b *budgetTarget) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&b.keyID, "key-id", "", "gateway key ID (key_<16 hex>)")
	cmd.Flags().StringVar(&b.key, "key", "", `raw API key, converted to its key ID locally ("-" reads stdin)`)
}

// resolve returns the key ID to send to the server.
func (b *budgetTarget) resolve(cmd *cobra.Command) (string, error) {
	switch {
	case b.keyID != "" && b.key != "":
		return "", errors.New("--key-id and --key are mutually exclusive")
	case b.keyID != "":
		id := strings.ToLower(strings.TrimSpace(b.keyID))
		if !keyIDPattern.MatchString(id) {
			return "", fmt.Errorf("invalid --key-id %q: want key_ followed by 16 hex characters", b.keyID)
		}
		return id, nil
	case b.key != "":
		raw, err := readSecretArg(cmd, b.key)
		if err != nil {
			return "", err
		}
		if raw == "" {
			return "", errors.New("--key is empty")
		}
		id := middleware.KeyID(raw)
		fmt.Fprintf(cmd.ErrOrStderr(), "using key ID %s for key %s\n", id, maskSecret(raw))
		return id, nil
	default:
		return "", errors.New("one of --key-id or --key is required")
	}
}

func newBudgetsGetCmd() *cobra.Command {
	var target budgetTarget
	cmd := &cobra.Command{
		Use:     "get",
		Short:   "Show a key's budget and current spend (GET /v1/budgets)",
		Example: "  aerollm budgets get --key-id key_0123456789abcdef\n  echo \"$KEY\" | aerollm budgets get --key - -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id, err := target.resolve(cmd)
			if err != nil {
				return err
			}
			return serverRequest(cmd, http.MethodGet, "/v1/budgets", url.Values{"key_id": {id}}, nil, formatTable, nil, nil)
		},
	}
	target.register(cmd)
	return cmd
}

func newBudgetsSetCmd() *cobra.Command {
	var (
		target budgetTarget
		maxUSD float64
		period string
	)
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set a key's spend limit in USD (PUT /v1/budgets)",
		Long: `Set (or replace) a key's spend limit. --period must match the gateway's
budget period (the server rejects per-key periods it does not support);
omit it to use the gateway setting.`,
		Example: "  aerollm budgets set --key-id key_0123456789abcdef --max-usd 25\n  aerollm budgets set --key - --max-usd 100 --period monthly < key.txt",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("max-usd") {
				return errors.New("--max-usd is required")
			}
			if maxUSD < 0 || math.IsNaN(maxUSD) || math.IsInf(maxUSD, 0) {
				return errors.New("--max-usd must be a non-negative number")
			}
			body := map[string]any{"max_usd": maxUSD}
			if cmd.Flags().Changed("period") {
				p, err := requireOneOf("period", period, "daily", "monthly", "none")
				if err != nil {
					return err
				}
				body["period"] = p
			}
			id, err := target.resolve(cmd)
			if err != nil {
				return err
			}
			body["key_id"] = id
			return serverRequest(cmd, http.MethodPut, "/v1/budgets", nil, body, formatTable, nil, nil)
		},
	}
	target.register(cmd)
	cmd.Flags().Float64Var(&maxUSD, "max-usd", 0, "spend limit in USD (0 blocks all paid usage)")
	cmd.Flags().StringVar(&period, "period", "", "budget period: daily|monthly|none (default: gateway setting)")
	return cmd
}

func newBudgetsDeleteCmd() *cobra.Command {
	var target budgetTarget
	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Remove a key's spend limit (DELETE /v1/budgets); recorded spend is kept",
		Example: "  aerollm budgets delete --key-id key_0123456789abcdef",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id, err := target.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodDelete, "/v1/budgets", url.Values{"key_id": {id}}, nil)
			if err != nil {
				return err
			}
			return printDone(cmd, data, "removed budget for "+id, map[string]any{"key_id": id, "deleted": true})
		},
	}
	target.register(cmd)
	return cmd
}
