package main

import (
	"errors"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/compliance"
	"github.com/spf13/cobra"
)

func newPolicyCmd() *cobra.Command {
	var id, name, expr, severity string
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "List, get or upsert compliance policy rules (/v1/policy)",
		Example: `  aerollm policy
  aerollm policy --id no-post
  aerollm policy --id no-post --expr deny-post --severity high`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if expr != "" {
				if id == "" {
					return errors.New("--id is required when creating a rule")
				}
				sev, err := requireOneOf("severity", severity, "low", "medium", "high", "critical", "block")
				if err != nil {
					return err
				}
				if name == "" {
					name = id
				}
				rule := compliance.HTTPPolicyRule{ID: id, Name: name, Expression: expr, Severity: sev}
				return serverRequest(cmd, http.MethodPost, "/v1/policy", nil, rule, formatJSON, nil, nil)
			}
			if id != "" {
				return serverRequest(cmd, http.MethodGet, "/v1/policy", idQuery(id), nil, formatJSON, nil, nil)
			}
			return serverRequest(cmd, http.MethodGet, "/v1/policy", nil, nil, formatTable,
				[]string{"id", "name", "expression", "severity"}, nil)
		},
	}
	cmd.Flags().StringVarP(&id, "id", "i", "", "rule id")
	cmd.Flags().StringVar(&name, "name", "", "rule name (defaults to --id)")
	cmd.Flags().StringVarP(&expr, "expr", "e", "", "rule expression, e.g. allow|deny|allow-post|deny-post (creates/updates the rule)")
	cmd.Flags().StringVarP(&severity, "severity", "s", "low", "severity: low|medium|high|critical|block")

	return cmd
}
