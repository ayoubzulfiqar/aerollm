package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
	"github.com/spf13/cobra"
)

func newQuotaCmd() *cobra.Command {
	var (
		id, scope, target  string
		limit, used, burst int64
	)
	cmd := &cobra.Command{
		Use:     "quota",
		Short:   "Check a quota against the gateway's quota enforcer (/v1/quota)",
		Long:    "Submit a quota (limit, usage) to the gateway for enforcement and show the resulting state.",
		Example: `  aerollm quota --id q1 --scope tenant --target t1 --limit 100 --used 25`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" || target == "" {
				return errors.New("--id and --target are required")
			}
			if limit < 0 || used < 0 || burst < 0 {
				return fmt.Errorf("--limit, --used and --burst must not be negative")
			}
			sc, err := requireOneOf("scope", scope, string(tenant.ScopeTenant), string(tenant.ScopeTeam), string(tenant.ScopeUser))
			if err != nil {
				return err
			}
			q := tenant.Quota{
				ID:       id,
				Scope:    tenant.QuotaScope(sc),
				TargetID: target,
				Limit:    limit,
				Used:     used,
				Burst:    burst,
			}
			return serverRequest(cmd, http.MethodPost, "/v1/quota", nil, q, formatTable, nil, nil)
		},
	}
	f := cmd.Flags()
	f.StringVar(&id, "id", "", "quota id")
	f.StringVar(&scope, "scope", "tenant", "quota scope: tenant|team|user")
	f.StringVar(&target, "target", "", "id of the tenant/team/user the quota applies to")
	f.Int64Var(&limit, "limit", 100, "quota limit")
	f.Int64Var(&used, "used", 0, "current usage")
	f.Int64Var(&burst, "burst", 0, "burst allowance")
	return cmd
}
