package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ayoubzulfiqar/aerollm/internal/flags"
	"github.com/spf13/cobra"
)

func newFlagsCmd() *cobra.Command {
	var key string
	var setPayload string
	cmd := &cobra.Command{
		Use:   "flags",
		Short: "List, get or set feature flags (/v1/flags)",
		Example: `  aerollm flags
  aerollm flags --key darkmode
  aerollm flags --set '{"key":"darkmode","enabled":true,"strategy":"global"}'
  aerollm flags --key beta --set @beta-flag.json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if setPayload != "" {
				raw, err := readValueArg(cmd, setPayload)
				if err != nil {
					return err
				}
				var f flags.FeatureFlag
				if err := json.Unmarshal([]byte(raw), &f); err != nil {
					return fmt.Errorf("--set must be a feature flag JSON object: %w", err)
				}
				switch {
				case f.Key == "" && key == "":
					return fmt.Errorf("feature flag key missing: set \"key\" in the JSON or pass --key")
				case f.Key == "":
					f.Key = key
				case key != "" && f.Key != key:
					return fmt.Errorf("--key %q does not match \"key\" %q in --set", key, f.Key)
				}
				if f.Percentage < 0 || f.Percentage > 100 {
					return fmt.Errorf("percentage must be between 0 and 100, got %d", f.Percentage)
				}
				return serverRequest(cmd, http.MethodPost, "/v1/flags", nil, f, formatJSON, nil, nil)
			}
			if key != "" {
				return serverRequest(cmd, http.MethodGet, "/v1/flags/"+url.PathEscape(key), nil, nil, formatJSON, nil, nil)
			}
			return serverRequest(cmd, http.MethodGet, "/v1/flags", nil, nil, formatTable,
				[]string{"key", "enabled", "strategy", "percentage", "description"}, nil)
		},
	}

	cmd.Flags().StringVarP(&key, "key", "k", "", "feature flag key")
	cmd.Flags().StringVarP(&setPayload, "set", "s", "", "create/update a flag from JSON (literal, @file, or - for stdin)")

	return cmd
}
