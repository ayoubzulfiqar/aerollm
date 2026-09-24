package main

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// serverRequest performs one request against the gateway and renders the
// response. Lists default to a table (with the given columns); single
// objects default to JSON.
func serverRequest(cmd *cobra.Command, method, path string, query url.Values, body any, defFormat string, columns []string, transform func(any) any) error {
	client, err := newServerClient(cmd)
	if err != nil {
		return err
	}
	data, err := client.call(cmd.Context(), method, path, query, body)
	if err != nil {
		return err
	}
	return renderResult(cmd, data, defFormat, columns, transform)
}

// idQuery returns ?id=<id> (properly escaped) or nil.
func idQuery(id string) url.Values {
	if id == "" {
		return nil
	}
	return url.Values{"id": {id}}
}

// requireOneOf validates an enum-like flag value (case-insensitive) and
// returns it lower-cased.
func requireOneOf(flag, value string, allowed ...string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(value))
	if slices.Contains(allowed, v) {
		return v, nil
	}
	return "", fmt.Errorf("invalid --%s %q: must be one of %s", flag, value, strings.Join(allowed, "|"))
}

// anyFlagChanged reports whether any of the named flags was set explicitly.
func anyFlagChanged(cmd *cobra.Command, names ...string) bool {
	for _, n := range names {
		if cmd.Flags().Changed(n) {
			return true
		}
	}
	return false
}

// parsePositiveDuration parses a Go duration flag that must be > 0.
func parsePositiveDuration(flag, v string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: %w", flag, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("--%s must be positive, got %s", flag, v)
	}
	return d, nil
}
