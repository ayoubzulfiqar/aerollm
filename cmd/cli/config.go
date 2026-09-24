package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

const (
	// maxConfigUpdateBytes mirrors the gateway's /config/update body cap.
	maxConfigUpdateBytes = 1 << 20
	redactedValue        = "***REDACTED***"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show or hot-reload the gateway configuration (admin)",
	}
	cmd.AddCommand(newConfigGetCmd(), newConfigUpdateCmd(), newModelsInfoCmd("models"))
	return cmd
}

func newConfigGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Show the active configuration with secrets redacted (GET /config/yaml)",
		Long: `Show the active gateway configuration (as JSON). The server redacts every
secret; the CLI additionally masks any secret-looking field or URL
credential it still finds. Durations are in nanoseconds.`,
		Example: "  aerollm config get > active.json\n  aerollm config get -o table",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serverRequest(cmd, http.MethodGet, "/config/yaml", nil, nil, formatJSON, nil, func(v any) any {
				v = redactSecrets(v)
				if f, _ := outputFormat(cmd, formatJSON); f == formatTable {
					if m, ok := v.(map[string]any); ok {
						return flattenMap(m)
					}
				}
				return v
			})
		},
	}
}

// secretFieldParts are substrings of field names whose string values are
// always masked.
var secretFieldParts = []string{"password", "secret", "api_key", "apikey", "private_key", "credential", "admin_key", "master_key"}

func isSecretField(k string) bool {
	k = strings.ToLower(k)
	if k == "token" || strings.HasSuffix(k, "_token") || k == "dsn" {
		return true
	}
	for _, p := range secretFieldParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

// redactSecrets masks secret-looking fields and URL passwords in decoded
// JSON. Empty values, env references (${VAR}) and already-redacted values
// are left as they are.
func redactSecrets(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isSecretField(k) {
				t[k] = maskSecretValue(val)
				continue
			}
			t[k] = redactSecrets(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = redactSecrets(t[i])
		}
		return t
	case string:
		if strings.Contains(t, "://") && strings.Contains(t, "@") {
			if u, err := url.Parse(t); err == nil && u.User != nil {
				if _, hasPass := u.User.Password(); hasPass {
					return u.Redacted()
				}
			}
		}
		return t
	default:
		return v
	}
}

func maskSecretValue(v any) any {
	switch t := v.(type) {
	case string:
		if t == "" || t == redactedValue || (strings.HasPrefix(t, "${") && strings.HasSuffix(t, "}")) {
			return t
		}
		return redactedValue
	case []any:
		for i := range t {
			t[i] = maskSecretValue(t[i])
		}
		return t
	case map[string]any:
		for k := range t {
			t[k] = maskSecretValue(t[k])
		}
		return t
	default:
		return v
	}
}

func newConfigUpdateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Merge a partial JSON config and hot-reload the gateway (POST /config/update)",
		Long: `Merge a (partial) JSON configuration onto the active one, validate it and
hot-reload the provider registry without dropping connections. Arrays such as
providers are replaced, not merged; secrets echoed back as "***REDACTED***"
keep their current value; durations are nanoseconds. Input comes from --file
PATH, or stdin when --file is "-" or omitted.`,
		Example: `  aerollm config update --file patch.json
  echo '{"rate_limit":{"default_tpm":200000}}' | aerollm config update`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			raw, err := readConfigInput(cmd, file)
			if err != nil {
				return err
			}
			trimmed := bytes.TrimSpace(raw)
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(trimmed, &obj); err != nil || obj == nil {
				if err == nil {
					err = errors.New("null")
				}
				return fmt.Errorf("config must be a JSON object: %w", err)
			}
			if len(obj) == 0 {
				return errors.New("config update is empty")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/config/update", nil, json.RawMessage(trimmed))
			if err != nil {
				return err
			}
			var res struct {
				Status    string       `json:"status"`
				Providers *json.Number `json:"providers"`
			}
			_ = json.Unmarshal(data, &res)
			msg := "configuration " + res.Status
			if res.Status == "" {
				msg = "configuration updated"
			}
			if res.Providers != nil {
				msg += fmt.Sprintf(" (%s provider(s))", res.Providers.String())
			}
			return printDone(cmd, data, msg, nil)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", `JSON config file ("-" or omitted: stdin)`)
	return cmd
}

func readConfigInput(cmd *cobra.Command, file string) ([]byte, error) {
	var (
		r    io.Reader
		name = "stdin"
	)
	if file == "" || file == "-" {
		if file == "" && stdinIsTerminal(cmd) {
			return nil, errors.New("no input: pass --file PATH or pipe JSON on stdin")
		}
		r = cmd.InOrStdin()
	} else {
		f, err := os.Open(filepath.Clean(file))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r, name = f, file
	}
	data, err := io.ReadAll(io.LimitReader(r, maxConfigUpdateBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	if len(data) > maxConfigUpdateBytes {
		return nil, fmt.Errorf("%s is larger than the gateway's %d byte limit", name, maxConfigUpdateBytes)
	}
	return data, nil
}

// newModelsInfoCmd builds "models info" (also mounted as "config models").
func newModelsInfoCmd(use string) *cobra.Command {
	return &cobra.Command{
		Use:     use,
		Short:   "Show loaded models with provider and capabilities (GET /model/info, admin)",
		Example: "  aerollm models info\n  aerollm config models -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/model/info", nil, nil)
			if err != nil {
				return err
			}
			return renderList(cmd, data, listView{
				fields:  []string{"data", "models"},
				columns: []string{"model", "provider", "provider_type", "capabilities", "context_window"},
			})
		},
	}
}
