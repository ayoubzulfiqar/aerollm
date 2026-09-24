package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/secrets"
	"github.com/spf13/cobra"
)

const maskedValue = "********"

func newSecretsCmd() *cobra.Command {
	var (
		id, name, secretType, value, valueFile string
		valueStdin, reveal, del                bool
	)
	cmd := &cobra.Command{
		Use:   "secrets",
		Short: "List, get, store or delete secrets (/v1/secrets); values are masked unless --reveal",
		Long: `Manage secrets stored by the gateway.

Secret values are masked in all output unless --reveal is given. Prefer
--value-stdin or --value-file over --value: command-line arguments are
visible to other local users (ps) and end up in shell history.`,
		Example: `  aerollm secrets
  printf %s "$TOKEN" | aerollm secrets --name github-token --value-stdin
  aerollm secrets --id sec_github-token --reveal
  aerollm secrets --id sec_github-token --delete`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			mask := maskSecretValues
			if reveal {
				mask = nil
			}
			switch {
			case del:
				if id == "" {
					return errors.New("--delete requires --id")
				}
				if err := serverRequest(cmd, http.MethodDelete, "/v1/secrets", idQuery(id), nil, formatJSON, nil, nil); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted secret %s\n", id)
				return nil
			case name != "" || valueStdin || valueFile != "" || value != "":
				if name == "" {
					return errors.New("--name is required to store a secret")
				}
				sources := 0
				for _, set := range []bool{value != "", valueStdin, valueFile != ""} {
					if set {
						sources++
					}
				}
				if sources != 1 {
					return errors.New("provide the secret with exactly one of --value, --value-stdin or --value-file")
				}
				val := value
				var err error
				switch {
				case valueStdin:
					val, err = readValueArg(cmd, "-")
				case valueFile != "":
					val, err = readValueArg(cmd, "@"+valueFile)
				default:
					fmt.Fprintln(cmd.ErrOrStderr(), "warning: --value exposes the secret in the process list and shell history; prefer --value-stdin")
				}
				if err != nil {
					return err
				}
				val = strings.TrimRight(val, "\r\n")
				if val == "" {
					return errors.New("secret value is empty")
				}
				body := secrets.Secret{ID: id, Name: name, Value: val, Type: secretType}
				return serverRequest(cmd, http.MethodPost, "/v1/secrets", nil, body, formatJSON, nil, maskSecretValues)
			case id != "":
				return serverRequest(cmd, http.MethodGet, "/v1/secrets", idQuery(id), nil, formatJSON, nil, mask)
			default:
				return serverRequest(cmd, http.MethodGet, "/v1/secrets", nil, nil, formatTable,
					[]string{"id", "name", "type", "value", "created_at"}, mask)
			}
		},
	}
	f := cmd.Flags()
	f.StringVar(&id, "id", "", "secret id (get/delete; optional on create)")
	f.StringVarP(&name, "name", "n", "", "secret name (stores a secret)")
	f.StringVarP(&value, "value", "v", "", "secret value (insecure: visible in ps/history; prefer --value-stdin)")
	f.BoolVar(&valueStdin, "value-stdin", false, "read the secret value from stdin")
	f.StringVar(&valueFile, "value-file", "", "read the secret value from a file")
	f.StringVarP(&secretType, "type", "t", "token", "secret type")
	f.BoolVar(&reveal, "reveal", false, "show secret values in output")
	f.BoolVar(&del, "delete", false, "delete secret --id")

	return cmd
}

// maskSecretValues replaces every "value" field (at any depth) of a decoded
// secrets response with a fixed mask.
func maskSecretValues(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			if strings.EqualFold(k, "value") {
				if s, ok := e.(string); ok && s != "" {
					t[k] = maskedValue
				}
				continue
			}
			t[k] = maskSecretValues(e)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = maskSecretValues(e)
		}
		return t
	default:
		return v
	}
}
