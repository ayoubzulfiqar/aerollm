package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/spf13/cobra"
)

func newKeysCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "Generate, inspect and delete virtual API keys",
		Long: `Manage virtual keys via /key/generate, /key/info and /key/delete.

These are admin endpoints: pass the gateway master key with --api-key or
$AEROLLM_API_KEY. Full keys are only ever printed once, by "keys generate".
To avoid leaking a key into shell history, pass "-" and pipe it on stdin.`,
	}
	cmd.AddCommand(newKeysGenerateCmd(), newKeysInfoCmd(), newKeysDeleteCmd())
	return cmd
}

func newKeysGenerateCmd() *cobra.Command {
	var (
		req      keymanager.GenerateRequest
		metadata string
		role     string
	)
	cmd := &cobra.Command{
		Use:     "generate",
		Short:   "Create a virtual key (the key is shown only once)",
		Example: "  aerollm keys generate --models gpt-4o,claude-3-5-sonnet --duration 720h --max-budget 25 --user-id alice",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if req.MaxBudget < 0 {
				return errors.New("--max-budget must not be negative")
			}
			if req.RateLimitRPS < 0 || req.RateLimitTPM < 0 {
				return errors.New("--rps/--tpm must not be negative")
			}
			if role != "" {
				r, err := requireOneOf("role", role, "member", string(keymanager.RoleTeamAdmin))
				if err != nil {
					return err
				}
				if r == string(keymanager.RoleTeamAdmin) {
					req.Role = keymanager.RoleTeamAdmin
				}
			}
			if metadata != "" {
				raw, err := readValueArg(cmd, metadata)
				if err != nil {
					return err
				}
				if err := json.Unmarshal([]byte(raw), &req.Metadata); err != nil {
					return fmt.Errorf("--metadata must be a JSON object: %w", err)
				}
			}
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			var resp keymanager.GenerateResponse
			if err := client.callJSON(cmd.Context(), http.MethodPost, "/key/generate", nil, req, &resp); err != nil {
				return err
			}
			key := resp.Key
			if key == "" {
				key = resp.Token
			}
			if key == "" {
				return errors.New("server response did not contain a key")
			}
			w := cmd.OutOrStdout()
			if format == formatJSON {
				return writeJSON(w, map[string]string{"key": key, "key_hash": resp.KeyHash, "expires": resp.Expires})
			}
			expires := resp.Expires
			if expires == "" || strings.HasPrefix(expires, "0001-01-01") {
				expires = "never"
			}
			if err := writeTable(w, nil, [][]string{
				{"key:", key},
				{"key_hash:", resp.KeyHash},
				{"expires:", expires},
			}); err != nil {
				return err
			}
			fmt.Fprintln(cmd.ErrOrStderr(), "Store this key now: it cannot be retrieved again.")
			return nil
		},
	}
	f := cmd.Flags()
	f.StringSliceVar(&req.Models, "models", nil, "models the key may use (comma separated; empty = all)")
	f.StringVar(&req.Duration, "duration", "", "key lifetime, e.g. 24h, 30d or 2w (empty = never expires)")
	f.Float64Var(&req.MaxBudget, "max-budget", 0, "maximum spend in USD (0 = unlimited)")
	f.StringSliceVar(&req.Aliases, "aliases", nil, "key aliases (comma separated)")
	f.StringVar(&metadata, "metadata", "", "metadata JSON object (or @file / - for stdin)")
	f.StringVar(&req.UserID, "user-id", "", "owning user id")
	f.StringVar(&req.TeamID, "team-id", "", "owning team id")
	f.StringVar(&req.TenantID, "tenant-id", "", "owning tenant id")
	f.StringVar(&req.BudgetDuration, "budget-duration", "", "budget reset period, e.g. 30d or 720h")
	f.Float64Var(&req.RateLimitRPS, "rps", 0, "per-key request rate limit (requests/second, 0 = default)")
	f.IntVar(&req.RateLimitTPM, "tpm", 0, "per-key token rate limit (tokens/minute, 0 = default)")
	f.StringVar(&role, "role", "", "key role: member|team_admin")
	return cmd
}

// keySelector identifies a key either by its plaintext value or by its hash.
type keySelector struct {
	Key  string
	Hash string
}

func (s keySelector) display() string {
	if s.Hash != "" {
		return "hash " + s.Hash
	}
	return maskSecret(s.Key)
}

// resolveKeyArgs reads keys from args (a lone "-" reads newline separated
// keys from stdin) or uses --key-hash values.
func resolveKeyArgs(cmd *cobra.Command, args, hashes []string) ([]keySelector, error) {
	var out []keySelector
	for _, h := range hashes {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, keySelector{Hash: h})
		}
	}
	for _, a := range args {
		if a == "-" {
			in, err := readAllLimited(cmd.InOrStdin(), "stdin")
			if err != nil {
				return nil, err
			}
			for _, line := range strings.Split(in, "\n") {
				if line = strings.TrimSpace(line); line != "" {
					out = append(out, keySelector{Key: line})
				}
			}
			continue
		}
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, keySelector{Key: a})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("specify a key argument, \"-\" to read keys from stdin, or --key-hash")
	}
	return out, nil
}

func newKeysInfoCmd() *cobra.Command {
	var keyHash string
	cmd := &cobra.Command{
		Use:     "info [KEY|-]",
		Short:   "Show metadata for a virtual key",
		Example: "  echo \"$KEY\" | aerollm keys info -\n  aerollm keys info --key-hash 3f9a...",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var hashes []string
			if keyHash != "" {
				hashes = []string{keyHash}
			}
			sels, err := resolveKeyArgs(cmd, args, hashes)
			if err != nil {
				return err
			}
			if len(sels) != 1 {
				return errors.New("keys info takes exactly one key")
			}
			sel := sels[0]
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			// Prefer a JSON body so the key never appears in a URL (and thus in
			// access logs); fall back to GET ?key_hash= only if the server does
			// not accept POST.
			body := map[string]string{}
			if sel.Hash != "" {
				body["key_hash"] = sel.Hash
			} else {
				body["key"] = sel.Key
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/key/info", nil, body)
			var apiErr *apiError
			if errors.As(err, &apiErr) && apiErr.Status == http.StatusMethodNotAllowed {
				// Send only the SHA-256 key hash in the URL, never the key.
				q := url.Values{}
				if sel.Hash != "" {
					q.Set("key_hash", sel.Hash)
				} else {
					q.Set("key_hash", keymanager.HashKey(sel.Key))
				}
				data, err = client.call(cmd.Context(), http.MethodGet, "/key/info", q, nil)
			}
			if err != nil {
				return fmt.Errorf("key %s: %w", sel.display(), err)
			}
			return renderResult(cmd, data, formatTable, nil, redactKeyFields)
		},
	}
	cmd.Flags().StringVar(&keyHash, "key-hash", "", "look up by key hash instead of the key itself")
	return cmd
}

func newKeysDeleteCmd() *cobra.Command {
	var keyHashes []string
	cmd := &cobra.Command{
		Use:     "delete [KEY...|-]",
		Short:   "Delete (revoke) one or more virtual keys",
		Example: "  aerollm keys delete sk-abc..._def...\n  aerollm keys delete --key-hash 3f9a... --key-hash 77c1...",
		RunE: func(cmd *cobra.Command, args []string) error {
			sels, err := resolveKeyArgs(cmd, args, keyHashes)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			var failed int
			for _, sel := range sels {
				// "keys" is the LiteLLM request shape; "key"/"key_hash" is the
				// shape understood by AeroLLM's keymanager handler. One request
				// per key keeps both server variants correct.
				body := map[string]any{}
				if sel.Hash != "" {
					body["key_hash"] = sel.Hash
					body["keys"] = []string{sel.Hash}
				} else {
					body["key"] = sel.Key
					body["keys"] = []string{sel.Key}
				}
				if _, err := client.call(cmd.Context(), http.MethodPost, "/key/delete", nil, body); err != nil {
					failed++
					fmt.Fprintf(cmd.ErrOrStderr(), "failed to delete key %s: %v\n", sel.display(), err)
					continue
				}
				fmt.Fprintf(w, "deleted key %s\n", sel.display())
			}
			if failed > 0 {
				return fmt.Errorf("%d of %d key(s) could not be deleted", failed, len(sels))
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&keyHashes, "key-hash", nil, "delete by key hash (repeatable)")
	return cmd
}

// redactKeyFields masks any plaintext key material a server might echo in
// a key info response.
func redactKeyFields(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	for _, f := range []string{"key", "token", "api_key"} {
		if s, ok := m[f].(string); ok && s != "" {
			m[f] = maskSecret(s)
		}
	}
	return m
}
