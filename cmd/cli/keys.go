package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/spf13/cobra"
)

func newKeysCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "Generate, list, update, block, rotate and delete virtual API keys",
		Long: `Manage virtual keys via the /key/* endpoints (generate, info, list,
update, block, unblock, regenerate, delete).

These are admin endpoints: pass the gateway master key with --api-key or
$AEROLLM_API_KEY. Full keys are only ever printed once, by "keys generate"
and "keys regenerate". Keys can be selected by value or with --key-hash; to
avoid leaking a key into shell history, pass "-" and pipe it on stdin.`,
	}
	cmd.AddCommand(newKeysGenerateCmd(), newKeysInfoCmd(), newKeysDeleteCmd(), newKeysListCmd(),
		newKeysUpdateCmd(), newKeysBlockCmd(true), newKeysBlockCmd(false), newKeysRegenerateCmd())
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
			return printNewKey(cmd, format, resp, "Store this key now: it cannot be retrieved again.")
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

// printNewKey prints a freshly issued key (the only time it is shown).
func printNewKey(cmd *cobra.Command, format string, resp keymanager.GenerateResponse, note string) error {
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
	fmt.Fprintln(cmd.ErrOrStderr(), note)
	return nil
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

// resolveOneKey resolves exactly one key from args or --key-hash.
func resolveOneKey(cmd *cobra.Command, name string, args []string, keyHash string) (keySelector, error) {
	var hashes []string
	if keyHash != "" {
		hashes = []string{keyHash}
	}
	sels, err := resolveKeyArgs(cmd, args, hashes)
	if err != nil {
		return keySelector{}, err
	}
	if len(sels) != 1 {
		return keySelector{}, fmt.Errorf("%s takes exactly one key", name)
	}
	return sels[0], nil
}

// body returns the {"key": ...} or {"key_hash": ...} request body.
func (s keySelector) body() map[string]any {
	if s.Hash != "" {
		return map[string]any{"key_hash": s.Hash}
	}
	return map[string]any{"key": s.Key}
}

func newKeysInfoCmd() *cobra.Command {
	var keyHash string
	cmd := &cobra.Command{
		Use:     "info [KEY|-]",
		Short:   "Show metadata for a virtual key",
		Example: "  echo \"$KEY\" | aerollm keys info -\n  aerollm keys info --key-hash 3f9a...",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sel, err := resolveOneKey(cmd, "keys info", args, keyHash)
			if err != nil {
				return err
			}
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
	redactKeyMap(m)
	return m
}

func redactKeyMap(m map[string]any) {
	for _, f := range []string{"key", "token", "api_key"} {
		if s, ok := m[f].(string); ok && s != "" {
			m[f] = maskSecret(s)
		}
	}
}

func newKeysListCmd() *cobra.Command {
	var (
		teamID, userID string
		includeRevoked bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List virtual keys (GET /key/list)",
		Example: "  aerollm keys list\n  aerollm keys list --team-id t1 --include-revoked -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q := url.Values{}
			if teamID != "" {
				q.Set("team_id", teamID)
			}
			if userID != "" {
				q.Set("user_id", userID)
			}
			if includeRevoked {
				q.Set("include_revoked", "true")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/key/list", q, nil)
			if err != nil {
				return err
			}
			return renderList(cmd, data, listView{
				fields:  []string{"keys", "data"},
				columns: []string{"key_hash", "prefix", "status", "blocked", "team_id", "user_id", "spend", "max_budget", "expires_at"},
				redact:  redactKeyMap,
				format: func(m map[string]any) {
					if s, _ := m["expires_at"].(string); s == "" || strings.HasPrefix(s, "0001-01-01") {
						m["expires_at"] = "never"
					}
				},
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&teamID, "team-id", "", "only keys of this team")
	f.StringVar(&userID, "user-id", "", "only keys of this user")
	f.BoolVar(&includeRevoked, "include-revoked", false, "include revoked (deleted) keys")
	return cmd
}

func newKeysUpdateCmd() *cobra.Command {
	var (
		keyHash, duration, budgetDuration, metadata, role string
		models, aliases                                   []string
		maxBudget, rps                                    float64
		tpm                                               int
		blocked, resetSpend                               bool
	)
	cmd := &cobra.Command{
		Use:   "update [KEY|-]",
		Short: "Update a virtual key's limits, models, expiry or role (POST /key/update)",
		Long: `Partially update a virtual key: only the flags you pass are changed.
Pass --models "" to allow all models again.`,
		Example: `  aerollm keys update --key-hash 3f9a... --max-budget 50 --rps 5
  echo "$KEY" | aerollm keys update - --models gpt-4o,gpt-4o-mini --duration 30d`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sel, err := resolveOneKey(cmd, "keys update", args, keyHash)
			if err != nil {
				return err
			}
			req := keymanager.UpdateRequest{KeyHash: sel.Hash, Key: sel.Key}
			f := cmd.Flags()
			if f.Changed("models") {
				m := append([]string{}, models...)
				req.Models = &m
			}
			if f.Changed("aliases") {
				a := append([]string{}, aliases...)
				req.Aliases = &a
			}
			if f.Changed("max-budget") {
				if maxBudget < 0 || math.IsNaN(maxBudget) || math.IsInf(maxBudget, 0) {
					return errors.New("--max-budget must be a non-negative number")
				}
				req.MaxBudget = &maxBudget
			}
			if f.Changed("duration") {
				req.Duration = &duration
			}
			if f.Changed("budget-duration") {
				req.BudgetDuration = &budgetDuration
			}
			if f.Changed("rps") {
				if rps < 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
					return errors.New("--rps must not be negative")
				}
				req.RateLimitRPS = &rps
			}
			if f.Changed("tpm") {
				if tpm < 0 {
					return errors.New("--tpm must not be negative")
				}
				req.RateLimitTPM = &tpm
			}
			if f.Changed("blocked") {
				req.Blocked = &blocked
			}
			if f.Changed("role") {
				r, err := requireOneOf("role", role, "member", string(keymanager.RoleTeamAdmin))
				if err != nil {
					return err
				}
				kr := keymanager.RoleMember
				if r == string(keymanager.RoleTeamAdmin) {
					kr = keymanager.RoleTeamAdmin
				}
				req.Role = &kr
			}
			if f.Changed("metadata") {
				raw, err := readValueArg(cmd, metadata)
				if err != nil {
					return err
				}
				if err := json.Unmarshal([]byte(raw), &req.Metadata); err != nil || req.Metadata == nil {
					return fmt.Errorf("--metadata must be a JSON object: %v", err)
				}
			}
			req.ResetSpend = resetSpend
			if !anyFlagChanged(cmd, "models", "aliases", "max-budget", "duration", "budget-duration",
				"rps", "tpm", "blocked", "role", "metadata", "reset-spend") {
				return errors.New("nothing to update: pass at least one of --models, --aliases, --max-budget, --duration, --budget-duration, --rps, --tpm, --blocked, --role, --metadata or --reset-spend")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/key/update", nil, req)
			if err != nil {
				return fmt.Errorf("key %s: %w", sel.display(), err)
			}
			return renderResult(cmd, data, formatTable, nil, redactKeyFields)
		},
	}
	f := cmd.Flags()
	f.StringVar(&keyHash, "key-hash", "", "select the key by hash instead of by value")
	f.StringSliceVar(&models, "models", nil, "models the key may use (comma separated; \"\" = all)")
	f.StringSliceVar(&aliases, "aliases", nil, "key aliases (comma separated)")
	f.Float64Var(&maxBudget, "max-budget", 0, "maximum spend in USD (0 = unlimited)")
	f.StringVar(&duration, "duration", "", "new lifetime from now, e.g. 24h, 30d or 2w (\"\" = never expires)")
	f.StringVar(&budgetDuration, "budget-duration", "", "budget reset period, e.g. 30d or 720h")
	f.Float64Var(&rps, "rps", 0, "per-key request rate limit (requests/second, 0 = default)")
	f.IntVar(&tpm, "tpm", 0, "per-key token rate limit (tokens/minute, 0 = default)")
	f.BoolVar(&blocked, "blocked", false, "block (true) or unblock (false) the key")
	f.StringVar(&role, "role", "", "key role: member|team_admin")
	f.StringVar(&metadata, "metadata", "", "replace metadata with this JSON object (or @file / - for stdin)")
	f.BoolVar(&resetSpend, "reset-spend", false, "reset the key's accumulated spend to 0")
	return cmd
}

// newKeysBlockCmd builds "keys block" (block=true) or "keys unblock".
func newKeysBlockCmd(block bool) *cobra.Command {
	verb, path, done := "block", "/key/block", "blocked"
	short := "Block one or more virtual keys (POST /key/block); blocked keys are rejected until unblocked"
	if !block {
		verb, path, done = "unblock", "/key/unblock", "unblocked"
		short = "Unblock one or more virtual keys (POST /key/unblock)"
	}
	var keyHashes []string
	cmd := &cobra.Command{
		Use:     verb + " [KEY...|-]",
		Short:   short,
		Example: "  aerollm keys " + verb + " --key-hash 3f9a...\n  echo \"$KEY\" | aerollm keys " + verb + " -",
		RunE: func(cmd *cobra.Command, args []string) error {
			sels, err := resolveKeyArgs(cmd, args, keyHashes)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			var failed int
			for _, sel := range sels {
				if _, err := client.call(cmd.Context(), http.MethodPost, path, nil, sel.body()); err != nil {
					failed++
					fmt.Fprintf(cmd.ErrOrStderr(), "failed to %s key %s: %v\n", verb, sel.display(), err)
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s key %s\n", done, sel.display())
			}
			if failed > 0 {
				return fmt.Errorf("%d of %d key(s) could not be %s", failed, len(sels), done)
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&keyHashes, "key-hash", nil, verb+" by key hash (repeatable)")
	return cmd
}

func newKeysRegenerateCmd() *cobra.Command {
	var keyHash string
	cmd := &cobra.Command{
		Use:   "regenerate [KEY|-]",
		Short: "Rotate a virtual key: issue a new key and revoke the old one (POST /key/regenerate)",
		Long: `Rotate a virtual key. The new key keeps the old key's settings and is
printed exactly once; the old key stops working immediately.`,
		Example: "  aerollm keys regenerate --key-hash 3f9a...\n  echo \"$OLD_KEY\" | aerollm keys regenerate - -o json",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sel, err := resolveOneKey(cmd, "keys regenerate", args, keyHash)
			if err != nil {
				return err
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
			if err := client.callJSON(cmd.Context(), http.MethodPost, "/key/regenerate", nil, sel.body(), &resp); err != nil {
				return fmt.Errorf("key %s: %w", sel.display(), err)
			}
			return printNewKey(cmd, format, resp, "The previous key has been revoked. Store this key now: it cannot be retrieved again.")
		},
	}
	cmd.Flags().StringVar(&keyHash, "key-hash", "", "select the key by hash instead of by value")
	return cmd
}
