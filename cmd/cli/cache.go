package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

func newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Inspect and clear the response caches (/v1/cache, admin)",
	}
	cmd.AddCommand(newCacheStatsCmd(), newCacheClearCmd(), newCacheInspectCmd())
	return cmd
}

func newCacheStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "stats",
		Short:   "Show exact and semantic cache statistics (GET /v1/cache/stats)",
		Example: "  aerollm cache stats\n  aerollm cache stats -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serverRequest(cmd, http.MethodGet, "/v1/cache/stats", nil, nil, formatTable, nil, func(v any) any {
				if f, _ := outputFormat(cmd, formatTable); f == formatTable {
					if m, ok := v.(map[string]any); ok {
						return flattenMap(m)
					}
				}
				return v
			})
		},
	}
}

// flattenMap turns nested objects into dotted keys ("exact.hits").
func flattenMap(m map[string]any) map[string]any {
	out := map[string]any{}
	var walk func(prefix string, v map[string]any)
	walk = func(prefix string, v map[string]any) {
		for k, val := range v {
			if nested, ok := val.(map[string]any); ok && len(nested) > 0 {
				walk(prefix+k+".", nested)
				continue
			}
			out[prefix+k] = val
		}
	}
	walk("", m)
	return out
}

func newCacheClearCmd() *cobra.Command {
	var typ string
	cmd := &cobra.Command{
		Use:     "clear",
		Short:   "Clear the exact, semantic or all caches (DELETE /v1/cache)",
		Example: "  aerollm cache clear\n  aerollm cache clear --type semantic",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			t, err := requireOneOf("type", typ, "exact", "semantic", "all")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodDelete, "/v1/cache", url.Values{"type": {t}}, nil)
			if err != nil {
				return err
			}
			var res map[string]any
			_ = json.Unmarshal(data, &res)
			var cleared []string
			for k, v := range res {
				if name, ok := strings.CutSuffix(k, "_cleared"); ok && v == true {
					cleared = append(cleared, name)
				}
			}
			sort.Strings(cleared)
			msg := "cleared: " + strings.Join(cleared, ", ")
			if len(cleared) == 0 {
				msg = "nothing cleared (no cache configured)"
				if t != "all" {
					msg = "nothing cleared (no " + t + " cache configured)"
				}
			}
			return printDone(cmd, data, msg, nil)
		},
	}
	cmd.Flags().StringVar(&typ, "type", "all", "cache to clear: exact|semantic|all")
	return cmd
}

func newCacheInspectCmd() *cobra.Command {
	var (
		typ, cursor string
		pageSize    int
		all         bool
	)
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "List cached entries' keys and models, never payloads (GET /v1/cache/inspect)",
		Example: `  aerollm cache inspect --page-size 100
  aerollm cache inspect --type semantic --cursor 42
  aerollm cache inspect --all -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			t, err := requireOneOf("type", typ, "exact", "semantic")
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("page-size") && (pageSize < 1 || pageSize > 500) {
				return errors.New("--page-size must be between 1 and 500")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			var (
				data    []byte
				entries []json.RawMessage
			)
			cur := cursor
			for page := 0; ; page++ {
				q := url.Values{"type": {t}}
				if cur != "" {
					q.Set("cursor", cur)
				}
				if pageSize > 0 {
					q.Set("page_size", strconv.Itoa(pageSize))
				}
				data, err = client.call(cmd.Context(), http.MethodGet, "/v1/cache/inspect", q, nil)
				if err != nil {
					return err
				}
				if !all {
					break
				}
				var pg struct {
					Entries []json.RawMessage `json:"entries"`
					Cursor  string            `json:"cursor"`
				}
				if err := json.Unmarshal(data, &pg); err != nil {
					return fmt.Errorf("decoding /v1/cache/inspect response: %w", err)
				}
				entries = append(entries, pg.Entries...)
				if pg.Cursor == "" || pg.Cursor == "0" || pg.Cursor == cur || page >= 9999 {
					if data, err = json.Marshal(map[string]any{"entries": entries, "cursor": "0"}); err != nil {
						return err
					}
					break
				}
				cur = pg.Cursor
			}
			return renderList(cmd, data, listView{
				fields:  []string{"entries"},
				columns: []string{"key", "model", "semantic", "token_count", "created_at"},
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&typ, "type", "exact", "cache to inspect: exact|semantic")
	f.StringVar(&cursor, "cursor", "", "pagination cursor from a previous page")
	f.IntVar(&pageSize, "page-size", 0, "entries per page, 1-500 (default: server setting)")
	f.BoolVar(&all, "all", false, "follow the cursor and list every entry")
	return cmd
}
