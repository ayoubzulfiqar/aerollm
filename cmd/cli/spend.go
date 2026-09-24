package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/spf13/cobra"
)

func newSpendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spend",
		Short: "Spend reports and transaction logs (/global/spend, admin)",
	}
	cmd.AddCommand(newSpendReportCmd(), newSpendLogsCmd())
	return cmd
}

// parseTimeFlag accepts RFC 3339 or YYYY-MM-DD (midnight UTC).
func parseTimeFlag(name, v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.DateOnly, v); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid --%s %q: want RFC 3339 (2026-01-02T15:04:05Z) or YYYY-MM-DD", name, v)
}

// parseLookback parses a duration that may use d (days) and w (weeks).
func parseLookback(name, v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	mult := time.Duration(0)
	switch {
	case strings.HasSuffix(v, "d"):
		mult = 24 * time.Hour
	case strings.HasSuffix(v, "w"):
		mult = 7 * 24 * time.Hour
	}
	if mult > 0 {
		n, err := strconv.Atoi(strings.TrimSpace(v[:len(v)-1]))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid --%s %q", name, v)
		}
		return time.Duration(n) * mult, nil
	}
	return parsePositiveDuration(name, v)
}

func newSpendReportCmd() *cobra.Command {
	var start, end, since, groupBy string
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Aggregate spend over a time range (GET /global/spend/report)",
		Long: `Aggregate spend over a time range, grouped by api_key (key IDs), customer,
team, model, provider or all. The range defaults to the last 24 hours.`,
		Example: `  aerollm spend report --since 7d --group-by model
  aerollm spend report --start 2026-09-01 --end 2026-09-30 --group-by all -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q := url.Values{}
			if since != "" && start != "" {
				return errors.New("--since and --start are mutually exclusive")
			}
			var st, et time.Time
			if since != "" {
				d, err := parseLookback("since", since)
				if err != nil {
					return err
				}
				st = time.Now().UTC().Add(-d)
			}
			if start != "" {
				t, err := parseTimeFlag("start", start)
				if err != nil {
					return err
				}
				st = t
			}
			if end != "" {
				t, err := parseTimeFlag("end", end)
				if err != nil {
					return err
				}
				et = t
			}
			if !st.IsZero() && !et.IsZero() && st.After(et) {
				return errors.New("start must be before end")
			}
			if !st.IsZero() {
				q.Set("start", st.Format(time.RFC3339))
			}
			if !et.IsZero() {
				q.Set("end", et.Format(time.RFC3339))
			}
			if groupBy != "" {
				g, err := requireOneOf("group-by", groupBy, "api_key", "customer", "team", "model", "provider", "all")
				if err != nil {
					return err
				}
				q.Set("group_by", g)
			}
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/global/spend/report", q, nil)
			if err != nil {
				return err
			}
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.UseNumber()
			var report map[string]any
			if err := dec.Decode(&report); err != nil {
				return renderResult(cmd, data, format, nil, nil)
			}
			// Group values of by_api_key should be key IDs; mask anything that
			// looks like a raw key.
			if buckets, ok := report["by_api_key"].(map[string]any); ok {
				for k, v := range buckets {
					if looksLikeRawKey(k) {
						delete(buckets, k)
						buckets[maskSecret(k)] = v
					}
				}
			}
			if format == formatJSON {
				return writeJSON(cmd.OutOrStdout(), report)
			}
			return writeSpendReportTable(cmd, report)
		},
	}
	f := cmd.Flags()
	f.StringVar(&start, "start", "", "range start: RFC 3339 or YYYY-MM-DD (default: 24h ago)")
	f.StringVar(&end, "end", "", "range end: RFC 3339 or YYYY-MM-DD (default: now)")
	f.StringVar(&since, "since", "", "range start relative to now, e.g. 6h, 7d or 2w")
	f.StringVar(&groupBy, "group-by", "", "api_key|customer|team|model|provider|all (default: api_key)")
	return cmd
}

func writeSpendReportTable(cmd *cobra.Command, report map[string]any) error {
	w := cmd.OutOrStdout()
	summary := [][]string{
		{"total_cost_usd", formatCell(report["total_cost_usd"])},
		{"total_requests", formatCell(report["total_requests"])},
	}
	if tok, ok := report["total_tokens"].(map[string]any); ok {
		summary = append(summary, []string{"total_tokens", fmt.Sprintf("input=%s output=%s total=%s",
			formatCell(tok["input"]), formatCell(tok["output"]), formatCell(tok["total"]))})
	}
	if tr, ok := report["time_range"].(map[string]any); ok {
		summary = append(summary, []string{"time_range", formatCell(tr["start"]) + " .. " + formatCell(tr["end"])})
	}
	if ds := formatCell(report["data_since"]); ds != "" && !strings.HasPrefix(ds, "0001-01-01") {
		summary = append(summary, []string{"data_since", ds})
	}
	if err := writeTable(w, nil, summary); err != nil {
		return err
	}
	type row struct {
		group, value string
		cost         float64
		cells        []string
	}
	var rows []row
	for k, v := range report {
		group, ok := strings.CutPrefix(k, "by_")
		buckets, isMap := v.(map[string]any)
		if !ok || !isMap {
			continue
		}
		for value, b := range buckets {
			bm, _ := b.(map[string]any)
			cost, _ := strconv.ParseFloat(formatCell(bm["cost_usd"]), 64)
			rows = append(rows, row{group: group, value: value, cost: cost, cells: []string{
				group, value, formatCell(bm["cost_usd"]), formatCell(bm["requests"]),
				formatCell(bm["input_tokens"]), formatCell(bm["output_tokens"]), formatCell(bm["total_tokens"]),
			}})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].group != rows[j].group {
			return rows[i].group < rows[j].group
		}
		if rows[i].cost != rows[j].cost {
			return rows[i].cost > rows[j].cost
		}
		return rows[i].value < rows[j].value
	})
	cells := make([][]string, len(rows))
	for i, r := range rows {
		cells[i] = r.cells
	}
	fmt.Fprintln(w)
	return writeTable(w, []string{"GROUP", "VALUE", "COST_USD", "REQUESTS", "INPUT_TOKENS", "OUTPUT_TOKENS", "TOTAL_TOKENS"}, cells)
}

func newSpendLogsCmd() *cobra.Command {
	var (
		filter, key    string
		page, pageSize int
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show per-request spend logs (GET /global/spend/logs)",
		Long: `Show paginated per-request spend logs, optionally filtered by key ID,
customer ID or team ID. --key accepts a raw API key ("-" reads stdin) and
filters by its key ID, computed locally so the key never appears in a URL.`,
		Example: `  aerollm spend logs --filter key_0123456789abcdef --page-size 100
  echo "$KEY" | aerollm spend logs --key -
  aerollm spend logs --filter team-search --page 2 -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q := url.Values{}
			switch {
			case filter != "" && key != "":
				return errors.New("--filter and --key are mutually exclusive")
			case key != "":
				raw, err := readSecretArg(cmd, key)
				if err != nil {
					return err
				}
				if raw == "" {
					return errors.New("--key is empty")
				}
				q.Set("filter", middleware.KeyID(raw))
			case filter != "":
				if looksLikeRawKey(filter) && strings.HasPrefix(filter, "sk-") {
					return errors.New("--filter looks like a raw API key; use --key so it is converted to its key ID and not sent in the URL")
				}
				q.Set("filter", filter)
			}
			if cmd.Flags().Changed("page") {
				if page < 1 {
					return errors.New("--page must be a positive integer")
				}
				q.Set("page", strconv.Itoa(page))
			}
			if cmd.Flags().Changed("page-size") {
				if pageSize < 1 || pageSize > 500 {
					return errors.New("--page-size must be between 1 and 500")
				}
				q.Set("page_size", strconv.Itoa(pageSize))
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/global/spend/logs", q, nil)
			if err != nil {
				return err
			}
			return renderList(cmd, data, listView{
				fields: []string{"logs", "data"},
				columns: []string{"timestamp", "request_id", "api_key", "team_id", "customer_id", "model", "provider",
					"input_tokens", "output_tokens", "cost_usd"},
				redact: func(m map[string]any) { maskKeyFields(m, "api_key") },
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&filter, "filter", "", "filter by key ID, customer ID or team ID")
	f.StringVar(&key, "key", "", `filter by a raw API key's key ID ("-" reads stdin)`)
	f.IntVar(&page, "page", 1, "page number")
	f.IntVar(&pageSize, "page-size", 50, "entries per page (1-500)")
	return cmd
}
