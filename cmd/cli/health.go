package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// probeResult is the outcome of one health endpoint check.
type probeResult struct {
	Endpoint string          `json:"endpoint"`
	OK       bool            `json:"ok"`
	Status   int             `json:"http_status,omitempty"`
	Body     json.RawMessage `json:"body,omitempty"`
	Error    string          `json:"error,omitempty"`
}

func newHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check gateway liveness (/health) and readiness (/ready); exits 1 if not ok",
		Example: `  aerollm health
  aerollm health --server https://gateway.example.com -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			results := []probeResult{
				probeHealth(cmd, client, "/health"),
				probeHealth(cmd, client, "/ready"),
			}
			healthy := true
			for _, r := range results {
				healthy = healthy && r.OK
			}
			w := cmd.OutOrStdout()
			if format == formatJSON {
				if err := writeJSON(w, map[string]any{"ok": healthy, "checks": results}); err != nil {
					return err
				}
			} else {
				rows := make([][]string, 0, len(results))
				for _, r := range results {
					state := "ok"
					if !r.OK {
						state = "FAIL"
					}
					detail := r.Error
					if detail == "" {
						detail = strings.TrimSpace(string(r.Body))
					}
					rows = append(rows, []string{r.Endpoint, state, detail})
				}
				if err := writeTable(w, []string{"ENDPOINT", "STATUS", "DETAIL"}, rows); err != nil {
					return err
				}
			}
			if !healthy {
				return fmt.Errorf("gateway at %s is not healthy", client.base.Redacted())
			}
			return nil
		},
	}
}

func probeHealth(cmd *cobra.Command, c *apiClient, path string) probeResult {
	res := probeResult{Endpoint: path}
	req, err := c.newRequest(cmd.Context(), http.MethodGet, path, nil, nil)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	resp, err := c.http.Do(req)
	if err != nil {
		res.Error = c.redact(err.Error())
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if json.Valid(body) {
		res.Body = json.RawMessage(strings.TrimSpace(string(body)))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		res.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, extractErrorMessage(body))
		return res
	}
	res.OK = healthBodyOK(body)
	if !res.OK {
		res.Error = "reported not ok: " + strings.TrimSpace(string(body))
	}
	return res
}

// healthBodyOK interprets a 2xx health/readiness body. Recognised shapes:
// {"status":"ok|healthy|ready|up|pass"} and {"ready":true|"true"}. Bodies
// with explicit negative values fail; unrecognised bodies pass on the
// strength of the 2xx status.
func healthBodyOK(body []byte) bool {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return true
	}
	if v, ok := m["ready"]; ok {
		switch t := v.(type) {
		case bool:
			return t
		case string:
			return strings.EqualFold(t, "true")
		}
	}
	if v, ok := m["status"].(string); ok {
		switch strings.ToLower(v) {
		case "ok", "healthy", "ready", "up", "pass", "serving":
			return true
		default:
			return false
		}
	}
	return true
}
