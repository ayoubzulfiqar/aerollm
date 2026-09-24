package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
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

// listView describes how renderList presents a (possibly enveloped) list.
type listView struct {
	// fields are the envelope keys that may hold the list, in order of
	// preference (e.g. "data", "keys"). A bare JSON array is also accepted.
	fields []string
	// columns are the table columns (all keys when empty).
	columns []string
	// redact is applied to every list item in both output modes.
	redact func(map[string]any)
	// format is applied to every list item in table mode only (e.g. to turn
	// Unix timestamps into RFC 3339).
	format func(map[string]any)
}

// renderList prints a list response. JSON mode prints the whole (redacted)
// document. Table mode prints the list as a table on stdout and the
// envelope's scalar fields (pagination cursors, totals) on stderr, so
// stdout stays machine-friendly.
func renderList(cmd *cobra.Command, raw []byte, lv listView) error {
	format, err := outputFormat(cmd, formatTable)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		_, werr := fmt.Fprintln(w, strings.TrimSpace(string(raw)))
		return werr
	}
	var (
		items    []any
		envelope map[string]any
		listKey  string
	)
	switch t := v.(type) {
	case []any:
		items = t
	case map[string]any:
		envelope = t
		for _, f := range lv.fields {
			if l, ok := t[f].([]any); ok {
				items, listKey = l, f
				break
			}
			if val, ok := t[f]; ok && val == nil {
				listKey = f // JSON null: an empty list
				break
			}
		}
	}
	each := func(fn func(map[string]any)) {
		if fn == nil {
			return
		}
		for _, it := range items {
			if m, ok := it.(map[string]any); ok {
				fn(m)
			}
		}
	}
	each(lv.redact)
	if format == formatJSON {
		return writeJSON(w, v)
	}
	if envelope != nil && listKey == "" {
		// Not the expected shape: fall back to a field table.
		return writeFieldTable(w, envelope)
	}
	each(lv.format)
	if err := writeItemsTable(w, items, lv.columns); err != nil {
		return err
	}
	if envelope != nil {
		var extras []string
		for k, val := range envelope {
			if k == listKey || k == "object" || val == nil || val == "" {
				continue
			}
			switch val.(type) {
			case map[string]any, []any:
				continue
			}
			extras = append(extras, k+"="+sanitizeCell(formatCell(val)))
		}
		if len(extras) > 0 {
			sort.Strings(extras)
			fmt.Fprintln(cmd.ErrOrStderr(), strings.Join(extras, " "))
		}
	}
	return nil
}

// printDone reports a mutating call. JSON mode prints the server's response
// (or fallback when the response is empty, e.g. 204); table mode prints msg.
func printDone(cmd *cobra.Command, data []byte, msg string, fallback any) error {
	format, err := outputFormat(cmd, formatTable)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if format == formatJSON {
		if len(bytes.TrimSpace(data)) == 0 {
			if fallback == nil {
				return nil
			}
			return writeJSON(w, fallback)
		}
		return writeRawJSON(w, data)
	}
	_, err = fmt.Fprintln(w, sanitizeCell(msg))
	return err
}

var resourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,256}$`)

// pathSegment validates a user-supplied resource id and returns it escaped
// for use as a single URL path segment.
func pathSegment(kind, id string) (string, error) {
	id = strings.TrimSpace(id)
	if !resourceIDPattern.MatchString(id) || id == "." || id == ".." {
		return "", fmt.Errorf("invalid %s id %q", kind, id)
	}
	return url.PathEscape(id), nil
}

// readSecretArg resolves a secret flag value: "-" reads the first non-empty
// line of stdin (keeps the secret out of shell history and ps output);
// anything else is used literally. A leading "Bearer " is stripped.
func readSecretArg(cmd *cobra.Command, v string) (string, error) {
	if v == "-" {
		in, err := readAllLimited(cmd.InOrStdin(), "stdin")
		if err != nil {
			return "", err
		}
		v = ""
		for _, line := range strings.Split(in, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				v = line
				break
			}
		}
	}
	v = strings.TrimSpace(v)
	if len(v) > 7 && strings.EqualFold(v[:7], "Bearer ") {
		v = strings.TrimSpace(v[7:])
	}
	return v, nil
}

// keyIDPattern matches gateway key IDs ("key_" + 16 lowercase hex chars).
var keyIDPattern = regexp.MustCompile(`^key_[0-9a-f]{16}$`)

// looksLikeRawKey reports whether a server-supplied key field holds
// something other than a non-reversible key ID or hash, i.e. a value that
// should be masked before printing.
func looksLikeRawKey(s string) bool {
	if s == "" || keyIDPattern.MatchString(s) || strings.HasPrefix(s, "key_") {
		return false
	}
	return strings.HasPrefix(s, "sk-") || len(s) > 12 && !isHex(s)
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// maskKeyFields masks raw-looking API keys in the given fields of m.
func maskKeyFields(m map[string]any, fields ...string) {
	for _, f := range fields {
		if s, ok := m[f].(string); ok && looksLikeRawKey(s) {
			m[f] = maskSecret(s)
		}
	}
}

// unixToRFC3339 rewrites Unix-second timestamp fields to RFC 3339.
func unixToRFC3339(m map[string]any, fields ...string) {
	for _, f := range fields {
		n, ok := m[f].(json.Number)
		if !ok {
			continue
		}
		if sec, err := n.Int64(); err == nil && sec > 0 {
			m[f] = time.Unix(sec, 0).UTC().Format(time.RFC3339)
		}
	}
}
