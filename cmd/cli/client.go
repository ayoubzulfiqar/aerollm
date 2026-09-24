package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultServerURL = "http://localhost:8080"
	defaultEdgeURL   = "http://localhost:7910"
	defaultTimeout   = 60 * time.Second

	// maxResponseBytes caps how much of a (non-streaming) response body the
	// CLI will buffer, so a misbehaving server cannot exhaust memory.
	maxResponseBytes = 64 << 20
	// maxErrorBodyBytes caps how much of an error body is echoed to stderr.
	maxErrorBodyBytes = 4 << 10
	// maxInputBytes caps prompt/dataset input read from stdin or files.
	maxInputBytes = 32 << 20
)

// addGlobalFlags registers the connection and output flags shared by every
// command that talks to an AeroLLM server.
func addGlobalFlags(root *cobra.Command) {
	pf := root.PersistentFlags()
	pf.String("server", "", "AeroLLM server base URL (env AEROLLM_URL, default "+defaultServerURL+")")
	pf.String("api-key", "", "API key sent as 'Authorization: Bearer <key>' (env AEROLLM_API_KEY)")
	pf.Duration("timeout", defaultTimeout, "HTTP request timeout")
	pf.StringP("output", "o", "", "output format: json|table (default depends on the command)")
}

// flagValue returns the string value of a (possibly inherited) flag, or "" if
// the command has no such flag.
func flagValue(cmd *cobra.Command, name string) string {
	if f := cmd.Flag(name); f != nil {
		return f.Value.String()
	}
	return ""
}

func flagChanged(cmd *cobra.Command, name string) bool {
	f := cmd.Flag(name)
	return f != nil && f.Changed
}

// resolveServerURL applies the precedence --server > --addr (deprecated) >
// $AEROLLM_URL > $AEROLLM_SERVER_URL > default.
func resolveServerURL(cmd *cobra.Command) string {
	if flagChanged(cmd, "server") {
		return flagValue(cmd, "server")
	}
	if flagChanged(cmd, "addr") {
		return flagValue(cmd, "addr")
	}
	for _, env := range []string{"AEROLLM_URL", "AEROLLM_SERVER_URL"} {
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			return v
		}
	}
	if v := flagValue(cmd, "server"); v != "" {
		return v
	}
	return defaultServerURL
}

func resolveAPIKey(cmd *cobra.Command) string {
	if flagChanged(cmd, "api-key") {
		return strings.TrimSpace(flagValue(cmd, "api-key"))
	}
	return strings.TrimSpace(os.Getenv("AEROLLM_API_KEY"))
}

func resolveTimeout(cmd *cobra.Command) time.Duration {
	if f := cmd.Flag("timeout"); f != nil {
		if d, err := time.ParseDuration(f.Value.String()); err == nil && d > 0 {
			return d
		}
	}
	return defaultTimeout
}

// apiClient is the single HTTP client used by every server-facing command.
type apiClient struct {
	base    *url.URL
	apiKey  string
	timeout time.Duration
	http    *http.Client
	stderr  io.Writer
}

// newServerClient builds a client for the AeroLLM gateway from the command's
// flags and environment.
func newServerClient(cmd *cobra.Command) (*apiClient, error) {
	return newAPIClient(resolveServerURL(cmd), resolveAPIKey(cmd), resolveTimeout(cmd), cmd.ErrOrStderr())
}

// newAPIClient validates baseURL and returns a client. TLS certificates are
// always verified (the default transport is used).
func newAPIClient(baseURL, apiKey string, timeout time.Duration, stderr io.Writer) (*apiClient, error) {
	u, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if stderr == nil {
		stderr = io.Discard
	}
	c := &apiClient{
		base:    u,
		apiKey:  apiKey,
		timeout: timeout,
		http:    &http.Client{Timeout: timeout},
		stderr:  stderr,
	}
	if apiKey != "" && u.Scheme == "http" && !isLoopbackHost(u.Hostname()) {
		fmt.Fprintf(stderr, "warning: sending API key over unencrypted http to %s; use https\n", u.Host)
	}
	return c, nil
}

func parseBaseURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("server URL is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid server URL %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid server URL %q: missing host", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid server URL %q: must not contain a query or fragment", raw)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// endpoint joins the base URL with path and query. path must start with "/"
// and is taken as already escaped (callers url.PathEscape user-supplied
// segments), so it is not escaped a second time.
func (c *apiClient) endpoint(path string, query url.Values) string {
	u := *c.base
	unescaped, err := url.PathUnescape(path)
	if err != nil {
		unescaped = path
	}
	u.Path = c.base.Path + unescaped
	u.RawPath = c.base.EscapedPath() + path
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

// newRequest builds a request with auth, JSON body and accept headers.
func (c *apiClient) newRequest(ctx context.Context, method, path string, query url.Values, body any) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case []byte:
			rdr = bytes.NewReader(b)
		case json.RawMessage:
			rdr = bytes.NewReader(b)
		default:
			buf, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("encoding request body: %w", err)
			}
			rdr = bytes.NewReader(buf)
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path, query), rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "aerollm-cli")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return req, nil
}

// apiError is returned for non-2xx responses.
type apiError struct {
	Method string
	Path   string
	Status int
	// Message is the server's error message (extracted from its JSON error
	// body when possible) or the raw body text.
	Message string
}

func (e *apiError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	return fmt.Sprintf("%s %s: server returned %d %s: %s", e.Method, e.Path, e.Status, http.StatusText(e.Status), msg)
}

// newAPIError reads (a bounded prefix of) the error body and extracts the
// server's message. The client's API key is redacted if it is echoed back.
func (c *apiClient) newAPIError(resp *http.Response, method, path string) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	return &apiError{Method: method, Path: path, Status: resp.StatusCode, Message: c.redact(extractErrorMessage(body))}
}

// extractErrorMessage understands {"error":"msg"}, {"error":{"message":..}}
// (OpenAI style), {"message":"msg"} and plain text bodies.
func extractErrorMessage(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return ""
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
		if raw, ok := obj["error"]; ok {
			var s string
			if json.Unmarshal(raw, &s) == nil && s != "" {
				return s
			}
			var nested struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    any    `json:"code"`
			}
			if json.Unmarshal(raw, &nested) == nil && nested.Message != "" {
				if nested.Type != "" {
					return nested.Message + " (" + nested.Type + ")"
				}
				return nested.Message
			}
		}
		if raw, ok := obj["message"]; ok {
			var s string
			if json.Unmarshal(raw, &s) == nil && s != "" {
				return s
			}
		}
	}
	return trimmed
}

func (c *apiClient) redact(s string) string {
	if len(c.apiKey) >= 8 {
		s = strings.ReplaceAll(s, c.apiKey, "[REDACTED]")
	}
	return s
}

// do sends the request and returns the response for 2xx statuses; any other
// status is converted to an *apiError. The caller must close the body.
func (c *apiClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, c.redactErr(err))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, c.newAPIError(resp, req.Method, req.URL.Path)
	}
	return resp, nil
}

func (c *apiClient) redactErr(err error) error {
	if len(c.apiKey) >= 8 && strings.Contains(err.Error(), c.apiKey) {
		return errors.New(c.redact(err.Error()))
	}
	return err
}

// call performs a request and returns the (bounded) response body.
func (c *apiClient) call(ctx context.Context, method, path string, query url.Values, body any) ([]byte, error) {
	req, err := c.newRequest(ctx, method, path, query, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s %s: reading response: %w", method, path, err)
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("%s %s: response larger than %d bytes", method, path, maxResponseBytes)
	}
	return data, nil
}

// callJSON performs a request and decodes the JSON response into out.
func (c *apiClient) callJSON(ctx context.Context, method, path string, query url.Values, body, out any) error {
	data, err := c.call(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s %s: decoding response: %w", method, path, err)
	}
	return nil
}

// ---- output helpers --------------------------------------------------------

const (
	formatJSON  = "json"
	formatTable = "table"
)

// outputFormat returns the requested --output format, falling back to def.
func outputFormat(cmd *cobra.Command, def string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(flagValue(cmd, "output")))
	switch v {
	case "":
		return def, nil
	case formatJSON, formatTable:
		return v, nil
	default:
		return "", fmt.Errorf("invalid --output %q: must be json or table", v)
	}
}

// writeJSON writes v as indented JSON followed by a newline.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// writeRawJSON re-indents raw JSON; non-JSON bodies are written verbatim.
func writeRawJSON(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, bytes.TrimSpace(raw), "", "  "); err != nil {
		_, werr := fmt.Fprintln(w, strings.TrimSpace(string(raw)))
		return werr
	}
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err
}

// writeTable renders rows with aligned columns.
func writeTable(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if len(headers) > 0 {
		fmt.Fprintln(tw, strings.Join(headers, "\t"))
	}
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, c := range r {
			cells[i] = sanitizeCell(c)
		}
		fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}
	return tw.Flush()
}

// sanitizeCell keeps table cells on one line and strips control characters
// (e.g. terminal escape sequences injected via server data).
func sanitizeCell(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\t' || r == '\n' || r == '\r':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		}
		return r
	}, s)
}

// formatCell renders a decoded JSON value for a table cell.
func formatCell(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, formatCell(e))
		}
		return strings.Join(parts, ",")
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}

// renderResult prints a JSON response body. In json mode it is re-indented;
// in table mode an array of objects becomes a table with the given columns
// (or all keys when columns is empty) and an object becomes a key/value
// table. transform, if non-nil, may rewrite the decoded value first (e.g. to
// mask secrets); it applies to both modes.
func renderResult(cmd *cobra.Command, raw []byte, defFormat string, columns []string, transform func(any) any) error {
	format, err := outputFormat(cmd, defFormat)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil // e.g. 204 No Content
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		// Not JSON: print as-is.
		_, werr := fmt.Fprintln(w, strings.TrimSpace(string(raw)))
		return werr
	}
	if transform != nil {
		v = transform(v)
	}
	if format == formatJSON {
		return writeJSON(w, v)
	}
	switch t := v.(type) {
	case []any:
		if len(t) == 0 {
			_, err := fmt.Fprintln(w, "(none)")
			return err
		}
		cols := columns
		if len(cols) == 0 {
			cols = collectKeys(t)
		}
		rows := make([][]string, 0, len(t))
		for _, item := range t {
			m, ok := item.(map[string]any)
			if !ok {
				rows = append(rows, []string{formatCell(item)})
				continue
			}
			row := make([]string, len(cols))
			for i, c := range cols {
				row[i] = formatCell(lookupField(m, c))
			}
			rows = append(rows, row)
		}
		headers := make([]string, len(cols))
		for i, c := range cols {
			headers[i] = strings.ToUpper(c)
		}
		return writeTable(w, headers, rows)
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		rows := make([][]string, 0, len(keys))
		for _, k := range keys {
			rows = append(rows, []string{k, formatCell(t[k])})
		}
		return writeTable(w, []string{"FIELD", "VALUE"}, rows)
	case nil:
		_, err := fmt.Fprintln(w, "(none)")
		return err
	default:
		_, err := fmt.Fprintln(w, formatCell(t))
		return err
	}
}

// lookupField finds a key case-insensitively (some server structs lack JSON
// tags and serialize with Go field names).
func lookupField(m map[string]any, key string) any {
	if v, ok := m[key]; ok {
		return v
	}
	norm := strings.ReplaceAll(strings.ToLower(key), "_", "")
	for k, v := range m {
		if strings.ReplaceAll(strings.ToLower(k), "_", "") == norm {
			return v
		}
	}
	return nil
}

func collectKeys(items []any) []string {
	seen := map[string]bool{}
	var keys []string
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		var ks []string
		for k := range m {
			if !seen[k] {
				seen[k] = true
				ks = append(ks, k)
			}
		}
		sort.Strings(ks)
		keys = append(keys, ks...)
	}
	return keys
}

// ---- input helpers ---------------------------------------------------------

// readValueArg resolves a flag/argument value: "-" reads stdin, "@path" reads
// a file, anything else is returned literally.
func readValueArg(cmd *cobra.Command, v string) (string, error) {
	switch {
	case v == "-":
		return readAllLimited(cmd.InOrStdin(), "stdin")
	case strings.HasPrefix(v, "@") && len(v) > 1:
		f, err := os.Open(filepath.Clean(v[1:]))
		if err != nil {
			return "", err
		}
		defer f.Close()
		return readAllLimited(f, v[1:])
	default:
		return v, nil
	}
}

func readAllLimited(r io.Reader, name string) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxInputBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", name, err)
	}
	if len(b) > maxInputBytes {
		return "", fmt.Errorf("reading %s: input larger than %d bytes", name, maxInputBytes)
	}
	return string(b), nil
}

// maskSecret shows only a short prefix and suffix of a key or secret.
func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 12 {
		return "****"
	}
	return s[:6] + "..." + s[len(s)-4:]
}

// writeFileSafely writes data to path with the given permissions. It refuses
// to overwrite an existing file unless force is set, never follows a
// symlink at the destination, and writes via a temp file + rename so a
// partially written file is never left behind.
func writeFileSafely(path string, data []byte, perm os.FileMode, force bool) error {
	path = filepath.Clean(path)
	if fi, err := os.Lstat(path); err == nil {
		if !force {
			return fmt.Errorf("%s already exists (use --force to overwrite)", path)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s is a symlink; refusing to overwrite", path)
		}
		if fi.IsDir() {
			return fmt.Errorf("%s is a directory", path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if !force {
		// Link fails if the destination appeared in the meantime.
		if err := os.Link(tmpName, path); err != nil {
			cleanup()
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("%s already exists (use --force to overwrite)", path)
			}
			// Filesystems without hard links: fall back to rename.
			return renameInto(tmpName, path)
		}
		cleanup()
		return nil
	}
	return renameInto(tmpName, path)
}

func renameInto(tmpName, path string) error {
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// jsonUnmarshalStrict decodes exactly one JSON value into v, rejecting
// unknown fields and trailing data.
func jsonUnmarshalStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected trailing data after JSON value")
	}
	return nil
}
