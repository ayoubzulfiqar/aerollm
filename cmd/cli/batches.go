package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	// maxBatchInputBytes mirrors the gateway's upload cap for batch input.
	maxBatchInputBytes = 100 << 20
	// jsonBatchUploadLimit is the largest JSON create body the gateway
	// accepts (10 MiB) minus headroom; larger inputs go multipart.
	jsonBatchUploadLimit = 10<<20 - 4<<10
	maxBatchMetadataKeys = 16
	maxBatchMetaKeyLen   = 64
	maxBatchMetaValueLen = 512
	maxBatchWindow       = 7 * 24 * time.Hour
)

// batchTerminal lists the statuses after which a batch never changes.
var batchTerminal = map[string]bool{"completed": true, "failed": true, "expired": true, "cancelled": true}

// batchStatus is the subset of a batch object the CLI inspects.
type batchStatus struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	RequestCounts struct {
		Total     int `json:"total"`
		Completed int `json:"completed"`
		Failed    int `json:"failed"`
	} `json:"request_counts"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Line    int    `json:"line"`
	} `json:"errors"`
	Error string `json:"error"`
}

func newBatchesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "batches",
		Aliases: []string{"batch"},
		Short:   "Create, inspect, wait for and download batch jobs (/v1/batches)",
		Long: `Manage OpenAI-compatible batch jobs. Input is JSONL, one request per line:

  {"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}}

Any client key may create batches; each key sees only its own batches
(admin keys see all).`,
	}
	cmd.AddCommand(newBatchesCreateCmd(), newBatchesListCmd(), newBatchesGetCmd(), newBatchesCancelCmd(),
		newBatchesFileCmd("results"), newBatchesFileCmd("errors"), newBatchesWaitCmd())
	return cmd
}

// batchPath returns /v1/batches/{id}[/suffix] with a validated id.
func batchPath(id, suffix string) (string, error) {
	seg, err := pathSegment("batch", id)
	if err != nil {
		return "", err
	}
	p := "/v1/batches/" + seg
	if suffix != "" {
		p += "/" + suffix
	}
	return p, nil
}

// formatBatch rewrites a batch object for table display.
func formatBatch(m map[string]any) {
	for k := range m {
		if strings.HasSuffix(k, "_at") {
			unixToRFC3339(m, k)
		}
	}
	if rc, ok := m["request_counts"].(map[string]any); ok {
		m["total"], m["completed"], m["failed"] = rc["total"], rc["completed"], rc["failed"]
		m["request_counts"] = fmt.Sprintf("total=%s completed=%s failed=%s",
			formatCell(rc["total"]), formatCell(rc["completed"]), formatCell(rc["failed"]))
	}
}

// renderBatch prints a single batch object (JSON by default).
func renderBatch(cmd *cobra.Command, data []byte) error {
	return renderResult(cmd, data, formatJSON, nil, func(v any) any {
		if m, ok := v.(map[string]any); ok {
			if f, _ := outputFormat(cmd, formatJSON); f == formatTable {
				formatBatch(m)
				delete(m, "total")
				delete(m, "completed")
				delete(m, "failed")
			}
		}
		return v
	})
}

func newBatchesCreateCmd() *cobra.Command {
	var (
		file, window, upload string
		metadata             []string
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a batch from a JSONL file or stdin (POST /v1/batches)",
		Long: `Create a batch job. Input comes from --file PATH, or from stdin when
--file is "-" or omitted. Every line is checked locally (JSON object with a
unique custom_id) before upload.

--upload auto (default) sends a JSON body ({"input": ...}) and switches to a
multipart upload for inputs too large for a JSON request; --metadata needs
the JSON form.`,
		Example: `  aerollm batches create --file requests.jsonl --metadata team=search
  cat requests.jsonl | aerollm batches create --completion-window 12h
  aerollm batches create --file big.jsonl --upload multipart`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			mode, err := requireOneOf("upload", upload, "auto", "json", "multipart")
			if err != nil {
				return err
			}
			if window != "" {
				d, err := parsePositiveDuration("completion-window", window)
				if err != nil {
					return err
				}
				if d > maxBatchWindow {
					return fmt.Errorf("--completion-window must be at most %s", maxBatchWindow)
				}
			}
			md, err := parseBatchMetadata(metadata)
			if err != nil {
				return err
			}
			input, name, err := readBatchInput(cmd, file)
			if err != nil {
				return err
			}
			n, err := validateBatchJSONL(input)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			body := map[string]any{"input": string(input)}
			if len(md) > 0 {
				body["metadata"] = md
			}
			if window != "" {
				body["completion_window"] = window
			}
			jsonBody, err := json.Marshal(body)
			if err != nil {
				return err
			}
			if mode == "auto" {
				mode = "json"
				if len(jsonBody) > jsonBatchUploadLimit {
					mode = "multipart"
				}
			}
			if mode == "multipart" && len(md) > 0 {
				return errors.New("--metadata requires a JSON upload (--upload json); multipart uploads cannot carry metadata")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			var req *http.Request
			if mode == "json" {
				req, err = client.newRequest(cmd.Context(), http.MethodPost, "/v1/batches", nil, jsonBody)
			} else {
				var buf bytes.Buffer
				mw := multipart.NewWriter(&buf)
				fw, ferr := mw.CreateFormFile("file", name)
				if ferr != nil {
					return ferr
				}
				if _, err := fw.Write(input); err != nil {
					return err
				}
				if window != "" {
					if err := mw.WriteField("completion_window", window); err != nil {
						return err
					}
				}
				if err := mw.Close(); err != nil {
					return err
				}
				req, err = client.newRequest(cmd.Context(), http.MethodPost, "/v1/batches", nil, buf.Bytes())
				if err == nil {
					req.Header.Set("Content-Type", mw.FormDataContentType())
				}
			}
			if err != nil {
				return err
			}
			data, err := client.send(req)
			if err != nil {
				return err
			}
			var st batchStatus
			_ = json.Unmarshal(data, &st)
			if err := renderBatch(cmd, data); err != nil {
				return err
			}
			if st.Status == "failed" {
				return fmt.Errorf("batch %s failed validation (%d error(s))", st.ID, len(st.Errors))
			}
			if st.ID != "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "created batch %s with %d request(s); follow it with: aerollm batches wait %s\n", st.ID, n, st.ID)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVarP(&file, "file", "f", "", `JSONL input file ("-" or omitted: stdin)`)
	f.StringVar(&window, "completion-window", "", "completion window, e.g. 24h (max 168h; default: server setting)")
	f.StringArrayVar(&metadata, "metadata", nil, "metadata key=value (repeatable, max 16)")
	f.StringVar(&upload, "upload", "auto", "upload encoding: auto|json|multipart")
	return cmd
}

func parseBatchMetadata(kvs []string) (map[string]string, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	md := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid --metadata %q: want key=value", kv)
		}
		if len(k) > maxBatchMetaKeyLen || len(v) > maxBatchMetaValueLen {
			return nil, fmt.Errorf("invalid --metadata %q: keys may be at most %d bytes and values %d bytes", kv, maxBatchMetaKeyLen, maxBatchMetaValueLen)
		}
		md[k] = v
	}
	if len(md) > maxBatchMetadataKeys {
		return nil, fmt.Errorf("at most %d --metadata keys are allowed", maxBatchMetadataKeys)
	}
	return md, nil
}

// readBatchInput reads JSONL from a file or stdin and returns it with the
// file name reported to the server.
func readBatchInput(cmd *cobra.Command, file string) ([]byte, string, error) {
	var (
		r    io.Reader
		name = "stdin.jsonl"
	)
	if file == "" || file == "-" {
		if file == "" && stdinIsTerminal(cmd) {
			return nil, "", errors.New("no input: pass --file PATH or pipe JSONL on stdin")
		}
		r = cmd.InOrStdin()
	} else {
		path := filepath.Clean(file)
		f, err := os.Open(path)
		if err != nil {
			return nil, "", err
		}
		defer f.Close()
		if fi, err := f.Stat(); err != nil {
			return nil, "", err
		} else if !fi.Mode().IsRegular() {
			return nil, "", fmt.Errorf("%s is not a regular file", path)
		}
		r, name = f, filepath.Base(path)
	}
	data, err := io.ReadAll(io.LimitReader(r, maxBatchInputBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("reading %s: %w", name, err)
	}
	if len(data) > maxBatchInputBytes {
		return nil, "", fmt.Errorf("%s is larger than %d bytes", name, maxBatchInputBytes)
	}
	return data, name, nil
}

// stdinIsTerminal reports whether stdin is an interactive terminal (so
// reading it would block waiting for the user).
func stdinIsTerminal(cmd *cobra.Command) bool {
	f, ok := cmd.InOrStdin().(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// validateBatchJSONL checks that every non-blank line is a JSON object with
// a unique, non-empty custom_id, and returns the number of requests. The
// server validates the rest (method, url, body).
func validateBatchJSONL(data []byte) (int, error) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64<<10), len(data)+1)
	seen := map[string]int{}
	lineNo, n := 0, 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var obj struct {
			CustomID *string `json:"custom_id"`
		}
		if line[0] != '{' || json.Unmarshal(line, &obj) != nil {
			return 0, fmt.Errorf("line %d: not a valid JSON object", lineNo)
		}
		if obj.CustomID == nil || strings.TrimSpace(*obj.CustomID) == "" {
			return 0, fmt.Errorf("line %d: custom_id is required", lineNo)
		}
		if prev, dup := seen[*obj.CustomID]; dup {
			return 0, fmt.Errorf("line %d: custom_id %q duplicates line %d", lineNo, *obj.CustomID, prev)
		}
		seen[*obj.CustomID] = lineNo
		n++
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, errors.New("input contains no requests")
	}
	return n, nil
}

func newBatchesListCmd() *cobra.Command {
	var (
		after string
		limit int
		all   bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List batches, newest first (GET /v1/batches)",
		Example: "  aerollm batches list --limit 50\n  aerollm batches list --all -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("limit") && (limit < 1 || limit > 100) {
				return errors.New("--limit must be between 1 and 100")
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			var (
				data  []byte
				items []json.RawMessage
			)
			cursor := after
			for page := 0; ; page++ {
				q := url.Values{}
				if cursor != "" {
					q.Set("after", cursor)
				}
				if limit > 0 {
					q.Set("limit", strconv.Itoa(limit))
				}
				data, err = client.call(cmd.Context(), http.MethodGet, "/v1/batches", q, nil)
				if err != nil {
					return err
				}
				if !all {
					break
				}
				var pg struct {
					Data    []json.RawMessage `json:"data"`
					LastID  string            `json:"last_id"`
					HasMore bool              `json:"has_more"`
				}
				if err := json.Unmarshal(data, &pg); err != nil {
					return fmt.Errorf("decoding /v1/batches response: %w", err)
				}
				items = append(items, pg.Data...)
				if !pg.HasMore || pg.LastID == "" || pg.LastID == cursor || page >= 999 {
					data, err = json.Marshal(map[string]any{"object": "list", "data": items, "has_more": pg.HasMore})
					if err != nil {
						return err
					}
					break
				}
				cursor = pg.LastID
			}
			return renderList(cmd, data, listView{
				fields:  []string{"data"},
				columns: []string{"id", "status", "created_at", "completion_window", "total", "completed", "failed"},
				format:  formatBatch,
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&after, "after", "", "cursor: list batches after this batch id")
	f.IntVar(&limit, "limit", 0, "page size, 1-100 (default: server setting)")
	f.BoolVar(&all, "all", false, "follow pagination and list every batch")
	return cmd
}

func newBatchesGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "get BATCH_ID",
		Short:   "Show a batch (GET /v1/batches/{id})",
		Example: "  aerollm batches get batch_0123abcd\n  aerollm batches get batch_0123abcd -o table",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := batchPath(args[0], "")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, p, nil, nil)
			if err != nil {
				return err
			}
			return renderBatch(cmd, data)
		},
	}
}

func newBatchesCancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "cancel BATCH_ID",
		Short:   "Cancel a batch (POST /v1/batches/{id}/cancel)",
		Example: "  aerollm batches cancel batch_0123abcd",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := batchPath(args[0], "cancel")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPost, p, nil, nil)
			if err != nil {
				return err
			}
			return renderBatch(cmd, data)
		},
	}
}

// newBatchesFileCmd builds "batches results" and "batches errors".
func newBatchesFileCmd(kind string) *cobra.Command {
	var (
		out   string
		force bool
	)
	short := "Download a batch's output JSONL (GET /v1/batches/{id}/results)"
	if kind == "errors" {
		short = "Download a batch's error JSONL (GET /v1/batches/{id}/errors)"
	}
	cmd := &cobra.Command{
		Use:     kind + " BATCH_ID",
		Short:   short,
		Example: "  aerollm batches " + kind + " batch_0123abcd > " + kind + ".jsonl\n  aerollm batches " + kind + " batch_0123abcd --out " + kind + ".jsonl",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := batchPath(args[0], kind)
			if err != nil {
				return err
			}
			if out != "" && !force {
				if _, err := os.Lstat(filepath.Clean(out)); err == nil {
					return fmt.Errorf("%s already exists (use --force to overwrite)", filepath.Clean(out))
				}
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			req, err := client.newRequest(cmd.Context(), http.MethodGet, p, nil, nil)
			if err != nil {
				return err
			}
			req.Header.Set("Accept", "application/jsonl, application/x-ndjson, application/json")
			resp, err := client.do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if out == "" {
				if _, err := io.Copy(cmd.OutOrStdout(), resp.Body); err != nil {
					return fmt.Errorf("GET %s: reading response: %w", p, err)
				}
				return nil
			}
			cr := &countingReader{r: resp.Body}
			if err := writeStreamSafely(out, cr, 0o600, force); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "wrote %d bytes to %s\n", cr.n, filepath.Clean(out))
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "write to this file (mode 0600) instead of stdout")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite --out if it exists")
	return cmd
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func newBatchesWaitCmd() *cobra.Command {
	var interval, maxWait time.Duration
	cmd := &cobra.Command{
		Use:   "wait BATCH_ID",
		Short: "Poll a batch until it reaches a terminal status",
		Long: `Poll GET /v1/batches/{id} until the batch is completed, failed, expired or
cancelled, printing progress on stderr and the final batch on stdout.
Exits non-zero unless the batch completed, or if --max-wait elapses first.
Transient errors (network failures, 429 and 5xx) are retried.`,
		Example: "  aerollm batches wait batch_0123abcd --interval 10s --max-wait 2h && aerollm batches results batch_0123abcd --out out.jsonl",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if interval <= 0 {
				return errors.New("--interval must be positive")
			}
			if maxWait < 0 {
				return errors.New("--max-wait must not be negative")
			}
			id := strings.TrimSpace(args[0])
			p, err := batchPath(id, "")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			if maxWait > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, maxWait)
				defer cancel()
			}
			stderr := cmd.ErrOrStderr()
			var (
				last     batchStatus
				lastLine string
				polled   bool // a first poll succeeded: the server is reachable
			)
			timedOut := func() error {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return fmt.Errorf("timed out after %s waiting for batch %s (last status %q)", maxWait, id, last.Status)
				}
				return ctx.Err()
			}
			for {
				data, err := client.call(ctx, http.MethodGet, p, nil, nil)
				switch {
				case ctx.Err() != nil:
					return timedOut()
				case err != nil && isTransient(err, polled):
					fmt.Fprintf(stderr, "warning: polling batch %s: %v (retrying)\n", id, err)
				case err != nil:
					return err
				default:
					var st batchStatus
					if err := json.Unmarshal(data, &st); err != nil {
						return fmt.Errorf("decoding batch %s: %w", id, err)
					}
					last, polled = st, true
					line := fmt.Sprintf("batch %s: %s (completed %d/%d, failed %d)", id, st.Status,
						st.RequestCounts.Completed, st.RequestCounts.Total, st.RequestCounts.Failed)
					if line != lastLine {
						fmt.Fprintln(stderr, sanitizeCell(line))
						lastLine = line
					}
					if batchTerminal[st.Status] {
						if err := renderBatch(cmd, data); err != nil {
							return err
						}
						if st.Status != "completed" {
							return fmt.Errorf("batch %s finished with status %q", id, st.Status)
						}
						return nil
					}
				}
				t := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					t.Stop()
					return timedOut()
				case <-t.C:
				}
			}
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "polling interval")
	cmd.Flags().DurationVar(&maxWait, "max-wait", 24*time.Hour, "give up after this long (0 = wait forever)")
	return cmd
}

// isTransient reports whether a failed poll is worth retrying: 429 and 5xx
// responses always, network failures only once the server has answered
// before (so a mistyped --server fails fast).
func isTransient(err error, reachable bool) bool {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.Status == http.StatusTooManyRequests || apiErr.Status >= 500
	}
	return reachable
}
