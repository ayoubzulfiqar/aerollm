package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

func newMetricsCmd() *cobra.Command {
	var grep string
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Fetch Prometheus metrics from the gateway (GET /metrics)",
		Long: `Print the gateway's Prometheus metrics.

--grep filters sample lines by a regular expression (matched against the
metric name and labels). With -o json the samples are parsed into
[{"name","labels","value"}] objects.`,
		Example: `  aerollm metrics
  aerollm metrics --grep 'aerollm_requests_total|latency'
  aerollm metrics --grep provider=\"openai\" -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var re *regexp.Regexp
			if grep != "" {
				var err error
				if re, err = regexp.Compile(grep); err != nil {
					return fmt.Errorf("invalid --grep pattern: %w", err)
				}
			}
			format, err := outputFormat(cmd, "")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			req, err := client.newRequest(cmd.Context(), http.MethodGet, "/metrics", nil, nil)
			if err != nil {
				return err
			}
			req.Header.Set("Accept", "text/plain;version=0.0.4;q=1,*/*;q=0.1")
			resp, err := client.do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
			if err != nil {
				return fmt.Errorf("reading metrics: %w", err)
			}
			w := cmd.OutOrStdout()
			if format == formatJSON {
				samples, err := parsePromText(bytes.NewReader(body), re)
				if err != nil {
					return err
				}
				if samples == nil {
					samples = []promSample{}
				}
				return writeJSON(w, samples)
			}
			if re == nil {
				_, err := w.Write(body)
				return err
			}
			return filterPromLines(bytes.NewReader(body), re, w)
		},
	}
	cmd.Flags().StringVar(&grep, "grep", "", "only show samples whose name/labels match this regexp")
	return cmd
}

// filterPromLines writes the sample lines matching re (comments are dropped).
func filterPromLines(r io.Reader, re *regexp.Regexp, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if re.MatchString(seriesPart(line)) {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// promSample is one parsed Prometheus text-format sample.
type promSample struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// seriesPart returns "name{labels}" of a sample line (without value/ts).
func seriesPart(line string) string {
	if i := strings.LastIndex(line, "}"); i >= 0 {
		return line[:i+1]
	}
	if i := strings.IndexAny(line, " \t"); i >= 0 {
		return line[:i]
	}
	return line
}

// parsePromText parses the Prometheus text exposition format. Samples whose
// series does not match filter (when non-nil) are skipped; NaN/Inf values
// are dropped because they cannot be represented in JSON.
func parsePromText(r io.Reader, filter *regexp.Regexp) ([]promSample, error) {
	var out []promSample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series := seriesPart(line)
		if filter != nil && !filter.MatchString(series) {
			continue
		}
		s, err := parsePromSample(line)
		if err != nil {
			return nil, fmt.Errorf("metrics line %d: %w", lineNo, err)
		}
		if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
			continue
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

func parsePromSample(line string) (promSample, error) {
	var s promSample
	rest := line
	if i := strings.Index(line, "{"); i >= 0 {
		j := strings.LastIndex(line, "}")
		if j < i {
			return s, fmt.Errorf("unterminated label set")
		}
		s.Name = strings.TrimSpace(line[:i])
		labels, err := parsePromLabels(line[i+1 : j])
		if err != nil {
			return s, err
		}
		s.Labels = labels
		rest = line[j+1:]
	} else {
		fields := strings.Fields(line)
		s.Name = fields[0]
		rest = strings.TrimPrefix(line, fields[0])
	}
	fields := strings.Fields(rest)
	if s.Name == "" || len(fields) == 0 {
		return s, fmt.Errorf("malformed sample %q", line)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return s, fmt.Errorf("invalid value %q", fields[0])
	}
	s.Value = v
	return s, nil
}

// parsePromLabels parses `a="x",b="y\"z"`.
func parsePromLabels(s string) (map[string]string, error) {
	labels := map[string]string{}
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return labels, nil
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return nil, fmt.Errorf("malformed labels %q", s)
		}
		name := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			return nil, fmt.Errorf("label %q: value must be quoted", name)
		}
		i++
		var b strings.Builder
		closed := false
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				closed = true
				i++
				break
			}
			b.WriteByte(c)
			i++
		}
		if !closed {
			return nil, fmt.Errorf("label %q: unterminated value", name)
		}
		labels[name] = b.String()
	}
}
