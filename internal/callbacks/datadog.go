package callbacks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Datadog metric intake types (v2 series API).
const (
	ddTypeCount = 1
	ddTypeGauge = 3
)

var ddSiteRe = regexp.MustCompile(`^[a-z0-9]+([.-][a-z0-9]+)*$`)

// DatadogCallback sends metrics (v2 series API) and logs (v2 logs intake)
// to Datadog, authenticating with the DD-API-KEY header.
type DatadogCallback struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	site       string // e.g. "datadoghq.com", "datadoghq.eu", "us5.datadoghq.com"
	service    string
}

// NewDatadogCallback creates a new Datadog callback handler. site selects
// the regional intake (default "datadoghq.com"). baseURL, when set,
// overrides both intake hosts (useful for proxies and tests).
func NewDatadogCallback(apiKey, baseURL, site string) *DatadogCallback {
	site = strings.ToLower(strings.TrimSpace(site))
	if site == "" {
		site = "datadoghq.com"
	}
	return &DatadogCallback{
		apiKey:     apiKey,
		baseURL:    strings.TrimRight(baseURL, "/"),
		site:       site,
		service:    "aerollm",
		httpClient: newHTTPClient(10 * time.Second),
	}
}

// Name implements CallbackHandler.
func (d *DatadogCallback) Name() string { return "datadog" }

type ddPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type ddSeries struct {
	Metric string    `json:"metric"`
	Type   int       `json:"type"`
	Points []ddPoint `json:"points"`
	Tags   []string  `json:"tags,omitempty"`
}

type ddLog struct {
	DDSource  string                 `json:"ddsource"`
	DDTags    string                 `json:"ddtags"`
	Service   string                 `json:"service"`
	Status    string                 `json:"status"`
	Message   string                 `json:"message"`
	RequestID string                 `json:"request_id,omitempty"`
	Model     string                 `json:"model,omitempty"`
	Provider  string                 `json:"provider,omitempty"`
	LatencyMs int64                  `json:"latency_ms,omitempty"`
	CostUSD   float64                `json:"cost_usd,omitempty"`
	Usage     map[string]int         `json:"usage,omitempty"`
	Error     string                 `json:"error,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func (d *DatadogCallback) endpoints() (metrics, logs string, err error) {
	if d.baseURL != "" {
		return d.baseURL + "/api/v2/series", d.baseURL + "/api/v2/logs", nil
	}
	if !ddSiteRe.MatchString(d.site) {
		return "", "", fmt.Errorf("datadog: invalid site %q", d.site)
	}
	return "https://api." + d.site + "/api/v2/series", "https://http-intake.logs." + d.site + "/api/v2/logs", nil
}

// ddTag sanitises a tag value: lower-case, no commas/whitespace, bounded.
func ddTag(k, v string) string {
	if v == "" {
		v = "unknown"
	}
	v = strings.ToLower(v)
	v = strings.Map(func(r rune) rune {
		switch {
		case r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r':
			return '_'
		}
		return r
	}, v)
	if len(v) > 150 {
		v = v[:150]
	}
	return k + ":" + v
}

func (d *DatadogCallback) post(ctx context.Context, endpoint string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("datadog: marshal: %w", err)
	}
	if err := validateEndpoint(endpoint); err != nil {
		return err
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("DD-API-KEY", d.apiKey)
	h.Set("User-Agent", "AeroLLM-Callbacks/1.0")
	if _, _, err := doRequest(ctx, d.httpClient, http.MethodPost, endpoint, body, h); err != nil {
		return err
	}
	return nil
}

// OnSuccess sends latency/token/cost metrics and a structured log.
func (d *DatadogCallback) OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	if d.apiKey == "" {
		return fmt.Errorf("datadog: missing api key")
	}
	if req == nil {
		req = &CallbackRequestData{}
	}
	if resp == nil {
		resp = &CallbackResponseData{}
	}
	metricsURL, logsURL, err := d.endpoints()
	if err != nil {
		return err
	}
	// request_id is deliberately not a metric tag (unbounded cardinality).
	tags := []string{ddTag("model", req.Model), ddTag("provider", req.Provider), ddTag("service", d.service)}
	now := time.Now().Unix()

	series := []ddSeries{
		{Metric: "aerollm.llm.latency_ms", Type: ddTypeGauge, Points: []ddPoint{{now, float64(resp.LatencyMs)}}, Tags: tags},
		{Metric: "aerollm.llm.requests", Type: ddTypeCount, Points: []ddPoint{{now, 1}}, Tags: tags},
	}
	tokenTypes := make([]string, 0, len(resp.TokenCount))
	for k := range resp.TokenCount {
		tokenTypes = append(tokenTypes, k)
	}
	sort.Strings(tokenTypes)
	for _, k := range tokenTypes {
		series = append(series, ddSeries{Metric: "aerollm.llm.tokens", Type: ddTypeCount, Points: []ddPoint{{now, float64(resp.TokenCount[k])}}, Tags: append(append([]string{}, tags...), ddTag("token_type", k))})
	}
	if req.CostUSD > 0 {
		series = append(series, ddSeries{Metric: "aerollm.llm.cost_usd", Type: ddTypeCount, Points: []ddPoint{{now, req.CostUSD}}, Tags: tags})
	}
	if err := d.post(ctx, metricsURL, map[string]interface{}{"series": series}); err != nil {
		return fmt.Errorf("datadog metric send failed: %w", err)
	}

	logEntry := ddLog{
		DDSource: "aerollm", DDTags: strings.Join(tags, ","), Service: d.service, Status: "info",
		Message:   fmt.Sprintf("LLM call succeeded: model=%s provider=%s latency_ms=%d", req.Model, req.Provider, resp.LatencyMs),
		RequestID: req.RequestID, Model: req.Model, Provider: req.Provider, LatencyMs: resp.LatencyMs,
		CostUSD: req.CostUSD, Usage: resp.TokenCount, Metadata: req.Metadata,
	}
	if err := d.post(ctx, logsURL, []ddLog{logEntry}); err != nil {
		return fmt.Errorf("datadog log send failed: %w", err)
	}
	return nil
}

// OnError sends an error-count metric and an error log.
func (d *DatadogCallback) OnError(ctx context.Context, req *CallbackRequestData, callErr error) error {
	if d.apiKey == "" {
		return fmt.Errorf("datadog: missing api key")
	}
	if req == nil {
		req = &CallbackRequestData{}
	}
	msg := "unknown error"
	if callErr != nil {
		msg = callErr.Error()
	}
	metricsURL, logsURL, err := d.endpoints()
	if err != nil {
		return err
	}
	tags := []string{ddTag("model", req.Model), ddTag("provider", req.Provider), ddTag("service", d.service), "error_type:llm_error"}
	now := time.Now().Unix()
	series := []ddSeries{{Metric: "aerollm.llm.errors", Type: ddTypeCount, Points: []ddPoint{{now, 1}}, Tags: tags}}
	if err := d.post(ctx, metricsURL, map[string]interface{}{"series": series}); err != nil {
		return fmt.Errorf("datadog metric send failed: %w", err)
	}
	logEntry := ddLog{
		DDSource: "aerollm", DDTags: strings.Join(tags, ","), Service: d.service, Status: "error",
		Message:   fmt.Sprintf("LLM call failed: model=%s provider=%s", req.Model, req.Provider),
		RequestID: req.RequestID, Model: req.Model, Provider: req.Provider, Error: msg, Metadata: req.Metadata,
	}
	if err := d.post(ctx, logsURL, []ddLog{logEntry}); err != nil {
		return fmt.Errorf("datadog log send failed: %w", err)
	}
	return nil
}
