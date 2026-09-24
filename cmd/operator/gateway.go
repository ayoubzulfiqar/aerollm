package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

// Gateway admin endpoints the operator drives.
const (
	configUpdatePath = "/config/update" // partial config.Config JSON (merge)
	budgetsPath      = "/v1/budgets"    // per-key budgets (api.BudgetHandler)
	maxGatewayBody   = 64 << 10
)

// gatewayError is a failed gateway call. Transient errors (network, 5xx,
// 408, 429) are retried with backoff; others wait for a spec change or the
// periodic resync.
type gatewayError struct {
	Status    int
	Msg       string
	Transient bool
}

func (e *gatewayError) Error() string {
	if e.Status == 0 {
		return "gateway unreachable: " + e.Msg
	}
	return fmt.Sprintf("gateway returned %d: %s", e.Status, e.Msg)
}

// gatewayClient calls the AeroLLM gateway's admin API with the admin key.
type gatewayClient struct {
	base     string
	adminKey string
	hc       *http.Client
}

// validateGatewayURL accepts an https URL (or http when allowHTTP) without
// credentials, query or fragment.
func validateGatewayURL(raw string, allowHTTP bool) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("gateway URL %q is not an absolute URL", raw)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowHTTP {
			return nil, errors.New("gateway URL uses http: the admin key would be sent in clear text (set gateway_allow_http / --gateway-allow-http to allow it, e.g. for an in-cluster service)")
		}
	default:
		return nil, fmt.Errorf("gateway URL must be http(s), got %q", u.Scheme)
	}
	if u.User != nil {
		return nil, errors.New("gateway URL must not embed credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("gateway URL must not have a query or fragment")
	}
	return u, nil
}

func newGatewayClient(rawURL, adminKey string, allowHTTP bool, caFile string, timeout time.Duration) (*gatewayClient, error) {
	u, err := validateGatewayURL(rawURL, allowHTTP)
	if err != nil {
		return nil, err
	}
	if adminKey == "" {
		return nil, errors.New("gateway admin key is empty")
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pemData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("gateway CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, errors.New("gateway CA file contains no PEM certificates")
		}
		tlsCfg.RootCAs = pool
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSClientConfig:       tlsCfg,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   4,
		ForceAttemptHTTP2:     true,
	}
	return &gatewayClient{
		base:     strings.TrimRight(u.String(), "/"),
		adminKey: adminKey,
		hc: &http.Client{
			Timeout:   timeout,
			Transport: transport,
			// Never follow redirects: they would carry the admin key.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// send performs one admin call. Response bodies are bounded; the admin key
// never appears in errors.
func (g *gatewayClient) send(ctx context.Context, method, path string, query url.Values, body interface{}) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &gatewayError{Msg: "encode request: " + err.Error()}
		}
		rd = bytes.NewReader(b)
	}
	target := g.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return &gatewayError{Msg: err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+g.adminKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "aero-operator/"+version)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		msg := err.Error()
		var ue *url.Error
		if errors.As(err, &ue) {
			msg = ue.Err.Error()
		}
		return &gatewayError{Msg: sanitize(msg, 256), Transient: true}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxGatewayBody))
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}
	st := resp.StatusCode
	return &gatewayError{
		Status:    st,
		Msg:       gatewayMessage(st, data),
		Transient: st >= 500 || st == http.StatusTooManyRequests || st == http.StatusRequestTimeout,
	}
}

// gatewayMessage extracts {"error": "..."} or {"error": {"message": "..."}}.
func gatewayMessage(status int, body []byte) string {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	msg := ""
	if json.Unmarshal(body, &env) == nil && len(env.Error) > 0 {
		var s string
		if json.Unmarshal(env.Error, &s) == nil {
			msg = s
		} else {
			var obj struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(env.Error, &obj) == nil {
				msg = obj.Message
			}
		}
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		msg = "admin key rejected"
	case status == http.StatusNotFound && msg == "":
		msg = "endpoint not found (is it mounted on this gateway?)"
	case msg == "":
		msg = http.StatusText(status)
	}
	return sanitize(msg, 512)
}

// sanitize strips control characters and bounds length.
func sanitize(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	if len(s) > n {
		s = s[:n] + "..."
	}
	return s
}

// gatewayKeyID mirrors middleware.KeyID: the non-secret identifier the
// gateway uses for per-key budgets. The raw key never leaves the operator.
func gatewayKeyID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "key_" + hex.EncodeToString(sum[:8])
}

// plan is the gateway call that realises one resource.
type plan struct {
	// target names the gateway state the call owns ("router", "finops",
	// "budget:<key_id>"); two resources may not own the same target.
	target string
	method string
	path   string
	query  url.Values
	body   map[string]interface{}
	// summary describes what is applied (never secrets).
	summary string
	// ignored lists spec fields the gateway admin API cannot express.
	ignored []string
	// unsupported, when set, means nothing can be applied (and why).
	unsupported string
	// keyID is set for per-key budgets.
	keyID string
}

func (p plan) note() string {
	if len(p.ignored) == 0 {
		return ""
	}
	return "; not expressible via the gateway admin API (ignored): " + strings.Join(p.ignored, ", ")
}

func (g *gatewayClient) apply(ctx context.Context, p plan) error {
	return g.send(ctx, p.method, p.path, p.query, p.body)
}

func (g *gatewayClient) deleteBudget(ctx context.Context, keyID string) error {
	return g.send(ctx, http.MethodDelete, budgetsPath, url.Values{"key_id": {keyID}}, nil)
}

// decodeSpec converts a validated spec map into its typed form.
func decodeSpec(spec map[string]interface{}, dst interface{}) error {
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// planRoute translates an AeroRoute into a /config/update patch. The
// gateway has a single, global router config: strategy and the circuit
// breaker settings are expressible; per-route model/provider lists,
// fallback chains and weights are not.
func planRoute(spec v1alpha1.AeroRouteSpec) (plan, error) {
	p := plan{target: "router", method: http.MethodPost, path: configUpdatePath}
	router := map[string]interface{}{}
	var parts []string
	if spec.Strategy != "" {
		router["strategy"] = spec.Strategy
		parts = append(parts, "router.strategy="+spec.Strategy)
	}
	cb, ignored, err := translateBreaker(spec.BreakerConfig)
	if err != nil {
		return plan{}, err
	}
	if len(cb) > 0 {
		router["circuit_break"] = cb
		keys := make([]string, 0, len(cb))
		for k := range cb {
			keys = append(keys, "router.circuit_break."+k)
		}
		sort.Strings(keys)
		parts = append(parts, keys...)
	}
	if len(spec.Models) > 0 {
		p.ignored = append(p.ignored, "models")
	}
	if len(spec.Providers) > 0 {
		p.ignored = append(p.ignored, "providers")
	}
	if len(spec.Fallback) > 0 {
		p.ignored = append(p.ignored, "fallback")
	}
	if len(spec.Weights) > 0 {
		p.ignored = append(p.ignored, "weights")
	}
	p.ignored = append(p.ignored, ignored...)
	if len(router) == 0 {
		p.unsupported = "validated; nothing to apply: only strategy and breaker_config (max_failures, reset_timeout, half_open_max_calls) map to the gateway router config" + p.note()
		return p, nil
	}
	p.body = map[string]interface{}{"router": router}
	p.summary = strings.Join(parts, ", ")
	return p, nil
}

// translateBreaker maps breaker_config onto config.CircuitBreakConfig.
// reset_timeout accepts seconds (number) or a Go duration string ("30s")
// and is sent as nanoseconds, which is how the gateway decodes durations.
func translateBreaker(bc map[string]interface{}) (map[string]interface{}, []string, error) {
	out := map[string]interface{}{}
	var ignored []string
	keys := make([]string, 0, len(bc))
	for k := range bc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := bc[k]
		switch k {
		case "max_failures", "half_open_max_calls":
			f, ok := v.(float64)
			if !ok || f < 0 || f != math.Trunc(f) || f > math.MaxInt32 {
				return nil, nil, fmt.Errorf("breaker_config[%q]: must be a non-negative integer", k)
			}
			out[k] = int64(f)
		case "reset_timeout":
			var d time.Duration
			switch t := v.(type) {
			case float64:
				if t < 0 || t > float64(math.MaxInt64/int64(time.Second)) {
					return nil, nil, fmt.Errorf("breaker_config[%q]: out of range", k)
				}
				d = time.Duration(t * float64(time.Second))
			case string:
				pd, err := time.ParseDuration(t)
				if err != nil || pd < 0 {
					return nil, nil, fmt.Errorf("breaker_config[%q]: must be seconds or a duration like \"30s\"", k)
				}
				d = pd
			default:
				return nil, nil, fmt.Errorf("breaker_config[%q]: must be seconds or a duration like \"30s\"", k)
			}
			out[k] = int64(d)
		default:
			ignored = append(ignored, "breaker_config."+k)
		}
	}
	return out, ignored, nil
}

// planBudget translates an AeroBudget. apiKey is the resolved key (inline
// or from the referenced Secret); empty means a gateway-wide budget, set
// via /config/update finops.default_max_usd. Per-key budgets are set via
// PUT /v1/budgets keyed by the key's non-secret ID.
func planBudget(spec v1alpha1.AeroBudgetSpec, apiKey string) plan {
	var p plan
	limit, period := spec.MaxUSD, ""
	switch {
	case limit == 0 && spec.MonthlyCap > 0:
		limit, period = spec.MonthlyCap, "monthly"
	case spec.MonthlyCap > 0:
		p.ignored = append(p.ignored, "monthly_cap (max_usd takes precedence)")
	}
	if spec.AlertWebhook != "" {
		p.ignored = append(p.ignored, "alert_webhook")
	}
	if limit == 0 {
		p.unsupported = "validated; nothing to apply: neither max_usd nor monthly_cap is set" + p.note()
		return p
	}
	if apiKey == "" {
		p.target, p.method, p.path = "finops", http.MethodPost, configUpdatePath
		p.body = map[string]interface{}{"finops": map[string]interface{}{"enabled": true, "default_max_usd": limit}}
		p.summary = fmt.Sprintf("finops.enabled=true, finops.default_max_usd=%g", limit)
		if period != "" {
			p.summary += " (from monthly_cap; the gateway-wide default budget has no period)"
		}
		return p
	}
	p.keyID = gatewayKeyID(apiKey)
	p.target, p.method, p.path = "budget:"+p.keyID, http.MethodPut, budgetsPath
	p.body = map[string]interface{}{"key_id": p.keyID, "max_usd": limit}
	p.summary = fmt.Sprintf("budget for %s: max_usd=%g", p.keyID, limit)
	if period != "" {
		p.body["period"] = period
		p.summary += " period=" + period
	}
	return p
}

// planPipeline: the gateway has no admin API for agent pipelines.
func planPipeline() plan {
	return plan{unsupported: "validated; the gateway exposes no admin API for agent pipelines, so nothing was applied"}
}

// planManifest builds a plan for manifest (file/URL) mode, where Secrets
// cannot be resolved.
func planManifest(kind k8s.ResourceKind, spec map[string]interface{}) (plan, error) {
	switch kind {
	case k8s.KindAeroRoute:
		var s v1alpha1.AeroRouteSpec
		if err := decodeSpec(spec, &s); err != nil {
			return plan{}, err
		}
		return planRoute(s)
	case k8s.KindAeroBudget:
		var s v1alpha1.AeroBudgetSpec
		if err := decodeSpec(spec, &s); err != nil {
			return plan{}, err
		}
		if s.APIKeySecretRef != "" {
			return plan{}, errors.New("api_key_secret_ref can only be resolved in --kube mode")
		}
		return planBudget(s, strings.TrimSpace(s.APIKey)), nil
	case k8s.KindAeroAgentPipeline:
		return planPipeline(), nil
	}
	return plan{}, fmt.Errorf("unsupported kind %s", kind)
}
