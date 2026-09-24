package schedule

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/notification"
)

// WebhookPayloadType is the "type" of a task payload handled by the webhook
// executor.
const WebhookPayloadType = "webhook"

// Webhook delivery headers set by the executor.
const (
	HeaderTaskID      = "X-AeroLLM-Task-ID"
	HeaderScheduledAt = "X-AeroLLM-Scheduled-At"
	HeaderTimestamp   = "X-AeroLLM-Timestamp"
	HeaderSignature   = "X-AeroLLM-Signature"
)

// Webhook executor defaults and limits.
const (
	DefaultWebhookTimeout      = 10 * time.Second
	DefaultWebhookUserAgent    = "AeroLLM-Scheduler/1.0"
	defaultWebhookRespLimit    = 64 << 10
	maxWebhookURLLen           = 2048
	maxWebhookHeaders          = 32
	maxWebhookHeaderNameLen    = 128
	maxWebhookHeaderValueLen   = 4096
	maxWebhookTimeout          = 5 * time.Minute
	webhookDialTimeout         = 5 * time.Second
	webhookTLSHandshakeTimeout = 5 * time.Second
)

var (
	// ErrNotWebhookTask is returned by the webhook executor for tasks whose
	// payload is not a webhook payload.
	ErrNotWebhookTask = errors.New(`schedule: task is not a webhook task (payload must be a JSON object with "type":"webhook" and "url")`)
	// ErrBlockedDestination is returned when a webhook target is (or
	// resolves to) a loopback, private, link-local, metadata or otherwise
	// non-public address and private networks are not allowed.
	ErrBlockedDestination = errors.New("schedule: webhook destination address is not allowed")
)

// WebhookPayload is the task payload understood by the webhook executor:
//
//	{"type":"webhook","url":"https://hooks.example.com/run",
//	 "headers":{"Authorization":"Bearer ..."},"body":{"any":"json"}}
//
// Body is POSTed verbatim; when it is omitted the executor sends a small
// JSON event describing the run (task_id, task_name, scheduled_at).
type WebhookPayload struct {
	Type    string            `json:"type"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// ParseWebhookPayload decodes and validates a webhook task payload. It
// returns ErrNotWebhookTask when payload is not a webhook payload at all and
// a ValidationError when it is one but is malformed. Destination addresses
// are checked when the webhook is delivered, not here.
func ParseWebhookPayload(payload string) (WebhookPayload, error) {
	var p WebhookPayload
	trimmed := strings.TrimSpace(payload)
	if !strings.HasPrefix(trimmed, "{") {
		return p, ErrNotWebhookTask
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(trimmed), &probe); err != nil || probe.Type != WebhookPayloadType {
		return p, ErrNotWebhookTask
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, invalid("invalid webhook payload: %v", err)
	}
	if _, err := parseWebhookURL(p.URL); err != nil {
		return p, invalid("invalid webhook payload: %v", err)
	}
	if len(p.Headers) > maxWebhookHeaders {
		return p, invalid("invalid webhook payload: at most %d headers allowed", maxWebhookHeaders)
	}
	for name, value := range p.Headers {
		if err := checkWebhookHeader(name, value); err != nil {
			return p, invalid("invalid webhook payload: %v", err)
		}
	}
	if len(p.Body) > 0 && !json.Valid(p.Body) {
		return p, invalid("invalid webhook payload: body is not valid JSON")
	}
	return p, nil
}

// validatePayload rejects malformed webhook payloads when a task is stored,
// so mistakes surface as 400s instead of failed runs. Other payloads are
// opaque to the store.
func validatePayload(payload string) error {
	if _, err := ParseWebhookPayload(payload); err != nil && !errors.Is(err, ErrNotWebhookTask) {
		return err
	}
	return nil
}

// parseWebhookURL checks the syntax of a webhook URL: absolute http(s), a
// host, no userinfo, a valid port.
func parseWebhookURL(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, errors.New("url is required")
	}
	if len(raw) > maxWebhookURLLen {
		return nil, fmt.Errorf("url exceeds %d characters", maxWebhookURLLen)
	}
	if strings.ContainsAny(raw, " \t\r\n") {
		return nil, errors.New("url must not contain whitespace")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" {
		return nil, errors.New("url is not a valid URL")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, errors.New("url must be an absolute http(s) URL")
	}
	if u.User != nil {
		return nil, errors.New("url must not embed credentials (userinfo)")
	}
	if u.Hostname() == "" {
		return nil, errors.New("url host is required")
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return nil, errors.New("url port is invalid")
		}
	}
	return u, nil
}

// reservedWebhookHeaders may not be set through a task payload.
var reservedWebhookHeaders = map[string]bool{
	"Host": true, "Content-Length": true, "Content-Type": true, "Transfer-Encoding": true,
	"Connection": true, "Keep-Alive": true, "Upgrade": true, "Te": true, "Trailer": true,
	"Proxy-Authorization": true, "Proxy-Connection": true, "User-Agent": true, "Expect": true,
}

func checkWebhookHeader(name, value string) error {
	if name == "" || len(name) > maxWebhookHeaderNameLen {
		return fmt.Errorf("header name must be 1-%d characters", maxWebhookHeaderNameLen)
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return fmt.Errorf("header name %q contains invalid characters", name)
		}
	}
	canon := http.CanonicalHeaderKey(name)
	if reservedWebhookHeaders[canon] || strings.HasPrefix(canon, "X-Aerollm-") || strings.HasPrefix(canon, "Proxy-") {
		return fmt.Errorf("header %q is reserved", canon)
	}
	if len(value) > maxWebhookHeaderValueLen {
		return fmt.Errorf("header %q value exceeds %d bytes", canon, maxWebhookHeaderValueLen)
	}
	for i := 0; i < len(value); i++ {
		if c := value[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return fmt.Errorf("header %q value contains control characters", canon)
		}
	}
	return nil
}

func isTokenChar(c byte) bool {
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

// WebhookExecutorOptions configures NewWebhookExecutor. Zero values select
// safe defaults: https only, public destinations only, 10s timeout.
type WebhookExecutorOptions struct {
	// Timeout bounds one delivery (connect, TLS, request and response
	// headers). Default 10s, maximum 5m. The Runner's TaskTimeout and
	// cancellation also apply.
	Timeout time.Duration
	// SigningSecret, when set, signs every delivery: HeaderTimestamp carries
	// the Unix time and HeaderSignature "v1=" + hex(HMAC-SHA256(secret,
	// timestamp + "." + body)).
	SigningSecret []byte
	// UserAgent overrides DefaultWebhookUserAgent.
	UserAgent string
	// AllowedHosts, when non-empty, restricts deliveries to these hosts
	// (case-insensitive exact match on the URL host name).
	AllowedHosts []string
	// AllowInsecureHTTP permits http:// targets. For development and tests.
	AllowInsecureHTTP bool
	// AllowPrivateNetworks permits loopback, private, link-local and other
	// non-public destinations. For development and tests.
	AllowPrivateNetworks bool
	// MaxResponseBytes bounds how much of a response body is drained
	// (default 64 KiB); the body is otherwise ignored.
	MaxResponseBytes int64
}

// WebhookExecutorOptionsFromEnv reads webhook executor settings from the
// environment through getenv (os.Getenv in production):
//
//	AEROLLM_SCHEDULE_WEBHOOK_SECRET         HMAC signing secret
//	AEROLLM_SCHEDULE_WEBHOOK_TIMEOUT        Go duration, e.g. "15s"
//	AEROLLM_SCHEDULE_WEBHOOK_ALLOWED_HOSTS  comma-separated host allowlist
func WebhookExecutorOptionsFromEnv(getenv func(string) string) (WebhookExecutorOptions, error) {
	var o WebhookExecutorOptions
	if getenv == nil {
		return o, errors.New("schedule: nil getenv")
	}
	if v := getenv("AEROLLM_SCHEDULE_WEBHOOK_SECRET"); v != "" {
		o.SigningSecret = []byte(v)
	}
	if v := strings.TrimSpace(getenv("AEROLLM_SCHEDULE_WEBHOOK_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > maxWebhookTimeout {
			return o, fmt.Errorf("schedule: AEROLLM_SCHEDULE_WEBHOOK_TIMEOUT must be a duration in (0, %s]", maxWebhookTimeout)
		}
		o.Timeout = d
	}
	if v := getenv("AEROLLM_SCHEDULE_WEBHOOK_ALLOWED_HOSTS"); v != "" {
		for _, h := range strings.Split(v, ",") {
			if h = strings.TrimSpace(h); h != "" {
				o.AllowedHosts = append(o.AllowedHosts, h)
			}
		}
	}
	return o, nil
}

type webhookExecutor struct {
	client       *http.Client
	opts         WebhookExecutorOptions
	allowedHosts map[string]bool
	now          func() time.Time
}

// NewWebhookExecutor returns an Executor that delivers webhook tasks (see
// WebhookPayload) with an HTTP POST. The client refuses non-public
// destinations at connect time (after DNS resolution, so DNS rebinding does
// not help), ignores proxy environment variables, does not follow redirects
// (a 3xx is a failed run) and bounds every phase with timeouts. A run
// succeeds on a 2xx response. Tasks without a webhook payload fail with
// ErrNotWebhookTask.
//
// Deliveries carry HeaderTaskID and HeaderScheduledAt so receivers can
// deduplicate: runs are at-least-once when persistence is enabled.
func NewWebhookExecutor(opts WebhookExecutorOptions) (Executor, error) {
	if opts.Timeout < 0 || opts.Timeout > maxWebhookTimeout {
		return nil, fmt.Errorf("schedule: webhook timeout must be between 0 and %s", maxWebhookTimeout)
	}
	if opts.Timeout == 0 {
		opts.Timeout = DefaultWebhookTimeout
	}
	if opts.MaxResponseBytes < 0 {
		return nil, errors.New("schedule: MaxResponseBytes must not be negative")
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = defaultWebhookRespLimit
	}
	if opts.UserAgent == "" {
		opts.UserAgent = DefaultWebhookUserAgent
	} else if strings.ContainsAny(opts.UserAgent, "\r\n") {
		return nil, errors.New("schedule: invalid user agent")
	}
	opts.SigningSecret = append([]byte(nil), opts.SigningSecret...)
	e := &webhookExecutor{opts: opts, now: time.Now}
	if len(opts.AllowedHosts) > 0 {
		e.allowedHosts = make(map[string]bool, len(opts.AllowedHosts))
		for _, h := range opts.AllowedHosts {
			e.allowedHosts[normalizeHost(h)] = true
		}
	}
	dialer := &net.Dialer{
		Timeout:   webhookDialTimeout,
		KeepAlive: 30 * time.Second,
		Control:   dialControl(opts.AllowPrivateNetworks),
	}
	transport := &http.Transport{
		Proxy:                 nil, // a proxy would bypass the dial-time address checks
		DialContext:           dialer.DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   webhookTLSHandshakeTimeout,
		ResponseHeaderTimeout: opts.Timeout,
		ExpectContinueTimeout: time.Second,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	e.client = &http.Client{
		Transport: transport,
		Timeout:   opts.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return e.execute, nil
}

func normalizeHost(h string) string {
	return strings.TrimSuffix(strings.ToLower(strings.Trim(strings.TrimSpace(h), "[]")), ".")
}

func (e *webhookExecutor) execute(ctx context.Context, task ScheduledTask) error {
	p, err := ParseWebhookPayload(task.Payload)
	if err != nil {
		return err
	}
	u, err := parseWebhookURL(p.URL)
	if err != nil {
		return fmt.Errorf("schedule: webhook: %w", err)
	}
	if u.Scheme == "http" && !e.opts.AllowInsecureHTTP {
		return errors.New("schedule: webhook: url must use https")
	}
	host := normalizeHost(u.Hostname())
	if e.allowedHosts != nil && !e.allowedHosts[host] {
		return fmt.Errorf("schedule: webhook: host %s is not in the allowed host list", host)
	}
	if !e.opts.AllowPrivateNetworks {
		if err := checkLiteralHost(host); err != nil {
			return err
		}
	}

	body := []byte(p.Body)
	if len(body) == 0 {
		body, err = json.Marshal(struct {
			Event       string    `json:"event"`
			TaskID      string    `json:"task_id"`
			TaskName    string    `json:"task_name"`
			ScheduledAt time.Time `json:"scheduled_at,omitzero"`
		}{"schedule.task.run", task.ID, task.Name, task.NextRun})
		if err != nil {
			return fmt.Errorf("schedule: webhook: encode event: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("schedule: webhook: build request for host %s", host)
	}
	for name, value := range p.Headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", e.opts.UserAgent)
	req.Header.Set(HeaderTaskID, task.ID)
	if !task.NextRun.IsZero() {
		req.Header.Set(HeaderScheduledAt, task.NextRun.UTC().Format(time.RFC3339))
	}
	if len(e.opts.SigningSecret) > 0 {
		ts := strconv.FormatInt(e.now().Unix(), 10)
		req.Header.Set(HeaderTimestamp, ts)
		req.Header.Set(HeaderSignature, "v1="+SignWebhook(e.opts.SigningSecret, ts, body))
	}

	resp, err := e.client.Do(req)
	if err != nil {
		// url.Error embeds the full URL, which may carry tokens; report the
		// host and the underlying cause only.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		if errors.Is(err, ErrBlockedDestination) {
			return fmt.Errorf("schedule: webhook to host %s: %w", host, ErrBlockedDestination)
		}
		return fmt.Errorf("schedule: webhook to host %s failed: %w", host, err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, e.opts.MaxResponseBytes))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return fmt.Errorf("schedule: webhook to host %s returned redirect status %d (redirects are not followed)", host, resp.StatusCode)
	default:
		return fmt.Errorf("schedule: webhook to host %s returned status %d", host, resp.StatusCode)
	}
}

// SignWebhook returns hex(HMAC-SHA256(secret, timestamp + "." + body)), the
// value (after "v1=") of HeaderSignature. Receivers recompute it with the
// HeaderTimestamp value and the raw request body and compare in constant
// time.
func SignWebhook(secret []byte, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// blockedHostnames always name the local host or a cloud metadata service.
var blockedHostnames = map[string]bool{
	"localhost":                  true,
	"metadata":                   true,
	"metadata.google.internal":   true,
	"metadata.goog":              true,
	"instance-data":              true,
	"instance-data.ec2.internal": true,
}

// checkLiteralHost rejects hosts that are, without DNS, known to be
// non-public: IP literals in blocked ranges, localhost/metadata names and
// resolver-dependent numeric forms such as "2130706433" or "0x7f.1".
// Names are checked again after resolution by dialControl.
func checkLiteralHost(host string) error {
	if addr, err := netip.ParseAddr(host); err == nil {
		if notification.IsBlockedIP(addr) {
			return fmt.Errorf("schedule: webhook host %s: %w", host, ErrBlockedDestination)
		}
		return nil
	}
	if blockedHostnames[host] || strings.HasSuffix(host, ".localhost") {
		return fmt.Errorf("schedule: webhook host %s: %w", host, ErrBlockedDestination)
	}
	if isNumericHost(host) {
		return fmt.Errorf("schedule: webhook host %s is an ambiguous numeric address: %w", host, ErrBlockedDestination)
	}
	return nil
}

// isNumericHost reports hosts made only of digits, dots and hex prefixes,
// which some resolvers interpret as IPv4 addresses in non-canonical form.
func isNumericHost(h string) bool {
	if h == "" {
		return false
	}
	for _, part := range strings.Split(strings.TrimSuffix(h, "."), ".") {
		p := part
		if strings.HasPrefix(p, "0x") {
			p = p[2:]
			if p == "" {
				return false
			}
			for i := 0; i < len(p); i++ {
				c := p[i]
				if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
					return false
				}
			}
			continue
		}
		if p == "" {
			return false
		}
		for i := 0; i < len(p); i++ {
			if p[i] < '0' || p[i] > '9' {
				return false
			}
		}
	}
	return true
}

// dialControl refuses connections to non-public addresses. It runs after DNS
// resolution for every address the dialer tries.
func dialControl(allowPrivate bool) func(network, address string, c syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		if allowPrivate {
			return nil
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return ErrBlockedDestination
		}
		addr, err := netip.ParseAddr(host)
		if err != nil || notification.IsBlockedIP(addr) {
			return ErrBlockedDestination
		}
		return nil
	}
}
