// Package traffic implements shadow testing: mirroring a copy of live
// requests to a secondary (shadow) deployment without affecting the caller.
//
// Safety properties of ShadowTester:
//   - The shadow target comes from configuration only. When a tester is built
//     with NewShadowTesterWithConfig and a TargetURL, RunAsync refuses any
//     other URL. The target must be an absolute http(s) URL without
//     credentials, query or fragment.
//   - Dispatch is asynchronous and detached from the caller's cancellation
//     (context.WithoutCancel) but bounded by its own timeout.
//   - Concurrency is bounded by a non-blocking semaphore; when it is full the
//     shadow request is dropped with ErrShadowBusy instead of queueing.
//   - No incoming client headers are forwarded. The configured API key is only
//     sent over https or to a loopback host, never in clear text elsewhere.
//   - Redirects are never followed. Proxy settings from the environment
//     (HTTP(S)_PROXY / NO_PROXY) are honoured, as for other outbound traffic.
package traffic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Errors returned by RunAsync.
var (
	ErrNoShadowTarget      = errors.New("traffic: no shadow target configured")
	ErrInvalidShadowTarget = errors.New("traffic: invalid shadow target")
	ErrShadowBusy          = errors.New("traffic: too many in-flight shadow requests")
	ErrPayloadTooLarge     = errors.New("traffic: shadow payload too large")
	ErrInvalidRequest      = errors.New("traffic: invalid shadow request")
)

// Defaults for ShadowTesterConfig.
const (
	DefaultTimeout      = 30 * time.Second
	DefaultMaxInFlight  = 16
	DefaultResultBuffer = 100
	// MaxPayloadBytes caps the encoded shadow request body.
	MaxPayloadBytes = 4 << 20

	maxResponseDrain = 1 << 20
	maxTargetLen     = 2048
	chatPath         = "v1/chat/completions"
	userAgent        = "AeroLLM-Shadow/1.0"
)

// ShadowConfig holds shadow testing configuration.
type ShadowConfig struct {
	Enabled      bool
	ShadowModels []string
}

// ShadowResult captures shadow execution metrics.
type ShadowResult struct {
	// Provider is the shadow base URL (never contains credentials).
	Provider   string        `json:"provider"`
	Latency    time.Duration `json:"latency"`
	StatusCode int           `json:"status_code,omitempty"`
	Timestamp  time.Time     `json:"timestamp"`
	// AuthDropped is set when an API key was configured but not sent
	// because the target is plain http on a non-loopback host.
	AuthDropped  bool   `json:"auth_dropped,omitempty"`
	Error        error  `json:"-"`
	ErrorMessage string `json:"error,omitempty"`
}

// ShadowTesterConfig configures a ShadowTester.
type ShadowTesterConfig struct {
	// TargetURL is the only shadow base URL this tester may call. When set,
	// RunAsync accepts an empty shadowURL (meaning TargetURL) or TargetURL
	// itself and rejects everything else.
	TargetURL string
	// APIKey is used when RunAsync is called with an empty apiKey.
	APIKey string
	// Timeout bounds each detached shadow call (default 30s).
	Timeout time.Duration
	// MaxInFlight bounds concurrent shadow calls (default 16).
	MaxInFlight int
	// ResultBuffer is the number of recent results kept (default 100).
	ResultBuffer int
}

// pool holds the state shared by testers: limiter, HTTP client, in-flight
// accounting and recent results.
type pool struct {
	sem     chan struct{}
	client  *http.Client
	results *ring

	mu       sync.Mutex
	idle     *sync.Cond
	inflight int
}

func newPool(maxInFlight, resultBuffer int) *pool {
	p := &pool{
		sem:     make(chan struct{}, maxInFlight),
		client:  newClient(),
		results: newRing(resultBuffer),
	}
	p.idle = sync.NewCond(&p.mu)
	return p
}

func (p *pool) begin() {
	p.mu.Lock()
	p.inflight++
	p.mu.Unlock()
}

func (p *pool) end() {
	p.mu.Lock()
	p.inflight--
	if p.inflight == 0 {
		p.idle.Broadcast()
	}
	p.mu.Unlock()
}

func (p *pool) wait() {
	p.mu.Lock()
	for p.inflight > 0 {
		p.idle.Wait()
	}
	p.mu.Unlock()
}

func newClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = DefaultMaxInFlight
	tr.ResponseHeaderTimeout = DefaultTimeout
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// defaultPool is shared by every tester created with NewShadowTester so the
// in-flight bound is process wide even if a tester is created per request.
var defaultPool = newPool(DefaultMaxInFlight, DefaultResultBuffer)

// ShadowTester runs asynchronous shadow requests against secondary providers.
type ShadowTester struct {
	cfg    ShadowTesterConfig
	pool   *pool
	client *http.Client // overrides pool.client when set (tests)
	now    func() time.Time
}

// NewShadowTester creates a shadow tester with default limits. All testers
// created this way share one process-wide limiter, HTTP client and result
// buffer. The target is whatever RunAsync is given; prefer
// NewShadowTesterWithConfig to pin the target.
func NewShadowTester() *ShadowTester {
	return &ShadowTester{
		cfg:  ShadowTesterConfig{Timeout: DefaultTimeout, MaxInFlight: DefaultMaxInFlight, ResultBuffer: DefaultResultBuffer},
		pool: defaultPool,
		now:  time.Now,
	}
}

// NewShadowTesterWithConfig creates a tester with its own limiter, client and
// result buffer. It returns an error if TargetURL is set but invalid.
func NewShadowTesterWithConfig(cfg ShadowTesterConfig) (*ShadowTester, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.ResultBuffer <= 0 {
		cfg.ResultBuffer = DefaultResultBuffer
	}
	if cfg.TargetURL != "" {
		if _, err := parseTarget(cfg.TargetURL); err != nil {
			return nil, err
		}
	}
	return &ShadowTester{cfg: cfg, pool: newPool(cfg.MaxInFlight, cfg.ResultBuffer), now: time.Now}, nil
}

// RunAsync validates the target and request, then sends the same payload to
// the shadow provider's /v1/chat/completions asynchronously. It returns nil
// once the request is dispatched (not when it completes); results are
// available from Results. The dispatched call survives cancellation of ctx
// (e.g. the originating HTTP handler returning) but is bounded by the
// tester's timeout. It returns ErrShadowBusy when the in-flight limit is
// reached, and ctx.Err() if ctx is already done.
func (s *ShadowTester) RunAsync(ctx context.Context, shadowURL, apiKey string, req interface{}) error {
	if s == nil {
		return ErrNoShadowTarget
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	target := strings.TrimSpace(shadowURL)
	if s.cfg.TargetURL != "" {
		switch {
		case target == "":
			target = s.cfg.TargetURL
		case !sameTarget(target, s.cfg.TargetURL):
			return fmt.Errorf("%w: target does not match the configured shadow URL", ErrInvalidShadowTarget)
		}
	}
	base, err := parseTarget(target)
	if err != nil {
		return err
	}
	endpoint := base.JoinPath(chatPath).String()
	if apiKey == "" {
		apiKey = s.cfg.APIKey
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrInvalidRequest, err)
	}
	if bytes.Equal(body, []byte("null")) {
		return fmt.Errorf("%w: nil request", ErrInvalidRequest)
	}
	if len(body) > MaxPayloadBytes {
		return ErrPayloadTooLarge
	}
	p := s.getPool()
	select {
	case p.sem <- struct{}{}:
	default:
		return ErrShadowBusy
	}
	p.begin()
	dctx := context.WithoutCancel(ctx)
	provider := base.String()
	go func() {
		defer p.end()
		defer func() { <-p.sem }()
		defer func() {
			if r := recover(); r != nil {
				p.results.add(ShadowResult{
					Provider:     provider,
					Timestamp:    s.clock(),
					Error:        fmt.Errorf("traffic: shadow dispatch panicked: %v", r),
					ErrorMessage: "shadow dispatch panicked",
				})
			}
		}()
		p.results.add(s.dispatch(dctx, base, endpoint, provider, apiKey, body))
	}()
	return nil
}

// getPool returns the tester's pool; a zero-value ShadowTester uses the
// shared default pool.
func (s *ShadowTester) getPool() *pool {
	if s.pool != nil {
		return s.pool
	}
	return defaultPool
}

func (s *ShadowTester) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *ShadowTester) dispatch(ctx context.Context, base *url.URL, endpoint, provider, apiKey string, body []byte) ShadowResult {
	timeout := s.cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := s.clock()
	res := ShadowResult{Provider: provider, Timestamp: start}
	fail := func(err error) ShadowResult {
		res.Latency = s.clock().Sub(start)
		res.Error = err
		res.ErrorMessage = err.Error()
		return res
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fail(fmt.Errorf("traffic: build shadow request: %w", err))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-AeroLLM-Shadow", "1")
	if apiKey != "" {
		if authAllowed(base) {
			httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		} else {
			res.AuthDropped = true
		}
	}
	client := s.client
	if client == nil {
		client = s.getPool().client
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fail(fmt.Errorf("traffic: shadow request: %w", err))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseDrain))
	_ = resp.Body.Close()
	res.StatusCode = resp.StatusCode
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return fail(fmt.Errorf("traffic: shadow target redirected (status %d); redirects are not followed", resp.StatusCode))
	case resp.StatusCode >= 400:
		return fail(fmt.Errorf("traffic: shadow target returned status %d", resp.StatusCode))
	}
	res.Latency = s.clock().Sub(start)
	return res
}

// Wait blocks until every dispatched shadow request of this tester's pool has
// finished (testers from NewShadowTester share one pool). Useful for graceful
// shutdown and tests.
func (s *ShadowTester) Wait() {
	s.getPool().wait()
}

// Results returns the most recent shadow results, oldest first.
func (s *ShadowTester) Results() []ShadowResult {
	return s.getPool().results.snapshot()
}

// parseTarget validates a shadow base URL.
func parseTarget(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrNoShadowTarget
	}
	if len(raw) > maxTargetLen {
		return nil, fmt.Errorf("%w: too long", ErrInvalidShadowTarget)
	}
	for _, c := range raw {
		if c <= ' ' || c == 0x7f {
			return nil, fmt.Errorf("%w: contains whitespace or control characters", ErrInvalidShadowTarget)
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: not a URL", ErrInvalidShadowTarget)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("%w: scheme must be http or https", ErrInvalidShadowTarget)
	}
	if u.Opaque != "" || u.Hostname() == "" {
		return nil, fmt.Errorf("%w: host is required", ErrInvalidShadowTarget)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%w: credentials in URL are not allowed", ErrInvalidShadowTarget)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("%w: query and fragment are not allowed", ErrInvalidShadowTarget)
	}
	if p := u.Port(); p != "" {
		n := 0
		for _, c := range p {
			n = n*10 + int(c-'0')
			if n > 65535 {
				break
			}
		}
		if n == 0 || n > 65535 {
			return nil, fmt.Errorf("%w: invalid port", ErrInvalidShadowTarget)
		}
	}
	return u, nil
}

// sameTarget compares two base URLs ignoring a trailing slash and scheme/host case.
func sameTarget(a, b string) bool {
	ua, errA := url.Parse(strings.TrimSpace(a))
	ub, errB := url.Parse(strings.TrimSpace(b))
	if errA != nil || errB != nil {
		return false
	}
	return strings.EqualFold(ua.Scheme, ub.Scheme) &&
		strings.EqualFold(ua.Host, ub.Host) &&
		strings.TrimSuffix(ua.EscapedPath(), "/") == strings.TrimSuffix(ub.EscapedPath(), "/") &&
		ua.User == nil && ub.User == nil &&
		ua.RawQuery == ub.RawQuery && ua.Fragment == ub.Fragment
}

// authAllowed reports whether credentials may be sent to u: only over TLS or
// to the local host.
func authAllowed(u *url.URL) bool {
	if u.Scheme == "https" {
		return true
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.Unmap().IsLoopback()
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// ring is a bounded, concurrency-safe buffer of recent results.
type ring struct {
	mu   sync.Mutex
	buf  []ShadowResult
	next int
	n    int
}

func newRing(size int) *ring {
	if size <= 0 {
		size = DefaultResultBuffer
	}
	return &ring{buf: make([]ShadowResult, size)}
}

func (r *ring) add(res ShadowResult) {
	r.mu.Lock()
	r.buf[r.next] = res
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

func (r *ring) snapshot() []ShadowResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ShadowResult, 0, r.n)
	start := (r.next - r.n + len(r.buf)) % len(r.buf)
	for i := 0; i < r.n; i++ {
		out = append(out, r.buf[(start+i)%len(r.buf)])
	}
	return out
}
