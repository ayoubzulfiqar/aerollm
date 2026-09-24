package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// StreamIdleTimeout aborts a stream when the upstream sends nothing for this
// long.
var StreamIdleTimeout = 5 * time.Minute

// NormalizeBaseURL validates an http(s) base URL, strips trailing slashes and
// appends defaultPath (e.g. "/v1") when the URL has no path. Query strings are
// preserved.
func NormalizeBaseURL(base, defaultPath string) (*url.URL, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		return nil, errors.New("base URL is empty")
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, errors.New("base URL is invalid")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("base URL scheme %q is not http or https", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("base URL has no host")
	}
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	if u.Path == "" && defaultPath != "" {
		u.Path = "/" + strings.Trim(defaultPath, "/")
	}
	return u, nil
}

// JoinURL appends p to base's path, keeping base's query string.
func JoinURL(base *url.URL, p string) string {
	u := *base
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(p, "/")
	u.RawPath = ""
	return u.String()
}

// MatchModel reports whether model matches pattern (case-insensitive).
// Patterns are exact names, "*" (any model) or a prefix ending in "*"
// ("gpt-4o*").
func MatchModel(pattern, model string) bool {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	model = strings.ToLower(strings.TrimSpace(model))
	if pattern == "" {
		return false
	}
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(model, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == model
}

// ModelList is a concurrency-safe list of model patterns (see MatchModel).
type ModelList struct {
	mu       sync.RWMutex
	patterns []string
}

// Set replaces the patterns.
func (l *ModelList) Set(patterns ...string) {
	cp := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p = strings.TrimSpace(p); p != "" {
			cp = append(cp, p)
		}
	}
	l.mu.Lock()
	l.patterns = cp
	l.mu.Unlock()
}

// Match reports whether model matches any pattern. ok is false when the list
// is empty (no opinion).
func (l *ModelList) Match(model string) (matched, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.patterns) == 0 {
		return false, false
	}
	for _, p := range l.patterns {
		if MatchModel(p, model) {
			return true, true
		}
	}
	return false, true
}

// HealthTracker records call outcomes to derive a provider's health. The
// zero value is ready to use.
type HealthTracker struct {
	mu                  sync.Mutex
	consecutiveFailures int64
	totalFailures       int64
	latencyEWMA         float64
	samples             int64
	lastChecked         time.Time
}

// unhealthyAfter is the number of consecutive failures after which a
// provider reports itself unhealthy.
const unhealthyAfter = 3

// Observe records one call. Caller cancellations are ignored; non-retryable
// errors (e.g. 400) prove the upstream is alive and count as success.
func (h *HealthTracker) Observe(latency time.Duration, err error) {
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastChecked = time.Now()
	if err != nil && IsRetryable(err) {
		h.consecutiveFailures++
		h.totalFailures++
		return
	}
	h.consecutiveFailures = 0
	if err == nil && latency > 0 {
		ms := float64(latency) / float64(time.Millisecond)
		if h.samples == 0 {
			h.latencyEWMA = ms
		} else {
			h.latencyEWMA = 0.2*ms + 0.8*h.latencyEWMA
		}
		h.samples++
	}
}

// Snapshot returns the current health.
func (h *HealthTracker) Snapshot(name string, typ ProviderType) ProviderHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	var last int64
	if !h.lastChecked.IsZero() {
		last = h.lastChecked.Unix()
	}
	return ProviderHealth{
		Name:        name,
		Type:        typ,
		Healthy:     h.consecutiveFailures < unhealthyAfter,
		LatencyMs:   h.latencyEWMA,
		Failures:    h.totalFailures,
		LastChecked: last,
	}
}

// DoJSON POSTs body as JSON to endpoint and decodes a 2xx JSON response into
// out. Non-2xx responses become *UpstreamError, transport failures
// *TransportError and undecodable bodies a 502 *UpstreamError.
func DoJSON(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header, body, out interface{}) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%s: marshal request: %w", provider, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%s: build request: %w", provider, err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	req.Header.Set("Content-Type", "application/json")
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	return DoRequest(client, provider, req, out)
}

// DoRequest executes req and decodes a 2xx JSON response into out (which may
// be nil to discard the body).
func DoRequest(client *http.Client, provider string, req *http.Request, out interface{}) error {
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctxErr := req.Context().Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return ctxErr
		}
		return &TransportError{Provider: provider, Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return redactSecrets(NewUpstreamError(provider, resp), req.Header)
	}
	b, err := ReadResponseBody(resp.Body, MaxResponseBytes)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return BadResponseError(provider, err)
		}
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return ctxErr
		}
		return &TransportError{Provider: provider, Err: err}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return BadResponseError(provider, err)
	}
	return nil
}

// secretHeaders carry credentials that must never appear in errors.
var secretHeaders = []string{"Authorization", "X-Api-Key", "Api-Key", "X-Goog-Api-Key", "X-Amz-Security-Token"}

// redactSecrets removes credential values sent in h from the upstream error
// message (some upstreams echo the offending key back).
func redactSecrets(e *UpstreamError, h http.Header) *UpstreamError {
	if e == nil || e.Message == "" {
		return e
	}
	for _, name := range secretHeaders {
		for _, v := range h.Values(name) {
			v = strings.TrimSpace(v)
			if i := strings.IndexByte(v, ' '); i > 0 && name == "Authorization" {
				v = strings.TrimSpace(v[i+1:]) // "Bearer <key>" / AWS4 signature
			}
			if len(v) >= 8 {
				e.Message = strings.ReplaceAll(e.Message, v, "[REDACTED]")
			}
		}
	}
	return e
}

// streamConn is an established upstream stream.
type streamConn struct {
	resp     *http.Response
	body     io.ReadCloser
	cancel   context.CancelFunc
	idle     *time.Timer
	timedOut atomic.Bool
}

// close releases the connection and its timers.
func (s *streamConn) close() {
	if s.idle != nil {
		s.idle.Stop()
	}
	_ = s.body.Close()
	s.cancel()
}

// idleReader resets the idle timer whenever data arrives.
type idleReader struct {
	r io.Reader
	t *time.Timer
	d time.Duration
}

func (i *idleReader) Read(p []byte) (int, error) {
	n, err := i.r.Read(p)
	if n > 0 {
		i.t.Reset(i.d)
	}
	return n, err
}

// openStream sends req and waits for the response headers. client.Timeout
// (if any) bounds only the wait for headers; the stream itself is bounded by
// ctx and StreamIdleTimeout. Non-2xx responses become *UpstreamError.
func openStream(ctx context.Context, client *http.Client, provider string, req *http.Request) (*streamConn, error) {
	if client == nil {
		client = http.DefaultClient
	}
	streamCtx, cancel := context.WithCancel(ctx)
	req = req.WithContext(streamCtx)
	sc := *client
	sc.Timeout = 0

	var headerTimer *time.Timer
	var headerTimedOut atomic.Bool
	if client.Timeout > 0 {
		headerTimer = time.AfterFunc(client.Timeout, func() {
			headerTimedOut.Store(true)
			cancel()
		})
	}
	resp, err := sc.Do(req)
	if headerTimer != nil {
		headerTimer.Stop()
	}
	if err != nil {
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if headerTimedOut.Load() {
			return nil, &TransportError{Provider: provider, Err: context.DeadlineExceeded}
		}
		return nil, &TransportError{Provider: provider, Err: err}
	}
	if headerTimedOut.Load() {
		resp.Body.Close()
		cancel()
		return nil, &TransportError{Provider: provider, Err: context.DeadlineExceeded}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		uerr := redactSecrets(NewUpstreamError(provider, resp), req.Header)
		resp.Body.Close()
		cancel()
		return nil, uerr
	}
	conn := &streamConn{resp: resp, body: resp.Body, cancel: cancel}
	if d := StreamIdleTimeout; d > 0 {
		conn.idle = time.AfterFunc(d, func() {
			conn.timedOut.Store(true)
			cancel()
		})
		conn.body = struct {
			io.Reader
			io.Closer
		}{&idleReader{r: resp.Body, t: conn.idle, d: d}, resp.Body}
	}
	return conn, nil
}

// isJSONResponse reports whether an upstream answered a stream request with
// a plain JSON body (some servers ignore stream=true).
func isJSONResponse(resp *http.Response) bool {
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	return strings.HasPrefix(ct, "application/json")
}

// streamDecoder converts one SSE event into zero or more chunks. done=true
// ends the stream successfully.
type streamDecoder func(ev SSEEvent) (chunks []models.StreamChunk, done bool, err error)

// runSSE pumps events from conn through decode into a channel following the
// StreamingProvider contract. onEOF is called when the body ends without the
// decoder signalling done; a non-nil return is sent as an error chunk.
// observe, if non-nil, is called once with the stream's final error.
func runSSE(ctx context.Context, provider string, conn *streamConn, decode streamDecoder, onEOF func() error, observe func(error)) <-chan models.StreamChunk {
	out := make(chan models.StreamChunk)
	go func() {
		var final error
		defer func() {
			conn.close()
			if observe != nil {
				observe(final)
			}
			close(out)
		}()
		send := func(c models.StreamChunk) bool {
			select {
			case out <- c:
				return true
			case <-ctx.Done():
				final = ctx.Err()
				return false
			}
		}
		fail := func(err error) {
			final = err
			send(models.StreamChunk{Object: "chat.completion.chunk", Choices: []models.StreamChoice{}, Err: err})
		}
		r := NewSSEReader(conn.body)
		for {
			ev, err := r.Next()
			if err != nil {
				if ctx.Err() != nil {
					final = ctx.Err()
					return
				}
				if conn.timedOut.Load() {
					fail(&UpstreamError{Provider: provider, StatusCode: http.StatusGatewayTimeout, Message: "upstream stream idle timeout"})
					return
				}
				if errors.Is(err, io.EOF) {
					if e := onEOF(); e != nil {
						fail(e)
					}
					return
				}
				if errors.Is(err, ErrSSEEventTooLarge) || errors.Is(err, bufio.ErrTooLong) {
					fail(BadResponseError(provider, err))
					return
				}
				fail(&TransportError{Provider: provider, Err: err})
				return
			}
			chunks, done, derr := decode(ev)
			for _, c := range chunks {
				if !send(c) {
					return
				}
			}
			if derr != nil {
				fail(derr)
				return
			}
			if done {
				return
			}
		}
	}()
	return out
}

// chunksFromJSONBody serves a stream request answered with a complete JSON
// response by converting it into chunks.
func chunksFromJSONBody(ctx context.Context, provider string, conn *streamConn, toResponse func([]byte) (*models.LLMResponse, error), observe func(error)) <-chan models.StreamChunk {
	out := make(chan models.StreamChunk)
	go func() {
		var final error
		defer func() {
			conn.close()
			if observe != nil {
				observe(final)
			}
			close(out)
		}()
		b, err := ReadResponseBody(conn.body, MaxResponseBytes)
		var resp *models.LLMResponse
		if err == nil {
			resp, err = toResponse(b)
		}
		if err != nil {
			if ctx.Err() != nil {
				final = ctx.Err()
				return
			}
			final = BadResponseError(provider, err)
			select {
			case out <- models.StreamChunk{Object: "chat.completion.chunk", Choices: []models.StreamChoice{}, Err: final}:
			case <-ctx.Done():
			}
			return
		}
		for _, c := range models.StreamChunksFromResponse(resp) {
			select {
			case out <- c:
			case <-ctx.Done():
				final = ctx.Err()
				return
			}
		}
	}()
	return out
}
