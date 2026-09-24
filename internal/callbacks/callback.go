package callbacks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CallbackHandler defines the interface for observability callbacks.
// Implementations fire asynchronously after a response is sent to the client,
// ensuring zero latency impact on the user request path.
type CallbackHandler interface {
	// OnSuccess is called when a provider call completes successfully.
	OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error
	// OnError is called when a provider call fails.
	OnError(ctx context.Context, req *CallbackRequestData, err error) error
	// Name returns a unique identifier for the callback handler.
	Name() string
}

// CallbackRequestData captures the request details needed for tracing/logging.
// It must never carry API keys or other credentials; Metadata keys that
// look like secrets are redacted by the CallbackManager.
type CallbackRequestData struct {
	RequestID string                   `json:"request_id"`
	Model     string                   `json:"model"`
	Provider  string                   `json:"provider"`
	Messages  []map[string]interface{} `json:"messages"`
	Metadata  map[string]interface{}   `json:"metadata"`
	Timestamp time.Time                `json:"timestamp"`
	CostUSD   float64                  `json:"cost_usd"`
	// User is an optional end-user identifier (e.g. the OpenAI "user" field).
	User string `json:"user,omitempty"`
	// Tags are optional labels forwarded to integrations that support them.
	Tags []string `json:"tags,omitempty"`
}

// CallbackResponseData captures the response details for tracing/logging.
type CallbackResponseData struct {
	ResponseID string                 `json:"response_id"`
	Usage      map[string]interface{} `json:"usage"`
	Model      string                 `json:"model"`
	Provider   string                 `json:"provider"`
	LatencyMs  int64                  `json:"latency_ms"`
	TokenCount map[string]int         `json:"token_count"`
	Metadata   map[string]interface{} `json:"metadata"`
	Timestamp  time.Time              `json:"timestamp"`
	// Output is the (optional) completion text; redacted when content
	// redaction is enabled.
	Output       string `json:"output,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
}

// ManagerOptions tunes the CallbackManager worker pool.
type ManagerOptions struct {
	// Workers is the number of concurrent delivery goroutines (default 4).
	Workers int
	// QueueSize bounds pending deliveries; overflow is dropped and counted
	// (default 1000).
	QueueSize int
	// RedactContent replaces message/output content with "[REDACTED]"
	// before any handler sees it.
	RedactContent bool
	// OnHandlerError, if set, is called for every failed delivery.
	OnHandlerError func(handler string, err error)
}

// CallbackStats is a snapshot of delivery counters.
type CallbackStats struct {
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Dropped   int64 `json:"dropped"`
	Pending   int64 `json:"pending"`
}

type callbackJob struct {
	handler CallbackHandler
	req     *CallbackRequestData
	resp    *CallbackResponseData
	err     error
	isError bool
}

// CallbackManager dispatches callbacks asynchronously after responses are
// sent, through a bounded queue drained by a fixed worker pool. When the
// queue is full events are dropped (never blocking the request path) and
// counted. Every handler receives its own sanitized copy of the data.
type CallbackManager struct {
	mu       sync.RWMutex
	handlers []CallbackHandler
	timeout  time.Duration
	opts     ManagerOptions
	redact   atomic.Bool

	startOnce sync.Once
	jobs      chan callbackJob
	quit      chan struct{}
	quitOnce  sync.Once
	closed    atomic.Bool
	workers   sync.WaitGroup

	pendMu  sync.Mutex
	pendCv  *sync.Cond
	pending int64

	succeeded atomic.Int64
	failed    atomic.Int64
	dropped   atomic.Int64
}

// NewCallbackManager creates a new callback manager with the given
// per-delivery timeout and default pool options.
func NewCallbackManager(timeout time.Duration) *CallbackManager {
	return NewCallbackManagerWithOptions(timeout, ManagerOptions{})
}

// NewCallbackManagerWithOptions creates a callback manager with explicit options.
func NewCallbackManagerWithOptions(timeout time.Duration, opts ManagerOptions) *CallbackManager {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1000
	}
	m := &CallbackManager{
		handlers: make([]CallbackHandler, 0),
		timeout:  timeout,
		opts:     opts,
		jobs:     make(chan callbackJob, opts.QueueSize),
		quit:     make(chan struct{}),
	}
	m.pendCv = sync.NewCond(&m.pendMu)
	m.redact.Store(opts.RedactContent)
	return m
}

// Register adds a callback handler to the manager. Nil handlers are ignored.
func (m *CallbackManager) Register(handler CallbackHandler) {
	if handler == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
}

// SetRedactContent toggles message/output content redaction.
func (m *CallbackManager) SetRedactContent(on bool) { m.redact.Store(on) }

// Handlers returns the names of the registered handlers.
func (m *CallbackManager) Handlers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.handlers))
	for i, h := range m.handlers {
		out[i] = h.Name()
	}
	return out
}

func (m *CallbackManager) snapshot() []CallbackHandler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]CallbackHandler(nil), m.handlers...)
}

// FireSuccess dispatches OnSuccess to all registered handlers asynchronously.
func (m *CallbackManager) FireSuccess(req *CallbackRequestData, resp *CallbackResponseData) {
	for _, h := range m.snapshot() {
		m.enqueue(callbackJob{handler: h, req: sanitizeRequest(req, m.redact.Load()), resp: sanitizeResponse(resp, m.redact.Load())})
	}
}

// FireError dispatches OnError to all registered handlers asynchronously.
func (m *CallbackManager) FireError(req *CallbackRequestData, err error) {
	if err == nil {
		err = fmt.Errorf("unknown error")
	}
	for _, h := range m.snapshot() {
		m.enqueue(callbackJob{handler: h, req: sanitizeRequest(req, m.redact.Load()), err: err, isError: true})
	}
}

func (m *CallbackManager) addPending(delta int64) {
	m.pendMu.Lock()
	m.pending += delta
	if m.pending <= 0 {
		m.pending = 0
		m.pendCv.Broadcast()
	}
	m.pendMu.Unlock()
}

func (m *CallbackManager) enqueue(job callbackJob) {
	if m.closed.Load() {
		m.dropped.Add(1)
		return
	}
	m.startOnce.Do(m.startWorkers)
	m.addPending(1)
	select {
	case m.jobs <- job:
	default:
		m.addPending(-1)
		m.dropped.Add(1)
	}
}

func (m *CallbackManager) startWorkers() {
	for i := 0; i < m.opts.Workers; i++ {
		m.workers.Add(1)
		go m.worker()
	}
}

func (m *CallbackManager) worker() {
	defer m.workers.Done()
	for {
		select {
		case job := <-m.jobs:
			m.run(job)
		case <-m.quit:
			for {
				select {
				case job := <-m.jobs:
					m.run(job)
				default:
					return
				}
			}
		}
	}
}

func (m *CallbackManager) run(job callbackJob) {
	defer m.addPending(-1)
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("callback %s panicked: %v", job.handler.Name(), r)
			}
		}()
		if job.isError {
			return job.handler.OnError(ctx, job.req, job.err)
		}
		return job.handler.OnSuccess(ctx, job.req, job.resp)
	}()
	if err != nil {
		m.failed.Add(1)
		if m.opts.OnHandlerError != nil {
			m.opts.OnHandlerError(job.handler.Name(), err)
		}
		return
	}
	m.succeeded.Add(1)
}

// Wait blocks until all queued callbacks have completed.
func (m *CallbackManager) Wait() {
	m.pendMu.Lock()
	defer m.pendMu.Unlock()
	for m.pending > 0 {
		m.pendCv.Wait()
	}
}

// Flush waits for queued callbacks or until ctx is done.
func (m *CallbackManager) Flush(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops accepting callbacks and drains the queue, waiting until
// ctx is done. In-flight deliveries are bounded by the per-call timeout.
func (m *CallbackManager) Shutdown(ctx context.Context) error {
	m.closed.Store(true)
	m.quitOnce.Do(func() { close(m.quit) })
	done := make(chan struct{})
	go func() {
		m.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close shuts down the manager, allowing up to 10s to drain.
func (m *CallbackManager) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.Shutdown(ctx)
}

// Stats returns delivery counters.
func (m *CallbackManager) Stats() CallbackStats {
	m.pendMu.Lock()
	p := m.pending
	m.pendMu.Unlock()
	return CallbackStats{
		Succeeded: m.succeeded.Load(),
		Failed:    m.failed.Load(),
		Dropped:   m.dropped.Load(),
		Pending:   p,
	}
}

// ---------------------------------------------------------------------------
// Sanitisation
// ---------------------------------------------------------------------------

const redacted = "[REDACTED]"

// sensitiveKeys are metadata keys (case-insensitive, "-" treated as "_")
// whose values are never forwarded to third parties.
var sensitiveKeys = map[string]bool{
	"api_key": true, "apikey": true, "x_api_key": true, "authorization": true,
	"auth": true, "secret": true, "client_secret": true, "password": true,
	"passwd": true, "access_token": true, "refresh_token": true, "bearer": true,
	"cookie": true, "set_cookie": true, "private_key": true, "token": true,
	"session_token": true, "virtual_key": true, "master_key": true,
}

func isSensitiveKey(k string) bool {
	return sensitiveKeys[strings.ReplaceAll(strings.ToLower(k), "-", "_")]
}

// sanitizeValue deep-copies maps/slices and redacts sensitive keys.
func sanitizeValue(v interface{}, depth int) interface{} {
	if depth > 16 {
		return nil
	}
	switch t := v.(type) {
	case map[string]interface{}:
		return sanitizeMap(t, depth+1)
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, x := range t {
			out[i] = sanitizeValue(x, depth+1)
		}
		return out
	case map[string]string:
		out := make(map[string]interface{}, len(t))
		for k, x := range t {
			if isSensitiveKey(k) {
				out[k] = redacted
			} else {
				out[k] = x
			}
		}
		return out
	default:
		return v
	}
}

func sanitizeMap(in map[string]interface{}, depth int) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		if isSensitiveKey(k) {
			out[k] = redacted
			continue
		}
		out[k] = sanitizeValue(v, depth)
	}
	return out
}

// sanitizeRequest returns an isolated, scrubbed copy of req.
func sanitizeRequest(req *CallbackRequestData, redactContent bool) *CallbackRequestData {
	if req == nil {
		return &CallbackRequestData{Timestamp: time.Now().UTC()}
	}
	cp := *req
	cp.Metadata = sanitizeMap(req.Metadata, 0)
	cp.Tags = append([]string(nil), req.Tags...)
	if req.Messages != nil {
		cp.Messages = make([]map[string]interface{}, len(req.Messages))
		for i, msg := range req.Messages {
			m := sanitizeMap(msg, 0)
			if redactContent && m != nil {
				for _, k := range []string{"content", "tool_result", "arguments", "tool_calls"} {
					if _, ok := m[k]; ok {
						m[k] = redacted
					}
				}
			}
			cp.Messages[i] = m
		}
	}
	return &cp
}

// sanitizeResponse returns an isolated, scrubbed copy of resp.
func sanitizeResponse(resp *CallbackResponseData, redactContent bool) *CallbackResponseData {
	if resp == nil {
		return &CallbackResponseData{Timestamp: time.Now().UTC()}
	}
	cp := *resp
	cp.Metadata = sanitizeMap(resp.Metadata, 0)
	cp.Usage = sanitizeMap(resp.Usage, 0)
	if resp.TokenCount != nil {
		cp.TokenCount = make(map[string]int, len(resp.TokenCount))
		for k, v := range resp.TokenCount {
			cp.TokenCount[k] = v
		}
	}
	if redactContent && cp.Output != "" {
		cp.Output = redacted
	}
	return &cp
}

// NoOpCallback is a callback handler that does nothing.
// Useful as a default when no callbacks are configured.
type NoOpCallback struct{}

// OnSuccess implements CallbackHandler.
func (n *NoOpCallback) OnSuccess(_ context.Context, _ *CallbackRequestData, _ *CallbackResponseData) error {
	return nil
}

// OnError implements CallbackHandler.
func (n *NoOpCallback) OnError(_ context.Context, _ *CallbackRequestData, _ error) error {
	return nil
}

// Name implements CallbackHandler.
func (n *NoOpCallback) Name() string { return "noop" }

// Ensure the interface is satisfied at compile time.
var _ CallbackHandler = (*NoOpCallback)(nil)
