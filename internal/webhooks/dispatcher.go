package webhooks

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

// EventType defines webhook event types.
type EventType string

const (
	EventBudgetExceeded        EventType = "budget_exceeded"
	EventBudgetThreshold       EventType = "budget_threshold_reached"
	EventAgentRequiresApproval EventType = "agent_requires_approval"
	EventShadowTestCompleted   EventType = "shadow_test_completed"
)

// Event represents a webhook event.
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// WebhookConfig holds webhook endpoint configuration.
//
// Retries is the maximum number of delivery attempts (values <= 0 mean a
// single attempt). RetryDelay is the base delay of the exponential backoff
// between attempts. When Secret is set every delivery is signed (see Sign).
type WebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
}

// Validate checks that the endpoint URL is an absolute http(s) URL.
func (c WebhookConfig) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("webhooks: invalid url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("webhooks: unsupported url scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("webhooks: url has no host")
	}
	return nil
}

// BudgetWebhookConfig holds budget webhook target configuration.
type BudgetWebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
}

// WebhookConfig converts the budget target into a generic WebhookConfig.
func (c BudgetWebhookConfig) WebhookConfig() WebhookConfig {
	return WebhookConfig{URL: c.URL, Secret: c.Secret, Timeout: c.Timeout, Retries: c.Retries, RetryDelay: c.RetryDelay}
}

// DispatcherOptions tunes the asynchronous delivery pool.
type DispatcherOptions struct {
	// Workers is the number of concurrent delivery goroutines (default 8).
	Workers int
	// QueueSize bounds the number of pending deliveries; further events are
	// dropped and counted (default 1024).
	QueueSize int
	// MaxRetryDelay caps the exponential backoff (default 30s).
	MaxRetryDelay time.Duration
	// HTTPClient overrides the HTTP client. Redirects are never followed by
	// the default client.
	HTTPClient *http.Client
	// QueueConsumers is the number of goroutines StartWorker runs against a
	// reliable (AckQueue) queue (default 4).
	QueueConsumers int
}

// DispatcherStats is a snapshot of delivery counters.
type DispatcherStats struct {
	Delivered int64 `json:"delivered"`
	Failed    int64 `json:"failed"`
	Dropped   int64 `json:"dropped"`
	Pending   int64 `json:"pending"`
}

var (
	// ErrDispatcherClosed is returned when dispatching after Shutdown.
	ErrDispatcherClosed = errors.New("webhooks: dispatcher closed")
	// ErrQueueEmpty is returned by WebhookQueue.Dequeue when no event arrived
	// within the blocking timeout.
	ErrQueueEmpty = errors.New("webhooks: queue empty")
	// ErrMalformedEvent is returned by Dequeue/Receive when a queued payload
	// could not be decoded; the message has been consumed (the reliable
	// queue keeps it in its dead-letter list).
	ErrMalformedEvent = errors.New("webhooks: malformed queued event")
	// ErrNoRedisClient is returned by RedisWebhookQueue without a client.
	ErrNoRedisClient = errors.New("webhooks: redis client not configured")
)

type deliveryJob struct {
	ctx   context.Context
	cfg   WebhookConfig
	event Event
}

// WebhookDispatcher dispatches webhook events asynchronously through a
// bounded worker pool with signed payloads and exponential backoff.
type WebhookDispatcher struct {
	mu      sync.RWMutex
	configs map[EventType][]WebhookConfig

	opts   DispatcherOptions
	client *http.Client

	startOnce sync.Once
	jobs      chan deliveryJob
	quit      chan struct{}
	quitOnce  sync.Once
	closed    atomic.Bool
	workers   sync.WaitGroup
	lifeCtx   context.Context
	lifeStop  context.CancelFunc

	delivered atomic.Int64
	failed    atomic.Int64
	dropped   atomic.Int64
	pending   atomic.Int64
}

// NewWebhookDispatcher creates a new webhook dispatcher with default options.
func NewWebhookDispatcher() *WebhookDispatcher {
	return NewWebhookDispatcherWithOptions(DispatcherOptions{})
}

// NewWebhookDispatcherWithOptions creates a dispatcher with explicit pool options.
func NewWebhookDispatcherWithOptions(opts DispatcherOptions) *WebhookDispatcher {
	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 1024
	}
	if opts.MaxRetryDelay <= 0 {
		opts.MaxRetryDelay = 30 * time.Second
	}
	if opts.QueueConsumers <= 0 {
		opts.QueueConsumers = 4
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	lifeCtx, stop := context.WithCancel(context.Background())
	return &WebhookDispatcher{
		configs:  make(map[EventType][]WebhookConfig),
		opts:     opts,
		client:   client,
		jobs:     make(chan deliveryJob, opts.QueueSize),
		quit:     make(chan struct{}),
		lifeCtx:  lifeCtx,
		lifeStop: stop,
	}
}

// Register registers a webhook URL for an event type.
func (d *WebhookDispatcher) Register(eventType EventType, cfg WebhookConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.configs[eventType] = append(d.configs[eventType], cfg)
}

// RegisterChecked validates cfg before registering it.
func (d *WebhookDispatcher) RegisterChecked(eventType EventType, cfg WebhookConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	d.Register(eventType, cfg)
	return nil
}

func (d *WebhookDispatcher) targets(eventType EventType) []WebhookConfig {
	d.mu.RLock()
	defer d.mu.RUnlock()
	src := d.configs[eventType]
	out := make([]WebhookConfig, len(src))
	copy(out, src)
	return out
}

// DispatchAsync queues the event for every webhook registered for its type.
// It never blocks: when the delivery queue is full the delivery is dropped
// and counted in Stats().Dropped. Delivery is detached from ctx cancellation
// (the caller's request usually finishes first) but stops on Shutdown.
func (d *WebhookDispatcher) DispatchAsync(ctx context.Context, event Event) {
	event = normalizeEvent(event)
	for _, cfg := range d.targets(event.Type) {
		_ = d.enqueue(ctx, deliveryJob{ctx: ctx, cfg: cfg, event: event}, false)
	}
}

// DispatchToAsync queues the event for a single explicit endpoint, regardless
// of the registrations for its type.
func (d *WebhookDispatcher) DispatchToAsync(ctx context.Context, cfg WebhookConfig, event Event) {
	_ = d.enqueue(ctx, deliveryJob{ctx: ctx, cfg: cfg, event: normalizeEvent(event)}, false)
}

// Dispatch delivers the event synchronously to every registered webhook,
// honoring ctx for cancellation, and returns the joined delivery errors.
func (d *WebhookDispatcher) Dispatch(ctx context.Context, event Event) error {
	event = normalizeEvent(event)
	var errs []error
	for _, cfg := range d.targets(event.Type) {
		if err := d.sendWithRetry(ctx, cfg, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (d *WebhookDispatcher) enqueue(ctx context.Context, job deliveryJob, block bool) error {
	if d.closed.Load() {
		d.dropped.Add(1)
		return ErrDispatcherClosed
	}
	if ctx == nil {
		ctx = context.Background()
		job.ctx = ctx
	}
	d.startOnce.Do(d.startWorkers)
	d.pending.Add(1)
	if block {
		select {
		case d.jobs <- job:
			return nil
		case <-ctx.Done():
		case <-d.quit:
		}
	} else {
		select {
		case d.jobs <- job:
			return nil
		default:
		}
	}
	d.pending.Add(-1)
	d.dropped.Add(1)
	telemetry.RecordError()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("webhooks: delivery queue full")
}

func (d *WebhookDispatcher) startWorkers() {
	for i := 0; i < d.opts.Workers; i++ {
		d.workers.Add(1)
		go d.worker()
	}
}

func (d *WebhookDispatcher) worker() {
	defer d.workers.Done()
	for {
		select {
		case job := <-d.jobs:
			d.process(job)
		case <-d.quit:
			// Drain whatever is already queued, then exit.
			for {
				select {
				case job := <-d.jobs:
					d.process(job)
				default:
					return
				}
			}
		}
	}
}

func (d *WebhookDispatcher) process(job deliveryJob) {
	defer d.pending.Add(-1)
	parent := context.WithoutCancel(job.ctx)
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(d.lifeCtx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	_ = d.sendWithRetry(ctx, job.cfg, job.event)
}

// Flush waits until every queued delivery has finished or ctx is done.
func (d *WebhookDispatcher) Flush(ctx context.Context) error {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for d.pending.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// Shutdown stops accepting events, delivers what is already queued and
// waits for the workers. When ctx expires in-flight retries are aborted.
func (d *WebhookDispatcher) Shutdown(ctx context.Context) error {
	d.closed.Store(true)
	d.quitOnce.Do(func() { close(d.quit) })
	done := make(chan struct{})
	go func() {
		d.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		d.lifeStop()
		return nil
	case <-ctx.Done():
		d.lifeStop()
		<-done
		return ctx.Err()
	}
}

// Close shuts the dispatcher down, allowing up to 10s for queued deliveries.
func (d *WebhookDispatcher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return d.Shutdown(ctx)
}

// Stats returns delivery counters.
func (d *WebhookDispatcher) Stats() DispatcherStats {
	return DispatcherStats{
		Delivered: d.delivered.Load(),
		Failed:    d.failed.Load(),
		Dropped:   d.dropped.Load(),
		Pending:   d.pending.Load(),
	}
}

func normalizeEvent(event Event) Event {
	if event.ID == "" {
		event.ID = "evt_" + randomHex(12)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	return event
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

// permanentError marks a delivery failure that must not be retried.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// sendWithRetry delivers the event payload with exponential backoff + jitter.
func (d *WebhookDispatcher) sendWithRetry(ctx context.Context, cfg WebhookConfig, event Event) error {
	if err := cfg.Validate(); err != nil {
		d.failed.Add(1)
		telemetry.RecordError()
		return err
	}
	body, err := json.Marshal(event)
	if err != nil {
		d.failed.Add(1)
		telemetry.RecordError()
		return fmt.Errorf("webhooks: marshal event: %w", err)
	}
	attempts := cfg.Retries
	if attempts <= 0 {
		attempts = 1
	}
	base := cfg.RetryDelay
	if base <= 0 {
		base = 200 * time.Millisecond
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			wait := backoffDelay(base, attempt, d.opts.MaxRetryDelay)
			var ra retryAfterError
			if errors.As(lastErr, &ra) && ra.after > wait {
				wait = min(ra.after, d.opts.MaxRetryDelay)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				d.failed.Add(1)
				return ctx.Err()
			case <-timer.C:
			}
		}
		err := d.send(ctx, cfg, event, body)
		if err == nil {
			d.delivered.Add(1)
			return nil
		}
		lastErr = err
		telemetry.RecordError()
		var perm permanentError
		if errors.As(err, &perm) || ctx.Err() != nil {
			break
		}
	}
	d.failed.Add(1)
	return fmt.Errorf("webhook delivery failed after retries: %w", lastErr)
}

// backoffDelay returns base*2^(attempt-1) capped at max, with "equal jitter"
// (a uniformly random value in [d/2, d]).
func backoffDelay(base time.Duration, attempt int, max time.Duration) time.Duration {
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max || d <= 0 {
		d = max
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(mrand.Int64N(int64(half)+1))
}

type retryAfterError struct {
	status int
	after  time.Duration
}

func (r retryAfterError) Error() string {
	return fmt.Sprintf("webhook returned status %d", r.status)
}

// send performs a single signed delivery attempt.
func (d *WebhookDispatcher) send(ctx context.Context, cfg WebhookConfig, event Event, body []byte) error {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return permanentError{fmt.Errorf("webhooks: build request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "AeroLLM-Webhooks/1.0")
	req.Header.Set(EventHeader, string(event.Type))
	req.Header.Set(DeliveryHeader, event.ID)
	SetSignatureHeaders(req.Header, cfg.Secret, body, time.Now())

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		ra := retryAfterError{status: resp.StatusCode}
		if s := resp.Header.Get("Retry-After"); s != "" {
			if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
				ra.after = time.Duration(secs) * time.Second
			}
		}
		return ra
	default:
		return permanentError{fmt.Errorf("webhook returned status %d", resp.StatusCode)}
	}
}

// WebhookQueue is the interface for persistent webhook queues.
//
// Dequeue should block for a bounded time and return ErrQueueEmpty when no
// event arrived, so that workers can observe context cancellation.
type WebhookQueue interface {
	Enqueue(ctx context.Context, event Event) error
	Dequeue(ctx context.Context) (Event, error)
}

// RedisListClient is the subset of the go-redis client used by RedisWebhookQueue.
type RedisListClient interface {
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
}

// RedisWebhookQueue implements WebhookQueue (and, when built over a full
// Redis client, the at-least-once AckQueue) using Redis lists. See
// NewReliableRedisQueue for the delivery guarantees.
type RedisWebhookQueue struct {
	client       RedisListClient
	key          string
	blockTimeout time.Duration
	rel          *reliableState
}

// NewRedisWebhookQueue creates a reliable (at-least-once) Redis-backed
// webhook queue with default ReliableQueueOptions. A nil client yields a
// queue whose operations return ErrNoRedisClient.
func NewRedisWebhookQueue(client *redis.Client, key string) *RedisWebhookQueue {
	if client == nil {
		return NewRedisWebhookQueueWithClient(nil, key)
	}
	return NewReliableRedisQueue(client, key, ReliableQueueOptions{})
}

// NewRedisWebhookQueueWithClient creates a queue over any RedisListClient.
// Clients that also implement RedisQueueClient get the reliable queue;
// minimal clients keep the legacy pop-then-deliver semantics (at most once).
func NewRedisWebhookQueueWithClient(client RedisListClient, key string) *RedisWebhookQueue {
	if rc, ok := client.(RedisQueueClient); ok && rc != nil {
		return NewReliableRedisQueue(rc, key, ReliableQueueOptions{})
	}
	if key == "" {
		key = "webhook:queue"
	}
	return &RedisWebhookQueue{client: client, key: key, blockTimeout: 2 * time.Second}
}

// Enqueue pushes an event onto the queue.
func (q *RedisWebhookQueue) Enqueue(ctx context.Context, event Event) error {
	if q == nil || q.client == nil {
		return ErrNoRedisClient
	}
	var (
		data []byte
		err  error
	)
	if q.rel != nil {
		data, err = json.Marshal(newEnvelope(event))
	} else {
		data, err = json.Marshal(normalizeEvent(event))
	}
	if err != nil {
		return err
	}
	return q.client.LPush(ctx, q.key, data).Err()
}

// Dequeue pops an event, blocking for at most the queue's block timeout.
// It returns ErrQueueEmpty when nothing arrived in time. Dequeue removes
// the event before the caller processes it (at most once); use
// Receive/Ack for at-least-once processing.
func (q *RedisWebhookQueue) Dequeue(ctx context.Context) (Event, error) {
	if q == nil || q.client == nil {
		return Event{}, ErrNoRedisClient
	}
	if q.rel != nil {
		m, err := q.Receive(ctx)
		if err != nil {
			return Event{}, err
		}
		bctx, cancel := bookkeeping(ctx)
		defer cancel()
		if err := q.Ack(bctx, m); err != nil && !errors.Is(err, ErrLeaseExpired) {
			return Event{}, err
		}
		return m.Event, nil
	}
	result, err := q.client.BRPop(ctx, q.blockTimeout, q.key).Result()
	if errors.Is(err, redis.Nil) {
		return Event{}, ErrQueueEmpty
	}
	if err != nil {
		return Event{}, err
	}
	if len(result) < 2 {
		return Event{}, ErrMalformedEvent
	}
	var event Event
	if err := json.Unmarshal([]byte(result[1]), &event); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrMalformedEvent, err)
	}
	return event, nil
}

// QueueDispatcher publishes events to a persistent WebhookQueue (drained by
// WebhookDispatcher.StartWorker), falling back to direct asynchronous
// delivery when the queue is unavailable.
type QueueDispatcher struct {
	Queue    WebhookQueue
	Fallback *WebhookDispatcher
}

// DispatchAsync implements the dispatcher contract used by finops and others.
func (q *QueueDispatcher) DispatchAsync(ctx context.Context, event Event) {
	if q.Queue != nil {
		if err := q.Queue.Enqueue(context.WithoutCancel(ctx), event); err == nil {
			return
		}
		telemetry.RecordError()
	}
	if q.Fallback != nil {
		q.Fallback.DispatchAsync(ctx, event)
	}
}

// StartWorker begins a background worker that processes webhook events from the queue.
func (d *WebhookDispatcher) StartWorker(ctx context.Context, queue WebhookQueue) {
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, queue, &wg)
}

// StartWorkerWithWaitGroup begins a background worker and tracks it in the
// provided WaitGroup. The worker exits promptly when ctx is cancelled (or,
// for reliable queues, when the dispatcher is shut down), backs off
// exponentially (100ms..5s) on queue errors and never busy-loops.
//
// When queue is a reliable AckQueue (the Redis queue built over a full
// client), DispatcherOptions.QueueConsumers goroutines lease messages,
// deliver them synchronously to every registered target and only then
// acknowledge them; failures are retried by the queue with backoff and
// eventually dead-lettered, and deliveries interrupted by ctx cancellation
// or Shutdown are released back to the queue. Otherwise events are popped
// and handed to the in-process delivery pool (at most once).
func (d *WebhookDispatcher) StartWorkerWithWaitGroup(ctx context.Context, queue WebhookQueue, wg *sync.WaitGroup) {
	if aq, ok := reliableQueue(queue); ok {
		for i := 0; i < d.opts.QueueConsumers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d.consume(ctx, aq)
			}()
		}
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		backoff := minQueueBackoff
		for {
			if ctx.Err() != nil {
				return
			}
			event, err := queue.Dequeue(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				switch {
				case errors.Is(err, ErrMalformedEvent):
					telemetry.RecordError()
					continue
				case errors.Is(err, ErrQueueEmpty):
					backoff = minQueueBackoff
					if !sleepCtx(ctx, queueEmptyWait) {
						return
					}
				default:
					if !sleepCtx(ctx, backoff) {
						return
					}
					backoff = min(backoff*2, maxQueueBackoff)
				}
				continue
			}
			backoff = minQueueBackoff
			event = normalizeEvent(event)
			for _, cfg := range d.targets(event.Type) {
				// Block (bounded by ctx) rather than drop: the event came
				// from a durable queue and has already been consumed.
				_ = d.enqueue(ctx, deliveryJob{ctx: ctx, cfg: cfg, event: event}, true)
			}
		}
	}()
}

const (
	queueEmptyWait  = 50 * time.Millisecond
	minQueueBackoff = 100 * time.Millisecond
	maxQueueBackoff = 5 * time.Second
)

func sleepCtx(ctx context.Context, dur time.Duration) bool {
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// reliableQueue reports whether queue supports at-least-once consumption.
func reliableQueue(queue WebhookQueue) (AckQueue, bool) {
	aq, ok := queue.(AckQueue)
	if !ok {
		return nil, false
	}
	if r, ok := queue.(interface{ Reliable() bool }); ok && !r.Reliable() {
		return nil, false
	}
	return aq, true
}

// consume is the at-least-once consumer loop. It stops when ctx is done or
// the dispatcher is shut down.
func (d *WebhookDispatcher) consume(ctx context.Context, q AckQueue) {
	backoff := minQueueBackoff
	for ctx.Err() == nil && d.lifeCtx.Err() == nil {
		m, err := q.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			switch {
			case errors.Is(err, ErrMalformedEvent), errors.Is(err, ErrDeadLettered):
				continue
			case errors.Is(err, ErrQueueEmpty):
				backoff = minQueueBackoff
				if !sleepCtx(ctx, queueEmptyWait) {
					return
				}
			default:
				telemetry.RecordError()
				if !sleepCtx(ctx, backoff) {
					return
				}
				backoff = min(backoff*2, maxQueueBackoff)
			}
			continue
		}
		backoff = minQueueBackoff
		d.deliverLeased(ctx, q, m)
	}
}

// deliverLeased delivers one leased message to every target registered for
// its type and settles it: Ack on success, Release when interrupted by
// shutdown, Nack otherwise (permanent when every failure was permanent).
func (d *WebhookDispatcher) deliverLeased(ctx context.Context, q AckQueue, m *QueueMessage) {
	event := normalizeEvent(m.Event)
	settle := func(fn func(context.Context) error) {
		sctx, cancel := bookkeeping(ctx)
		defer cancel()
		if err := fn(sctx); err != nil && !errors.Is(err, ErrLeaseExpired) {
			telemetry.RecordError()
		}
	}
	targets := d.targets(event.Type)
	if len(targets) == 0 {
		settle(func(c context.Context) error { return q.Ack(c, m) })
		return
	}

	dctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(d.lifeCtx, cancel)
	defer stop()
	if !m.Deadline.IsZero() {
		var cancelDeadline context.CancelFunc
		dctx, cancelDeadline = context.WithDeadline(dctx, m.Deadline)
		defer cancelDeadline()
	}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, cfg := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = d.sendWithRetry(dctx, cfg, event)
		}()
	}
	wg.Wait()

	var failed []error
	permanent := true
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err)
			permanent = permanent && IsPermanent(err)
		}
	}
	switch {
	case len(failed) == 0:
		settle(func(c context.Context) error { return q.Ack(c, m) })
	case ctx.Err() != nil || d.lifeCtx.Err() != nil:
		settle(func(c context.Context) error { return q.Release(c, m) })
	default:
		cause := errors.Join(failed...)
		if permanent {
			cause = MarkPermanent(cause)
		}
		settle(func(c context.Context) error { return q.Nack(c, m, cause) })
	}
}
