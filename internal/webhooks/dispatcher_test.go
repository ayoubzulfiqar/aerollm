package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

func TestWebhookDispatcherRegisterAndDispatch(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Errorf("decode error: %v", err)
		}
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "s",
		Timeout: 2 * time.Second,
	})

	evt := Event{ID: "1", Type: EventBudgetExceeded, Timestamp: time.Now(), Payload: map[string]interface{}{"api_key_hint": "sk-...1"}}
	d.DispatchAsync(context.Background(), evt)

	select {
	case got := <-received:
		if got.Type != EventBudgetExceeded {
			t.Fatalf("unexpected event type: %s", got.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook dispatch timed out")
	}
}

func TestWebhookDispatcherError(t *testing.T) {
	before := telemetry.ErrorCount()
	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventShadowTestCompleted, WebhookConfig{URL: "http://127.0.0.1:1", Timeout: 100 * time.Millisecond})
	d.DispatchAsync(context.Background(), Event{ID: "1", Type: EventShadowTestCompleted})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if telemetry.ErrorCount() == before {
		t.Fatal("expected error count to increase")
	}
	if d.Stats().Failed != 1 {
		t.Fatalf("expected 1 failed delivery, got %+v", d.Stats())
	}
}

func TestWebhookDispatcherRetrySuccessAfterFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:        server.URL,
		Secret:     "",
		Timeout:    2 * time.Second,
		Retries:    3,
		RetryDelay: 50 * time.Millisecond,
	})

	d.DispatchAsync(context.Background(), Event{ID: "retry-1", Type: EventBudgetExceeded, Timestamp: time.Now()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
	if d.Stats().Delivered != 1 {
		t.Fatalf("expected 1 delivered, got %+v", d.Stats())
	}
}

func TestWebhookDispatcherNoRetryOnClientError(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	err := d.sendWithRetry(context.Background(), WebhookConfig{URL: server.URL, Retries: 5, RetryDelay: time.Millisecond}, Event{ID: "x", Type: EventBudgetExceeded})
	if err == nil {
		t.Fatal("expected error for 400")
	}
	if attempts.Load() != 1 {
		t.Fatalf("4xx must not be retried, got %d attempts", attempts.Load())
	}
}

func TestWebhookDispatcherBackoffHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := d.sendWithRetry(ctx, WebhookConfig{URL: server.URL, Retries: 10, RetryDelay: 5 * time.Second}, Event{ID: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("backoff ignored context cancellation (%s)", time.Since(start))
	}
}

func TestWebhookDeliveryIsSignedAndVerifiable(t *testing.T) {
	const secret = "top-secret"
	type captured struct {
		body   []byte
		header http.Header
	}
	got := make(chan captured, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- captured{body: b, header: r.Header.Clone()}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: server.URL, Secret: secret})
	d.DispatchAsync(context.Background(), Event{ID: "sig-1", Type: EventBudgetExceeded})

	var c captured
	select {
	case c = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery")
	}
	if c.header.Get("X-Webhook-Secret") != "" {
		t.Fatal("raw secret must never be sent")
	}
	if c.header.Get(DeliveryHeader) != "sig-1" || c.header.Get(EventHeader) != string(EventBudgetExceeded) {
		t.Fatalf("missing delivery headers: %v", c.header)
	}
	if err := Verify(secret, c.header.Get(SignatureHeader), c.header.Get(TimestampHeader), c.body, time.Minute, time.Now()); err != nil {
		t.Fatalf("signature did not verify: %v", err)
	}
	if err := Verify("wrong", c.header.Get(SignatureHeader), c.header.Get(TimestampHeader), c.body, time.Minute, time.Now()); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature with wrong secret, got %v", err)
	}
	tampered := append([]byte{}, c.body...)
	tampered[0] = ' '
	if err := Verify(secret, c.header.Get(SignatureHeader), c.header.Get(TimestampHeader), tampered, time.Minute, time.Now()); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("expected invalid signature for tampered body, got %v", err)
	}
}

func TestVerifyRejectsReplayAndMissing(t *testing.T) {
	body := []byte(`{"a":1}`)
	old := time.Now().Add(-time.Hour).Unix()
	sig := Sign("s", old, body)
	if err := Verify("s", sig, strconv.FormatInt(old, 10), body, 5*time.Minute, time.Now()); !errors.Is(err, ErrSignatureExpired) {
		t.Fatalf("expected expired, got %v", err)
	}
	if err := Verify("s", "", "", body, time.Minute, time.Now()); !errors.Is(err, ErrMissingSignature) {
		t.Fatalf("expected missing, got %v", err)
	}
	now := time.Now().Unix()
	// Rotation: second signature matches.
	multi := Sign("old", now, body) + "," + Sign("s", now, body)
	if err := Verify("s", multi, strconv.FormatInt(now, 10), body, time.Minute, time.Now()); err != nil {
		t.Fatalf("expected rotation signature to verify: %v", err)
	}
}

func TestVerifyRequest(t *testing.T) {
	body := []byte(`{"ok":true}`)
	req := httptest.NewRequest(http.MethodPost, "/hook", nil)
	SetSignatureHeaders(req.Header, "k", body, time.Now())
	req.Body = io.NopCloser(bytes.NewReader(body))
	got, err := VerifyRequest(req, "k", 0, 0)
	if err != nil || string(got) != string(body) {
		t.Fatalf("VerifyRequest failed: %v %q", err, got)
	}
	req2 := httptest.NewRequest(http.MethodPost, "/hook", nil)
	SetSignatureHeaders(req2.Header, "k", body, time.Now())
	req2.Body = io.NopCloser(bytes.NewReader(body))
	if _, err := VerifyRequest(req2, "k", 0, 3); err == nil {
		t.Fatal("expected body size limit error")
	}
}

func TestWebhookDispatcherBoundedQueueDrops(t *testing.T) {
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(block)

	d := NewWebhookDispatcherWithOptions(DispatcherOptions{Workers: 1, QueueSize: 2})
	d.Register(EventBudgetExceeded, WebhookConfig{URL: server.URL, Timeout: 5 * time.Second})
	for i := 0; i < 20; i++ {
		d.DispatchAsync(context.Background(), Event{ID: fmt.Sprint(i), Type: EventBudgetExceeded})
	}
	if d.Stats().Dropped == 0 {
		t.Fatalf("expected drops with a full queue, got %+v", d.Stats())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = d.Shutdown(ctx) // aborts in-flight deliveries
}

func TestWebhookDispatcherShutdownRejectsNewEvents(t *testing.T) {
	d := NewWebhookDispatcher()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: "http://127.0.0.1:1"})
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d.DispatchAsync(context.Background(), Event{Type: EventBudgetExceeded})
	if d.Stats().Dropped != 1 {
		t.Fatalf("expected event dropped after close, got %+v", d.Stats())
	}
}

func TestDispatchToAsyncAndInvalidURL(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	d := NewWebhookDispatcher()
	defer d.Close()
	d.DispatchToAsync(context.Background(), WebhookConfig{URL: server.URL}, Event{Type: EventBudgetThreshold})
	d.DispatchToAsync(context.Background(), WebhookConfig{URL: "file:///etc/passwd"}, Event{Type: EventBudgetThreshold})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = d.Flush(ctx)
	if hits.Load() != 1 {
		t.Fatalf("expected 1 hit, got %d", hits.Load())
	}
	if st := d.Stats(); st.Delivered != 1 || st.Failed != 1 {
		t.Fatalf("unexpected stats %+v", st)
	}
	if err := d.RegisterChecked(EventBudgetExceeded, WebhookConfig{URL: "gopher://x"}); err == nil {
		t.Fatal("expected RegisterChecked to reject non-http scheme")
	}
}

type fakeWebhookQueue struct {
	events []Event
	mu     sync.Mutex
	idx    int
	calls  atomic.Int32
}

func (f *fakeWebhookQueue) Enqueue(_ context.Context, event Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeWebhookQueue) Dequeue(_ context.Context) (Event, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.events) {
		return Event{}, fmt.Errorf("no events")
	}
	event := f.events[f.idx]
	f.idx++
	return event, nil
}

func TestWebhookDispatcherStartWorker(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		_ = json.NewDecoder(r.Body).Decode(&evt)
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "",
		Timeout: 2 * time.Second,
	})

	fq := &fakeWebhookQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.StartWorker(ctx, fq)
	_ = fq.Enqueue(ctx, Event{ID: "q-1", Type: EventBudgetExceeded, Timestamp: time.Now()})

	select {
	case got := <-received:
		if got.ID != "q-1" {
			t.Fatalf("expected event ID q-1, got %s", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not dispatch queued event")
	}
}

func TestWebhookDispatcherStartWorkerCancelStops(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		_ = json.NewDecoder(r.Body).Decode(&evt)
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "",
		Timeout: 2 * time.Second,
	})

	fq := &fakeWebhookQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, fq, &wg)
	_ = fq.Enqueue(ctx, Event{ID: "q-2", Type: EventBudgetExceeded, Timestamp: time.Now()})

	select {
	case got := <-received:
		if got.ID != "q-2" {
			t.Fatalf("unexpected event ID: %s", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not dispatch queued event")
	}
	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

func TestWorkerDoesNotBusyLoopOnErrors(t *testing.T) {
	d := NewWebhookDispatcher()
	defer d.Close()
	fq := &fakeWebhookQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, fq, &wg)
	time.Sleep(400 * time.Millisecond)
	cancel()
	wg.Wait()
	if calls := fq.calls.Load(); calls > 10 {
		t.Fatalf("worker busy-looped: %d dequeue calls in 400ms", calls)
	}
}

// fakeRedisList implements RedisListClient in memory.
type fakeRedisList struct {
	mu    sync.Mutex
	items []string
}

func (f *fakeRedisList) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range values {
		switch b := v.(type) {
		case []byte:
			f.items = append([]string{string(b)}, f.items...)
		case string:
			f.items = append([]string{b}, f.items...)
		}
	}
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(f.items)))
	return cmd
}

func (f *fakeRedisList) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.items) == 0 {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	last := f.items[len(f.items)-1]
	f.items = f.items[:len(f.items)-1]
	cmd.SetVal([]string{keys[0], last})
	return cmd
}

func TestRedisWebhookQueueRoundTrip(t *testing.T) {
	fr := &fakeRedisList{}
	q := NewRedisWebhookQueueWithClient(fr, "")
	ctx := context.Background()
	if _, err := q.Dequeue(ctx); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}
	if err := q.Enqueue(ctx, Event{Type: EventBudgetExceeded}); err != nil {
		t.Fatal(err)
	}
	ev, err := q.Dequeue(ctx)
	if err != nil || ev.Type != EventBudgetExceeded || ev.ID == "" {
		t.Fatalf("unexpected dequeue: %+v %v", ev, err)
	}
	fr.items = []string{"not json"}
	if _, err := q.Dequeue(ctx); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("expected ErrMalformedEvent, got %v", err)
	}
	nilQ := NewRedisWebhookQueue(nil, "k")
	if err := nilQ.Enqueue(ctx, Event{}); !errors.Is(err, ErrNoRedisClient) {
		t.Fatalf("expected ErrNoRedisClient, got %v", err)
	}
}

func TestQueueDispatcherFallsBack(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	d := NewWebhookDispatcher()
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: server.URL})
	qd := &QueueDispatcher{Queue: NewRedisWebhookQueue(nil, "k"), Fallback: d}
	qd.DispatchAsync(context.Background(), Event{Type: EventBudgetExceeded})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = d.Flush(ctx)
	if hits.Load() != 1 {
		t.Fatalf("expected fallback delivery, got %d", hits.Load())
	}

	fr := &fakeRedisList{}
	qd2 := &QueueDispatcher{Queue: NewRedisWebhookQueueWithClient(fr, "k"), Fallback: d}
	qd2.DispatchAsync(context.Background(), Event{Type: EventBudgetExceeded})
	if len(fr.items) != 1 {
		t.Fatalf("expected event persisted to queue")
	}
}

func TestBackoffDelayBounds(t *testing.T) {
	for attempt := 1; attempt < 40; attempt++ {
		d := backoffDelay(100*time.Millisecond, attempt, time.Second)
		if d < 0 || d > time.Second {
			t.Fatalf("attempt %d: delay %s out of bounds", attempt, d)
		}
	}
}
