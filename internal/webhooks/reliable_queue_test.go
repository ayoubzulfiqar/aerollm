package webhooks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedis returns a client for AEROLLM_TEST_REDIS_ADDR or skips.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// testQueueKey returns a unique queue key and removes every key of the
// queue (main list and "{key}:*" auxiliaries) when the test ends.
func testQueueKey(t *testing.T, c *redis.Client) string {
	t.Helper()
	key := "aerollm-test:webhooks:" + randomHex(8)
	t.Cleanup(func() {
		ctx := context.Background()
		keys := []string{key}
		iter := c.Scan(ctx, 0, keyTag(key)+":*", 100).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		c.Del(ctx, keys...)
	})
	return key
}

func newTestQueue(c *redis.Client, key, consumer string, opts ReliableQueueOptions) *RedisWebhookQueue {
	opts.ConsumerID = consumer
	if opts.BlockTimeout == 0 {
		opts.BlockTimeout = 100 * time.Millisecond
	}
	return NewReliableRedisQueue(c, key, opts)
}

// receive polls until a message (or a non-empty error) arrives.
func receive(t *testing.T, q *RedisWebhookQueue, within time.Duration) (*QueueMessage, error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		m, err := q.Receive(context.Background())
		if !errors.Is(err, ErrQueueEmpty) || time.Now().After(deadline) {
			return m, err
		}
	}
}

func mustStats(t *testing.T, q *RedisWebhookQueue) QueueStats {
	t.Helper()
	st, err := q.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestReliableQueueAckRoundTripFIFO(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	q := newTestQueue(c, key, "a", ReliableQueueOptions{})
	ctx := context.Background()
	if !q.Reliable() {
		t.Fatal("queue over *redis.Client must be reliable")
	}
	for _, id := range []string{"e1", "e2", "e3"} {
		if err := q.Enqueue(ctx, Event{ID: id, Type: EventBudgetExceeded, Payload: map[string]interface{}{"n": id}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"e1", "e2", "e3"} {
		m, err := receive(t, q, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if m.Event.ID != want || m.Attempt != 1 || m.ID == "" || m.Deadline.IsZero() {
			t.Fatalf("unexpected message %+v (want %s)", m, want)
		}
		if st := mustStats(t, q); st.InFlight != 1 {
			t.Fatalf("leased message must be in flight: %+v", st)
		}
		if err := q.Ack(ctx, m); err != nil {
			t.Fatal(err)
		}
		if err := q.Ack(ctx, m); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("double ack must report ErrLeaseExpired, got %v", err)
		}
	}
	if st := mustStats(t, q); st != (QueueStats{}) {
		t.Fatalf("queue must be empty: %+v", st)
	}
	if n, _ := c.HLen(ctx, q.rel.attempts).Result(); n != 0 {
		t.Fatalf("attempt counters leaked: %d", n)
	}
	if _, err := q.Receive(ctx); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}
}

// A consumer that leases a message and dies without acknowledging it must
// not lose the event: after the visibility timeout another consumer (a new
// process with a new consumer ID) recovers and delivers it.
func TestReliableQueueCrashRecovery(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	opts := ReliableQueueOptions{VisibilityTimeout: 300 * time.Millisecond, MaintenanceInterval: time.Hour}
	crashed := newTestQueue(c, key, "crashed", opts)
	if err := crashed.Enqueue(ctx, Event{ID: "evt-crash", Type: EventBudgetExceeded}); err != nil {
		t.Fatal(err)
	}
	m, err := receive(t, crashed, 3*time.Second)
	if err != nil || m.Event.ID != "evt-crash" {
		t.Fatalf("receive: %+v %v", m, err)
	}
	// "Crash": the consumer disappears without Ack.

	restarted := newTestQueue(c, key, "restarted", opts)
	if _, err := restarted.Receive(ctx); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("leased message must stay invisible before the timeout, got %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	res, err := restarted.Maintain(ctx)
	if err != nil || res.Recovered != 1 {
		t.Fatalf("expected 1 recovered message, got %+v %v", res, err)
	}
	m2, err := receive(t, restarted, 3*time.Second)
	if err != nil || m2.Event.ID != "evt-crash" || m2.Attempt != 2 || m2.ID != m.ID {
		t.Fatalf("recovered message: %+v %v", m2, err)
	}
	if err := restarted.Ack(ctx, m2); err != nil {
		t.Fatal(err)
	}
	// The zombie consumer's late ack must not touch the redelivered copy.
	if err := crashed.Ack(ctx, m); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late ack must report ErrLeaseExpired, got %v", err)
	}
	if st := mustStats(t, restarted); st != (QueueStats{}) {
		t.Fatalf("queue must be empty: %+v", st)
	}
}

// Crash between BLMOVE and the lease record: the element sits in the dead
// consumer's processing list without a deadline. Maintenance gives it one
// and re-queues it after the visibility timeout.
func TestReliableQueueCrashBeforeClaim(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	opts := ReliableQueueOptions{VisibilityTimeout: 200 * time.Millisecond, MaintenanceInterval: time.Hour}
	dead := newTestQueue(c, key, "dead", opts)
	if err := dead.Enqueue(ctx, Event{ID: "evt-unclaimed", Type: EventBudgetExceeded}); err != nil {
		t.Fatal(err)
	}
	if err := c.LMove(ctx, key, dead.rel.processing, "RIGHT", "LEFT").Err(); err != nil {
		t.Fatal(err)
	}
	c.SAdd(ctx, dead.rel.consumers, "dead")

	other := newTestQueue(c, key, "other", opts)
	if res, err := other.Maintain(ctx); err != nil || res.Recovered != 0 {
		t.Fatalf("untracked element must first get a lease: %+v %v", res, err)
	}
	time.Sleep(300 * time.Millisecond)
	res, err := other.Maintain(ctx)
	if err != nil || res.Recovered != 1 {
		t.Fatalf("expected recovery after the timeout: %+v %v", res, err)
	}
	pruned := res.Pruned
	m, err := receive(t, other, 2*time.Second)
	if err != nil || m.Event.ID != "evt-unclaimed" || m.Attempt != 1 {
		t.Fatalf("recovered: %+v %v", m, err)
	}
	if err := other.Ack(ctx, m); err != nil {
		t.Fatal(err)
	}
	// The dead consumer (no heartbeat, empty list) is forgotten; the live
	// one stays registered.
	res, err = other.Maintain(ctx)
	if err != nil || pruned+res.Pruned != 1 {
		t.Fatalf("dead consumer must be pruned once: %d+%+v %v", pruned, res, err)
	}
	members, _ := c.SMembers(ctx, dead.rel.consumers).Result()
	if len(members) != 1 || members[0] != "other" {
		t.Fatalf("unexpected consumer registry %v", members)
	}
}

func TestReliableQueueNackRetryDeadLetterRedrive(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{
		MaxAttempts: 3, RetryBaseDelay: 30 * time.Millisecond, RetryMaxDelay: 60 * time.Millisecond,
		MaintenanceInterval: 10 * time.Millisecond,
	})
	if err := q.Enqueue(ctx, Event{ID: "evt-retry", Type: EventBudgetThreshold}); err != nil {
		t.Fatal(err)
	}
	transient := errors.New("endpoint returned 503")
	for attempt := 1; attempt <= 3; attempt++ {
		m, err := receive(t, q, 2*time.Second)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if m.Attempt != attempt {
			t.Fatalf("expected attempt %d, got %d", attempt, m.Attempt)
		}
		if err := q.Nack(ctx, m, transient); err != nil {
			t.Fatal(err)
		}
		st := mustStats(t, q)
		if attempt < 3 && st.Delayed != 1 {
			t.Fatalf("nacked message must be delayed: %+v", st)
		}
	}
	st := mustStats(t, q)
	if st.DeadLetters != 1 || st.Ready != 0 || st.Delayed != 0 || st.InFlight != 0 {
		t.Fatalf("message must be dead-lettered after 3 attempts: %+v", st)
	}
	dls, err := q.DeadLetters(ctx, 10)
	if err != nil || len(dls) != 1 {
		t.Fatalf("dead letters: %+v %v", dls, err)
	}
	if dl := dls[0]; dl.Event == nil || dl.Event.ID != "evt-retry" || dl.Attempts != 3 || !dl.Redrivable || dl.DeadAt.IsZero() {
		t.Fatalf("unexpected dead letter %+v", dl)
	}
	if n, err := q.Redrive(ctx, 0); err != nil || n != 1 {
		t.Fatalf("redrive: %d %v", n, err)
	}
	m, err := receive(t, q, 3*time.Second)
	if err != nil || m.Event.ID != "evt-retry" || m.Attempt != 1 {
		t.Fatalf("redriven message must restart its attempt budget: %+v %v", m, err)
	}
	if err := q.Ack(ctx, m); err != nil {
		t.Fatal(err)
	}
}

func TestReliableQueuePermanentFailureDeadLettersImmediately(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{})
	_ = q.Enqueue(ctx, Event{ID: "evt-perm", Type: EventBudgetExceeded})
	m, err := receive(t, q, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, m, MarkPermanent(errors.New("410 gone"))); err != nil {
		t.Fatal(err)
	}
	st := mustStats(t, q)
	if st.DeadLetters != 1 || st.Delayed != 0 {
		t.Fatalf("permanent failure must dead-letter: %+v", st)
	}
	dls, _ := q.DeadLetters(ctx, 1)
	if len(dls) != 1 || dls[0].Attempts != 1 {
		t.Fatalf("unexpected dead letters %+v", dls)
	}
}

func TestReliableQueueMalformedMessage(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{})
	c.LPush(ctx, key, "not json")
	if _, err := receive(t, q, 3*time.Second); !errors.Is(err, ErrMalformedEvent) {
		t.Fatalf("expected ErrMalformedEvent, got %v", err)
	}
	dls, _ := q.DeadLetters(ctx, 10)
	if len(dls) != 1 || dls[0].Redrivable || dls[0].Message != "not json" {
		t.Fatalf("malformed message must be kept, not redrivable: %+v", dls)
	}
	if n, err := q.Redrive(ctx, 0); err != nil || n != 0 {
		t.Fatalf("malformed messages must not be redriven: %d %v", n, err)
	}
	if st := mustStats(t, q); st.DeadLetters != 1 || st.Ready != 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
}

func TestReliableQueueReleaseDoesNotCountAttempt(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{})
	_ = q.Enqueue(ctx, Event{ID: "first", Type: EventBudgetExceeded})
	_ = q.Enqueue(ctx, Event{ID: "second", Type: EventBudgetExceeded})
	m, err := receive(t, q, 3*time.Second)
	if err != nil || m.Event.ID != "first" {
		t.Fatalf("receive: %+v %v", m, err)
	}
	if err := q.Release(ctx, m); err != nil {
		t.Fatal(err)
	}
	again, err := receive(t, q, 3*time.Second)
	if err != nil || again.Event.ID != "first" || again.Attempt != 1 {
		t.Fatalf("released message must be next, attempt unchanged: %+v %v", again, err)
	}
	_ = q.Ack(ctx, again)
}

// A message whose consumers keep crashing is dead-lettered once its lease
// count exceeds MaxAttempts instead of crash-looping forever.
func TestReliableQueueCrashLoopIsDeadLettered(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{MaxAttempts: 2, VisibilityTimeout: 100 * time.Millisecond, MaintenanceInterval: time.Hour})
	_ = q.Enqueue(ctx, Event{ID: "poison", Type: EventBudgetExceeded})
	for i := 1; i <= 2; i++ {
		m, err := receive(t, q, 3*time.Second)
		if err != nil || m.Attempt != i {
			t.Fatalf("lease %d: %+v %v", i, m, err)
		}
		time.Sleep(150 * time.Millisecond)
		if res, err := q.Maintain(ctx); err != nil || res.Recovered != 1 {
			t.Fatalf("recover %d: %+v %v", i, res, err)
		}
	}
	if _, err := receive(t, q, 3*time.Second); !errors.Is(err, ErrDeadLettered) {
		t.Fatalf("expected ErrDeadLettered, got %v", err)
	}
	if st := mustStats(t, q); st.DeadLetters != 1 || st.InFlight != 0 || st.Ready != 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
}

// Bare events written by older versions are still consumed, and envelopes
// remain decodable as plain events by older consumers.
func TestReliableQueueWireCompatibility(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	ctx := context.Background()
	q := newTestQueue(c, key, "a", ReliableQueueOptions{})
	legacy, _ := json.Marshal(Event{ID: "legacy-1", Type: EventShadowTestCompleted})
	c.LPush(ctx, key, legacy)
	m, err := receive(t, q, 3*time.Second)
	if err != nil || m.Event.ID != "legacy-1" || m.Event.Type != EventShadowTestCompleted || m.ID == "" {
		t.Fatalf("legacy message: %+v %v", m, err)
	}
	if err := q.Ack(ctx, m); err != nil {
		t.Fatal(err)
	}

	_ = q.Enqueue(ctx, Event{ID: "new-1", Type: EventBudgetExceeded, Payload: map[string]interface{}{"k": "v"}})
	raw, err := c.LIndex(ctx, key, 0).Result()
	if err != nil {
		t.Fatal(err)
	}
	var old Event
	if err := json.Unmarshal([]byte(raw), &old); err != nil || old.ID != "new-1" || old.Type != EventBudgetExceeded || old.Payload["k"] != "v" {
		t.Fatalf("envelope must decode as a bare event: %+v %v", old, err)
	}
	// Dequeue (at-most-once API) still works in reliable mode.
	ev, err := q.Dequeue(ctx)
	if err != nil || ev.ID != "new-1" {
		t.Fatalf("dequeue: %+v %v", ev, err)
	}
	if st := mustStats(t, q); st != (QueueStats{}) {
		t.Fatalf("dequeue must acknowledge: %+v", st)
	}
}

func TestDispatcherReliableWorkerRetriesUntilDelivered(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	var hits atomic.Int32
	delivered := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		delivered <- r.Header.Get(DeliveryHeader)
	}))
	defer srv.Close()

	d := NewWebhookDispatcherWithOptions(DispatcherOptions{QueueConsumers: 2})
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: srv.URL, Timeout: time.Second, Retries: 1})
	q := newTestQueue(c, key, "worker", ReliableQueueOptions{RetryBaseDelay: 20 * time.Millisecond, RetryMaxDelay: 40 * time.Millisecond, MaintenanceInterval: 10 * time.Millisecond})
	qd := &QueueDispatcher{Queue: q, Fallback: d}
	qd.DispatchAsync(context.Background(), Event{ID: "evt-e2e", Type: EventBudgetExceeded})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, q, &wg)
	select {
	case id := <-delivered:
		if id != "evt-e2e" {
			t.Fatalf("unexpected delivery id %q", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not delivered after a transient failure")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st := mustStats(t, q)
		if st == (QueueStats{}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered message was not acknowledged: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumers did not stop after cancel")
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 delivery attempts, got %d", hits.Load())
	}
}

// Stopping the worker mid-delivery releases the message (no lost event, no
// attempt charged).
func TestDispatcherReliableWorkerReleasesOnCancel(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body so the server notices the client going away.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer srv.Close()

	d := NewWebhookDispatcherWithOptions(DispatcherOptions{QueueConsumers: 1})
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: srv.URL, Timeout: 30 * time.Second})
	q := newTestQueue(c, key, "worker", ReliableQueueOptions{})
	_ = q.Enqueue(context.Background(), Event{ID: "evt-cancel", Type: EventBudgetExceeded})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, q, &wg)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery did not start")
	}
	cancel()
	wg.Wait()
	st := mustStats(t, q)
	if st.Ready != 1 || st.InFlight != 0 || st.Delayed != 0 || st.DeadLetters != 0 {
		t.Fatalf("interrupted delivery must be released: %+v", st)
	}
	m, err := receive(t, newTestQueue(c, key, "next", ReliableQueueOptions{}), 3*time.Second)
	if err != nil || m.Event.ID != "evt-cancel" || m.Attempt != 1 {
		t.Fatalf("released message: %+v %v", m, err)
	}
}

func TestDispatcherReliableWorkerDeadLettersPermanentFailures(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	d := NewWebhookDispatcherWithOptions(DispatcherOptions{QueueConsumers: 1})
	defer d.Close()
	d.Register(EventBudgetExceeded, WebhookConfig{URL: srv.URL, Timeout: time.Second, Retries: 3, RetryDelay: time.Millisecond})
	q := newTestQueue(c, key, "worker", ReliableQueueOptions{})
	_ = q.Enqueue(context.Background(), Event{ID: "evt-400", Type: EventBudgetExceeded})
	// Events without registered targets are acknowledged and dropped.
	_ = q.Enqueue(context.Background(), Event{ID: "evt-untargeted", Type: EventShadowTestCompleted})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, q, &wg)
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := mustStats(t, q)
		if st.DeadLetters == 1 && st.Ready == 0 && st.InFlight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("permanent failure was not dead-lettered: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if hits.Load() != 1 {
		t.Fatalf("4xx must not be retried, got %d attempts", hits.Load())
	}
}

func TestKeyTagSharesHashSlot(t *testing.T) {
	cases := map[string]string{
		"webhook:queue":   "{webhook:queue}",
		"{wh}:queue":      "{wh}:queue",
		"odd{":            "{odd{}",
		"x{}":             "x{}",
		"prefix:{tag}:ev": "prefix:{tag}:ev",
	}
	for in, want := range cases {
		if got := keyTag(in); got != want {
			t.Errorf("keyTag(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReliableQueueWithoutClient(t *testing.T) {
	q := NewReliableRedisQueue(nil, "", ReliableQueueOptions{})
	ctx := context.Background()
	if _, err := q.Receive(ctx); !errors.Is(err, ErrNoRedisClient) {
		t.Fatalf("expected ErrNoRedisClient, got %v", err)
	}
	if err := q.Enqueue(ctx, Event{}); !errors.Is(err, ErrNoRedisClient) {
		t.Fatalf("expected ErrNoRedisClient, got %v", err)
	}
	legacy := NewRedisWebhookQueueWithClient(&fakeRedisList{}, "k")
	if legacy.Reliable() {
		t.Fatal("minimal clients must use the legacy queue")
	}
	if _, err := legacy.Receive(ctx); !errors.Is(err, ErrReliableUnsupported) {
		t.Fatalf("expected ErrReliableUnsupported, got %v", err)
	}
	if err := (&RedisWebhookQueue{}).Ack(ctx, &QueueMessage{}); !errors.Is(err, ErrNoRedisClient) {
		t.Fatalf("expected ErrNoRedisClient, got %v", err)
	}
	if !IsPermanent(MarkPermanent(errors.New("x"))) || IsPermanent(errors.New("x")) || MarkPermanent(nil) != nil {
		t.Fatal("MarkPermanent/IsPermanent mismatch")
	}
}

func TestDispatcherShutdownStopsReliableConsumers(t *testing.T) {
	c := testRedis(t)
	key := testQueueKey(t, c)
	d := NewWebhookDispatcherWithOptions(DispatcherOptions{QueueConsumers: 2})
	q := newTestQueue(c, key, "worker", ReliableQueueOptions{})
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(context.Background(), q, &wg)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumers must stop when the dispatcher shuts down")
	}
}
