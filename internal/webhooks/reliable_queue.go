package webhooks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

// AckQueue is a WebhookQueue with at-least-once delivery: a received
// message stays leased (invisible to other consumers) until it is
// acknowledged, negatively acknowledged or released. A consumer that dies
// while holding a lease loses it after the queue's visibility timeout and
// the message is delivered again, so receivers must de-duplicate on the
// event ID (sent as the X-AeroLLM-Delivery header).
type AckQueue interface {
	WebhookQueue
	// Receive leases the next message, blocking for a bounded time. It
	// returns ErrQueueEmpty when nothing arrived, ErrMalformedEvent or
	// ErrDeadLettered when the popped message was dead-lettered instead of
	// being returned.
	Receive(ctx context.Context) (*QueueMessage, error)
	// Ack removes a delivered message for good.
	Ack(ctx context.Context, m *QueueMessage) error
	// Nack reports a failed delivery: the message is retried later with
	// exponential backoff, or dead-lettered when cause is permanent (see
	// MarkPermanent) or the attempt budget is exhausted.
	Nack(ctx context.Context, m *QueueMessage, cause error) error
	// Release returns a message to the front of the queue without counting
	// the attempt (used when a consumer stops mid-delivery).
	Release(ctx context.Context, m *QueueMessage) error
}

// QueueMessage is a message leased from an AckQueue.
type QueueMessage struct {
	Event Event
	// ID identifies this queue entry (distinct from Event.ID, which may be
	// shared by several enqueues of the same logical event).
	ID string
	// Attempt is the 1-based number of times the message has been leased.
	Attempt int
	// Deadline is the local time by which the consumer should settle the
	// message; after it the lease may expire and the message be redelivered.
	Deadline time.Time

	raw string
}

var (
	// ErrDeadLettered is returned by Receive when the popped message was
	// moved to the dead-letter list (e.g. it exceeded its attempt budget
	// because consumers kept crashing while delivering it).
	ErrDeadLettered = errors.New("webhooks: message moved to the dead-letter list")
	// ErrLeaseExpired is returned by Ack/Nack/Release when the message is no
	// longer leased by this consumer (its visibility timeout elapsed and it
	// was recovered for redelivery).
	ErrLeaseExpired = errors.New("webhooks: message lease expired")
	// ErrReliableUnsupported is returned by Receive when the queue was built
	// over a client lacking the commands needed for reliable delivery.
	ErrReliableUnsupported = errors.New("webhooks: redis client does not support reliable queue operations")
)

// MarkPermanent wraps err so that AckQueue.Nack dead-letters the message
// immediately instead of retrying it.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err (or an error it wraps) is permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// RedisQueueClient is the set of Redis commands used by the reliable queue.
// *redis.Client, *redis.ClusterClient and redis.UniversalClient implement it.
type RedisQueueClient interface {
	RedisListClient
	redis.Scripter
	BLMove(ctx context.Context, source, destination, srcpos, destpos string, timeout time.Duration) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
}

// ReliableQueueOptions tunes a reliable Redis webhook queue. Zero values
// select the defaults.
type ReliableQueueOptions struct {
	// ConsumerID names this consumer's processing list. It defaults to
	// "<hostname>-<pid>-<random>"; it does not need to survive restarts
	// (orphaned processing lists of dead consumers are recovered).
	ConsumerID string
	// VisibilityTimeout is how long a leased message stays invisible before
	// it is considered abandoned and re-queued (default 5m). Deliveries are
	// bounded to ~90% of it, so live consumers settle in time.
	VisibilityTimeout time.Duration
	// MaxAttempts bounds how many times a message is leased before it is
	// dead-lettered (default 10).
	MaxAttempts int
	// BlockTimeout bounds how long Receive blocks (default 2s, rounded to
	// whole seconds, minimum 1s); it is also the worst-case latency for
	// observing context cancellation.
	BlockTimeout time.Duration
	// RetryBaseDelay / RetryMaxDelay shape the exponential backoff applied
	// to negatively acknowledged messages (defaults 5s / 10m).
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// MaxDeadLetters caps the dead-letter list; the oldest dead letters are
	// trimmed beyond it (default 10,000).
	MaxDeadLetters int
	// MaintenanceInterval is how often Receive recovers expired leases,
	// promotes due retries and refreshes the consumer heartbeat
	// (default 5s). Maintenance also runs on the first Receive (startup).
	MaintenanceInterval time.Duration
}

func (o ReliableQueueOptions) withDefaults() ReliableQueueOptions {
	if o.ConsumerID == "" {
		o.ConsumerID = defaultConsumerID()
	}
	o.ConsumerID = sanitizeConsumerID(o.ConsumerID)
	if o.VisibilityTimeout <= 0 {
		o.VisibilityTimeout = 5 * time.Minute
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	if o.BlockTimeout <= 0 {
		o.BlockTimeout = 2 * time.Second
	}
	// Redis clients express blocking timeouts in whole seconds.
	o.BlockTimeout = max(o.BlockTimeout.Round(time.Second), time.Second)
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = 5 * time.Second
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = 10 * time.Minute
	}
	if o.RetryMaxDelay < o.RetryBaseDelay {
		o.RetryMaxDelay = o.RetryBaseDelay
	}
	if o.MaxDeadLetters <= 0 {
		o.MaxDeadLetters = 10_000
	}
	if o.MaintenanceInterval <= 0 {
		o.MaintenanceInterval = 5 * time.Second
	}
	return o
}

var consumerIDRe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func sanitizeConsumerID(s string) string {
	s = consumerIDRe.ReplaceAllString(s, "_")
	if len(s) > 128 {
		s = s[:128]
	}
	return s
}

func defaultConsumerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "aerollm"
	}
	return host + "-" + strconv.Itoa(os.Getpid()) + "-" + randomHex(4)
}

// queueEnvelope is the stored form of a queued event: the event's own JSON
// fields plus queue metadata, so consumers of older versions (which decode
// a bare Event) keep working during rolling upgrades. The per-enqueue ID
// makes every list element unique, so LREM removes exactly the leased one.
type queueEnvelope struct {
	Event
	Queue *queueMeta `json:"aerollm_queue,omitempty"`
}

type queueMeta struct {
	V          int       `json:"v"`
	ID         string    `json:"id"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

const envelopeV = 1

func newEnvelope(event Event) queueEnvelope {
	return queueEnvelope{
		Event: normalizeEvent(event),
		Queue: &queueMeta{V: envelopeV, ID: "q_" + randomHex(16), EnqueuedAt: time.Now().UTC()},
	}
}

// id returns the queue entry ID; bare events written by older versions get
// an ID derived from their payload.
func (e queueEnvelope) id(raw string) string {
	if e.Queue != nil && e.Queue.ID != "" {
		return e.Queue.ID
	}
	return rawID(raw)
}

// decodeQueued decodes an envelope or a bare event (older versions).
func decodeQueued(raw string) (queueEnvelope, error) {
	var env queueEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return queueEnvelope{}, err
	}
	return env, nil
}

func rawID(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "raw-" + hex.EncodeToString(sum[:12])
}

// DeadLetter is an entry of the dead-letter list.
type DeadLetter struct {
	ID       string    `json:"id"`
	Event    *Event    `json:"event,omitempty"`
	Reason   string    `json:"reason"`
	Attempts int       `json:"attempts"`
	DeadAt   time.Time `json:"dead_at"`
	// Message is the original queue element (re-queued by Redrive).
	Message string `json:"message"`
	// Redrivable is false for undecodable messages.
	Redrivable bool `json:"redrivable"`
}

// QueueStats is a snapshot of the reliable queue's lists.
type QueueStats struct {
	Ready       int64 `json:"ready"`
	Delayed     int64 `json:"delayed"`
	InFlight    int64 `json:"in_flight"`
	DeadLetters int64 `json:"dead_letters"`
}

// MaintenanceResult reports what one maintenance pass did.
type MaintenanceResult struct {
	// Recovered counts leased messages whose visibility timeout expired
	// and which were re-queued.
	Recovered int
	// Promoted counts delayed retries moved back to the ready list.
	Promoted int
	// Pruned counts dead consumers removed from the registry.
	Pruned int
}

// Lua helpers. All deadlines use the Redis server clock (TIME), so clock
// skew between gateway instances cannot cause premature recovery.
const luaNow = `
local function nowms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
`

// claim: KEYS inflight, attempts, consumers; ARGV msg, id, vtMs, consumer.
var claimScript = redis.NewScript(luaNow + `
redis.call('ZADD', KEYS[1], nowms() + tonumber(ARGV[3]), ARGV[1])
redis.call('SADD', KEYS[3], ARGV[4])
return redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
`)

// ack: KEYS processing, inflight, attempts; ARGV msg, id.
var ackScript = redis.NewScript(`
local n = redis.call('LREM', KEYS[1], -1, ARGV[1])
if n > 0 then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('HDEL', KEYS[3], ARGV[2])
end
return n
`)

// settle: KEYS processing, inflight, attempts, delayed, dead, main;
// ARGV msg, id, mode (retry|release|dead), delayMs, deadRecord, maxDead.
var settleScript = redis.NewScript(luaNow + `
local n = redis.call('LREM', KEYS[1], -1, ARGV[1])
if n == 0 then return 0 end
redis.call('ZREM', KEYS[2], ARGV[1])
local mode = ARGV[3]
if mode == 'dead' then
  redis.call('HDEL', KEYS[3], ARGV[2])
  redis.call('LPUSH', KEYS[5], ARGV[5])
  redis.call('LTRIM', KEYS[5], 0, tonumber(ARGV[6]) - 1)
elseif mode == 'release' then
  if redis.call('HINCRBY', KEYS[3], ARGV[2], -1) <= 0 then
    redis.call('HDEL', KEYS[3], ARGV[2])
  end
  redis.call('RPUSH', KEYS[6], ARGV[1])
else
  redis.call('ZADD', KEYS[4], nowms() + tonumber(ARGV[4]), ARGV[1])
end
return 1
`)

// recover: KEYS processing, inflight, main; ARGV vtMs, maxScan.
// Elements without a lease record (the consumer died between BLMOVE and
// the claim) get a fresh deadline; expired ones go back to the ready list.
var recoverScript = redis.NewScript(luaNow + `
local now = nowms()
local items = redis.call('LRANGE', KEYS[1], -tonumber(ARGV[2]), -1)
local moved = 0
for i = 1, #items do
  local m = items[i]
  local s = redis.call('ZSCORE', KEYS[2], m)
  if not s then
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), m)
  elseif tonumber(s) <= now then
    redis.call('LREM', KEYS[1], -1, m)
    redis.call('ZREM', KEYS[2], m)
    redis.call('RPUSH', KEYS[3], m)
    moved = moved + 1
  end
end
return moved
`)

// promote: KEYS delayed, main; ARGV max.
var promoteScript = redis.NewScript(luaNow + `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', nowms(), 'LIMIT', 0, tonumber(ARGV[1]))
for _, m in ipairs(due) do
  redis.call('ZREM', KEYS[1], m)
  redis.call('LPUSH', KEYS[2], m)
end
return #due
`)

// register: KEYS consumers, heartbeat; ARGV consumer, ttlMs. Returns the
// registered consumers.
var registerScript = redis.NewScript(`
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '1', 'PX', tonumber(ARGV[2]))
return redis.call('SMEMBERS', KEYS[1])
`)

// prune: KEYS consumers, heartbeat, processing; ARGV consumer.
var pruneScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 0 and redis.call('LLEN', KEYS[3]) == 0 then
  redis.call('SREM', KEYS[1], ARGV[1])
  return 1
end
return 0
`)

// redrive: KEYS dead, main, attempts; ARGV max. Oldest first.
var redriveScript = redis.NewScript(`
local len = redis.call('LLEN', KEYS[1])
local n = tonumber(ARGV[1])
if n <= 0 or n > len then n = len end
local moved, kept = 0, 0
for i = 1, n do
  local item = redis.call('RPOP', KEYS[1])
  if not item then break end
  local ok, d = pcall(cjson.decode, item)
  if ok and type(d) == 'table' and d.redrivable == true and type(d.message) == 'string' then
    if type(d.id) == 'string' then redis.call('HDEL', KEYS[3], d.id) end
    redis.call('LPUSH', KEYS[2], d.message)
    moved = moved + 1
  else
    redis.call('LPUSH', KEYS[1], item)
    kept = kept + 1
  end
end
return {moved, kept}
`)

// maxRecoverScan bounds the elements inspected per processing list and pass.
const maxRecoverScan = 1000

// reliableState holds the key layout and options of a reliable queue.
type reliableState struct {
	client     RedisQueueClient
	opts       ReliableQueueOptions
	tag        string
	processing string
	inflight   string
	attempts   string
	delayed    string
	dead       string
	consumers  string
	lastMaint  atomic.Int64
}

// keyTag returns the prefix of the auxiliary keys. They share the main
// key's Redis Cluster hash slot: an untagged key K hashes like "{K}".
func keyTag(key string) string {
	if i := strings.IndexByte(key, '{'); i >= 0 && strings.IndexByte(key[i+1:], '}') > 0 {
		return key // key has a hash tag; key+suffix keeps it
	}
	if strings.IndexByte(key, '}') >= 0 {
		return key // cannot be wrapped faithfully (standalone Redis only)
	}
	return "{" + key + "}"
}

func newReliableState(client RedisQueueClient, key string, opts ReliableQueueOptions) *reliableState {
	opts = opts.withDefaults()
	tag := keyTag(key)
	return &reliableState{
		client:     client,
		opts:       opts,
		tag:        tag,
		processing: tag + ":processing:" + opts.ConsumerID,
		inflight:   tag + ":inflight",
		attempts:   tag + ":attempts",
		delayed:    tag + ":delayed",
		dead:       tag + ":dead",
		consumers:  tag + ":consumers",
	}
}

func (s *reliableState) processingOf(consumer string) string {
	return s.tag + ":processing:" + consumer
}
func (s *reliableState) heartbeatOf(consumer string) string { return s.tag + ":heartbeat:" + consumer }

// NewReliableRedisQueue creates an at-least-once Redis webhook queue.
//
// Enqueue LPUSHes onto key. Receive atomically moves the oldest message
// into this consumer's processing list (BLMOVE) and records a lease in a
// sorted set; Ack removes it (LREM). Leases of crashed consumers expire
// after VisibilityTimeout and their messages are re-queued by the periodic
// maintenance that Receive runs (and immediately on the first Receive
// after a restart). Failed deliveries are retried with exponential backoff
// via a delayed sorted set; after MaxAttempts, or on a permanent error,
// messages move to the "<key>:dead" list (see DeadLetters and Redrive).
// Auxiliary keys share key's Redis Cluster hash slot.
func NewReliableRedisQueue(client RedisQueueClient, key string, opts ReliableQueueOptions) *RedisWebhookQueue {
	if key == "" {
		key = "webhook:queue"
	}
	q := &RedisWebhookQueue{key: key, blockTimeout: 2 * time.Second}
	if client == nil {
		return q
	}
	q.client = client
	q.rel = newReliableState(client, key, opts)
	q.blockTimeout = q.rel.opts.BlockTimeout
	return q
}

// Reliable reports whether the queue provides at-least-once delivery
// (Receive/Ack). Queues built over a minimal RedisListClient fall back to
// the legacy pop semantics.
func (q *RedisWebhookQueue) Reliable() bool { return q != nil && q.rel != nil }

// ConsumerID returns the consumer name used for this queue's processing list.
func (q *RedisWebhookQueue) ConsumerID() string {
	if q == nil || q.rel == nil {
		return ""
	}
	return q.rel.opts.ConsumerID
}

// DeadLetterKey returns the Redis key of the dead-letter list.
func (q *RedisWebhookQueue) DeadLetterKey() string {
	if q == nil || q.rel == nil {
		return ""
	}
	return q.rel.dead
}

func (q *RedisWebhookQueue) reliable() (*reliableState, error) {
	if q == nil || q.client == nil {
		return nil, ErrNoRedisClient
	}
	if q.rel == nil {
		return nil, ErrReliableUnsupported
	}
	return q.rel, nil
}

// bookkeeping returns a context for settling a message that survives the
// caller's cancellation (so shutdown does not strand leases) but is bounded.
func bookkeeping(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

// Receive implements AckQueue.
func (q *RedisWebhookQueue) Receive(ctx context.Context) (*QueueMessage, error) {
	s, err := q.reliable()
	if err != nil {
		return nil, err
	}
	q.maybeMaintain(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := s.client.BLMove(ctx, q.key, s.processing, "RIGHT", "LEFT", s.opts.BlockTimeout).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrQueueEmpty
	}
	if err != nil {
		return nil, err
	}
	leasedAt := time.Now()
	bctx, cancel := bookkeeping(ctx)
	defer cancel()

	env, derr := decodeQueued(raw)
	if derr != nil {
		dl := DeadLetter{ID: rawID(raw), Reason: "malformed message: " + truncate(derr.Error(), 200), Message: raw}
		if err := q.settle(bctx, raw, dl.ID, "dead", 0, &dl); err != nil {
			return nil, err
		}
		telemetry.RecordError()
		return nil, fmt.Errorf("%w: %v", ErrMalformedEvent, derr)
	}
	id := env.id(raw)
	n, err := claimScript.Run(bctx, s.client, []string{s.inflight, s.attempts, s.consumers},
		raw, id, s.opts.VisibilityTimeout.Milliseconds(), s.opts.ConsumerID).Int64()
	if err != nil {
		// The message stays in our processing list; maintenance gives it a
		// lease and re-queues it after the visibility timeout.
		return nil, fmt.Errorf("webhooks: claim leased message: %w", err)
	}
	if n > int64(s.opts.MaxAttempts) {
		ev := env.Event
		dl := DeadLetter{ID: id, Event: &ev, Attempts: int(n - 1), Message: raw, Redrivable: true,
			Reason: fmt.Sprintf("exceeded %d delivery attempts (consumers kept failing or crashing)", s.opts.MaxAttempts)}
		if err := q.settle(bctx, raw, id, "dead", 0, &dl); err != nil {
			return nil, err
		}
		telemetry.RecordError()
		return nil, ErrDeadLettered
	}
	vt := s.opts.VisibilityTimeout
	margin := min(vt/10, 30*time.Second)
	return &QueueMessage{Event: env.Event, ID: id, Attempt: int(n), Deadline: leasedAt.Add(vt - margin), raw: raw}, nil
}

func (q *RedisWebhookQueue) settle(ctx context.Context, raw, id, mode string, delay time.Duration, dl *DeadLetter) error {
	s := q.rel
	record := ""
	if dl != nil {
		dl.DeadAt = time.Now().UTC()
		b, err := json.Marshal(dl)
		if err != nil {
			return err
		}
		record = string(b)
	}
	n, err := settleScript.Run(ctx, s.client, []string{s.processing, s.inflight, s.attempts, s.delayed, s.dead, q.key},
		raw, id, mode, delay.Milliseconds(), record, s.opts.MaxDeadLetters).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseExpired
	}
	return nil
}

func checkMessage(m *QueueMessage) error {
	if m == nil || m.raw == "" {
		return errors.New("webhooks: message was not received from this queue")
	}
	return nil
}

// Ack implements AckQueue. It returns ErrLeaseExpired when the lease was
// lost (the message has been or will be delivered again).
func (q *RedisWebhookQueue) Ack(ctx context.Context, m *QueueMessage) error {
	s, err := q.reliable()
	if err != nil {
		return err
	}
	if err := checkMessage(m); err != nil {
		return err
	}
	n, err := ackScript.Run(ctx, s.client, []string{s.processing, s.inflight, s.attempts}, m.raw, m.ID).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseExpired
	}
	return nil
}

// Nack implements AckQueue.
func (q *RedisWebhookQueue) Nack(ctx context.Context, m *QueueMessage, cause error) error {
	s, err := q.reliable()
	if err != nil {
		return err
	}
	if err := checkMessage(m); err != nil {
		return err
	}
	if IsPermanent(cause) || m.Attempt >= s.opts.MaxAttempts {
		reason := "delivery failed"
		if cause != nil {
			reason = truncate(cause.Error(), 500)
		}
		if IsPermanent(cause) {
			reason = "permanent failure: " + reason
		} else {
			reason = fmt.Sprintf("gave up after %d attempts: %s", m.Attempt, reason)
		}
		ev := m.Event
		dl := DeadLetter{ID: m.ID, Event: &ev, Reason: reason, Attempts: m.Attempt, Message: m.raw, Redrivable: true}
		return q.settle(ctx, m.raw, m.ID, "dead", 0, &dl)
	}
	return q.settle(ctx, m.raw, m.ID, "retry", backoffDelay(s.opts.RetryBaseDelay, m.Attempt, s.opts.RetryMaxDelay), nil)
}

// Release implements AckQueue: the message goes back to the front of the
// ready list and the attempt is not counted.
func (q *RedisWebhookQueue) Release(ctx context.Context, m *QueueMessage) error {
	if _, err := q.reliable(); err != nil {
		return err
	}
	if err := checkMessage(m); err != nil {
		return err
	}
	return q.settle(ctx, m.raw, m.ID, "release", 0, nil)
}

func (q *RedisWebhookQueue) maybeMaintain(ctx context.Context) {
	s := q.rel
	now := time.Now().UnixNano()
	last := s.lastMaint.Load()
	if last != 0 && time.Duration(now-last) < s.opts.MaintenanceInterval {
		return
	}
	if !s.lastMaint.CompareAndSwap(last, now) {
		return
	}
	bctx, cancel := bookkeeping(ctx)
	defer cancel()
	if _, err := q.Maintain(bctx); err != nil {
		telemetry.RecordError()
	}
}

// Maintain runs one maintenance pass: it refreshes this consumer's
// heartbeat, re-queues messages whose lease expired in any consumer's
// processing list (including consumers that no longer exist), promotes due
// retries and forgets dead consumers with empty processing lists. Receive
// calls it periodically; call it explicitly at startup to recover
// immediately.
func (q *RedisWebhookQueue) Maintain(ctx context.Context) (MaintenanceResult, error) {
	var res MaintenanceResult
	s, err := q.reliable()
	if err != nil {
		return res, err
	}
	ttl := 3 * s.opts.MaintenanceInterval
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	consumers, err := registerScript.Run(ctx, s.client, []string{s.consumers, s.heartbeatOf(s.opts.ConsumerID)},
		s.opts.ConsumerID, ttl.Milliseconds()).StringSlice()
	if err != nil {
		return res, err
	}
	var errs []error
	for _, c := range consumers {
		n, err := recoverScript.Run(ctx, s.client, []string{s.processingOf(c), s.inflight, q.key},
			s.opts.VisibilityTimeout.Milliseconds(), maxRecoverScan).Int()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		res.Recovered += n
		if c == s.opts.ConsumerID {
			continue
		}
		p, err := pruneScript.Run(ctx, s.client, []string{s.consumers, s.heartbeatOf(c), s.processingOf(c)}, c).Int()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		res.Pruned += p
	}
	for {
		n, err := promoteScript.Run(ctx, s.client, []string{s.delayed, q.key}, 500).Int()
		if err != nil {
			errs = append(errs, err)
			break
		}
		res.Promoted += n
		if n < 500 {
			break
		}
	}
	return res, errors.Join(errs...)
}

// Stats returns the sizes of the ready, delayed, in-flight and dead-letter sets.
func (q *RedisWebhookQueue) Stats(ctx context.Context) (QueueStats, error) {
	var st QueueStats
	s, err := q.reliable()
	if err != nil {
		return st, err
	}
	if st.Ready, err = s.client.LLen(ctx, q.key).Result(); err != nil {
		return st, err
	}
	if st.Delayed, err = s.client.ZCard(ctx, s.delayed).Result(); err != nil {
		return st, err
	}
	if st.InFlight, err = s.client.ZCard(ctx, s.inflight).Result(); err != nil {
		return st, err
	}
	st.DeadLetters, err = s.client.LLen(ctx, s.dead).Result()
	return st, err
}

// DeadLetters returns up to limit dead letters, newest first (limit <= 0
// selects 100).
func (q *RedisWebhookQueue) DeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	s, err := q.reliable()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	vals, err := s.client.LRange(ctx, s.dead, 0, int64(limit)-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DeadLetter, 0, len(vals))
	for _, v := range vals {
		var dl DeadLetter
		if json.Unmarshal([]byte(v), &dl) != nil {
			dl = DeadLetter{Reason: "undecodable dead letter", Message: v}
		}
		out = append(out, dl)
	}
	return out, nil
}

// Redrive moves up to max redrivable dead letters (oldest first; max <= 0
// means all) back to the ready list with a fresh attempt budget and
// reports how many were moved. Undecodable messages stay dead-lettered.
func (q *RedisWebhookQueue) Redrive(ctx context.Context, max int) (int, error) {
	s, err := q.reliable()
	if err != nil {
		return 0, err
	}
	res, err := redriveScript.Run(ctx, s.client, []string{s.dead, q.key, s.attempts}, max).Int64Slice()
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, nil
	}
	return int(res[0]), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var _ AckQueue = (*RedisWebhookQueue)(nil)
