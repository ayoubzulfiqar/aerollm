package ratelimit

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript atomically refills and consumes a token bucket stored in
// a Redis hash. It uses the Redis server clock so replicas agree on time.
//
// KEYS[1] = bucket key; ARGV = rate (tokens/s), capacity, cost (0 = peek).
// Returns {allowed (0|1), tokens remaining (string), ms until full}.
var tokenBucketScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = cap
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(cap, tokens + elapsed * rate)
local allowed = 0
if cost == 0 or tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
local ttl = math.ceil((cap / rate) * 1000) + 1000
redis.call('PEXPIRE', KEYS[1], ttl)
local untilFull = math.ceil(((cap - tokens) / rate) * 1000)
return {allowed, tostring(tokens), untilFull}
`)

// RedisLimiter is a token bucket shared across gateway replicas through Redis.
// If Redis is unreachable it degrades to the in-process Fallback limiter, so
// an outage of the cache tier never takes the gateway down.
type RedisLimiter struct {
	Client   redis.Scripter
	Prefix   string
	Fallback *TokenBucketLimiter

	local *TokenBucketLimiter // holds default rate + per-key overrides
}

// NewRedisLimiter creates a Redis-backed limiter with the same rate semantics
// as NewTokenBucketLimiter.
func NewRedisLimiter(client redis.Scripter, defaultRPS float64, burstMultiplier int) *RedisLimiter {
	local := NewTokenBucketLimiter(defaultRPS, burstMultiplier)
	return &RedisLimiter{
		Client:   client,
		Prefix:   "ratelimit:",
		Fallback: NewTokenBucketLimiter(defaultRPS, burstMultiplier),
		local:    local,
	}
}

// DefaultRPS returns the default per-key rate.
func (l *RedisLimiter) DefaultRPS() float64 { return l.local.DefaultRPS() }

// SetDefaultRPS changes the default per-key rate at runtime.
func (l *RedisLimiter) SetDefaultRPS(rps float64) {
	l.local.SetDefaultRPS(rps)
	l.Fallback.SetDefaultRPS(rps)
}

// SetLimit overrides the rate for a single API key.
func (l *RedisLimiter) SetLimit(apiKey string, lim Limit) {
	l.local.SetLimit(apiKey, lim)
	l.Fallback.SetLimit(apiKey, lim)
}

func (l *RedisLimiter) rate(apiKey string) (float64, float64) {
	l.local.mu.Lock()
	defer l.local.mu.Unlock()
	return l.local.rateFor(apiKey)
}

func (l *RedisLimiter) run(ctx context.Context, apiKey, provider string, cost int) (bool, float64, time.Duration, error) {
	rps, capacity := l.rate(apiKey)
	key := l.Prefix + apiKey + ":" + provider
	res, err := tokenBucketScript.Run(ctx, l.Client, []string{key},
		strconv.FormatFloat(rps, 'f', -1, 64), strconv.FormatFloat(capacity, 'f', -1, 64), cost).Slice()
	if err != nil || len(res) != 3 {
		if err == nil {
			err = redis.Nil
		}
		return false, 0, 0, err
	}
	allowed, _ := res[0].(int64)
	tokensStr, _ := res[1].(string)
	tokens, _ := strconv.ParseFloat(tokensStr, 64)
	msFull, _ := res[2].(int64)
	return allowed == 1, tokens, time.Duration(msFull) * time.Millisecond, nil
}

// Allow consumes one token for (apiKey, provider).
func (l *RedisLimiter) Allow(ctx context.Context, apiKey string, provider string) (bool, error) {
	if rps, _ := l.rate(apiKey); rps <= 0 {
		return true, nil
	}
	if l.Client == nil {
		return l.Fallback.Allow(ctx, apiKey, provider)
	}
	allowed, _, _, err := l.run(ctx, apiKey, provider, 1)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return l.Fallback.Allow(ctx, apiKey, provider)
	}
	return allowed, nil
}

// GetLimits reports the bucket state without consuming a token.
func (l *RedisLimiter) GetLimits(ctx context.Context, apiKey string, provider string) (*RateLimitRecord, error) {
	rps, capacity := l.rate(apiKey)
	if rps <= 0 || l.Client == nil {
		return l.Fallback.GetLimits(ctx, apiKey, provider)
	}
	_, tokens, untilFull, err := l.run(ctx, apiKey, provider, 0)
	if err != nil {
		return l.Fallback.GetLimits(ctx, apiKey, provider)
	}
	rec := &RateLimitRecord{
		APIKey:    apiKey,
		Provider:  provider,
		Limit:     int(capacity),
		Remaining: int(math.Floor(tokens)),
		ResetAt:   time.Now().Add(untilFull).Unix(),
	}
	if tokens < 1 {
		rec.RetryAfter = time.Duration((1 - tokens) / rps * float64(time.Second))
	}
	return rec, nil
}
