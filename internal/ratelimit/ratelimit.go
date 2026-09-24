// Package ratelimit provides per-API-key request rate limiting.
//
// TokenBucketLimiter is an in-process token bucket keyed by (api key,
// provider). RedisLimiter implements the same interface on top of Redis so
// that limits are shared by every gateway replica.
package ratelimit

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"
)

// RateLimiter is the interface for rate limiting.
type RateLimiter interface {
	Allow(ctx context.Context, apiKey string, provider string) (bool, error)
	GetLimits(ctx context.Context, apiKey string, provider string) (*RateLimitRecord, error)
}

// RateLimitRecord holds rate limiting information.
type RateLimitRecord struct {
	APIKey    string
	Provider  string
	Limit     int
	Remaining int
	ResetAt   int64 // unix seconds when the bucket is full again
	TotalUsed int
	// RetryAfter is how long a rejected caller should wait before retrying.
	RetryAfter time.Duration
}

// Limit is a per-key override of the default rate.
type Limit struct {
	RPS   float64
	Burst int
}

type bucket struct {
	tokens   float64
	last     time.Time
	used     int
	rps      float64
	capacity float64
}

func (b *bucket) refill(now time.Time) {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rps)
		b.last = now
	}
}

func (b *bucket) record(apiKey, provider string, now time.Time) *RateLimitRecord {
	missing := b.capacity - b.tokens
	full := now
	if missing > 0 && b.rps > 0 {
		full = now.Add(time.Duration(missing / b.rps * float64(time.Second)))
	}
	rec := &RateLimitRecord{
		APIKey:    apiKey,
		Provider:  provider,
		Limit:     int(b.capacity),
		Remaining: int(math.Floor(b.tokens)),
		ResetAt:   full.Unix(),
		TotalUsed: b.used,
	}
	if b.tokens < 1 && b.rps > 0 {
		rec.RetryAfter = time.Duration((1 - b.tokens) / b.rps * float64(time.Second))
	}
	return rec
}

// TokenBucketLimiter implements the token bucket algorithm in memory.
// A rate of zero (or less) disables limiting.
type TokenBucketLimiter struct {
	defaultRPS      float64
	burstMultiplier int

	mu        sync.Mutex
	buckets   map[string]*bucket
	overrides map[string]Limit
	idleTTL   time.Duration
	lastSweep time.Time
	now       func() time.Time
}

// NewTokenBucketLimiter creates a new token bucket rate limiter. Each key may
// burst up to defaultRPS*burstMultiplier requests and refills at defaultRPS.
func NewTokenBucketLimiter(defaultRPS float64, burstMultiplier int) *TokenBucketLimiter {
	if burstMultiplier < 1 {
		burstMultiplier = 1
	}
	return &TokenBucketLimiter{
		defaultRPS:      defaultRPS,
		burstMultiplier: burstMultiplier,
		buckets:         make(map[string]*bucket),
		overrides:       make(map[string]Limit),
		idleTTL:         10 * time.Minute,
		now:             time.Now,
	}
}

// SetLimit overrides the rate for a single API key. A zero RPS removes the
// override. Existing buckets for the key are reset to the new capacity.
func (t *TokenBucketLimiter) SetLimit(apiKey string, l Limit) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if l.RPS <= 0 {
		delete(t.overrides, apiKey)
	} else {
		t.overrides[apiKey] = l
	}
	prefix := apiKey + "\x00"
	for k := range t.buckets {
		if strings.HasPrefix(k, prefix) {
			delete(t.buckets, k)
		}
	}
}

func (t *TokenBucketLimiter) rateFor(apiKey string) (rps, capacity float64) {
	if l, ok := t.overrides[apiKey]; ok {
		burst := l.Burst
		if burst <= 0 {
			burst = int(math.Ceil(l.RPS)) * t.burstMultiplier
		}
		return l.RPS, math.Max(1, float64(burst))
	}
	return t.defaultRPS, math.Max(1, math.Ceil(t.defaultRPS*float64(t.burstMultiplier)))
}

func (t *TokenBucketLimiter) bucketFor(apiKey, provider string, now time.Time) *bucket {
	key := apiKey + "\x00" + provider
	b, ok := t.buckets[key]
	if !ok {
		rps, capacity := t.rateFor(apiKey)
		b = &bucket{tokens: capacity, last: now, rps: rps, capacity: capacity}
		t.buckets[key] = b
	}
	b.refill(now)
	return b
}

// sweep drops buckets that have been idle long enough to be full again, so
// memory stays bounded by the number of recently active keys.
func (t *TokenBucketLimiter) sweep(now time.Time) {
	if now.Sub(t.lastSweep) < time.Minute {
		return
	}
	t.lastSweep = now
	for k, b := range t.buckets {
		if now.Sub(b.last) > t.idleTTL {
			delete(t.buckets, k)
		}
	}
}

// Allow consumes one token for (apiKey, provider) and reports whether the
// request may proceed.
func (t *TokenBucketLimiter) Allow(ctx context.Context, apiKey string, provider string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rps, _ := t.rateFor(apiKey)
	if rps <= 0 {
		return true, nil
	}
	now := t.now()
	t.sweep(now)
	b := t.bucketFor(apiKey, provider, now)
	if b.tokens < 1 {
		return false, nil
	}
	b.tokens--
	b.used++
	return true, nil
}

// GetLimits returns the current rate limit status without consuming a token.
func (t *TokenBucketLimiter) GetLimits(ctx context.Context, apiKey string, provider string) (*RateLimitRecord, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rps, capacity := t.rateFor(apiKey)
	now := t.now()
	if rps <= 0 {
		return &RateLimitRecord{APIKey: apiKey, Provider: provider, Limit: int(capacity), Remaining: int(capacity), ResetAt: now.Unix()}, nil
	}
	return t.bucketFor(apiKey, provider, now).record(apiKey, provider, now), nil
}
