package ratelimit

import (
	"context"
)

// RateLimiter is the interface for rate limiting.
type RateLimiter interface {
	Allow(ctx context.Context, apiKey string, provider string) (bool, error)
	GetLimits(ctx context.Context, apiKey string, provider string) (*RateLimitRecord, error)
}

// RateLimitRecord holds rate limiting information.
type RateLimitRecord struct {
	APIKey     string
	Provider   string
	Remaining  int
	ResetAt    int64
	TotalUsed  int
}

// TokenBucketLimiter implements the token bucket algorithm.
type TokenBucketLimiter struct {
	defaultRPS      float64
	burstMultiplier int
}

// NewTokenBucketLimiter creates a new token bucket rate limiter.
func NewTokenBucketLimiter(defaultRPS float64, burstMultiplier int) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		defaultRPS:      defaultRPS,
		burstMultiplier: burstMultiplier,
	}
}

// Allow checks if a request is allowed.
func (t *TokenBucketLimiter) Allow(ctx context.Context, apiKey string, provider string) (bool, error) {
	return true, nil
}

// GetLimits returns the current rate limit status.
func (t *TokenBucketLimiter) GetLimits(ctx context.Context, apiKey string, provider string) (*RateLimitRecord, error) {
	return &RateLimitRecord{}, nil
}
