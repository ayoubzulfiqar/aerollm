package ratelimit

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func fakeClock(start time.Time) (func() time.Time, func(time.Duration)) {
	var mu sync.Mutex
	now := start
	return func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		}, func(d time.Duration) {
			mu.Lock()
			now = now.Add(d)
			mu.Unlock()
		}
}

func TestTokenBucketEnforcesBurstAndRefill(t *testing.T) {
	l := NewTokenBucketLimiter(2, 2) // 2 rps, burst 4
	now, advance := fakeClock(time.Unix(1000, 0))
	l.now = now
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if ok, err := l.Allow(ctx, "k", ""); err != nil || !ok {
			t.Fatalf("request %d should be allowed (ok=%v err=%v)", i, ok, err)
		}
	}
	if ok, _ := l.Allow(ctx, "k", ""); ok {
		t.Fatal("5th request should be rejected once the burst is spent")
	}
	rec, _ := l.GetLimits(ctx, "k", "")
	if rec.Remaining != 0 || rec.RetryAfter <= 0 || rec.Limit != 4 {
		t.Fatalf("unexpected record after exhaustion: %+v", rec)
	}

	advance(500 * time.Millisecond) // refills one token at 2 rps
	if ok, _ := l.Allow(ctx, "k", ""); !ok {
		t.Fatal("request should be allowed after refill")
	}
	if ok, _ := l.Allow(ctx, "k", ""); ok {
		t.Fatal("only one token should have been refilled")
	}
}

func TestTokenBucketKeysAreIsolated(t *testing.T) {
	l := NewTokenBucketLimiter(1, 1)
	ctx := context.Background()
	if ok, _ := l.Allow(ctx, "a", ""); !ok {
		t.Fatal("a should be allowed")
	}
	if ok, _ := l.Allow(ctx, "a", ""); ok {
		t.Fatal("a should be limited")
	}
	if ok, _ := l.Allow(ctx, "b", ""); !ok {
		t.Fatal("b must not share a's bucket")
	}
}

func TestTokenBucketZeroRateDisablesLimiting(t *testing.T) {
	l := NewTokenBucketLimiter(0, 1)
	for i := 0; i < 100; i++ {
		if ok, _ := l.Allow(context.Background(), "k", ""); !ok {
			t.Fatal("zero rate should disable limiting")
		}
	}
}

func TestTokenBucketPerKeyOverride(t *testing.T) {
	l := NewTokenBucketLimiter(100, 1)
	l.SetLimit("slow", Limit{RPS: 1, Burst: 1})
	ctx := context.Background()
	if ok, _ := l.Allow(ctx, "slow", ""); !ok {
		t.Fatal("first request allowed")
	}
	if ok, _ := l.Allow(ctx, "slow", ""); ok {
		t.Fatal("override should cap slow key at burst 1")
	}
	l.SetLimit("slow", Limit{})
	if ok, _ := l.Allow(ctx, "slow", ""); !ok {
		t.Fatal("removing the override restores the default rate")
	}
}

func TestTokenBucketSweepsIdleBuckets(t *testing.T) {
	l := NewTokenBucketLimiter(10, 1)
	now, advance := fakeClock(time.Unix(1000, 0))
	l.now = now
	for i := 0; i < 50; i++ {
		_, _ = l.Allow(context.Background(), string(rune('a'+i)), "")
	}
	advance(time.Hour)
	_, _ = l.Allow(context.Background(), "fresh", "")
	l.mu.Lock()
	n := len(l.buckets)
	l.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected idle buckets to be swept, have %d", n)
	}
}

func TestTokenBucketConcurrent(t *testing.T) {
	l := NewTokenBucketLimiter(1, 50) // burst 50, slow refill
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.Allow(context.Background(), "k", ""); ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed < 50 || allowed > 51 {
		t.Fatalf("expected ~50 allowed under contention, got %d", allowed)
	}
}

func TestRedisLimiterFallsBackWhenRedisDown(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	l := NewRedisLimiter(client, 1, 1)
	ctx := context.Background()
	if ok, err := l.Allow(ctx, "k", ""); err != nil || !ok {
		t.Fatalf("fallback should allow first request: ok=%v err=%v", ok, err)
	}
	if ok, _ := l.Allow(ctx, "k", ""); ok {
		t.Fatal("fallback limiter should still enforce the rate")
	}
	if rec, err := l.GetLimits(ctx, "k", ""); err != nil || rec.Remaining != 0 {
		t.Fatalf("fallback GetLimits: rec=%+v err=%v", rec, err)
	}
}

// TestRedisLimiterIntegration runs against a real Redis when
// AEROLLM_TEST_REDIS_ADDR is set.
func TestRedisLimiterIntegration(t *testing.T) {
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	l := NewRedisLimiter(client, 1, 3)
	l.Prefix = "ratelimit-test:" + time.Now().Format("150405.000") + ":"
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if ok, err := l.Allow(ctx, "k", "p"); err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, _ := l.Allow(ctx, "k", "p"); ok {
		t.Fatal("4th request should be limited")
	}
	rec, err := l.GetLimits(ctx, "k", "p")
	if err != nil || rec.Remaining != 0 || rec.Limit != 3 {
		t.Fatalf("GetLimits: %+v %v", rec, err)
	}
}

func TestSetDefaultRPSAtRuntime(t *testing.T) {
	l := NewTokenBucketLimiter(10, 1)
	ctx := context.Background()
	_, _ = l.Allow(ctx, "k", "")
	l.SetDefaultRPS(1)
	if l.DefaultRPS() != 1 {
		t.Fatalf("default rps: %v", l.DefaultRPS())
	}
	if ok, _ := l.Allow(ctx, "k", ""); !ok {
		t.Fatal("one token must remain after shrinking capacity")
	}
	if ok, _ := l.Allow(ctx, "k", ""); ok {
		t.Fatal("capacity must shrink to the new default rate")
	}
	l.SetLimit("vip", Limit{RPS: 100, Burst: 100})
	l.SetDefaultRPS(0.5)
	if ok, _ := l.Allow(ctx, "vip", ""); !ok {
		t.Fatal("per-key overrides are unaffected by the default")
	}
	l.SetDefaultRPS(-1)
	if l.DefaultRPS() != 0.5 {
		t.Fatal("invalid rates must be ignored")
	}
}
