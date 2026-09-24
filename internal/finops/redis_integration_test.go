package finops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/redis/go-redis/v9"
)

// integrationRedis returns a client for AEROLLM_TEST_REDIS_ADDR (e.g.
// "127.0.0.1:6379") and a unique key prefix whose keys are deleted when
// the test ends; it skips when the variable is unset.
func integrationRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("redis unavailable: %v", err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "aerollm-test:finops:" + hex.EncodeToString(b[:]) + ":"
	t.Cleanup(func() {
		var keys []string
		iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	})
	return client, prefix
}

// TestRedisBudgetStoreIntegration runs the real Lua script against Redis.
func TestRedisBudgetStoreIntegration(t *testing.T) {
	client, prefix := integrationRedis(t)
	c := NewCostTrackerWithStore(NewRedisBudgetStore(client, prefix), nil, nil)
	runBudgetLifecycle(t, c)
	concurrentExceeded(t, NewCostTrackerWithStore(NewRedisBudgetStore(client, prefix+"conc:"), nil, nil))
}

// Budgets and buffered usage live in Redis, so a new tracker (a restarted
// or second gateway instance) sees the same state.
func TestRedisBudgetStoreSharedStateAndUsage(t *testing.T) {
	client, prefix := integrationRedis(t)
	ctx := context.Background()
	const key = "sk-redis-shared-123456"
	c1 := NewCostTrackerWithStore(NewRedisBudgetStore(client, prefix), nil, nil)
	c1.SetBudgetPeriod(PeriodDaily)
	if err := c1.SetBudget(ctx, key, 2); err != nil {
		t.Fatal(err)
	}
	if err := c1.RecordUsage(ctx, CostRequest{APIKey: key, CostUSD: 0.5}); err != nil {
		t.Fatal(err)
	}
	if err := c1.AppendUsage(ctx,
		billing.MeterEntry{CustomerID: "cus_1", EventName: "tokens", Value: 10, ID: "a"},
		billing.MeterEntry{CustomerID: "cus_1", EventName: "tokens", Value: 20, ID: "b"},
		billing.MeterEntry{CustomerID: "cus_2", EventName: "tokens", Value: 30, ID: "c"},
	); err != nil {
		t.Fatal(err)
	}

	c2 := NewCostTrackerWithStore(NewRedisBudgetStore(client, prefix), nil, nil)
	c2.SetBudgetPeriod(PeriodDaily)
	st, err := c2.GetBudget(ctx, key)
	if err != nil || !approxEqual(st.SpendUSD, 0.5, 1e-12) || !approxEqual(st.LimitUSD, 2, 1e-12) {
		t.Fatalf("shared state: %+v %v", st, err)
	}
	ttl, err := client.TTL(ctx, prefix+"spend:{"+BudgetID(key)+"}:"+st.PeriodKey).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("daily spend bucket must expire: %v %v", ttl, err)
	}
	got, err := c2.DrainUsage(ctx, "cus_1", 1)
	if err != nil || len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("drain first: %+v %v", got, err)
	}
	got, err = c2.DrainUsage(ctx, "cus_1", 0)
	if err != nil || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("drain rest: %+v %v", got, err)
	}
	if got, _ := c2.DrainUsage(ctx, "cus_1", 0); len(got) != 0 {
		t.Fatalf("drained queue must be empty: %+v", got)
	}
	if got, _ := c1.DrainUsage(ctx, "cus_2", 0); len(got) != 1 || got[0].Value != 30 {
		t.Fatalf("other customer: %+v", got)
	}
}
