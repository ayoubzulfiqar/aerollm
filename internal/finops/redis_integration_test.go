package finops

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestRedisBudgetStoreIntegration runs the real Lua script against Redis
// when AEROLLM_TEST_REDIS_ADDR is set (e.g. "127.0.0.1:6379").
func TestRedisBudgetStoreIntegration(t *testing.T) {
	addr := os.Getenv("AEROLLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("AEROLLM_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	prefix := "aerollm-test:" + t.Name() + ":"
	store := NewRedisBudgetStore(client, prefix)
	t.Cleanup(func() {
		keys, _ := client.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
	})
	c := NewCostTrackerWithStore(store, nil, nil)
	runBudgetLifecycle(t, c)
	concurrentExceeded(t, NewCostTrackerWithStore(NewRedisBudgetStore(client, prefix+"conc:"), nil, nil))
}
