package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
	"github.com/redis/go-redis/v9"
)

type mockProvider struct {
	name string
}

func (m *mockProvider) Name() string                          { return m.name }
func (m *mockProvider) Type() providers.ProviderType         { return providers.ProviderOpenAI }
func (m *mockProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return &models.LLMResponse{Model: m.name, Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: strPtr("ok")}}}}, nil
}
func (m *mockProvider) Health() providers.ProviderHealth     { return providers.ProviderHealth{Name: m.name, Healthy: true} }
func (m *mockProvider) Close() error                          { return nil }

type mockRateLimiter struct{}

func (m *mockRateLimiter) Allow(ctx context.Context, apiKey, provider string) (bool, error) { return true, nil }
func (m *mockRateLimiter) GetLimits(ctx context.Context, apiKey, provider string) (*ratelimit.RateLimitRecord, error) {
	return &ratelimit.RateLimitRecord{}, nil
}

type mockToolProvider struct{}

func (m *mockToolProvider) CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	return &models.LLMResponse{Model: "p1", Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: strPtr("ok")}}}}, nil
}

type mockRedisClient struct {
	hit            bool
	budgetRemaining float64
	budgetKey      string
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if m.budgetKey != "" && key == m.budgetKey {
		if m.budgetRemaining <= 0 {
			cmd.SetVal("0")
		} else {
			cmd.SetVal(fmt.Sprintf("%f", m.budgetRemaining))
		}
		return cmd
	}
	if m.hit {
		cmd.SetVal(`{"model":"mock","choices":[{"message":{"role":"assistant","content":"cached"}}]}`)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}
func (m *mockRedisClient) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	return redis.NewStatusCmd(ctx)
}
func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return redis.NewIntCmd(ctx)
}
func (m *mockRedisClient) Close() error { return nil }

func strPtr(s string) *string { return &s }

type testLogger struct{}

func (t *testLogger) Info(msg string, keysAndValues ...interface{})   {}
func (t *testLogger) Error(msg string, keysAndValues ...interface{}) {}

func newTestTelemetry() *telemetry.Provider {
	tp, err := telemetry.NewProvider(context.Background(), telemetry.Config{ServiceName: "test"})
	if err != nil {
		panic(err)
	}
	tp.Start()
	return tp
}

func TestChatCompletionsSuccess(t *testing.T) {
	logger := &testLogger{}
	tp := newTestTelemetry()
	defer tp.Stop(context.Background())

	r := router.New(router.Config{Strategy: "round_robin"})
	p := &mockProvider{name: "p1"}
	r.RegisterProvider(p)

	a := agent.NewAgentEngine(&mockToolProvider{}, nil)
	c := cache.NewRedisCache(&mockRedisClient{}, time.Hour)
	rl := &mockRateLimiter{}
	h := NewHandler(r, a, c, rl, tp, logger)

	body := `{"model":"p1","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestChatCompletionsInvalidRequest(t *testing.T) {
	logger := &testLogger{}
	tp := newTestTelemetry()
	defer tp.Stop(context.Background())

	r := router.New(router.Config{Strategy: "round_robin"})
	a := agent.NewAgentEngine(&mockToolProvider{}, nil)
	c := cache.NewRedisCache(&mockRedisClient{}, time.Hour)
	rl := &mockRateLimiter{}
	h := NewHandler(r, a, c, rl, tp, logger)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString("invalid"))
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestChatCompletionsCacheHit(t *testing.T) {
	logger := &testLogger{}
	tp := newTestTelemetry()
	defer tp.Stop(context.Background())

	r := router.New(router.Config{Strategy: "round_robin"})
	r.RegisterProvider(&mockProvider{name: "p1"})

	a := agent.NewAgentEngine(&mockToolProvider{}, nil)
	c := cache.NewRedisCache(&mockRedisClient{hit: true}, time.Hour)
	rl := &mockRateLimiter{}
	h := NewHandler(r, a, c, rl, tp, logger)

	body := `{"model":"p1","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "cached") {
		t.Fatalf("expected cached response, got: %s", w.Body.String())
	}
}

func TestChatCompletionsBudgetExceeded(t *testing.T) {
	logger := &testLogger{}
	tp := newTestTelemetry()
	defer tp.Stop(context.Background())

	r := router.New(router.Config{Strategy: "round_robin"})
	r.RegisterProvider(&mockProvider{name: "p1"})

	a := agent.NewAgentEngine(&mockToolProvider{}, nil)
	c := cache.NewRedisCache(&mockRedisClient{}, time.Hour)
	rl := &mockRateLimiter{}
	h := NewHandler(r, a, c, rl, tp, logger)
	h.UsageRecorder = &fakeBudgetChecker{err: errors.New("budget exceeded")}

	body := `{"model":"p1","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "sk-budget")
	w := httptest.NewRecorder()
	h.ChatCompletions(w, req)
	if w.Code != http.StatusPaymentRequired {
		t.Fatalf("expected 402, got %d: %s", w.Code, w.Body.String())
	}
}

type fakeBudgetChecker struct {
	err error
}

func (f *fakeBudgetChecker) CheckBudget(ctx context.Context, apiKey string, estimatedCost float64) (float64, error) {
	return 0, f.err
}

func (f *fakeBudgetChecker) RecordUsage(ctx context.Context, req interface{}) error {
	return nil
}
