package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// syncApprovalStore is a concurrency-safe in-memory ApprovalStore.
type syncApprovalStore struct {
	mu      sync.Mutex
	records map[string]ApprovalRecord
}

func newSyncApprovalStore() *syncApprovalStore {
	return &syncApprovalStore{records: map[string]ApprovalRecord{}}
}

func (s *syncApprovalStore) Save(_ context.Context, r ApprovalRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[r.ApprovalID] = r
	return nil
}

func (s *syncApprovalStore) Get(_ context.Context, id string) (*ApprovalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return nil, ErrApprovalNotFound
	}
	return &r, nil
}

func (s *syncApprovalStore) Update(ctx context.Context, r ApprovalRecord) error {
	return s.Save(ctx, r)
}

func hitlSetup(t *testing.T, responses ...*models.LLMResponse) (*AdvancedAgentEngine, *scriptedProvider, *syncApprovalStore, *countingTool) {
	t.Helper()
	p := &scriptedProvider{responses: responses}
	reg := NewToolRegistry()
	inner := &countingTool{name: "wire_money", result: map[string]string{"status": "sent"}}
	if err := reg.Register(NewApprovalTool(inner)); err != nil {
		t.Fatal(err)
	}
	store := newSyncApprovalStore()
	return NewAdvancedAgentEngine(p, reg, store), p, store, inner
}

func TestHITLPauseStoresStateAndResumeContinues(t *testing.T) {
	e, p, store, inner := hitlSetup(t,
		callResp("r1", &models.Usage{PromptTokens: 5, CompletionTokens: 1, TotalTokens: 6}, tc("c1", "wire_money", `{"amount":10}`)),
		&models.LLMResponse{ID: "r2", Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: ptr("money sent")}, FinishReason: "stop"}}, Usage: &models.Usage{PromptTokens: 9, CompletionTokens: 2, TotalTokens: 11}},
	)
	resp, id, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil || resp != nil || len(id) != 32 {
		t.Fatalf("expected pause with a 128-bit hex approval id, got resp=%v id=%q err=%v", resp, id, err)
	}
	if inner.calls.Load() != 0 {
		t.Fatal("tool executed before approval")
	}
	rec := store.records[id]
	if rec.Request == nil || len(rec.Request.Messages) != 2 || rec.Request.Messages[1].Role != models.RoleAssistant {
		t.Fatalf("pending conversation state not stored: %+v", rec.Request)
	}
	if rec.Status != ApprovalStatusPending || rec.ExpiresAt.Before(time.Now()) || len(rec.ToolCalls) != 1 {
		t.Fatalf("bad record %+v", rec)
	}

	// The HTTP handler passes an empty request: the stored state must suffice.
	final, err := e.ResumeApproval(context.Background(), id, true, &models.LLMRequest{})
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if *final.Choices[0].Message.Content != "money sent" {
		t.Fatalf("unexpected final answer %+v", final)
	}
	if final.Usage == nil || final.Usage.TotalTokens != 17 {
		t.Fatalf("usage not aggregated across the pause: %+v", final.Usage)
	}
	if inner.calls.Load() != 1 {
		t.Fatalf("approved tool should run exactly once, ran %d", inner.calls.Load())
	}
	msgs := p.requests[1].Messages
	if len(msgs) != 3 || msgs[2].Role != models.RoleTool || !strings.Contains(*msgs[2].Content, "sent") {
		t.Fatalf("tool result not fed back on resume: %+v", msgs)
	}
	if _, err := e.ResumeApproval(context.Background(), id, true, nil); !errors.Is(err, ErrApprovalProcessed) {
		t.Fatalf("second resume must fail with ErrApprovalProcessed, got %v", err)
	}
}

func TestHITLRejectContinuesWithoutExecuting(t *testing.T) {
	e, p, _, inner := hitlSetup(t, callResp("r1", nil, tc("c1", "wire_money", `{}`)))
	_, id, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil || id == "" {
		t.Fatalf("expected pause: %v", err)
	}
	resp, err := e.ResumeApproval(context.Background(), id, false, nil)
	if err != nil {
		t.Fatalf("rejection should let the model answer: %v", err)
	}
	if *resp.Choices[0].Message.Content != "done" || inner.calls.Load() != 0 {
		t.Fatalf("rejected tool must not run: %d", inner.calls.Load())
	}
	if !strings.Contains(*p.requests[1].Messages[2].Content, "rejected") {
		t.Fatalf("model not told about the rejection: %q", *p.requests[1].Messages[2].Content)
	}
}

func TestHITLExpiredAndForbidden(t *testing.T) {
	e, _, store, _ := hitlSetup(t, callResp("r1", nil, tc("c1", "wire_money", `{}`)), callResp("r2", nil, tc("c2", "wire_money", `{}`)))
	owner := WithPrincipal(context.Background(), "sk-alice")
	_, id, err := e.RunToolExecutionLoopWithHITL(owner, userReq("wire_money"))
	if err != nil {
		t.Fatal(err)
	}
	if store.records[id].Owner == "" || strings.Contains(store.records[id].Owner, "alice") {
		t.Fatalf("owner must be stored as a fingerprint, got %q", store.records[id].Owner)
	}
	if _, err := e.ResumeApproval(WithPrincipal(context.Background(), "sk-mallory"), id, true, nil); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("expected ErrApprovalForbidden, got %v", err)
	}
	if _, err := e.ResumeApproval(context.Background(), id, true, nil); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("anonymous caller must not resume an owned approval, got %v", err)
	}

	_, id2, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil {
		t.Fatal(err)
	}
	rec := store.records[id2]
	rec.ExpiresAt = time.Now().Add(-time.Minute)
	store.records[id2] = rec
	if _, err := e.ResumeApproval(context.Background(), id2, true, nil); !errors.Is(err, ErrApprovalExpired) {
		t.Fatalf("expected ErrApprovalExpired, got %v", err)
	}
	if store.records[id2].Status != ApprovalStatusExpired {
		t.Fatalf("expired approval not marked: %s", store.records[id2].Status)
	}
	if _, err := e.ResumeApproval(context.Background(), "../../etc", true, nil); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("malformed id should be rejected, got %v", err)
	}
}

func TestHITLConcurrentResumeRunsOnce(t *testing.T) {
	e, _, _, inner := hitlSetup(t, callResp("r1", nil, tc("c1", "wire_money", `{}`)))
	_, id, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil {
		t.Fatal(err)
	}
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.ResumeApproval(context.Background(), id, true, nil); err == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || inner.calls.Load() != 1 {
		t.Fatalf("approval resolved %d times, tool ran %d times; want 1/1", ok.Load(), inner.calls.Load())
	}
}

func TestHITLResumeWithoutProviderKeepsApprovalPending(t *testing.T) {
	store := newSyncApprovalStore()
	reg := NewToolRegistry()
	_ = reg.Register(NewApprovalTool(&countingTool{name: "wire_money"}))
	e := NewAdvancedAgentEngine(nil, reg, store)
	_ = store.Save(context.Background(), ApprovalRecord{ApprovalID: "abc", Status: ApprovalStatusPending, Request: userReq("wire_money"), ToolCalls: []models.ToolCall{tc("c1", "wire_money", "{}")}})
	if _, err := e.ResumeApproval(context.Background(), "abc", true, nil); !errors.Is(err, ErrNoProvider) {
		t.Fatalf("expected ErrNoProvider, got %v", err)
	}
	if store.records["abc"].Status != ApprovalStatusPending {
		t.Fatal("approval consumed although it could not be continued")
	}
}

// fakeRedis implements cache.RedisClient plus SetNX over an in-memory map.
type fakeRedis struct {
	mu   sync.Mutex
	data map[string]string
	ttls map[string]time.Duration
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: map[string]string{}, ttls: map[string]time.Duration{}}
}

func (f *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringCmd(ctx)
	if v, ok := f.data[key]; ok {
		cmd.SetVal(v)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (f *fakeRedis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = fmt.Sprint(value)
	f.ttls[key] = ttl
	return redis.NewStatusCmd(ctx)
}

func (f *fakeRedis) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewBoolCmd(ctx)
	if _, ok := f.data[key]; ok {
		cmd.SetVal(false)
		return cmd
	}
	f.data[key] = fmt.Sprint(value)
	cmd.SetVal(true)
	return cmd
}

func (f *fakeRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return redis.NewIntCmd(ctx)
}
func (f *fakeRedis) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return redis.NewStringSliceCmd(ctx)
}
func (f *fakeRedis) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return redis.NewScanCmdResult(nil, 0, nil)
}
func (f *fakeRedis) Close() error { return nil }

func TestRedisApprovalStoreRoundTrip(t *testing.T) {
	rc := newFakeRedis()
	store := NewRedisApprovalStore(rc, "approval:", 0)
	ctx := context.Background()
	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expected ErrApprovalNotFound, got %v", err)
	}
	rec := ApprovalRecord{ApprovalID: "id1", Status: ApprovalStatusRejected, Error: errors.New("nope"), Request: userReq("x")}
	if err := store.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if rc.ttls["approval:id1"] != DefaultApprovalTTL {
		t.Fatalf("records must expire, ttl=%v", rc.ttls["approval:id1"])
	}
	got, err := store.Get(ctx, "id1")
	if err != nil {
		t.Fatalf("a record with an error must round-trip: %v", err)
	}
	if got.Error == nil || got.Error.Error() != "nope" || got.Request == nil || len(got.Request.Messages) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}
	if ok, _ := store.Claim(ctx, "id1"); !ok {
		t.Fatal("first claim should succeed")
	}
	if ok, _ := store.Claim(ctx, "id1"); ok {
		t.Fatal("second claim must fail")
	}
	if err := store.Save(ctx, ApprovalRecord{ApprovalID: "bad id*"}); err == nil {
		t.Fatal("invalid approval ids must be rejected")
	}
}

func TestHITLWithRedisStoreEndToEnd(t *testing.T) {
	p := &scriptedProvider{responses: []*models.LLMResponse{callResp("r1", nil, tc("c1", "wire_money", `{}`))}}
	reg := NewToolRegistry()
	inner := &countingTool{name: "wire_money"}
	_ = reg.Register(NewApprovalTool(inner))
	e := NewAdvancedAgentEngine(p, reg, NewRedisApprovalStore(newFakeRedis(), "approval:", time.Hour))
	_, id, err := e.RunToolExecutionLoopWithHITL(context.Background(), userReq("wire_money"))
	if err != nil || id == "" {
		t.Fatalf("pause failed: %v", err)
	}
	resp, err := e.ResumeApproval(context.Background(), id, true, nil)
	if err != nil || resp == nil || inner.calls.Load() != 1 {
		t.Fatalf("resume failed: resp=%v err=%v calls=%d", resp, err, inner.calls.Load())
	}
	if _, err := e.ResumeApproval(context.Background(), id, true, nil); !errors.Is(err, ErrApprovalProcessed) {
		t.Fatalf("expected ErrApprovalProcessed, got %v", err)
	}
}
