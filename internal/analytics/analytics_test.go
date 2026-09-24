package analytics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// TestRecordAndReport tests basic record + report generation with grouping.
func TestRecordAndReport(t *testing.T) {
	engine := NewAnalyticsEngine()
	usage := &models.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}

	engine.RecordFromUsage("req-1", "sk-key1", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.05)
	engine.RecordFromUsage("req-2", "sk-key2", "cust-2", "team-b", "claude-3", "anthropic", usage, 0.03)
	engine.RecordFromUsage("req-3", "sk-key1", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.04)

	ctx := context.Background()
	now := time.Now().UTC()
	tr := TimeRange{
		Start: now.Add(-1 * time.Hour),
		End:   now.Add(1 * time.Hour),
	}

	// Test api_key grouping.
	report, err := engine.GenerateReport(ctx, tr, "api_key")
	if err != nil {
		t.Fatalf("GenerateReport failed: %v", err)
	}
	if report.TotalCostUSD != 0.12 {
		t.Errorf("expected total cost 0.12, got %f", report.TotalCostUSD)
	}
	if report.TotalRequests != 3 {
		t.Errorf("expected 3 requests, got %d", report.TotalRequests)
	}
	if len(report.ByAPIKey) != 2 {
		t.Errorf("expected 2 api_key buckets, got %d", len(report.ByAPIKey))
	}
	key1Bucket := report.ByAPIKey[KeyLabel("sk-key1")]
	if key1Bucket == nil {
		t.Fatal("expected sk-key1 bucket")
	}
	if key1Bucket.Requests != 2 {
		t.Errorf("expected 2 requests for sk-key1, got %d", key1Bucket.Requests)
	}
	if key1Bucket.CostUSD != 0.09 {
		t.Errorf("expected cost 0.09 for sk-key1, got %f", key1Bucket.CostUSD)
	}
}

// TestReportGrouping tests all grouping dimensions.
func TestReportGrouping(t *testing.T) {
	engine := NewAnalyticsEngine()
	usage := &models.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}

	engine.RecordFromUsage("req-1", "sk-key1", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.05)
	engine.RecordFromUsage("req-2", "sk-key2", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.03)
	engine.RecordFromUsage("req-3", "sk-key1", "cust-2", "team-b", "claude-3", "anthropic", usage, 0.04)

	ctx := context.Background()
	now := time.Now().UTC()
	tr := TimeRange{Start: now.Add(-1 * time.Hour), End: now.Add(1 * time.Hour)}

	// Test customer grouping.
	report, _ := engine.GenerateReport(ctx, tr, "customer")
	if len(report.ByCustomer) != 2 {
		t.Errorf("expected 2 customer buckets, got %d", len(report.ByCustomer))
	}
	if len(report.ByAPIKey) != 0 {
		t.Errorf("expected no api_key buckets for customer grouping")
	}

	// Test model grouping.
	report, _ = engine.GenerateReport(ctx, tr, "model")
	if len(report.ByModel) != 2 {
		t.Errorf("expected 2 model buckets, got %d", len(report.ByModel))
	}

	// Test team grouping.
	report, _ = engine.GenerateReport(ctx, tr, "team")
	if len(report.ByTeam) != 2 {
		t.Errorf("expected 2 team buckets, got %d", len(report.ByTeam))
	}
}

// TestTimeRangeFilter tests that entries outside the time range are excluded.
func TestTimeRangeFilter(t *testing.T) {
	engine := NewAnalyticsEngine()
	past := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	engine.Record(CostEntry{APIKey: "sk-old", CostUSD: 0.10, Timestamp: past})
	engine.Record(CostEntry{APIKey: "sk-new", CostUSD: 0.05, Timestamp: recent})

	ctx := context.Background()
	tr := TimeRange{
		Start: time.Now().Add(-24 * time.Hour),
		End:   time.Now(),
	}

	report, err := engine.GenerateReport(ctx, tr, "api_key")
	if err != nil {
		t.Fatalf("GenerateReport failed: %v", err)
	}
	if report.TotalRequests != 1 {
		t.Errorf("expected 1 request in time range, got %d", report.TotalRequests)
	}
	if report.TotalCostUSD != 0.05 {
		t.Errorf("expected cost 0.05, got %f", report.TotalCostUSD)
	}
}

// TestGetLogs tests pagination and filtering of spend logs.
func TestGetLogs(t *testing.T) {
	engine := NewAnalyticsEngine()
	usage := &models.Usage{}

	for i := 0; i < 10; i++ {
		engine.RecordFromUsage("req", "sk-key1", "cust-1", "", "gpt-4o", "openai", usage, float64(i)*0.01)
	}
	engine.RecordFromUsage("req", "sk-key2", "cust-2", "", "claude-3", "anthropic", usage, 0.50)

	ctx := context.Background()

	// Test filtering by api_key.
	logs, err := engine.GetLogs(ctx, "sk-key1", 1, 50)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}
	if logs.Total != 10 {
		t.Errorf("expected 10 logs for sk-key1, got %d", logs.Total)
	}
	if len(logs.Logs) != 10 {
		t.Errorf("expected 10 log entries, got %d", len(logs.Logs))
	}
	if logs.Page != 1 {
		t.Errorf("expected page 1, got %d", logs.Page)
	}
	if logs.HasMore {
		t.Error("expected no more pages")
	}

	// Test pagination.
	logs, err = engine.GetLogs(ctx, "sk-key1", 1, 5)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}
	if len(logs.Logs) != 5 {
		t.Errorf("expected 5 log entries on page 1, got %d", len(logs.Logs))
	}
	if !logs.HasMore {
		t.Error("expected has_more=true for page 1")
	}

	logs, err = engine.GetLogs(ctx, "sk-key1", 2, 5)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}
	if len(logs.Logs) != 5 {
		t.Errorf("expected 5 log entries on page 2, got %d", len(logs.Logs))
	}
	if logs.HasMore {
		t.Error("expected no more pages on page 2")
	}

	// Test filtering by customer.
	logs, err = engine.GetLogs(ctx, "cust-2", 1, 50)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}
	if logs.Total != 1 {
		t.Errorf("expected 1 log for cust-2, got %d", logs.Total)
	}
}

// TestTokenAggregation tests that token counts are properly aggregated.
func TestTokenAggregation(t *testing.T) {
	engine := NewAnalyticsEngine()
	usage := &models.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}

	engine.RecordFromUsage("req-1", "sk-key1", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.05)
	engine.RecordFromUsage("req-2", "sk-key1", "cust-1", "team-a", "gpt-4o", "openai", usage, 0.03)

	ctx := context.Background()
	now := time.Now().UTC()
	tr := TimeRange{Start: now.Add(-1 * time.Hour), End: now.Add(1 * time.Hour)}

	report, _ := engine.GenerateReport(ctx, tr, "api_key")

	if report.TotalTokens.Input != 200 {
		t.Errorf("expected 200 input tokens, got %d", report.TotalTokens.Input)
	}
	if report.TotalTokens.Output != 100 {
		t.Errorf("expected 100 output tokens, got %d", report.TotalTokens.Output)
	}
	if report.TotalTokens.Total != 300 {
		t.Errorf("expected 300 total tokens, got %d", report.TotalTokens.Total)
	}
}

func TestRingBufferBoundsMemory(t *testing.T) {
	engine := NewAnalyticsEngineWithCapacity(5)
	for i := 0; i < 12; i++ {
		engine.Record(CostEntry{RequestID: fmt.Sprint(i), CostUSD: 1, Timestamp: time.Unix(int64(1000+i), 0)})
	}
	if engine.Len() != 5 || engine.Evicted() != 7 {
		t.Fatalf("len=%d evicted=%d", engine.Len(), engine.Evicted())
	}
	logs, _ := engine.GetLogs(context.Background(), "", 1, 50)
	if logs.Total != 5 || logs.Logs[0].RequestID != "11" || logs.Logs[4].RequestID != "7" {
		t.Fatalf("unexpected retained logs %+v", logs.Logs)
	}
	report, _ := engine.GenerateReport(context.Background(), TimeRange{}, "api_key")
	if report.TotalRequests != 5 || !report.DataSince.Equal(time.Unix(1007, 0)) {
		t.Fatalf("unexpected report %+v", report)
	}
}

func TestReportValidationAndProviderGrouping(t *testing.T) {
	engine := NewAnalyticsEngine()
	usage := &models.Usage{PromptTokens: 1, CompletionTokens: 2}
	engine.RecordFromUsage("r1", "sk-a", "c", "t", "gpt-4o", "openai", usage, 0.1)
	engine.RecordFromUsage("r2", "sk-b", "c", "t", "claude-3", "anthropic", usage, 0.2)
	ctx := context.Background()
	if _, err := engine.GenerateReport(ctx, TimeRange{}, "nope"); !errors.Is(err, ErrInvalidGroupBy) {
		t.Fatalf("expected ErrInvalidGroupBy, got %v", err)
	}
	now := time.Now()
	if _, err := engine.GenerateReport(ctx, TimeRange{Start: now, End: now.Add(-time.Hour)}, ""); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("expected ErrInvalidTimeRange, got %v", err)
	}
	r, err := engine.GenerateReport(ctx, TimeRange{}, "provider")
	if err != nil || len(r.ByProvider) != 2 || r.ByProvider["openai"].Requests != 1 || r.ByAPIKey != nil {
		t.Fatalf("provider grouping failed: %+v %v", r, err)
	}
	if r.TotalTokens.Total != 6 {
		t.Fatalf("total tokens must default to prompt+completion, got %d", r.TotalTokens.Total)
	}
	all, _ := engine.GenerateReport(ctx, TimeRange{}, "all")
	if len(all.ByModel) != 2 || len(all.ByAPIKey) != 2 || len(all.ByTeam) != 1 {
		t.Fatalf("all grouping failed: %+v", all)
	}
}

func TestZeroTimestampIsNotAlwaysInRange(t *testing.T) {
	engine := NewAnalyticsEngine()
	engine.Record(CostEntry{APIKey: "k", CostUSD: 1}) // stamped "now"
	past := time.Now().Add(-48 * time.Hour)
	r, _ := engine.GenerateReport(context.Background(), TimeRange{Start: past.Add(-time.Hour), End: past}, "")
	if r.TotalRequests != 0 {
		t.Fatal("entry without timestamp leaked into an unrelated time range")
	}
}

func TestPaginationOverflowSafe(t *testing.T) {
	engine := NewAnalyticsEngine()
	engine.Record(CostEntry{APIKey: "k"})
	for _, page := range []int{math.MaxInt, math.MaxInt / 2, 1 << 40} {
		logs, err := engine.GetLogs(context.Background(), "", page, 500)
		if err != nil || len(logs.Logs) != 0 || logs.HasMore {
			t.Fatalf("page %d: unexpected %+v %v", page, logs, err)
		}
	}
	logs, _ := engine.GetLogs(context.Background(), "", 1, 1_000_000)
	if logs.PageSize != MaxPageSize {
		t.Fatalf("page size must be capped, got %d", logs.PageSize)
	}
}

func TestAPIKeysAreRedacted(t *testing.T) {
	engine := NewAnalyticsEngine()
	const secret = "sk-live-supersecretvalue-9876"
	engine.RecordFromUsage("r", "Bearer "+secret, "", "", "m", "p", nil, 0.5)
	logs, _ := engine.GetLogs(context.Background(), secret, 1, 10)
	if logs.Total != 1 {
		t.Fatal("filtering by the raw key must still work")
	}
	if strings.Contains(logs.Logs[0].APIKey, "supersecret") || !strings.HasSuffix(strings.Split(logs.Logs[0].APIKey, "#")[0], "9876") {
		t.Fatalf("api key not redacted: %q", logs.Logs[0].APIKey)
	}
	byLabel, _ := engine.GetLogs(context.Background(), logs.Logs[0].APIKey, 1, 10)
	if byLabel.Total != 1 {
		t.Fatal("filtering by label must work")
	}
	if KeyLabel(KeyLabel(secret)) != KeyLabel(secret) || KeyLabel("sk-a") == KeyLabel("sk-b") {
		t.Fatal("labels must be idempotent and distinct")
	}
}

func TestQueryLogsFilters(t *testing.T) {
	engine := NewAnalyticsEngine()
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	engine.Record(CostEntry{APIKey: "sk-1", Model: "a", Provider: "x", Timestamp: base})
	engine.Record(CostEntry{APIKey: "sk-1", Model: "b", Provider: "y", Timestamp: base.Add(time.Hour)})
	engine.Record(CostEntry{APIKey: "sk-2", Model: "a", Provider: "x", Timestamp: base.Add(2 * time.Hour)})
	ctx := context.Background()
	res, _ := engine.QueryLogs(ctx, LogQuery{Model: "a"})
	if res.Total != 2 || !res.Logs[0].Timestamp.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("model filter/order wrong: %+v", res)
	}
	res, _ = engine.QueryLogs(ctx, LogQuery{APIKey: "sk-1", Range: TimeRange{Start: base.Add(30 * time.Minute)}})
	if res.Total != 1 || res.Logs[0].Model != "b" {
		t.Fatalf("api key + range filter wrong: %+v", res)
	}
	if _, err := engine.QueryLogs(ctx, LogQuery{Range: TimeRange{Start: base, End: base.Add(-1)}}); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatal("expected invalid range error")
	}
}

func TestConcurrentRecordAndQuery(t *testing.T) {
	engine := NewAnalyticsEngineWithCapacity(1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				engine.RecordFromUsage("r", fmt.Sprintf("sk-%d", g), "", "", "m", "p", &models.Usage{PromptTokens: 1}, 0.001)
				if i%50 == 0 {
					_, _ = engine.GenerateReport(context.Background(), TimeRange{}, "all")
					_, _ = engine.GetLogs(context.Background(), "", 1, 10)
				}
			}
		}(g)
	}
	wg.Wait()
	if engine.Len() != 1000 || engine.Evicted() != 3000 {
		t.Fatalf("len=%d evicted=%d", engine.Len(), engine.Evicted())
	}
}

func TestFromLedgerParsesPayloads(t *testing.T) {
	store := ledger.NewInMemoryLedgerStore()
	ts := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Append(context.Background(), ledger.LedgerRecord{
		Timestamp:       ts,
		RequestPayload:  `{"model":"gpt-4o","messages":[]}`,
		ResponsePayload: `{"id":"resp-1","usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
		Metadata:        map[string]interface{}{"cost_usd": 0.25, "provider": "openai"},
	})
	entries, err := FromLedger(store, "sk-ledger-key-123")
	if err != nil || len(entries) != 1 {
		t.Fatalf("FromLedger: %v %d", err, len(entries))
	}
	e := entries[0]
	if e.Model != "gpt-4o" || e.RequestID != "resp-1" || e.TotalTokens != 7 || e.CostUSD != 0.25 || e.Provider != "openai" || strings.Contains(e.APIKey, "ledger-key") {
		t.Fatalf("unexpected entry %+v", e)
	}
	if _, err := FromLedger(nil, ""); err == nil {
		t.Fatal("nil store must error")
	}
}
