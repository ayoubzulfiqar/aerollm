package analytics

import (
	"context"
	"testing"
	"time"

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
	key1Bucket := report.ByAPIKey["sk-key1"]
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
