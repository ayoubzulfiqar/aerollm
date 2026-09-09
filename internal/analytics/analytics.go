package analytics

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// CostEntry represents a single billable transaction for analytics.
// This is the unit of data the AnalyticsEngine aggregates.
type CostEntry struct {
	RequestID   string
	APIKey      string
	CustomerID  string
	Model       string
	Provider    string
	TeamID      string
	PromptTokens int
	CompletionTokens int
	TotalTokens  int
	CostUSD      float64
	Timestamp    time.Time
}

// SpendReport aggregates spend over a time range, grouped by dimension.
type SpendReport struct {
	TotalCostUSD     float64                 `json:"total_cost_usd"`
	TotalRequests    int                     `json:"total_requests"`
	TotalTokens      SpendTokens             `json:"total_tokens"`
	ByAPIKey         map[string]*SpendBucket `json:"by_api_key,omitempty"`
	ByCustomer       map[string]*SpendBucket `json:"by_customer,omitempty"`
	ByTeam           map[string]*SpendBucket `json:"by_team,omitempty"`
	ByModel          map[string]*SpendBucket `json:"by_model,omitempty"`
	TimeRange        TimeRange               `json:"time_range"`
	GeneratedAt      time.Time               `json:"generated_at"`
}

// SpendTokens holds token usage aggregation.
type SpendTokens struct {
	Input   int `json:"input"`
	Output  int `json:"output"`
	Total   int `json:"total"`
}

// SpendBucket holds aggregated spend for a single dimension value.
type SpendBucket struct {
	CostUSD    float64 `json:"cost_usd"`
	Requests   int     `json:"requests"`
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	TotalTokens  int     `json:"total_tokens"`
}

// TimeRange defines the analytics query window.
type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// LogEntry is a single transaction log entry for /global/spend/logs.
type LogEntry struct {
	RequestID   string                 `json:"request_id"`
	APIKey      string                 `json:"api_key"`
	CustomerID  string                 `json:"customer_id"`
	Model       string                 `json:"model"`
	Provider    string                 `json:"provider"`
	TeamID      string                 `json:"team_id"`
	InputTokens int                    `json:"input_tokens"`
	OutputTokens int                   `json:"output_tokens"`
	CostUSD     float64                `json:"cost_usd"`
	Timestamp   time.Time              `json:"timestamp"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// SpendLogsResponse is the paginated response for /global/spend/logs.
type SpendLogsResponse struct {
	Logs      []LogEntry `json:"logs"`
	Total     int        `json:"total"`
	Page      int        `json:"page"`
	PageSize  int        `json:"page_size"`
	HasMore   bool       `json:"has_more"`
}

// AnalyticsEngine aggregates spend data from finops, ledger, and billing sources.
// It uses in-memory aggregation for time-series queries and supports grouping
// by api_key, customer, team, or model.
type AnalyticsEngine struct {
	mu     sync.RWMutex
	entries []CostEntry
}

// NewAnalyticsEngine creates a new analytics engine.
func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{
		entries: make([]CostEntry, 0),
	}
}

// Record stores a cost entry for later aggregation.
func (a *AnalyticsEngine) Record(entry CostEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, entry)
}

// RecordFromUsage converts a finops CostRequest + models.Usage into a CostEntry
// and stores it for analytics.
func (a *AnalyticsEngine) RecordFromUsage(reqID, apiKey, customerID, teamID, model, provider string, usage *models.Usage, cost float64) {
	var promptTokens, completionTokens, totalTokens int
	if usage != nil {
		promptTokens = usage.PromptTokens
		completionTokens = usage.CompletionTokens
		totalTokens = usage.TotalTokens
	}
	a.Record(CostEntry{
		RequestID:      reqID,
		APIKey:         apiKey,
		CustomerID:     customerID,
		TeamID:         teamID,
		Model:          model,
		Provider:       provider,
		PromptTokens:   promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:    totalTokens,
		CostUSD:        cost,
		Timestamp:      time.Now().UTC(),
	})
}

// RecordFromMeter converts billing.MeterEntry into a CostEntry.
func (a *AnalyticsEngine) RecordFromMeter(entry billing.MeterEntry, model, provider string, cost float64) {
	a.Record(CostEntry{
		CustomerID:     entry.CustomerID,
		Model:          model,
		Provider:       provider,
		CostUSD:        cost,
		Timestamp:      entry.Timestamp,
	})
}

// GenerateReport aggregates spend over the given time range and groups by
// the specified dimension ("api_key", "customer", "team", "model").
func (a *AnalyticsEngine) GenerateReport(ctx context.Context, tr TimeRange, groupBy string) (*SpendReport, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	report := &SpendReport{
		ByAPIKey:   make(map[string]*SpendBucket),
		ByCustomer: make(map[string]*SpendBucket),
		ByTeam:     make(map[string]*SpendBucket),
		ByModel:    make(map[string]*SpendBucket),
		TimeRange:  tr,
		GeneratedAt: time.Now().UTC(),
	}

	for _, entry := range a.entries {
		// Filter by time range.
		if !entry.Timestamp.IsZero() && (entry.Timestamp.Before(tr.Start) || entry.Timestamp.After(tr.End)) {
			continue
		}

		report.TotalCostUSD += entry.CostUSD
		report.TotalRequests++
		report.TotalTokens.Input += entry.PromptTokens
		report.TotalTokens.Output += entry.CompletionTokens
		report.TotalTokens.Total += entry.TotalTokens

		// Add to all grouping dimensions.
		a.addToBucket(report.ByAPIKey, entry.APIKey, entry)
		a.addToBucket(report.ByCustomer, entry.CustomerID, entry)
		a.addToBucket(report.ByTeam, entry.TeamID, entry)
		a.addToBucket(report.ByModel, entry.Model, entry)
	}

	// Zero out unused grouping maps based on the requested groupBy.
	switch groupBy {
	case "api_key", "":
		report.ByCustomer = nil
		report.ByTeam = nil
		report.ByModel = nil
	case "customer":
		report.ByAPIKey = nil
		report.ByTeam = nil
		report.ByModel = nil
	case "team":
		report.ByAPIKey = nil
		report.ByCustomer = nil
		report.ByModel = nil
	case "model":
		report.ByAPIKey = nil
		report.ByCustomer = nil
		report.ByTeam = nil
	}

	return report, nil
}

// addToBucket increments a bucket's counters for the given dimension value.
func (a *AnalyticsEngine) addToBucket(buckets map[string]*SpendBucket, key string, entry CostEntry) {
	if key == "" {
		return
	}
	if b, ok := buckets[key]; ok {
		b.CostUSD += entry.CostUSD
		b.Requests++
		b.InputTokens += entry.PromptTokens
		b.OutputTokens += entry.CompletionTokens
		b.TotalTokens += entry.TotalTokens
	} else {
		buckets[key] = &SpendBucket{
			CostUSD:      entry.CostUSD,
			Requests:     1,
			InputTokens:  entry.PromptTokens,
			OutputTokens: entry.CompletionTokens,
			TotalTokens:  entry.TotalTokens,
		}
	}
}

// GetLogs returns paginated transaction logs filtered by key or customer.
func (a *AnalyticsEngine) GetLogs(ctx context.Context, filter string, page, pageSize int) (*SpendLogsResponse, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 500 {
		pageSize = 500
	}

	// Filter entries.
	filtered := make([]CostEntry, 0, len(a.entries))
	for _, entry := range a.entries {
		if filter == "" || entry.APIKey == filter || entry.CustomerID == filter || entry.TeamID == filter {
			filtered = append(filtered, entry)
		}
	}

	// Sort by timestamp descending (newest first).
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.After(filtered[j].Timestamp)
	})

	total := len(filtered)
	startIdx := (page - 1) * pageSize
	if startIdx >= total {
		return &SpendLogsResponse{
			Logs:     []LogEntry{},
			Total:    total,
			Page:     page,
			PageSize: pageSize,
			HasMore:  false,
		}, nil
	}

	endIdx := startIdx + pageSize
	if endIdx > total {
		endIdx = total
	}

	logs := make([]LogEntry, 0, endIdx-startIdx)
	for _, e := range filtered[startIdx:endIdx] {
		logs = append(logs, LogEntry{
			RequestID:   e.RequestID,
			APIKey:      e.APIKey,
			CustomerID:  e.CustomerID,
			Model:       e.Model,
			Provider:    e.Provider,
			TeamID:      e.TeamID,
			InputTokens:   e.PromptTokens,
			OutputTokens:  e.CompletionTokens,
			CostUSD:     e.CostUSD,
			Timestamp:   e.Timestamp,
		})
	}

	return &SpendLogsResponse{
		Logs:     logs,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		HasMore:  endIdx < total,
	}, nil
}

// FromLedger converts ledger records into cost entries for analytics.
// This allows backfilling analytics from the append-only ledger.
func FromLedger(store ledger.LedgerStore, apiKey string) ([]CostEntry, error) {
	records, err := store.All(context.Background())
	if err != nil {
		return nil, err
	}
	entries := make([]CostEntry, 0, len(records))
	for _, rec := range records {
		entries = append(entries, CostEntry{
			APIKey:      apiKey,
			Timestamp:   rec.Timestamp,
		})
	}
	return entries, nil
}
