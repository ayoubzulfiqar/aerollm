package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// DefaultMaxEntries is the default retention (number of entries) of an
// AnalyticsEngine. Older entries are overwritten (ring buffer).
const DefaultMaxEntries = 100_000

// MaxPageSize bounds GetLogs/QueryLogs page sizes.
const MaxPageSize = 500

var (
	// ErrInvalidGroupBy is returned for an unsupported group_by dimension.
	ErrInvalidGroupBy = errors.New("analytics: invalid group_by")
	// ErrInvalidTimeRange is returned when Start is after End.
	ErrInvalidTimeRange = errors.New("analytics: start must not be after end")
)

// ValidGroupBy reports whether g is a supported GenerateReport dimension.
func ValidGroupBy(g string) bool {
	switch g {
	case "", "api_key", "customer", "team", "model", "provider", "all":
		return true
	}
	return false
}

// CostEntry represents a single billable transaction for analytics.
// APIKey is stored as a redacted label (see KeyLabel), never the raw key.
type CostEntry struct {
	RequestID        string
	APIKey           string
	CustomerID       string
	Model            string
	Provider         string
	TeamID           string
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CostUSD          float64
	Timestamp        time.Time
}

// SpendReport aggregates spend over a time range, grouped by dimension.
type SpendReport struct {
	TotalCostUSD  float64                 `json:"total_cost_usd"`
	TotalRequests int                     `json:"total_requests"`
	TotalTokens   SpendTokens             `json:"total_tokens"`
	ByAPIKey      map[string]*SpendBucket `json:"by_api_key,omitempty"`
	ByCustomer    map[string]*SpendBucket `json:"by_customer,omitempty"`
	ByTeam        map[string]*SpendBucket `json:"by_team,omitempty"`
	ByModel       map[string]*SpendBucket `json:"by_model,omitempty"`
	ByProvider    map[string]*SpendBucket `json:"by_provider,omitempty"`
	TimeRange     TimeRange               `json:"time_range"`
	GeneratedAt   time.Time               `json:"generated_at"`
	// DataSince is the timestamp of the oldest retained entry; spend before
	// it has been evicted from the in-memory window.
	DataSince time.Time `json:"data_since,omitempty"`
}

// SpendTokens holds token usage aggregation.
type SpendTokens struct {
	Input  int `json:"input"`
	Output int `json:"output"`
	Total  int `json:"total"`
}

// SpendBucket holds aggregated spend for a single dimension value.
type SpendBucket struct {
	CostUSD      float64 `json:"cost_usd"`
	Requests     int     `json:"requests"`
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	TotalTokens  int     `json:"total_tokens"`
}

// TimeRange defines the analytics query window (inclusive). A zero Start
// or End leaves that side unbounded.
type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (tr TimeRange) contains(t time.Time) bool {
	if !tr.Start.IsZero() && t.Before(tr.Start) {
		return false
	}
	if !tr.End.IsZero() && t.After(tr.End) {
		return false
	}
	return true
}

// LogEntry is a single transaction log entry for /global/spend/logs.
type LogEntry struct {
	RequestID    string                 `json:"request_id"`
	APIKey       string                 `json:"api_key"`
	CustomerID   string                 `json:"customer_id"`
	Model        string                 `json:"model"`
	Provider     string                 `json:"provider"`
	TeamID       string                 `json:"team_id"`
	InputTokens  int                    `json:"input_tokens"`
	OutputTokens int                    `json:"output_tokens"`
	CostUSD      float64                `json:"cost_usd"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// SpendLogsResponse is the paginated response for /global/spend/logs.
type SpendLogsResponse struct {
	Logs     []LogEntry `json:"logs"`
	Total    int        `json:"total"`
	Page     int        `json:"page"`
	PageSize int        `json:"page_size"`
	HasMore  bool       `json:"has_more"`
}

// LogQuery filters and paginates spend logs. Empty fields do not filter.
type LogQuery struct {
	// Filter matches api key (raw or label), customer ID or team ID.
	Filter     string
	APIKey     string
	CustomerID string
	TeamID     string
	Model      string
	Provider   string
	Range      TimeRange
	Page       int // 1-based, default 1
	PageSize   int // default 50, max MaxPageSize
}

// AnalyticsEngine aggregates spend data. It keeps a bounded, in-memory
// ring buffer of entries and is safe for concurrent use.
type AnalyticsEngine struct {
	mu      sync.RWMutex
	buf     []CostEntry
	head    int // index of the oldest entry
	size    int
	max     int
	evicted int64
	now     func() time.Time
}

// NewAnalyticsEngine creates an engine retaining DefaultMaxEntries entries.
func NewAnalyticsEngine() *AnalyticsEngine {
	return NewAnalyticsEngineWithCapacity(DefaultMaxEntries)
}

// NewAnalyticsEngineWithCapacity creates an engine retaining at most
// maxEntries entries (<= 0 selects DefaultMaxEntries).
func NewAnalyticsEngineWithCapacity(maxEntries int) *AnalyticsEngine {
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}
	return &AnalyticsEngine{max: maxEntries, now: time.Now}
}

var keyLabelRe = regexp.MustCompile(`^[^\s]{0,3}\.\.\.[^\s]{0,4}#[0-9a-f]{8}$`)

// KeyLabel converts an API key into a stable, redacted label such as
// "sk-...wxyz#1a2b3c4d" (last four characters plus a short hash so that
// distinct keys never collide in practice). A "Bearer " prefix is ignored
// and existing labels are returned unchanged.
func KeyLabel(apiKey string) string {
	k := strings.TrimSpace(apiKey)
	if len(k) > 7 && strings.EqualFold(k[:7], "bearer ") {
		k = strings.TrimSpace(k[7:])
	}
	if k == "" || keyLabelRe.MatchString(k) {
		return k
	}
	sum := sha256.Sum256([]byte("aerollm-key-label\x00" + k))
	h := hex.EncodeToString(sum[:4])
	if len(k) <= 8 {
		return "..." + "#" + h
	}
	return k[:3] + "..." + k[len(k)-4:] + "#" + h
}

func finiteNonNeg(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return 0
	}
	return f
}

// Record stores a cost entry for later aggregation. The API key is
// replaced by its KeyLabel, missing timestamps are set to now, negative
// token counts and invalid costs are clamped to zero.
func (a *AnalyticsEngine) Record(entry CostEntry) {
	entry.APIKey = KeyLabel(entry.APIKey)
	if entry.Timestamp.IsZero() {
		entry.Timestamp = a.now().UTC()
	}
	entry.PromptTokens = max(entry.PromptTokens, 0)
	entry.CompletionTokens = max(entry.CompletionTokens, 0)
	entry.TotalTokens = max(entry.TotalTokens, 0)
	if entry.TotalTokens == 0 {
		entry.TotalTokens = entry.PromptTokens + entry.CompletionTokens
	}
	entry.CostUSD = finiteNonNeg(entry.CostUSD)

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.buf == nil {
		a.buf = make([]CostEntry, 0, min(a.max, 1024))
	}
	if a.size < a.max {
		if len(a.buf) < a.max {
			a.buf = append(a.buf, entry)
		} else {
			a.buf[(a.head+a.size)%a.max] = entry
		}
		a.size++
		return
	}
	// Full: overwrite the oldest entry.
	a.buf[a.head] = entry
	a.head = (a.head + 1) % a.max
	a.evicted++
}

// at returns the i-th oldest entry; callers must hold the lock.
func (a *AnalyticsEngine) at(i int) *CostEntry {
	return &a.buf[(a.head+i)%len(a.buf)]
}

// Len returns the number of retained entries.
func (a *AnalyticsEngine) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.size
}

// Evicted returns how many entries were dropped due to the retention cap.
func (a *AnalyticsEngine) Evicted() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.evicted
}

// RecordFromUsage converts request usage into a CostEntry and stores it.
func (a *AnalyticsEngine) RecordFromUsage(reqID, apiKey, customerID, teamID, model, provider string, usage *models.Usage, cost float64) {
	var promptTokens, completionTokens, totalTokens int
	if usage != nil {
		promptTokens = usage.PromptTokens
		completionTokens = usage.CompletionTokens
		totalTokens = usage.TotalTokens
	}
	a.Record(CostEntry{
		RequestID:        reqID,
		APIKey:           apiKey,
		CustomerID:       customerID,
		TeamID:           teamID,
		Model:            model,
		Provider:         provider,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      totalTokens,
		CostUSD:          cost,
	})
}

// RecordFromMeter converts billing.MeterEntry into a CostEntry.
func (a *AnalyticsEngine) RecordFromMeter(entry billing.MeterEntry, model, provider string, cost float64) {
	a.Record(CostEntry{
		CustomerID: entry.CustomerID,
		Model:      model,
		Provider:   provider,
		CostUSD:    cost,
		Timestamp:  entry.Timestamp,
	})
}

// GenerateReport aggregates spend over the given time range and groups by
// the requested dimension: "api_key" (default), "customer", "team",
// "model", "provider" or "all". It returns ErrInvalidGroupBy /
// ErrInvalidTimeRange (wrapped) for bad input.
func (a *AnalyticsEngine) GenerateReport(ctx context.Context, tr TimeRange, groupBy string) (*SpendReport, error) {
	if !ValidGroupBy(groupBy) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidGroupBy, groupBy)
	}
	if !tr.Start.IsZero() && !tr.End.IsZero() && tr.Start.After(tr.End) {
		return nil, ErrInvalidTimeRange
	}
	report := &SpendReport{TimeRange: tr, GeneratedAt: a.now().UTC()}
	want := func(dim string) bool {
		return groupBy == dim || groupBy == "all" || (groupBy == "" && dim == "api_key")
	}
	if want("api_key") {
		report.ByAPIKey = make(map[string]*SpendBucket)
	}
	if want("customer") {
		report.ByCustomer = make(map[string]*SpendBucket)
	}
	if want("team") {
		report.ByTeam = make(map[string]*SpendBucket)
	}
	if want("model") {
		report.ByModel = make(map[string]*SpendBucket)
	}
	if want("provider") {
		report.ByProvider = make(map[string]*SpendBucket)
	}

	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.size > 0 {
		report.DataSince = a.at(0).Timestamp
	}
	for i := 0; i < a.size; i++ {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		entry := a.at(i)
		if entry.Timestamp.Before(report.DataSince) {
			report.DataSince = entry.Timestamp
		}
		if !tr.contains(entry.Timestamp) {
			continue
		}
		report.TotalCostUSD += entry.CostUSD
		report.TotalRequests++
		report.TotalTokens.Input += entry.PromptTokens
		report.TotalTokens.Output += entry.CompletionTokens
		report.TotalTokens.Total += entry.TotalTokens

		addToBucket(report.ByAPIKey, entry.APIKey, entry)
		addToBucket(report.ByCustomer, entry.CustomerID, entry)
		addToBucket(report.ByTeam, entry.TeamID, entry)
		addToBucket(report.ByModel, entry.Model, entry)
		addToBucket(report.ByProvider, entry.Provider, entry)
	}
	return report, nil
}

// addToBucket increments a bucket's counters for the given dimension value.
func addToBucket(buckets map[string]*SpendBucket, key string, entry *CostEntry) {
	if buckets == nil || key == "" {
		return
	}
	b, ok := buckets[key]
	if !ok {
		b = &SpendBucket{}
		buckets[key] = b
	}
	b.CostUSD += entry.CostUSD
	b.Requests++
	b.InputTokens += entry.PromptTokens
	b.OutputTokens += entry.CompletionTokens
	b.TotalTokens += entry.TotalTokens
}

// GetLogs returns paginated transaction logs (newest first) filtered by api
// key, customer or team.
func (a *AnalyticsEngine) GetLogs(ctx context.Context, filter string, page, pageSize int) (*SpendLogsResponse, error) {
	return a.QueryLogs(ctx, LogQuery{Filter: filter, Page: page, PageSize: pageSize})
}

// QueryLogs returns paginated transaction logs (newest first).
func (a *AnalyticsEngine) QueryLogs(ctx context.Context, q LogQuery) (*SpendLogsResponse, error) {
	if q.Page < 1 {
		q.Page = 1
	}
	if q.PageSize < 1 {
		q.PageSize = 50
	}
	if q.PageSize > MaxPageSize {
		q.PageSize = MaxPageSize
	}
	if !q.Range.Start.IsZero() && !q.Range.End.IsZero() && q.Range.Start.After(q.Range.End) {
		return nil, ErrInvalidTimeRange
	}
	filterLabel := KeyLabel(q.Filter)
	apiKeyLabel := KeyLabel(q.APIKey)

	a.mu.RLock()
	defer a.mu.RUnlock()

	matches := make([]int, 0, 64)
	for i := 0; i < a.size; i++ {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		e := a.at(i)
		if q.Filter != "" && e.APIKey != filterLabel && e.CustomerID != q.Filter && e.TeamID != q.Filter {
			continue
		}
		if (q.APIKey != "" && e.APIKey != apiKeyLabel) ||
			(q.CustomerID != "" && e.CustomerID != q.CustomerID) ||
			(q.TeamID != "" && e.TeamID != q.TeamID) ||
			(q.Model != "" && e.Model != q.Model) ||
			(q.Provider != "" && e.Provider != q.Provider) ||
			!q.Range.contains(e.Timestamp) {
			continue
		}
		matches = append(matches, i)
	}
	// Newest first; ties keep reverse insertion order.
	sort.SliceStable(matches, func(x, y int) bool {
		tx, ty := a.at(matches[x]).Timestamp, a.at(matches[y]).Timestamp
		if tx.Equal(ty) {
			return matches[x] > matches[y]
		}
		return tx.After(ty)
	})

	total := len(matches)
	resp := &SpendLogsResponse{Logs: []LogEntry{}, Total: total, Page: q.Page, PageSize: q.PageSize}
	// Overflow-safe offset computation.
	if q.Page-1 > total/q.PageSize {
		return resp, nil
	}
	startIdx := (q.Page - 1) * q.PageSize
	if startIdx >= total {
		return resp, nil
	}
	endIdx := min(startIdx+q.PageSize, total)
	for _, idx := range matches[startIdx:endIdx] {
		e := a.at(idx)
		resp.Logs = append(resp.Logs, LogEntry{
			RequestID:    e.RequestID,
			APIKey:       e.APIKey,
			CustomerID:   e.CustomerID,
			Model:        e.Model,
			Provider:     e.Provider,
			TeamID:       e.TeamID,
			InputTokens:  e.PromptTokens,
			OutputTokens: e.CompletionTokens,
			CostUSD:      e.CostUSD,
			Timestamp:    e.Timestamp,
		})
	}
	resp.HasMore = endIdx < total
	return resp, nil
}

// FromLedger converts ledger records into cost entries for analytics
// backfilling. Model and token usage are recovered from the recorded
// request/response payloads when present; cost and provider are read from
// record metadata ("cost_usd", "provider") when available.
func FromLedger(store ledger.LedgerStore, apiKey string) ([]CostEntry, error) {
	if store == nil {
		return nil, errors.New("analytics: nil ledger store")
	}
	records, err := store.All(context.Background())
	if err != nil {
		return nil, err
	}
	entries := make([]CostEntry, 0, len(records))
	for _, rec := range records {
		e := CostEntry{APIKey: KeyLabel(apiKey), Timestamp: rec.Timestamp}
		var req struct {
			Model string `json:"model"`
		}
		if rec.RequestPayload != "" && json.Unmarshal([]byte(rec.RequestPayload), &req) == nil {
			e.Model = req.Model
		}
		var resp struct {
			ID    string        `json:"id"`
			Model string        `json:"model"`
			Usage *models.Usage `json:"usage"`
		}
		if rec.ResponsePayload != "" && json.Unmarshal([]byte(rec.ResponsePayload), &resp) == nil {
			e.RequestID = resp.ID
			if e.Model == "" {
				e.Model = resp.Model
			}
			if resp.Usage != nil {
				e.PromptTokens = max(resp.Usage.PromptTokens, 0)
				e.CompletionTokens = max(resp.Usage.CompletionTokens, 0)
				e.TotalTokens = max(resp.Usage.TotalTokens, 0)
			}
		}
		if v, ok := rec.Metadata["cost_usd"].(float64); ok {
			e.CostUSD = finiteNonNeg(v)
		}
		if v, ok := rec.Metadata["provider"].(string); ok {
			e.Provider = v
		}
		entries = append(entries, e)
	}
	return entries, nil
}
