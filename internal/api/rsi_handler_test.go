package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/rsi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type testLedgerReader struct {
	records []ledger.LedgerRecord
}

func (m *testLedgerReader) All(_ context.Context) ([]ledger.LedgerRecord, error) {
	out := make([]ledger.LedgerRecord, len(m.records))
	copy(out, m.records)
	return out, nil
}

func (m *testLedgerReader) Latest(_ context.Context) (*ledger.LedgerRecord, error) {
	if len(m.records) == 0 {
		return nil, nil
	}
	r := m.records[len(m.records)-1]
	return &r, nil
}

type testCostCalculator struct{}

func (t *testCostCalculator) CalculateCost(model string, usage *models.Usage) float64 {
	return 0.01
}

type testProviderLister struct{}

func (t *testProviderLister) ProviderNames() []string {
	return []string{"openai", "anthropic"}
}

func newTestOrchestrator(records []ledger.LedgerRecord) *rsi.RSIOrchestrator {
	return rsi.NewRSIOrchestrator(
		&testLedgerReader{records: records},
		nil, // metrics — nil is OK for non-RUN cycles
		&testCostCalculator{},
		&testProviderLister{},
		rsi.DefaultRSIConfig(),
	)
}

// ---------------------------------------------------------------------------
// Headroom Handler Tests
// ---------------------------------------------------------------------------

func TestRSIHandler_Headroom_NilOrchestrator(t *testing.T) {
	h := &RSIHandler{}
	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/headroom", nil)
	w := httptest.NewRecorder()
	h.Headroom()(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestRSIHandler_Headroom_WithOrchestrator(t *testing.T) {
	records := makeSimpleLedgerRecords(5, time.Now())
	orch := newTestOrchestrator(records)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/headroom", nil)
	w := httptest.NewRecorder()
	h.Headroom()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	assert.Len(t, out, 6, "should have all 6 dimensions")
}

// ---------------------------------------------------------------------------
// Cycles Handler Tests
// ---------------------------------------------------------------------------

func TestRSIHandler_Cycles_Empty(t *testing.T) {
	orch := newTestOrchestrator(nil)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/cycles", nil)
	w := httptest.NewRecorder()
	h.Cycles()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "[]\n", w.Body.String())
}

func TestRSIHandler_Cycles_WithHistory(t *testing.T) {
	records := makeSimpleLedgerRecords(10, time.Now())
	orch := newTestOrchestrator(records)
	// Run a cycle to populate history.
	orch.SetConfig(rsi.RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         2,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       -1.0,
	})
	_, _ = orch.RunCycle(context.Background())

	h := NewRSIHandler(orch)
	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/cycles", nil)
	w := httptest.NewRecorder()
	h.Cycles()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var out []map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	assert.GreaterOrEqual(t, len(out), 1)
}

// ---------------------------------------------------------------------------
// CurrentCycle Handler Tests
// ---------------------------------------------------------------------------

func TestRSIHandler_CurrentCycle_Nil(t *testing.T) {
	orch := newTestOrchestrator(nil)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/current", nil)
	w := httptest.NewRecorder()
	h.CurrentCycle()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "null\n", w.Body.String())
}

func TestRSIHandler_CurrentCycle_WithCycle(t *testing.T) {
	records := makeSimpleLedgerRecords(10, time.Now())
	orch := newTestOrchestrator(records)
	orch.SetConfig(rsi.RSIConfig{
		ImprovementThresholdPct: 1_000_000.0,
		BroadIterations:         2,
		DeepIterations:          1,
		KFold:                   2,
		HeadroomThreshold:       -1.0,
	})
	_, _ = orch.RunCycle(context.Background())

	h := NewRSIHandler(orch)
	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/current", nil)
	w := httptest.NewRecorder()
	h.CurrentCycle()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	assert.NotZero(t, out["id"])
}

// ---------------------------------------------------------------------------
// Config Handler Tests
// ---------------------------------------------------------------------------

func TestRSIHandler_GetConfig(t *testing.T) {
	orch := newTestOrchestrator(nil)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodGet, "/v1/rsi/config", nil)
	w := httptest.NewRecorder()
	h.Config()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var cfg rsi.RSIConfig
	require.NoError(t, json.NewDecoder(w.Body).Decode(&cfg))
	assert.Equal(t, 5.0, cfg.ImprovementThresholdPct)
}

func TestRSIHandler_UpdateConfig(t *testing.T) {
	orch := newTestOrchestrator(nil)
	h := NewRSIHandler(orch)

	body := `{"improvement_threshold_pct": 10.0, "broad_iterations": 20, "k_fold": 5}`
	req := httptest.NewRequest(http.MethodPut, "/v1/rsi/config", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Config()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "ok", resp["status"])

	cfg := orch.Config()
	assert.Equal(t, 10.0, cfg.ImprovementThresholdPct)
	assert.Equal(t, 20, cfg.BroadIterations)
	assert.Equal(t, 5, cfg.KFold)
}

func TestRSIHandler_UpdateConfig_InvalidJSON(t *testing.T) {
	orch := newTestOrchestrator(nil)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodPut, "/v1/rsi/config", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	h.Config()(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// ---------------------------------------------------------------------------
// TriggerCycle Handler Tests
// ---------------------------------------------------------------------------

func TestRSIHandler_TriggerCycle(t *testing.T) {
	records := makeSimpleLedgerRecords(10, time.Now())
	orch := newTestOrchestrator(records)
	h := NewRSIHandler(orch)

	req := httptest.NewRequest(http.MethodPost, "/v1/rsi/cycle", nil)
	w := httptest.NewRecorder()
	h.TriggerCycle()(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var cycle rsi.RSICycle
	require.NoError(t, json.NewDecoder(w.Body).Decode(&cycle))
	assert.Greater(t, cycle.ID, 0)
}

// ---------------------------------------------------------------------------
// NewRSIHandler Tests
// ---------------------------------------------------------------------------

func TestNewRSIHandler(t *testing.T) {
	h := NewRSIHandler(nil)
	assert.NotNil(t, h)
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func makeSimpleLedgerRecords(n int, ts time.Time) []ledger.LedgerRecord {
	records := make([]ledger.LedgerRecord, n)
	for i := range records {
		records[i] = ledger.LedgerRecord{
			Timestamp:         ts,
			RequestPayload:    `{"model":"gpt-4o","messages":[{"role":"user","content":"test"}]}`,
			ResponsePayload:   `{"id":"resp-1","model":"gpt-4o","choices":[{"message":{"role":"assistant","content":"ok"}}]}`,
			ChainHash:         "hash",
		}
	}
	return records
}
