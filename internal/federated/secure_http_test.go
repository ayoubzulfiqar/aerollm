package federated

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func serve(h http.HandlerFunc, method, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/", nil)
	} else {
		r = httptest.NewRequest(method, "/", strings.NewReader(body))
	}
	h(rec, r)
	return rec
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func errorMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var out map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())
	return out["error"]
}

func TestSecureAggregatorHTTPFlow(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	round := RoundHandler(agg)
	submit := SubmitUpdateHandler(agg)
	finalize := AggregateRoundHandler(agg)

	// No round yet.
	rec := serve(round, http.MethodGet, "")
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"open":false,"contributors":[],"opened_at":"0001-01-01T00:00:00Z"}`, rec.Body.String())
	su1 := nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2)
	rec = serve(submit, http.MethodPost, mustJSON(t, su1))
	require.Equal(t, http.StatusConflict, rec.Code)

	// Open a round.
	require.Equal(t, http.StatusBadRequest, serve(round, http.MethodPost, `{"round_id":"bad id"}`).Code)
	rec = serve(round, http.MethodPost, `{"round_id":"r1"}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, http.StatusConflict, serve(round, http.MethodPost, `{"round_id":"r1"}`).Code)

	// Submit: the JSON round trip (base64 signature) verifies.
	rec = serve(submit, http.MethodPost, mustJSON(t, su1))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	// Replay.
	rec = serve(submit, http.MethodPost, mustJSON(t, su1))
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Equal(t, ErrReplay.Error(), errorMessage(t, rec), "only the sentinel message is exposed")

	// Forged signature and unknown owner look identical to the caller.
	forged := nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4)
	forged.Signature[0] ^= 1
	rec = serve(submit, http.MethodPost, mustJSON(t, forged))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "update not authenticated", errorMessage(t, rec))
	stranger := nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4)
	stranger.Update.Owner = "stranger"
	rec = serve(submit, http.MethodPost, mustJSON(t, stranger))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "update not authenticated", errorMessage(t, rec))

	// Malformed.
	require.Equal(t, http.StatusBadRequest, serve(submit, http.MethodPost, `{"update":null}`).Code)
	require.Equal(t, http.StatusBadRequest, serve(submit, http.MethodPost, `{`).Code)
	require.Equal(t, http.StatusMethodNotAllowed, serve(submit, http.MethodGet, "").Code)

	require.Equal(t, http.StatusAccepted, serve(submit, http.MethodPost, mustJSON(t, nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4))).Code)

	rec = serve(round, http.MethodGet, "")
	var st RoundStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &st))
	require.Equal(t, []string{"n1", "n2"}, st.Contributors)

	rec = serve(finalize, http.MethodPost, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res RoundResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	require.Equal(t, "r1", res.RoundID)
	require.Equal(t, []float64{2, 3}, res.Aggregate.Data)
	require.Equal(t, http.StatusConflict, serve(finalize, http.MethodPost, "").Code)
	require.Equal(t, http.StatusMethodNotAllowed, serve(finalize, http.MethodGet, "").Code)

	// Abort.
	require.Equal(t, http.StatusConflict, serve(round, http.MethodDelete, "").Code)
	require.Equal(t, http.StatusCreated, serve(round, http.MethodPost, `{"round_id":"r2"}`).Code)
	require.Equal(t, http.StatusOK, serve(round, http.MethodDelete, "").Code)
	require.Equal(t, http.StatusMethodNotAllowed, serve(round, http.MethodPut, "").Code)

	// Nil aggregator fails closed.
	for _, h := range []http.HandlerFunc{SubmitUpdateHandler(nil), RoundHandler(nil), AggregateRoundHandler(nil), SignedAggregateHandler(nil)} {
		require.Equal(t, http.StatusServiceUnavailable, serve(h, http.MethodPost, `{}`).Code)
	}
}

func TestSignedAggregateHandler(t *testing.T) {
	reg := NewGatewayRegistry()
	nodes := newTestNodes(t, reg, "n1", "n2")
	clk := newFakeClock()
	agg := newTestAggregator(t, reg, clk)
	h := SignedAggregateHandler(agg)
	require.NoError(t, agg.OpenRound("r1"))

	s1 := nodes[0].sign(t, "r1", 1, clk.Now(), 1, 2)
	s2 := nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4)
	bad := nodes[1].sign(t, "r1", 1, clk.Now(), 3, 4)
	bad.TimestampMs++

	rec := serve(h, http.MethodPost, mustJSON(t, []*SignedUpdate{s1, bad}))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Empty(t, agg.Round().Contributors)

	rec = serve(h, http.MethodPost, mustJSON(t, []*SignedUpdate{s1, s2}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res RoundResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	require.Equal(t, []float64{2, 3}, res.Aggregate.Data)
	require.Equal(t, []string{"n1", "n2"}, res.Contributors)

	// The same batch cannot be replayed into the next round.
	require.NoError(t, agg.OpenRound("r2"))
	require.Equal(t, http.StatusConflict, serve(h, http.MethodPost, mustJSON(t, []*SignedUpdate{s1, s2})).Code)
	require.Equal(t, http.StatusBadRequest, serve(h, http.MethodPost, `[]`).Code)
	require.Equal(t, http.StatusMethodNotAllowed, serve(h, http.MethodGet, "").Code)
}
