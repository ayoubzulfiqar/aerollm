package federated

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// MaxRoundBodyBytes caps the round-management request body.
const MaxRoundBodyBytes = 4 << 10

// writeSecureError maps SecureAggregator errors to HTTP responses. Only the
// sentinel messages are returned (never wrapped details such as the last
// accepted sequence), because submissions can come from unauthenticated
// callers.
func writeSecureError(w http.ResponseWriter, err error) {
	type mapping struct {
		target error
		status int
	}
	for _, m := range []mapping{
		{ErrInvalidSignature, http.StatusUnauthorized},
		{ErrUnknownOwner, http.StatusUnauthorized},
		{ErrVerificationNotConfigured, http.StatusServiceUnavailable},
		{ErrPersistence, http.StatusServiceUnavailable},
		{ErrReplay, http.StatusConflict},
		{ErrStaleRound, http.StatusConflict},
		{ErrNoOpenRound, http.StatusConflict},
		{ErrTimestampOutOfWindow, http.StatusConflict},
		{ErrAlreadyContributed, http.StatusConflict},
		{ErrRoundReused, http.StatusConflict},
		{ErrRoundInProgress, http.StatusConflict},
		{ErrRoundFull, http.StatusConflict},
		{ErrRegistryFull, http.StatusServiceUnavailable},
		{ErrInvalidSignedUpdate, http.StatusBadRequest},
		{ErrInvalidRoundID, http.StatusBadRequest},
		{ErrInvalidMatrix, http.StatusBadRequest},
		{ErrDimensionMismatch, http.StatusBadRequest},
		{ErrNoUpdates, http.StatusBadRequest},
		{ErrTooManyUpdates, http.StatusBadRequest},
	} {
		if errors.Is(err, m.target) {
			msg := m.target.Error()
			if m.target == ErrInvalidSignature || m.target == ErrUnknownOwner {
				// Do not reveal whether the owner is registered.
				msg = "update not authenticated"
			}
			writeJSONError(w, m.status, msg)
			return
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		writeJSONError(w, http.StatusServiceUnavailable, "request cancelled")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, "federated operation failed")
}

// SubmitUpdateHandler returns an HTTP handler (POST) that accepts one
// SignedUpdate (JSON) into the current round. The update authenticates
// itself through its signature, so the handler may be mounted for nodes
// without admin credentials; keep it behind the gateway's rate limiting.
// Responses: 202 accepted, 400 malformed, 401 unauthenticated, 409
// replay/stale round/timestamp window/duplicate, 413 oversized.
func SubmitUpdateHandler(agg *SecureAggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if agg == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "aggregator not initialized")
			return
		}
		var su SignedUpdate
		if !decodeJSONBody(w, r, MaxAggregateBodyBytes, &su) {
			return
		}
		if err := agg.Submit(r.Context(), &su); err != nil {
			writeSecureError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":   "accepted",
			"round_id": su.RoundID,
			"node_id":  su.Update.Owner,
			"sequence": su.Sequence,
		})
	}
}

// RoundHandler returns an admin HTTP handler for round management:
// GET/HEAD returns the RoundStatus; POST {"round_id": "..."} opens a new
// round; DELETE aborts the current round (discarding contributions).
func RoundHandler(agg *SecureAggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodGet, http.MethodHead, http.MethodPost, http.MethodDelete) {
			return
		}
		if agg == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "aggregator not initialized")
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req struct {
				RoundID string `json:"round_id"`
			}
			if !decodeJSONBody(w, r, MaxRoundBodyBytes, &req) {
				return
			}
			if err := agg.OpenRound(req.RoundID); err != nil {
				writeSecureError(w, err)
				return
			}
			writeJSON(w, http.StatusCreated, agg.Round())
		case http.MethodDelete:
			n, err := agg.AbortRound()
			if err != nil {
				writeSecureError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"status": "aborted", "discarded": n})
		default:
			writeJSON(w, http.StatusOK, agg.Round())
		}
	}
}

// AggregateRoundHandler returns an admin HTTP handler (POST) that finalizes
// the current round and returns its RoundResult.
func AggregateRoundHandler(agg *SecureAggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if agg == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "aggregator not initialized")
			return
		}
		res, err := agg.AggregateRound(r.Context())
		if err != nil {
			writeSecureError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

// SignedAggregateHandler returns an admin HTTP handler (POST) that takes a
// JSON array of SignedUpdate for the current round, accepts all of them
// atomically (any failure rejects the whole batch without changing replay
// state) and then finalizes the round, returning its RoundResult. It is the
// authenticated, replay-protected replacement for AggregateHandler.
func SignedAggregateHandler(agg *SecureAggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if agg == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "aggregator not initialized")
			return
		}
		var updates []*SignedUpdate
		if !decodeJSONBody(w, r, MaxAggregateBodyBytes, &updates) {
			return
		}
		if len(updates) > MaxUpdates {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("too many updates (max %d)", MaxUpdates))
			return
		}
		if err := agg.SubmitBatch(r.Context(), updates); err != nil {
			writeSecureError(w, err)
			return
		}
		res, err := agg.AggregateRound(r.Context())
		if err != nil {
			writeSecureError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}
