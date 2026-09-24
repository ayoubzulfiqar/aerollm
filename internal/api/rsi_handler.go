package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/rsi"
)

// RSIHandler exposes RSI engine endpoints.
type RSIHandler struct {
	orchestrator *rsi.RSIOrchestrator
}

// NewRSIHandler creates a handler for RSI engine endpoints.
func NewRSIHandler(orch *rsi.RSIOrchestrator) *RSIHandler {
	return &RSIHandler{orchestrator: orch}
}

func (h *RSIHandler) ready(w http.ResponseWriter) bool {
	if h == nil || h.orchestrator == nil {
		writeError(w, http.StatusServiceUnavailable, "RSI orchestrator not initialized")
		return false
	}
	return true
}

// Headroom returns the current HCI headroom assessment across all dimensions.
func (h *RSIHandler) Headroom() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if !h.ready(w) {
			return
		}
		assessments, err := h.orchestrator.Headroom(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "headroom assessment failed: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, assessments)
	}
}

// Cycles returns the RSI cycle history.
func (h *RSIHandler) Cycles() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if !h.ready(w) {
			return
		}
		writeJSON(w, http.StatusOK, h.orchestrator.Cycles())
	}
}

// TriggerCycle runs one RSI cycle synchronously (POST) and returns it. A
// cycle that ran but ended in error is returned as {"cycle":..,"error":..}.
func (h *RSIHandler) TriggerCycle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if !h.ready(w) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		cycle, err := h.orchestrator.RunCycle(ctx)
		if err != nil {
			if errors.Is(err, rsi.ErrCycleInProgress) {
				writeError(w, http.StatusConflict, "an RSI cycle is already in progress")
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				writeError(w, http.StatusGatewayTimeout, "RSI cycle timed out")
				return
			}
			if cycle != nil {
				writeJSON(w, http.StatusOK, map[string]interface{}{"cycle": cycle, "error": err.Error()})
				return
			}
			writeError(w, http.StatusConflict, "RSI cycle failed: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, cycle)
	}
}

// CurrentCycle returns the most recent cycle, or null if none exist.
func (h *RSIHandler) CurrentCycle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if !h.ready(w) {
			return
		}
		writeJSON(w, http.StatusOK, h.orchestrator.CurrentCycle())
	}
}

// Config dispatches GET (return config) and PUT (update config).
func (h *RSIHandler) Config() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.ready(w) {
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, h.orchestrator.Config())
		case http.MethodPut:
			// Start from the current config so partial updates keep the
			// fields they do not mention.
			cfg := h.orchestrator.Config()
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&cfg); err != nil {
				writeError(w, http.StatusBadRequest, "invalid JSON: "+sanitizeDecodeError(err))
				return
			}
			if err := cfg.Validate(); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := h.orchestrator.SetConfig(cfg); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "config": h.orchestrator.Config()})
		default:
			methodNotAllowed(w, http.MethodGet, http.MethodPut)
		}
	}
}

// Stats returns cumulative RSI counters (GET /v1/rsi/stats).
func (h *RSIHandler) Stats() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if !h.ready(w) {
			return
		}
		writeJSON(w, http.StatusOK, h.orchestrator.Stats())
	}
}

// Rollback reverts the most recent deployment (POST /v1/rsi/rollback).
func (h *RSIHandler) Rollback() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if !h.ready(w) {
			return
		}
		err := h.orchestrator.Rollback(r.Context())
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, map[string]interface{}{"status": "rolled_back", "stats": h.orchestrator.Stats()})
		case errors.Is(err, rsi.ErrNothingToRollback):
			writeError(w, http.StatusConflict, "no deployment to roll back")
		case errors.Is(err, rsi.ErrNoRollbackHook):
			writeError(w, http.StatusNotImplemented, "no rollback hook configured")
		default:
			writeError(w, http.StatusInternalServerError, "rollback failed: "+err.Error())
		}
	}
}
