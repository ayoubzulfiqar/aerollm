package api

import (
	"context"
	"encoding/json"
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

// Headroom returns the current HCI headroom assessment across all dimensions.
func (h *RSIHandler) Headroom() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}

		assessments, err := h.orchestrator.Headroom(r.Context())
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}

		out := make(map[string]interface{}, len(assessments))
		for dim, a := range assessments {
			out[string(dim)] = a
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

// Cycles returns the RSI cycle history.
func (h *RSIHandler) Cycles() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}

		cycles := h.orchestrator.Cycles()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cycles)
	}
}

// TriggerCycle runs one RSI cycle synchronously and returns the result.
func (h *RSIHandler) TriggerCycle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		cycle, err := h.orchestrator.RunCycle(ctx)
		if err != nil {
			if cycle != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusPartialContent)
				_ = json.NewEncoder(w).Encode(cycle)
				return
			}
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(cycle)
	}
}

// CurrentCycle returns the most recent cycle, or null if none exist.
func (h *RSIHandler) CurrentCycle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}

		cycle := h.orchestrator.CurrentCycle()
		w.Header().Set("Content-Type", "application/json")
		if cycle == nil {
			_ = json.NewEncoder(w).Encode(nil)
			return
		}
		_ = json.NewEncoder(w).Encode(cycle)
	}
}

// GetConfig returns the current RSI configuration.
func (h *RSIHandler) GetConfig() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.orchestrator.Config())
	}
}

// UpdateConfig accepts a new RSI configuration in the JSON request body.
func (h *RSIHandler) UpdateConfig() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h == nil || h.orchestrator == nil {
			http.Error(w, `{"error":"RSI orchestrator not initialized"}`, http.StatusServiceUnavailable)
			return
		}

		var cfg rsi.RSIConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		h.orchestrator.SetConfig(cfg)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"config": cfg,
		})
	}
}
