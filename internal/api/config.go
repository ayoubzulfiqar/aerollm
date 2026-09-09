package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
)

// ConfigHandler holds dependencies for dynamic config endpoints.
type ConfigHandler struct {
	Reloader  *config.ConfigReloader
	Logger    func(msg string, kv ...interface{})
}

// NewConfigHandler creates a new config handler.
func NewConfigHandler(reloader *config.ConfigReloader, logger func(msg string, kv ...interface{})) *ConfigHandler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	return &ConfigHandler{Reloader: reloader, Logger: logger}
}

// ModelInfo handles GET /model/info.
// Returns all currently loaded models, their providers, and capabilities.
func (h *ConfigHandler) ModelInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	models := h.Reloader.GetModels()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"data":   models,
		"count":  len(models),
	})
}

// ConfigYaml handles GET /config/yaml.
// Returns the current active configuration with all secrets masked.
func (h *ConfigHandler) ConfigYaml(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	cfg := h.Reloader.MaskSecrets()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(cfg)
}

// ConfigUpdate handles POST /config/update.
// Accepts a partial or full config (JSON), validates it, and hot-reloads
// the provider registry without dropping active connections.
func (h *ConfigHandler) ConfigUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var cfg config.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		h.Logger("config update failed: invalid body", "error", err)
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	// Hot-reload atomically.
	if err := h.Reloader.Reload(r.Context(), &cfg); err != nil {
		h.Logger("config reload failed", "error", err)
		http.Error(w, `{"error":"reload failed"}`, http.StatusInternalServerError)
		return
	}
	h.Logger("config hot-reloaded", "providers", len(cfg.Providers))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "reloaded",
		"providers": len(cfg.Providers),
	})
}

// ConfigHandlerInterface satisfies the ProviderRegistry's RegisterFromConfig.
// The actual registry swap is performed by the caller (cmd/server/main.go)
// which has access to both the registry and the reloader.
var _ = context.Background
