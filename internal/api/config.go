package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/config"
)

// maxConfigBody caps /config/update payloads.
const maxConfigBody = 1 << 20

// ConfigHandler holds dependencies for dynamic config endpoints.
type ConfigHandler struct {
	Reloader *config.ConfigReloader
	Logger   func(msg string, kv ...interface{})
}

// NewConfigHandler creates a new config handler.
func NewConfigHandler(reloader *config.ConfigReloader, logger func(msg string, kv ...interface{})) *ConfigHandler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	return &ConfigHandler{Reloader: reloader, Logger: logger}
}

// ModelInfo handles GET /model/info.
// @Summary Get model info
// @Description Returns all currently loaded models, their providers, and capabilities.
// @Tags config
// @Success 200 {object} map[string]interface{}
// @Router /model/info [get]
func (h *ConfigHandler) ModelInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	models := h.Reloader.GetModels()
	if models == nil {
		models = []config.ModelInfo{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data":  models,
		"count": len(models),
	})
}

// ConfigYaml handles GET /config/yaml.
// @Summary Get active config
// @Description Returns the active configuration (as JSON) with every secret redacted.
// @Tags config
// @Success 200 {object} config.Config
// @Router /config/yaml [get]
func (h *ConfigHandler) ConfigYaml(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	cfg := h.Reloader.MaskSecrets()
	if cfg == nil {
		writeError(w, http.StatusServiceUnavailable, "no active configuration")
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

// ConfigUpdate handles POST /config/update.
// @Summary Update config
// @Description Merges a partial JSON config onto the active one (arrays such as providers are replaced), validates it, and hot-reloads the provider registry without dropping connections. Secrets echoed back as "***REDACTED***" keep their current value. Durations are nanoseconds.
// @Tags config
// @Accept json
// @Produce json
// @Param req body config.Config true "Partial or full configuration"
// @Success 200 {object} map[string]interface{}
// @Router /config/update [post]
func (h *ConfigHandler) ConfigUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "missing request body")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxConfigBody))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "config too large")
			return
		}
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	cfg, err := h.Reloader.ApplyUpdate(r.Context(), body)
	if err != nil {
		h.Logger("config update rejected", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.Logger("config hot-reloaded", "providers", len(cfg.Providers))
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "reloaded",
		"providers": len(cfg.Providers),
	})
}
