// Package chaos provides opt-in HTTP fault injection (latency, error
// responses and panics) for resilience testing.
//
// Safety model:
//   - Fault injection is DISABLED unless the Injector is constructed with
//     Config.Enabled = true. The runtime HTTP API cannot turn it on.
//   - Panic faults additionally require Config.AllowPanic (or constructing
//     the injector with Type: FaultPanic) and must only be used inside HTTP
//     handlers (net/http recovers handler panics; RecoverPanic turns them into
//     500 responses).
//   - Latency is capped at MaxLatency and aborts when the request context is
//     cancelled; error faults only emit 4xx/5xx codes; percentages must be
//     finite and within [0,100].
package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// FaultType defines the category of fault to inject.
type FaultType string

const (
	FaultNone    FaultType = ""
	FaultLatency FaultType = "latency"
	FaultError   FaultType = "error"
	FaultPanic   FaultType = "panic"
)

// Limits.
const (
	MaxLatency       = 30 * time.Second
	MaxMessageLength = 256
	defaultMessage   = "chaos fault injected"
	maxBodyBytes     = 64 << 10
)

var (
	// ErrInjected is returned by Apply when an error fault was written.
	ErrInjected = errors.New("chaos fault injected")
	// ErrDisabled is returned when configuring a disabled injector.
	ErrDisabled = errors.New("chaos fault injection is disabled")
	// ErrInvalidConfig is returned (wrapped) for invalid fault configs.
	ErrInvalidConfig = errors.New("invalid chaos config")
)

// Config controls a fault injector.
type Config struct {
	Type       FaultType
	Percent    float64
	Duration   time.Duration
	StatusCode int
	Message    string
	// Enabled is the master switch, honored only at construction (or via
	// SetEnabled from code). Without it no fault is ever injected.
	Enabled bool
	// AllowPanic permits panic faults. Honored only at construction.
	AllowPanic bool
}

// Injector applies configured faults to requests. It is safe for concurrent use.
type Injector struct {
	mu         sync.RWMutex
	cfg        Config
	enabled    bool
	allowPanic bool
}

// validate checks a fault config (ignoring the master switches).
func validate(cfg Config, allowPanic bool) error {
	var errs []error
	switch cfg.Type {
	case FaultNone, FaultLatency, FaultError:
	case FaultPanic:
		if !allowPanic {
			errs = append(errs, errors.New("panic faults are not allowed by server configuration"))
		}
	default:
		errs = append(errs, fmt.Errorf("unknown fault type %q (want latency|error|panic)", cfg.Type))
	}
	if math.IsNaN(cfg.Percent) || cfg.Percent < 0 || cfg.Percent > 100 {
		errs = append(errs, errors.New("percent must be between 0 and 100"))
	}
	if cfg.Duration < 0 || cfg.Duration > MaxLatency {
		errs = append(errs, fmt.Errorf("duration must be between 0 and %s", MaxLatency))
	}
	if cfg.StatusCode != 0 && (cfg.StatusCode < 400 || cfg.StatusCode > 599) {
		errs = append(errs, errors.New("status_code must be a 4xx or 5xx code"))
	}
	if len(cfg.Message) > MaxMessageLength {
		errs = append(errs, fmt.Errorf("message exceeds %d bytes", MaxMessageLength))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

func withDefaults(cfg Config) Config {
	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusInternalServerError
	}
	if cfg.Message == "" {
		cfg.Message = defaultMessage
	}
	return cfg
}

// sanitize clamps out-of-range values for the non-failing constructor.
func sanitize(cfg Config, allowPanic bool) Config {
	if math.IsNaN(cfg.Percent) || cfg.Percent < 0 {
		cfg.Percent = 0
	}
	if cfg.Percent > 100 {
		cfg.Percent = 100
	}
	if cfg.Duration < 0 {
		cfg.Duration = 0
	}
	if cfg.Duration > MaxLatency {
		cfg.Duration = MaxLatency
	}
	if cfg.StatusCode != 0 && (cfg.StatusCode < 400 || cfg.StatusCode > 599) {
		cfg.StatusCode = http.StatusInternalServerError
	}
	if len(cfg.Message) > MaxMessageLength {
		cfg.Message = cfg.Message[:MaxMessageLength]
	}
	switch cfg.Type {
	case FaultNone, FaultLatency, FaultError:
	case FaultPanic:
		if !allowPanic {
			cfg.Type = FaultNone
		}
	default:
		cfg.Type = FaultNone
	}
	return withDefaults(cfg)
}

// NewInjector creates a fault injector. Out-of-range values are clamped;
// use Config.Validate for strict checking. Injection stays disabled unless
// cfg.Enabled is true.
func NewInjector(cfg Config) *Injector {
	allowPanic := cfg.AllowPanic || cfg.Type == FaultPanic
	return &Injector{cfg: sanitize(cfg, allowPanic), enabled: cfg.Enabled, allowPanic: allowPanic}
}

// Validate reports whether cfg is a valid fault configuration.
func (c Config) Validate() error {
	return validate(c, c.AllowPanic || c.Type == FaultPanic)
}

// SetEnabled flips the master switch. It is intended for server code (e.g.
// wired to a config flag), never for untrusted input.
func (i *Injector) SetEnabled(enabled bool) {
	i.mu.Lock()
	i.enabled = enabled
	i.mu.Unlock()
}

// Enabled reports whether fault injection is enabled.
func (i *Injector) Enabled() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.enabled
}

// ShouldFault returns true when injection is enabled and a uniformly random
// sample falls within the configured percent (100 always faults).
func (i *Injector) ShouldFault() bool {
	i.mu.RLock()
	enabled, cfg := i.enabled, i.cfg
	i.mu.RUnlock()
	if !enabled || cfg.Type == FaultNone || cfg.Percent <= 0 {
		return false
	}
	if cfg.Percent >= 100 {
		return true
	}
	return rand.Float64()*100 < cfg.Percent
}

type injectedPanic struct{ msg string }

func (p injectedPanic) String() string { return "chaos: injected panic: " + p.msg }

// Apply injects the configured fault into the response path. It is a no-op
// when injection is disabled. Latency faults return ctx.Err() if the request
// is cancelled while sleeping; error faults write a JSON error and return
// ErrInjected; panic faults panic (recover with RecoverPanic or net/http).
func (i *Injector) Apply(w http.ResponseWriter, r *http.Request) error {
	i.mu.RLock()
	enabled, cfg := i.enabled, i.cfg
	i.mu.RUnlock()
	if !enabled {
		return nil
	}

	switch cfg.Type {
	case FaultLatency:
		if cfg.Duration <= 0 {
			return nil
		}
		ctx := context.Background()
		if r != nil {
			ctx = r.Context()
		}
		t := time.NewTimer(cfg.Duration)
		defer t.Stop()
		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case FaultError:
		body, _ := json.Marshal(map[string]string{"error": cfg.Message})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cfg.StatusCode)
		_, _ = w.Write(body)
		return fmt.Errorf("%w: %s", ErrInjected, cfg.Message)
	case FaultPanic:
		panic(injectedPanic{msg: cfg.Message})
	default:
		return nil
	}
}

// Middleware injects faults into requests passing through it. Error faults
// short-circuit the request; latency faults delay it (and abort if the client
// goes away).
func (i *Injector) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if i.ShouldFault() {
			if err := i.Apply(w, r); err != nil {
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Update replaces the injector's fault configuration. Out-of-range values
// are clamped, panic faults are dropped unless allowed, and the master
// switches (Enabled, AllowPanic) are NOT changed by Update.
func (i *Injector) Update(cfg Config) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cfg = sanitize(cfg, i.allowPanic)
}

// Configure strictly validates and applies a fault configuration. It fails
// with ErrDisabled when injection is disabled.
func (i *Injector) Configure(cfg Config) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.enabled {
		return ErrDisabled
	}
	if err := validate(cfg, i.allowPanic); err != nil {
		return err
	}
	i.cfg = withDefaults(cfg)
	return nil
}

// Config returns the current config, including the master switches.
func (i *Injector) Config() Config {
	i.mu.RLock()
	defer i.mu.RUnlock()
	cfg := i.cfg
	cfg.Enabled = i.enabled
	cfg.AllowPanic = i.allowPanic
	return cfg
}

// RecoverPanic is an HTTP middleware that converts panics into a JSON 500
// response without leaking panic details (except the configured message of an
// injected chaos panic). http.ErrAbortHandler is re-raised.
func RecoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			msg := "internal server error"
			if p, ok := rec.(injectedPanic); ok {
				msg = p.msg
			}
			body, _ := json.Marshal(map[string]string{"error": msg})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write(body)
		}()
		next.ServeHTTP(w, r)
	})
}

// StatusResponse exposes injector status for the HTTP handler.
type StatusResponse struct {
	Type       FaultType `json:"type"`
	Percent    float64   `json:"percent"`
	Duration   string    `json:"duration"`
	Enabled    bool      `json:"enabled"`
	StatusCode int       `json:"status_code,omitempty"`
	Message    string    `json:"message,omitempty"`
}

func statusOf(cfg Config) StatusResponse {
	return StatusResponse{
		Type:       cfg.Type,
		Percent:    cfg.Percent,
		Duration:   cfg.Duration.String(),
		Enabled:    cfg.Enabled,
		StatusCode: cfg.StatusCode,
		Message:    cfg.Message,
	}
}

// faultRequest is the body accepted by the HTTP handler. Duration accepts a
// Go duration string ("250ms") or a number of nanoseconds (legacy).
type faultRequest struct {
	Type             FaultType       `json:"type"`
	Percent          float64         `json:"percent"`
	Duration         json.RawMessage `json:"duration"`
	StatusCode       int             `json:"status_code"`
	LegacyStatusCode int             `json:"statuscode"`
	Message          string          `json:"message"`
}

func (f faultRequest) toConfig() (Config, error) {
	cfg := Config{Type: f.Type, Percent: f.Percent, StatusCode: f.StatusCode, Message: f.Message}
	if cfg.StatusCode == 0 {
		cfg.StatusCode = f.LegacyStatusCode
	}
	raw := bytes.TrimSpace(f.Duration)
	switch {
	case len(raw) == 0 || bytes.Equal(raw, []byte("null")):
	case raw[0] == '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Config{}, fmt.Errorf("%w: invalid duration", ErrInvalidConfig)
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return Config{}, fmt.Errorf("%w: invalid duration %q", ErrInvalidConfig, s)
		}
		cfg.Duration = d
	default:
		var ns float64
		if err := json.Unmarshal(raw, &ns); err != nil || math.IsNaN(ns) || ns < 0 || ns > float64(MaxLatency) {
			return Config{}, fmt.Errorf("%w: duration must be between 0 and %s", ErrInvalidConfig, MaxLatency)
		}
		cfg.Duration = time.Duration(ns)
	}
	return cfg, nil
}

// Handler returns an HTTP handler for `/v1/chaos/fault`:
//
//	GET          current status
//	POST | PUT   configure a fault (202); 403 when injection is disabled
//	DELETE       clear the fault (200)
func Handler(inj *Injector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			writeJSON(w, http.StatusOK, statusOf(inj.Config()))
		case http.MethodPost, http.MethodPut:
			if r.Body == nil {
				writeError(w, http.StatusBadRequest, "missing body")
				return
			}
			defer r.Body.Close()
			var req faultRequest
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
			if err := dec.Decode(&req); err != nil {
				var mbe *http.MaxBytesError
				switch {
				case errors.As(err, &mbe):
					writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
				case errors.Is(err, io.EOF):
					writeError(w, http.StatusBadRequest, "missing body")
				default:
					writeError(w, http.StatusBadRequest, "bad request")
				}
				return
			}
			cfg, err := req.toConfig()
			if err == nil {
				err = inj.Configure(cfg)
			}
			switch {
			case errors.Is(err, ErrDisabled):
				writeError(w, http.StatusForbidden, "chaos fault injection is disabled by server configuration")
				return
			case err != nil:
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSON(w, http.StatusAccepted, statusOf(inj.Config()))
		case http.MethodDelete:
			inj.Update(Config{})
			writeJSON(w, http.StatusOK, statusOf(inj.Config()))
		default:
			w.Header().Set("Allow", "GET, HEAD, POST, PUT, DELETE")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
