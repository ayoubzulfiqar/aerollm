package chaos

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// FaultType defines the category of fault to inject.
type FaultType string

const (
	FaultLatency    FaultType = "latency"
	FaultError      FaultType = "error"
	FaultPanic      FaultType = "panic"
)

// Config controls a fault injector.
type Config struct {
	Type       FaultType
	Percent    float64
	Duration   time.Duration
	StatusCode int
	Message    string
}

// Injector applies configured faults to requests.
type Injector struct {
	cfg Config
	mu  sync.RWMutex
}

// NewInjector creates a new fault injector.
func NewInjector(cfg Config) *Injector {
	if cfg.Percent < 0 {
		cfg.Percent = 0
	}
	if cfg.Percent > 100 {
		cfg.Percent = 100
	}
	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusInternalServerError
	}
	if cfg.Message == "" {
		cfg.Message = "chaos fault injected"
	}
	return &Injector{cfg: cfg}
}

// ShouldFault returns true when the randomized sample falls within the configured percent.
func (i *Injector) ShouldFault() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.cfg.Percent <= 0 {
		return false
	}
	var b [1]byte
	_, _ = rand.Read(b[:])
	return float64(b[0])/255.0*100 < i.cfg.Percent
}

// Apply injects the configured fault into the response path.
func (i *Injector) Apply(w http.ResponseWriter, r *http.Request) error {
	i.mu.RLock()
	cfg := i.cfg
	i.mu.RUnlock()

	switch cfg.Type {
	case FaultLatency:
		time.Sleep(cfg.Duration)
		return nil
	case FaultError:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cfg.StatusCode)
		_, _ = w.Write([]byte(`{"error":"` + cfg.Message + `"}`))
		return errors.New(cfg.Message)
	case FaultPanic:
		panic(cfg.Message)
	default:
		return nil
	}
}

// Update replaces the injector configuration.
func (i *Injector) Update(cfg Config) {
	i.mu.Lock()
	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusInternalServerError
	}
	if cfg.Message == "" {
		cfg.Message = "chaos fault injected"
	}
	if cfg.Percent < 0 {
		cfg.Percent = 0
	}
	if cfg.Percent > 100 {
		cfg.Percent = 100
	}
	i.cfg = cfg
	i.mu.Unlock()
}

// Config returns the current config.
func (i *Injector) Config() Config {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.cfg
}

// RecoverPanic is an HTTP middleware helper that recovers from injected panics.
func RecoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				msg := fmt.Sprintf("%v", rec)
				_, _ = w.Write([]byte(`{"error":"` + msg + `"}`))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// StatusResponse exposes injector status for the HTTP handler.
type StatusResponse struct {
	Type     FaultType `json:"type"`
	Percent  float64   `json:"percent"`
	Duration string    `json:"duration"`
}

// Handler returns an HTTP handler for `/v1/chaos/fault`.
func Handler(inj *Injector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"missing body"}`))
			return
		}
		defer r.Body.Close()
		var cfg Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad request"}`))
			return
		}
		inj.Update(cfg)
		resp := StatusResponse{Type: cfg.Type, Percent: cfg.Percent, Duration: cfg.Duration.String()}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
