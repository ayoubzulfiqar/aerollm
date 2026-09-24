package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/aiops"
	"github.com/ayoubzulfiqar/aerollm/internal/autoscale"
	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/guardrails"
	"github.com/ayoubzulfiqar/aerollm/internal/learning"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
	"github.com/ayoubzulfiqar/aerollm/internal/redteam"
	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// parseOperatorKeys decodes comma-separated hex ed25519 public keys.
func parseOperatorKeys(spec string) ([]ed25519.PublicKey, error) {
	var keys []ed25519.PublicKey
	for _, f := range splitList(spec) {
		b, err := hex.DecodeString(f)
		if err != nil || len(b) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid operator key %q (want %d-byte hex ed25519 public key)", f, ed25519.PublicKeySize)
		}
		keys = append(keys, ed25519.PublicKey(b))
	}
	return keys, nil
}

// mountFederated wires signed, replay-protected federated aggregation.
// Node registration is accepted only with AEROLLM_FEDERATION_TOKEN or an
// attestation from AEROLLM_FEDERATION_OPERATOR_KEYS; without either, the
// registration endpoint is disabled.
func (a *app) mountFederated(mux *http.ServeMux, admin, client func(http.Handler) http.Handler) error {
	reg := federated.NewGatewayRegistry()
	if a.persist != nil {
		if err := reg.EnablePersistence(a.persist); err != nil {
			return fmt.Errorf("federated registry: %w", err)
		}
	}
	agg, err := federated.NewSecureAggregator(reg, federated.SecureAggregatorConfig{})
	if err != nil {
		return fmt.Errorf("federated aggregator: %w", err)
	}
	if a.persist != nil {
		if err := agg.EnablePersistence(a.persist); err != nil {
			return fmt.Errorf("federated aggregator: %w", err)
		}
	}
	opKeys, err := parseOperatorKeys(os.Getenv("AEROLLM_FEDERATION_OPERATOR_KEYS"))
	if err != nil {
		return fmt.Errorf("AEROLLM_FEDERATION_OPERATOR_KEYS: %w", err)
	}
	var tokens []string
	if t := os.Getenv("AEROLLM_FEDERATION_TOKEN"); t != "" {
		tokens = append(tokens, t)
	}
	auth, err := federated.NewRegistrationAuth(federated.RegistrationAuthConfig{Tokens: tokens, OperatorKeys: opKeys})
	switch {
	case err == nil:
		// Registration carries its own credential (token or attestation).
		mux.Handle("/v1/federated/nodes/register", middleware.Chain(federated.RegisterNodeHandlerWithAuth(reg, auth), middleware.BodyLimit(1<<20)))
	case errors.Is(err, federated.ErrRegistrationAuthNotConfigured):
		mux.Handle("/v1/federated/nodes/register", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middleware.WriteJSONError(w, http.StatusNotImplemented, "federated registration not configured (set AEROLLM_FEDERATION_TOKEN or AEROLLM_FEDERATION_OPERATOR_KEYS)", "")
		}))
	default:
		return fmt.Errorf("federated registration auth: %w", err)
	}
	// Updates are ed25519-signed by registered nodes; they still need a
	// gateway key and are rate limited like any client call.
	mux.Handle("/v1/federated/updates", client(federated.SubmitUpdateHandler(agg)))
	mux.Handle("/v1/federated/rounds", admin(federated.RoundHandler(agg)))
	mux.Handle("/v1/federated/rounds/aggregate", admin(federated.AggregateRoundHandler(agg)))
	mux.Handle("/v1/federated/aggregate", admin(federated.SignedAggregateHandler(agg)))
	return nil
}

// configureAutoscaleBootstrap pins the node bootstrap artifact when
// AEROLLM_AUTOSCALE_BOOTSTRAP_URL/_SHA256/_VERSION are set.
func (a *app) configureAutoscaleBootstrap() error {
	cfg, err := autoscale.BootstrapConfigFromEnv(os.Getenv)
	if errors.Is(err, autoscale.ErrBootstrapNotConfigured) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("autoscale bootstrap: %w", err)
	}
	return autoscale.SetDefaultBootstrapConfig(cfg)
}

// mountFineTuning wires the fine-tuning trainer. Jobs run on an
// OpenAI-compatible fine-tuning API configured with
// AEROLLM_FINETUNE_BASE_URL/_API_KEY/_MODEL; without it the endpoints report
// 503 instead of queueing forever.
func (a *app) mountFineTuning(mux *http.ServeMux, adminFunc func(http.HandlerFunc) http.Handler, feedback *flywheel.FeedbackExporter) error {
	dir := os.Getenv("AEROLLM_LEARNING_DIR")
	if dir == "" {
		dir = filepath.Join(stateDir(), "fine-tune-jobs")
	}
	trainer := learning.NewTrainer(&flywheel.DatasetExporter{Ledger: a.ledger, Feedback: feedback}, a.ledger, dir)
	if key := os.Getenv("AEROLLM_FINETUNE_API_KEY"); key != "" {
		ft, err := learning.NewOpenAIFineTuner(learning.OpenAIFineTuneConfig{
			BaseURL: os.Getenv("AEROLLM_FINETUNE_BASE_URL"),
			APIKey:  key,
			Model:   os.Getenv("AEROLLM_FINETUNE_MODEL"),
		})
		if err != nil {
			return fmt.Errorf("fine-tuning: %w", err)
		}
		if a.persist != nil {
			if err := ft.EnablePersistence(a.persist); err != nil {
				return fmt.Errorf("fine-tuning: %w", err)
			}
		}
		trainer.SetFineTuneBackend(ft)
	}

	writeFTError := func(w http.ResponseWriter, err error) {
		switch {
		case errors.Is(err, learning.ErrFineTuneNotConfigured):
			middleware.WriteJSONError(w, http.StatusServiceUnavailable, "fine-tuning backend not configured (AEROLLM_FINETUNE_API_KEY)", "")
		case errors.Is(err, learning.ErrJobNotFound):
			middleware.WriteJSONError(w, http.StatusNotFound, "fine-tuning job not found", "")
		default:
			middleware.WriteJSONError(w, http.StatusBadGateway, err.Error(), "")
		}
	}
	// POST /v1/fine-tuning/jobs {"model","min_rating"} creates a job from
	// rated feedback; GET lists jobs.
	mux.Handle("/v1/fine-tuning/jobs", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]interface{}{"object": "list", "data": trainer.Jobs()})
		case http.MethodPost:
			var body struct {
				Model     string `json:"model"`
				MinRating string `json:"min_rating"`
			}
			if !decodeBody(w, r, &body) {
				return
			}
			job, err := trainer.Enqueue(r.Context(), body.Model, body.MinRating)
			if err != nil {
				writeFTError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, job)
		default:
			w.Header().Set("Allow", "GET, POST")
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
		}
	}))
	// GET /v1/fine-tuning/jobs/{id} refreshes status; POST .../{id}/cancel.
	mux.Handle("/v1/fine-tuning/jobs/", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/fine-tuning/jobs/"), "/")
		id, action, _ := strings.Cut(rest, "/")
		switch {
		case id == "":
			middleware.WriteJSONError(w, http.StatusNotFound, "job id required", "")
		case action == "" && r.Method == http.MethodGet:
			job, err := trainer.Refresh(r.Context(), id)
			if err != nil {
				writeFTError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, job)
		case action == "cancel" && r.Method == http.MethodPost:
			if err := trainer.CancelContext(r.Context(), id); err != nil {
				writeFTError(w, err)
				return
			}
			job, _ := trainer.Status(id)
			writeJSON(w, http.StatusOK, job)
		default:
			middleware.WriteJSONError(w, http.StatusNotFound, "unknown fine-tuning endpoint", "")
		}
	}))
	return nil
}

// defaultRateController is implemented by the gateway rate limiters.
type defaultRateController interface {
	DefaultRPS() float64
	SetDefaultRPS(rps float64)
}

// startAIOps runs the self-tuning loop. Actions are dry-run (logged in the
// audit trail only) unless AEROLLM_AIOPS_ACTIONS_LIVE=true.
func (a *app) startAIOps(mux *http.ServeMux, adminFunc func(http.HandlerFunc) http.Handler, limiter ratelimit.RateLimiter) error {
	source := aiops.NewDefaultMetricsSource(telemetry.RequestCount, telemetry.ErrorCount, telemetry.AvgLatency).
		WithProviders(func() map[string]aiops.ProviderStats {
			out := map[string]aiops.ProviderStats{}
			for _, m := range telemetry.ProviderMetrics() {
				out[m.Name] = aiops.ProviderStats{RequestsTotal: m.Requests + m.Errors, ErrorsTotal: m.Errors}
			}
			return out
		})
	tuner := aiops.NewMetaAgentTuner(source, 30*time.Second, 0)
	tuner.SetDryRun(!envBool("AEROLLM_AIOPS_ACTIONS_LIVE"))

	latency, err := aiops.NewLatencyStrategyAction(aiops.LatencyStrategyConfig{
		HighLatencyMs:    5000,
		RecoverLatencyMs: 2000,
		GetStrategy:      a.router.Strategy,
		SetStrategy: func(_ context.Context, s string) error {
			a.router.SetStrategy(s)
			a.logger.Info("aiops: routing strategy changed", "strategy", s)
			return nil
		},
	})
	if err != nil {
		return fmt.Errorf("aiops latency action: %w", err)
	}
	if err := tuner.Register(latency); err != nil {
		return err
	}
	if rc, ok := limiter.(defaultRateController); ok && rc.DefaultRPS() > 0 {
		rl, err := aiops.NewRateLimitAction(aiops.RateLimitConfig{
			HighErrorRate:    0.25,
			RecoverErrorRate: 0.05,
			GetLimit:         rc.DefaultRPS,
			SetLimit: func(_ context.Context, limit float64) error {
				rc.SetDefaultRPS(limit)
				a.logger.Info("aiops: default rate limit changed", "rps", limit)
				return nil
			},
		})
		if err != nil {
			return fmt.Errorf("aiops rate-limit action: %w", err)
		}
		if err := tuner.Register(rl); err != nil {
			return err
		}
	}
	cb, err := aiops.NewCircuitBreakerAction(aiops.CircuitBreakerConfig{
		HighErrorRate:    0.5,
		RecoverErrorRate: 0.1,
		OpenCircuit: func(_ context.Context, provider string) error {
			a.logger.Warn("aiops: forcing provider circuit open", "provider", provider)
			return a.router.ForceOpen(provider, 5*time.Minute)
		},
		CloseCircuit: func(_ context.Context, provider string) error {
			a.logger.Info("aiops: closing provider circuit", "provider", provider)
			return a.router.ResetCircuit(provider)
		},
	})
	if err != nil {
		return fmt.Errorf("aiops circuit action: %w", err)
	}
	if err := tuner.Register(cb); err != nil {
		return err
	}
	a.goWorker("aiops", tuner.Run)
	mux.Handle("/v1/aiops/actions", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"dry_run": tuner.DryRun(), "actions": tuner.ActionStatuses(), "audit": tuner.Audit(), "stats": tuner.Stats(),
		})
	}))
	return nil
}

// mountRedTeam wires the adversarial prober. It records what it would send
// (dry run) unless AEROLLM_REDTEAM_LIVE_PROBE=true and a target model is set
// with AEROLLM_REDTEAM_MODEL; live probes go through the normal gateway path.
func (a *app) mountRedTeam(mux *http.ServeMux, adminFunc func(http.HandlerFunc) http.Handler) error {
	model := os.Getenv("AEROLLM_REDTEAM_MODEL")
	live := envBool("AEROLLM_REDTEAM_LIVE_PROBE") && model != ""
	shield := guardrails.NewPromptInjectionShield()
	cfg := redteam.ProberConfig{
		Live: live,
		Scan: shield.Scan,
		OnError: func(err error) {
			a.logger.Error("redteam run failed", "error", err)
		},
	}
	if live {
		cfg.Probe = func(ctx context.Context, prompt string) (string, error) {
			resp, _, err := a.gateway.Complete(ctx, chatRequest(model, prompt))
			if err != nil {
				return "", err
			}
			if len(resp.Choices) == 0 {
				return "", errors.New("no choices")
			}
			return resp.Choices[0].Message.TextContent(), nil
		}
	}
	prober, err := redteam.NewProber(cfg)
	if err != nil {
		return fmt.Errorf("redteam: %w", err)
	}
	if a.persist != nil {
		if err := prober.EnablePersistence(a.persist); err != nil {
			return fmt.Errorf("redteam: %w", err)
		}
	}
	if live {
		a.goWorker("redteam", prober.Start)
	}
	mux.Handle("/v1/redteam/findings", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"dry_run": prober.DryRun(), "findings": prober.Findings(), "last_report": prober.LastReport()})
	}))
	mux.Handle("/v1/redteam/run", adminFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			middleware.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed", "")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		report, err := prober.Run(ctx)
		if err != nil {
			middleware.WriteJSONError(w, http.StatusBadGateway, "redteam run failed: "+err.Error(), "")
			return
		}
		writeJSON(w, http.StatusOK, report)
	}))
	return nil
}
