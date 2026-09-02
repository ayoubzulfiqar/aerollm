package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

// MetricsResponse represents the metrics response.
type MetricsResponse struct {
	Requests    int64         `json:"requests"`
	CacheHits   int64         `json:"cache_hits"`
	Errors      int64         `json:"errors"`
	AvgLatency  float64       `json:"avg_latency_ms"`
	Providers   []ProviderMetric `json:"providers"`
}

// ProviderMetric represents metrics for a single provider.
type ProviderMetric struct {
	Name      string  `json:"name"`
	Requests  int64   `json:"requests"`
	LatencyMs float64 `json:"avg_latency_ms"`
}

// MetricsMiddleware exposes a /metrics endpoint with basic stats.
func MetricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(MetricsResponse{
				Requests:   telemetry.RequestCount(),
				CacheHits:  telemetry.CacheHits(),
				Errors:     telemetry.ErrorCount(),
				AvgLatency: telemetry.AvgLatency(),
			})
			return
		}
		next(w, r)
	}
}
