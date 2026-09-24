package traffic

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits for ResultsHandler.
const (
	// DefaultResultsLimit is the number of results returned without ?limit=.
	DefaultResultsLimit = 50
	// MaxResultsLimit caps ?limit= (the ring buffer bounds it further).
	MaxResultsLimit = 1000

	maxErrorMessageLen = 512
)

// ShadowResultView is the public, secrets-free JSON form of a ShadowResult
// served by ResultsHandler. The provider is reduced to scheme://host[:port]
// so path segments of the shadow URL (which may embed tenant tokens) are
// never exposed; API keys are never part of a result.
type ShadowResultView struct {
	Provider    string    `json:"provider"`
	Timestamp   time.Time `json:"timestamp"`
	LatencyMS   float64   `json:"latency_ms"`
	StatusCode  int       `json:"status_code,omitempty"`
	OK          bool      `json:"ok"`
	AuthDropped bool      `json:"auth_dropped,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// ShadowResultsSummary aggregates the returned results.
type ShadowResultsSummary struct {
	Count        int     `json:"count"`
	Errors       int     `json:"errors"`
	AvgLatencyMS float64 `json:"avg_latency_ms"`
	MaxLatencyMS float64 `json:"max_latency_ms"`
}

// ShadowResultsResponse is the body returned by ResultsHandler.
type ShadowResultsResponse struct {
	Results []ShadowResultView   `json:"results"`
	Summary ShadowResultsSummary `json:"summary"`
	// Buffered is the total number of results currently held in memory.
	Buffered int `json:"buffered"`
}

// ResultsHandler serves the most recent shadow results, newest first, for
// GET/HEAD /v1/shadow/results. It must be mounted behind admin auth.
//
// Query parameters:
//
//	limit=N       number of results (default 50, max 1000, bounded by the buffer)
//	errors=true   only failed results
//
// A nil tester serves an empty list.
func (s *ShadowTester) ResultsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeResultsError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		q := r.URL.Query()
		limit := DefaultResultsLimit
		if raw := q.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 {
				writeResultsError(w, http.StatusBadRequest, "limit must be a positive integer")
				return
			}
			limit = min(n, MaxResultsLimit)
		}
		errorsOnly := false
		if raw := q.Get("errors"); raw != "" {
			b, err := strconv.ParseBool(raw)
			if err != nil {
				writeResultsError(w, http.StatusBadRequest, "errors must be a boolean")
				return
			}
			errorsOnly = b
		}

		var all []ShadowResult
		if s != nil {
			all = s.Results()
		}
		resp := ShadowResultsResponse{Results: make([]ShadowResultView, 0, min(limit, len(all))), Buffered: len(all)}
		var total float64
		for i := len(all) - 1; i >= 0 && len(resp.Results) < limit; i-- {
			v := viewOf(all[i])
			if errorsOnly && v.OK {
				continue
			}
			resp.Results = append(resp.Results, v)
			total += v.LatencyMS
			if !v.OK {
				resp.Summary.Errors++
			}
			resp.Summary.MaxLatencyMS = max(resp.Summary.MaxLatencyMS, v.LatencyMS)
		}
		resp.Summary.Count = len(resp.Results)
		if resp.Summary.Count > 0 {
			resp.Summary.AvgLatencyMS = total / float64(resp.Summary.Count)
		}
		w.Header().Set("Cache-Control", "no-store")
		writeResultsJSON(w, http.StatusOK, resp)
	})
}

func viewOf(res ShadowResult) ShadowResultView {
	v := ShadowResultView{
		Provider:    redactProvider(res.Provider),
		Timestamp:   res.Timestamp.UTC(),
		LatencyMS:   float64(res.Latency) / float64(time.Millisecond),
		StatusCode:  res.StatusCode,
		AuthDropped: res.AuthDropped,
	}
	msg := res.ErrorMessage
	if msg == "" && res.Error != nil {
		msg = res.Error.Error()
	}
	v.OK = msg == ""
	if msg != "" {
		// Error strings from net/http quote the full endpoint URL; reduce it
		// to the redacted provider like the provider field itself.
		if base := strings.TrimSuffix(res.Provider, "/"); base != "" && base != v.Provider {
			msg = strings.ReplaceAll(msg, base, v.Provider)
		}
		msg = strings.ToValidUTF8(msg, "�")
		if len(msg) > maxErrorMessageLen {
			cut := maxErrorMessageLen
			for cut > 0 && !utf8.RuneStart(msg[cut]) {
				cut--
			}
			msg = msg[:cut] + "…"
		}
		v.Error = msg
	}
	return v
}

// redactProvider reduces a shadow base URL to scheme://host[:port].
func redactProvider(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
}

func writeResultsJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeResultsError(w http.ResponseWriter, status int, msg string) {
	writeResultsJSON(w, status, map[string]string{"error": msg})
}
