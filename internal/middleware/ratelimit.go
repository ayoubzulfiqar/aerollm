package middleware

import (
	"context"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
)

// RateLimitOptions configures RateLimit.
type RateLimitOptions struct {
	Limiter ratelimit.RateLimiter
	// DefaultTPM is advertised in X-RateLimit-*-Tokens headers.
	DefaultTPM int
}

// limitSetter is implemented by limiters that support per-key overrides.
type limitSetter interface {
	SetLimit(apiKey string, l ratelimit.Limit)
}

// perKeyRPS reads a virtual key's rate override from its metadata.
func perKeyRPS(p *Principal) float64 {
	if p == nil || p.Virtual == nil || p.Virtual.Metadata == nil {
		return 0
	}
	return metadataFloat(p.Virtual.Metadata["rate_limit_rps"])
}

func perKeyTPM(p *Principal) int {
	if p == nil || p.Virtual == nil || p.Virtual.Metadata == nil {
		return 0
	}
	return int(metadataFloat(p.Virtual.Metadata["rate_limit_tpm"]))
}

func metadataFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

// RateLimit enforces the limiter per authenticated key (by KeyID, never the
// raw key) and sets OpenAI-compatible X-RateLimit-* headers. Rejected
// requests get 429 with Retry-After. Requests without a principal are keyed
// by client IP.
func RateLimit(opts RateLimitOptions) Middleware {
	tpm := opts.DefaultTPM
	if tpm <= 0 {
		tpm = 60000
	}
	// appliedLimits remembers which per-key overrides were pushed into the
	// limiter so they are applied once instead of resetting the bucket on
	// every request.
	var appliedLimits sync.Map // map[string]float64
	return func(next http.Handler) http.Handler {
		if opts.Limiter == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			p, _ := PrincipalFromContext(ctx)
			bucket := clientBucket(r, p)

			if rps := perKeyRPS(p); rps > 0 {
				if s, ok := opts.Limiter.(limitSetter); ok {
					if prev, loaded := appliedLimits.Load(bucket); !loaded || prev.(float64) != rps {
						s.SetLimit(bucket, ratelimit.Limit{RPS: rps})
						appliedLimits.Store(bucket, rps)
					}
				}
			}
			keyTPM := tpm
			if t := perKeyTPM(p); t > 0 {
				keyTPM = t
			}

			allowed, err := opts.Limiter.Allow(ctx, bucket, "")
			if err != nil && ctx.Err() != nil {
				return // client went away
			}
			rec, _ := opts.Limiter.GetLimits(ctx, bucket, "")
			setRateLimitHeaders(w, rec, keyTPM)
			if err != nil || !allowed {
				retry := time.Second
				if rec != nil && rec.RetryAfter > 0 {
					retry = rec.RetryAfter
				}
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
				WriteJSONError(w, http.StatusTooManyRequests, "rate limit exceeded", "")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, rateLimitAppliedKey{}, true)))
		})
	}
}

type rateLimitAppliedKey struct{}

// RateLimitApplied reports whether the RateLimit middleware already charged
// this request, so handlers don't consume a second token.
func RateLimitApplied(ctx context.Context) bool {
	v, _ := ctx.Value(rateLimitAppliedKey{}).(bool)
	return v
}

func clientBucket(r *http.Request, p *Principal) string {
	if p != nil && p.KeyID != "" {
		return p.KeyID
	}
	if key := APIKeyFromRequest(r); key != "" {
		return KeyID(key)
	}
	return "ip:" + clientIP(r)
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func setRateLimitHeaders(w http.ResponseWriter, rec *ratelimit.RateLimitRecord, tpm int) {
	if rec == nil {
		return
	}
	reset := time.Until(time.Unix(rec.ResetAt, 0))
	if reset < 0 {
		reset = 0
	}
	h := w.Header()
	h.Set("X-RateLimit-Limit-Requests", strconv.Itoa(rec.Limit))
	h.Set("X-RateLimit-Remaining-Requests", strconv.Itoa(max(rec.Remaining, 0)))
	h.Set("X-RateLimit-Reset-Requests", strconv.Itoa(int(math.Ceil(reset.Seconds())))+"s")
	h.Set("X-RateLimit-Limit-Tokens", strconv.Itoa(tpm))
}

// RateLimitHeadersMiddleware injects standard OpenAI-compatible rate limit
// headers (X-RateLimit-{Limit,Remaining,Reset}-{Requests,Tokens}) into every
// response, using the limiter's live bucket state for the caller's key.
// It does not reject requests; use RateLimit to enforce.
type RateLimitHeadersMiddleware struct {
	Next        http.HandlerFunc
	RateLimiter ratelimit.RateLimiter
	DefaultRPS  float64
	// DefaultTPM is the default token-per-minute limit applied when no per-key
	// or per-tenant limit is configured.
	DefaultTPM int
}

// NewRateLimitHeadersMiddleware creates a middleware that injects OpenAI-compatible
// rate limit headers into every response.
func NewRateLimitHeadersMiddleware(next http.HandlerFunc, rl ratelimit.RateLimiter, defaultRPS float64, defaultTPM int) *RateLimitHeadersMiddleware {
	if defaultRPS <= 0 {
		defaultRPS = 10.0
	}
	if defaultTPM <= 0 {
		defaultTPM = 60000
	}
	return &RateLimitHeadersMiddleware{
		Next:        next,
		RateLimiter: rl,
		DefaultRPS:  defaultRPS,
		DefaultTPM:  defaultTPM,
	}
}

// ServeHTTP sets the headers and calls the next handler.
func (m *RateLimitHeadersMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	info := m.getLimits(r)
	h := w.Header()
	h.Set("X-RateLimit-Limit-Requests", strconv.Itoa(info.LimitRequests))
	h.Set("X-RateLimit-Limit-Tokens", strconv.Itoa(info.LimitTokens))
	h.Set("X-RateLimit-Remaining-Requests", strconv.Itoa(info.RemainingRequests))
	h.Set("X-RateLimit-Remaining-Tokens", strconv.Itoa(info.RemainingTokens))
	h.Set("X-RateLimit-Reset-Requests", strconv.Itoa(int(info.ResetRequests)))
	h.Set("X-RateLimit-Reset-Tokens", strconv.Itoa(int(info.ResetTokens)))
	m.Next(w, r)
}

func (m *RateLimitHeadersMiddleware) getLimits(r *http.Request) RateLimitInfo {
	p, _ := PrincipalFromContext(r.Context())
	if p == nil {
		if vk, ok := VirtualKeyFromContext(r.Context()); ok {
			p = &Principal{Virtual: vk}
		}
	}
	bucket := clientBucket(r, p)
	tpm := m.DefaultTPM
	if t := perKeyTPM(p); t > 0 {
		tpm = t
	}
	info := RateLimitInfo{
		APIKey:            bucket,
		LimitRequests:     int(m.DefaultRPS),
		LimitTokens:       tpm,
		RemainingRequests: int(m.DefaultRPS),
		RemainingTokens:   tpm,
		ResetRequests:     0,
		ResetTokens:       60,
	}
	if rps := perKeyRPS(p); rps > 0 {
		info.LimitRequests = int(rps)
		info.RemainingRequests = int(rps)
	}
	if m.RateLimiter != nil {
		if rec, err := m.RateLimiter.GetLimits(r.Context(), bucket, ""); err == nil && rec != nil && rec.Limit > 0 {
			info.LimitRequests = rec.Limit
			info.RemainingRequests = max(rec.Remaining, 0)
			if reset := time.Until(time.Unix(rec.ResetAt, 0)).Seconds(); reset > 0 {
				info.ResetRequests = math.Ceil(reset)
			}
		}
	}
	return info
}

// RateLimitInfo holds the computed rate limit headers for the current request.
type RateLimitInfo struct {
	APIKey            string
	LimitRequests     int
	LimitTokens       int
	RemainingRequests int
	RemainingTokens   int
	ResetRequests     float64 // seconds until reset
	ResetTokens       float64 // seconds until reset
}

// VirtualKeyOrAuth extracts the API key from the request, preferring the
// Bearer token. This is used as a fallback when no VirtualKey is in context.
func VirtualKeyOrAuth(r *http.Request) string {
	return APIKeyFromRequest(r)
}
