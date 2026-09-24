package marketplace

import (
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
)

// maxAPIKeyLen bounds the credential length accepted from the Authorization header.
const maxAPIKeyLen = 512

// apiKeyFromHeader extracts the credential from "Authorization: Bearer <key>"
// (scheme case-insensitive) or a bare key.
func apiKeyFromHeader(h string) string {
	h = strings.TrimSpace(h)
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		h = strings.TrimSpace(h[7:])
	}
	return h
}

// RequesterMiddleware returns an HTTP middleware that resolves the caller tenant
// and injects it into the request context. Requests without a valid API key
// receive HTTP 401. A nil resolver fails closed (HTTP 503) instead of silently
// disabling authentication.
func RequesterMiddleware(resolver tenant.Resolver, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if resolver == nil || next == nil {
			WriteJSONError(w, http.StatusServiceUnavailable, "authentication not configured")
			return
		}
		apiKey := apiKeyFromHeader(r.Header.Get("Authorization"))
		if apiKey == "" {
			w.Header().Set("WWW-Authenticate", "Bearer")
			WriteJSONError(w, http.StatusUnauthorized, "missing api key")
			return
		}
		if len(apiKey) > maxAPIKeyLen {
			WriteJSONError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		key, err := resolver.ResolveByAPIKey(r.Context(), apiKey)
		if err != nil || key == nil || !key.Active {
			w.Header().Set("WWW-Authenticate", "Bearer")
			WriteJSONError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		ctx := tenant.WithTenantContext(r.Context(), key)
		next(w, r.WithContext(ctx))
	}
}
