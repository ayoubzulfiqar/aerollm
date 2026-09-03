package marketplace

import (
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/tenant"
)

// RequesterMiddleware returns an HTTP middleware that resolves the caller tenant
// and injects it into the request context. Requests without a valid API key
// receive HTTP 401.
func RequesterMiddleware(resolver tenant.Resolver, next http.HandlerFunc) http.HandlerFunc {
	if resolver == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("Authorization")
		if apiKey == "" {
			http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
			return
		}
		key, err := resolver.ResolveByAPIKey(r.Context(), apiKey)
		if err != nil || key == nil {
			http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
			return
		}
		ctx := tenant.WithTenantContext(r.Context(), key)
		next(w, r.WithContext(ctx))
	}
}
