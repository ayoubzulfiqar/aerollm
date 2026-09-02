package tenant

import (
	"context"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

// Resolver resolves tenant information from an API key.
type Resolver interface {
	ResolveByAPIKey(ctx context.Context, apiKey string) (*APIKey, error)
}

// Middleware returns an HTTP middleware that resolves the tenant from the
// Authorization header and injects it into the request context.
//
// It delegates to the standard AeroLLM auth middleware first, then enriches
// the context with tenant information for downstream handlers.
func Middleware(resolver Resolver, next http.HandlerFunc) http.HandlerFunc {
	if resolver == nil {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		apiKey := middleware.APIKeyFromRequest(r)
		if apiKey == "" {
			http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
			return
		}

		key, err := resolver.ResolveByAPIKey(ctx, apiKey)
		if err != nil || key == nil {
			http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
			return
		}

		ctx = WithTenantContext(ctx, key)
		next(w, r.WithContext(ctx))
	}
}
