package tenant

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// Resolver resolves tenant information from an API key.
type Resolver interface {
	ResolveByAPIKey(ctx context.Context, apiKey string) (*APIKey, error)
}

// TenantHeaders are request headers clients commonly use to name a tenant.
// They are never trusted for authorization: Middleware rejects a request
// whose header disagrees with the tenant of the authenticated API key.
var TenantHeaders = []string{"X-Tenant-ID", "X-AeroLLM-Tenant", "X-Org-ID"}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// APIKeyFromRequest extracts the credential from the Authorization header
// ("Bearer <key>" or a bare key) or the x-api-key header.
func APIKeyFromRequest(r *http.Request) string {
	if k := normalizeAPIKey(r.Header.Get("Authorization")); k != "" {
		return k
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

// Middleware returns an HTTP middleware that resolves the tenant from the
// request's API key and injects it into the request context.
//
// The tenant is derived exclusively from the authenticated API key. A
// client-supplied tenant header (see TenantHeaders) that names a different
// tenant is rejected with 403; it can never switch tenants. Inactive keys
// are rejected with 401. A nil resolver disables the middleware.
func Middleware(resolver Resolver, next http.HandlerFunc) http.HandlerFunc {
	if resolver == nil {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		apiKey := APIKeyFromRequest(r)
		if apiKey == "" {
			writeJSONError(w, http.StatusUnauthorized, "missing api key")
			return
		}

		key, err := resolver.ResolveByAPIKey(ctx, apiKey)
		if err != nil || key == nil || !key.Active {
			writeJSONError(w, http.StatusUnauthorized, "invalid api key")
			return
		}

		for _, h := range TenantHeaders {
			if v := strings.TrimSpace(r.Header.Get(h)); v != "" && TenantID(v) != key.TenantID {
				writeJSONError(w, http.StatusForbidden, "tenant header does not match api key")
				return
			}
		}

		ctx = WithTenantContext(ctx, key)
		next(w, r.WithContext(ctx))
	}
}
