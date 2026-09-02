package licensing

import (
	"fmt"
	"net/http"
)

// Middleware returns an HTTP middleware that gates enterprise features by license.
func Middleware(checker LicenseChecker, feature Feature) func(http.Handler) http.Handler {
	if checker == nil {
		checker = &EnvLicenseChecker{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !checker.IsFeatureEnabled(feature) {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, (&ErrFeatureGated{Feature: string(feature)}).Error()), http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
