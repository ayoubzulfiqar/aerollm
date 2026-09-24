package licensing

import (
	"encoding/json"
	"net/http"
)

// Middleware returns an HTTP middleware that gates an enterprise feature by
// license. Unlicensed requests get 403 with a JSON error. A nil checker
// means community mode (enterprise features disabled).
func Middleware(checker LicenseChecker, feature Feature) func(http.Handler) http.Handler {
	if checker == nil {
		checker = &EnvLicenseChecker{err: ErrNoLicense}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !checker.IsFeatureEnabled(feature) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   (&ErrFeatureGated{Feature: string(feature)}).Error(),
					"feature": string(feature),
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
