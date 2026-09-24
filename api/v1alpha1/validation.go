package v1alpha1

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

// Kind names of the AeroLLM custom resources.
const (
	KindAeroRoute         = "AeroRoute"
	KindAeroBudget        = "AeroBudget"
	KindAeroAgentPipeline = "AeroAgentPipeline"
)

// MaxNameLength is the maximum length of a Kubernetes object name
// (DNS-1123 subdomain).
const MaxNameLength = 253

// maxEntryLength bounds list entries such as model, provider, node and tool names.
const maxEntryLength = 128

var dns1123Subdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// ValidateName checks that name is a valid DNS-1123 subdomain, the format
// Kubernetes requires for object names.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("metadata.name: required")
	}
	if len(name) > MaxNameLength {
		return fmt.Errorf("metadata.name: longer than %d characters", MaxNameLength)
	}
	if !dns1123Subdomain.MatchString(name) {
		return fmt.Errorf("metadata.name %q: must be a lowercase RFC 1123 subdomain", name)
	}
	return nil
}

// validateObject checks the common object envelope and folds in specErr.
func validateObject(wantKind, kind string, metadata map[string]interface{}, specErr error) error {
	var errs []error
	if kind != "" && kind != wantKind {
		errs = append(errs, fmt.Errorf("kind: expected %q, got %q", wantKind, kind))
	}
	name, _ := metadata["name"].(string)
	if err := ValidateName(name); err != nil {
		errs = append(errs, err)
	}
	if specErr != nil {
		errs = append(errs, fmt.Errorf("spec: %w", specErr))
	}
	return errors.Join(errs...)
}

func validateMoney(field string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("%s: must be a finite number", field)
	}
	if v < 0 {
		return fmt.Errorf("%s: must be >= 0, got %g", field, v)
	}
	return nil
}

func validateHTTPSURL(field, raw string) error {
	if hasControlChars(raw) {
		return fmt.Errorf("%s: contains control characters", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s: invalid URL", field)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("%s: must use https", field)
	}
	if u.Host == "" || u.Hostname() == "" {
		return fmt.Errorf("%s: missing host", field)
	}
	if u.User != nil {
		return fmt.Errorf("%s: must not embed credentials", field)
	}
	return nil
}

var secretRefPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?/[-._a-zA-Z0-9]+$`)

func validateSecretRef(ref string) error {
	if len(ref) > MaxNameLength+maxEntryLength+1 || !secretRefPattern.MatchString(ref) {
		return fmt.Errorf("api_key_secret_ref %q: must be <secret-name>/<key>", ref)
	}
	return nil
}

// validateEntries checks that list entries are non-empty, bounded and unique.
func validateEntries(field string, entries []string) []error {
	var errs []error
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		switch {
		case strings.TrimSpace(e) == "":
			errs = append(errs, fmt.Errorf("%s[%d]: must not be empty", field, i))
			continue
		case len(e) > maxEntryLength:
			errs = append(errs, fmt.Errorf("%s[%d]: longer than %d characters", field, i, maxEntryLength))
			continue
		case hasControlChars(e):
			errs = append(errs, fmt.Errorf("%s[%d]: contains control characters", field, i))
			continue
		}
		if _, dup := seen[e]; dup {
			errs = append(errs, fmt.Errorf("%s: duplicate entry %q", field, e))
			continue
		}
		seen[e] = struct{}{}
	}
	return errs
}

func hasControlChars(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
