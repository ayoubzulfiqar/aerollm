package guardrails

import (
	"io"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// PIIPatterns defines regex patterns for common PII.
type PIIPatterns struct {
	Email      *regexp.Regexp
	PhoneUS    *regexp.Regexp
	SSN        *regexp.Regexp
	CreditCard *regexp.Regexp
}

// DefaultPIIPatterns returns compiled regexes for common PII types.
func DefaultPIIPatterns() PIIPatterns {
	return PIIPatterns{
		Email:      regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`),
		PhoneUS:    regexp.MustCompile(`\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){2}\d{4}\b`),
		SSN:        regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`),
		CreditCard: regexp.MustCompile(`\b(?:\d{4}[-\s]){3}\d{4}\b`),
	}
}

// PIIRedactor masks PII in text and tracks replacements for reversal.
type PIIRedactor struct {
	patterns PIIPatterns
	counters map[string]int
}

// NewPIIRedactor creates a new redactor with default patterns.
func NewPIIRedactor() *PIIRedactor {
	return &PIIRedactor{
		patterns: DefaultPIIPatterns(),
		counters: make(map[string]int),
	}
}

// Redact masks PII with placeholders like <PII_EMAIL_1>.
func (p *PIIRedactor) Redact(text string) string {
	p.counters = make(map[string]int)
	result := text

	result = p.patterns.Email.ReplaceAllStringFunc(result, func(match string) string {
		p.counters["email"]++
		return fmt.Sprintf("<PII_EMAIL_%d>", p.counters["email"])
	})

	result = p.patterns.PhoneUS.ReplaceAllStringFunc(result, func(match string) string {
		p.counters["phone"]++
		return fmt.Sprintf("<PII_PHONE_%d>", p.counters["phone"])
	})

	result = p.patterns.SSN.ReplaceAllStringFunc(result, func(match string) string {
		p.counters["ssn"]++
		return fmt.Sprintf("<PII_SSN_%d>", p.counters["ssn"])
	})

	result = p.patterns.CreditCard.ReplaceAllStringFunc(result, func(match string) string {
		p.counters["credit_card"]++
		return fmt.Sprintf("<PII_CC_%d>", p.counters["credit_card"])
	})

	return result
}

// Restore reverses redaction using the stored placeholders.
func (p *PIIRedactor) Restore(original, redacted string) string {
	if original == "" {
		return redacted
	}

	type replacement struct {
		placeholder string
		original    string
	}

	var replacements []replacement

	addReplacements := func(pattern *regexp.Regexp) {
		matches := pattern.FindAllString(original, -1)
		placeholders := pattern.FindAllString(redacted, -1)
		for i, m := range matches {
			if i < len(placeholders) {
				replacements = append(replacements, replacement{placeholder: placeholders[i], original: m})
			}
		}
	}

	addReplacements(p.patterns.Email)
	addReplacements(p.patterns.PhoneUS)
	addReplacements(p.patterns.SSN)
	addReplacements(p.patterns.CreditCard)

	result := redacted
	for _, r := range replacements {
		result = strings.Replace(result, r.placeholder, r.original, -1)
	}

	return result
}

// InjectionPatterns returns common prompt injection signatures.
func InjectionPatterns() []string {
	return []string{
		"ignore previous instructions",
		"ignore all previous instructions",
		"disregard all prior instructions",
		"disregard previous instructions",
		"forget your instructions",
		"override your programming",
		"jailbreak",
		"pretend you are",
		"act as if you are not",
		"you are now",
		"new instructions",
		"ignore the above",
		"system prompt",
		"reveal your prompt",
		"show me your instructions",
	}
}

// PromptInjectionShield detects prompt injection attempts.
type PromptInjectionShield struct {
	patterns []string
}

// NewPromptInjectionShield creates a new shield with default patterns.
func NewPromptInjectionShield() *PromptInjectionShield {
	return &PromptInjectionShield{patterns: InjectionPatterns()}
}

// Scan checks text for injection patterns and returns true if blocked.
func (s *PromptInjectionShield) Scan(text string) bool {
	lower := strings.ToLower(text)
	for _, pattern := range s.patterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// APIKeyScope defines allowed constraints for an API key.
type APIKeyScope struct {
	APIKey       string
	AllowedModels []string
	MaxBudgetUSD float64
	IPAllowlist  []string
}

// APIKeyScoper validates requests against API key scopes.
type APIKeyScoper struct {
	scopes map[string]APIKeyScope
}

// NewAPIKeyScoper creates a new scoper.
func NewAPIKeyScoper() *APIKeyScoper {
	return &APIKeyScoper{scopes: make(map[string]APIKeyScope)}
}

// AddScope registers an API key scope.
func (a *APIKeyScoper) AddScope(scope APIKeyScope) {
	a.scopes[scope.APIKey] = scope
}

// Validate checks whether the request is allowed by the API key scope.
func (a *APIKeyScoper) Validate(apiKey, model, clientIP string) error {
	scope, ok := a.scopes[apiKey]
	if !ok {
		return fmt.Errorf("unknown api key")
	}

	if len(scope.AllowedModels) > 0 {
		allowed := false
		for _, m := range scope.AllowedModels {
			if strings.EqualFold(m, model) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("model %q not allowed for this api key", model)
		}
	}

	if len(scope.IPAllowlist) > 0 {
		allowed := false
		for _, ip := range scope.IPAllowlist {
			if strings.EqualFold(ip, clientIP) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("ip %q not allowed for this api key", clientIP)
		}
	}

	return nil
}

// PIIMiddleware returns a handler that redacts PII from the request body.
func PIIMiddleware(next http.HandlerFunc) http.HandlerFunc {
	redactor := NewPIIRedactor()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > 0 && strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(r.Body)
			redacted := redactor.Redact(buf.String())
			r.Body = io.NopCloser(bytes.NewBufferString(redacted))
			ctx := context.WithValue(r.Context(), "original_body", buf.String())
			r = r.WithContext(ctx)
		}
		next(w, r)
	}
}

// InjectionShieldMiddleware returns a handler that blocks prompt injection.
func InjectionShieldMiddleware(next http.HandlerFunc) http.HandlerFunc {
	shield := NewPromptInjectionShield()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > 0 && strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(r.Body)
			body := buf.String()
			if shield.Scan(body) {
				http.Error(w, `{"error":"prompt injection detected"}`, http.StatusForbidden)
				return
			}
			r.Body = io.NopCloser(bytes.NewBufferString(body))
		}
		next(w, r)
	}
}

// APIKeyScopingMiddleware returns a handler that enforces API key scope constraints.
func APIKeyScopingMiddleware(scoper *APIKeyScoper) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			apiKey := r.Header.Get("Authorization")
			if apiKey == "" {
				http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
				return
			}
			if err := scoper.Validate(apiKey, r.URL.Query().Get("model"), r.RemoteAddr); err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusForbidden)
				return
			}
			next(w, r)
		}
	}
}
