package guardrails

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
)

// APIKeyScope defines allowed constraints for an API key.
//
//   - AllowedModels: if non-empty, only these models may be requested
//     (case-insensitive; "*" allows all, a trailing "*" is a prefix wildcard).
//   - MaxBudgetUSD: if > 0, requests are rejected once the spend recorded via
//     APIKeyScoper.RecordSpend reaches it. Budget is only enforced if spend is
//     reported.
//   - IPAllowlist: if non-empty, the client IP must match one of the entries
//     (single IPs such as "127.0.0.1"/"::1" or CIDRs such as "10.0.0.0/8").
//     Invalid entries are ignored, so an allowlist containing only invalid
//     entries denies everything (fail closed).
type APIKeyScope struct {
	APIKey        string
	AllowedModels []string
	MaxBudgetUSD  float64
	IPAllowlist   []string
}

type compiledScope struct {
	scope    APIKeyScope
	prefixes []netip.Prefix
	spend    float64
}

// APIKeyScoper validates requests against API key scopes. It is safe for
// concurrent use. Keys are stored by SHA-256 hash only.
//
// Keys WITHOUT a registered scope are not restricted by the scoper: Validate
// returns nil for them. Authentication is the responsibility of the auth
// middleware.
type APIKeyScoper struct {
	mu             sync.RWMutex
	scopes         map[[sha256.Size]byte]*compiledScope
	trustedProxies []netip.Prefix
}

// Scope errors.
var (
	ErrModelNotAllowed = errors.New("model not allowed for this api key")
	ErrModelRequired   = errors.New("model must be specified for this api key")
	ErrIPNotAllowed    = errors.New("client ip not allowed for this api key")
	ErrBudgetExceeded  = errors.New("budget exceeded for this api key")
)

// NewAPIKeyScoper creates a new scoper.
func NewAPIKeyScoper() *APIKeyScoper {
	return &APIKeyScoper{scopes: make(map[[sha256.Size]byte]*compiledScope)}
}

func keyID(apiKey string) [sha256.Size]byte {
	return sha256.Sum256([]byte(bearerToken(apiKey)))
}

// bearerToken strips surrounding whitespace and an optional case-insensitive
// "Bearer " scheme.
func bearerToken(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 7 && strings.EqualFold(s[:7], "bearer ") {
		s = strings.TrimSpace(s[7:])
	}
	return s
}

// parseIPOrPrefix parses "1.2.3.4", "::1", "10.0.0.0/8" or "fe80::/10".
func parseIPOrPrefix(s string) (netip.Prefix, error) {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, err
		}
		if p.Addr().Is4In6() {
			p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
		}
		return p.Masked(), nil
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	a = a.Unmap().WithZone("")
	return netip.PrefixFrom(a, a.BitLen()), nil
}

// AddScopeChecked registers (or replaces) an API key scope and reports
// invalid IP allowlist entries. Invalid entries are dropped; if every entry
// was invalid the key is denied from all IPs (fail closed).
func (a *APIKeyScoper) AddScopeChecked(scope APIKeyScope) error {
	if bearerToken(scope.APIKey) == "" {
		return errors.New("guardrails: empty api key in scope")
	}
	if math.IsNaN(scope.MaxBudgetUSD) || scope.MaxBudgetUSD < 0 {
		return errors.New("guardrails: invalid MaxBudgetUSD")
	}
	cs := &compiledScope{scope: scope}
	cs.scope.AllowedModels = append([]string(nil), scope.AllowedModels...)
	cs.scope.IPAllowlist = append([]string(nil), scope.IPAllowlist...)
	var bad []string
	for _, entry := range scope.IPAllowlist {
		p, err := parseIPOrPrefix(entry)
		if err != nil {
			bad = append(bad, entry)
			continue
		}
		cs.prefixes = append(cs.prefixes, p)
	}
	id := keyID(scope.APIKey)
	a.mu.Lock()
	if prev, ok := a.scopes[id]; ok {
		cs.spend = prev.spend
	}
	a.scopes[id] = cs
	a.mu.Unlock()
	if len(bad) > 0 {
		return fmt.Errorf("guardrails: ignored invalid ip allowlist entries: %q", bad)
	}
	return nil
}

// AddScope registers an API key scope. See AddScopeChecked for error details.
func (a *APIKeyScoper) AddScope(scope APIKeyScope) {
	_ = a.AddScopeChecked(scope)
}

// RemoveScope unregisters the scope for apiKey.
func (a *APIKeyScoper) RemoveScope(apiKey string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.scopes, keyID(apiKey))
}

// HasScope reports whether apiKey has a registered scope.
func (a *APIKeyScoper) HasScope(apiKey string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, ok := a.scopes[keyID(apiKey)]
	return ok
}

// SetTrustedProxies configures reverse proxies (IPs or CIDRs) whose
// X-Forwarded-For header is trusted when determining the client IP. By
// default no proxy is trusted and only r.RemoteAddr is used.
func (a *APIKeyScoper) SetTrustedProxies(entries ...string) error {
	var prefixes []netip.Prefix
	for _, e := range entries {
		p, err := parseIPOrPrefix(e)
		if err != nil {
			return fmt.Errorf("guardrails: invalid trusted proxy %q: %w", e, err)
		}
		prefixes = append(prefixes, p)
	}
	a.mu.Lock()
	a.trustedProxies = prefixes
	a.mu.Unlock()
	return nil
}

// RecordSpend adds usd to the tracked spend of a scoped key. Unscoped keys
// are ignored.
func (a *APIKeyScoper) RecordSpend(apiKey string, usd float64) error {
	if math.IsNaN(usd) || math.IsInf(usd, 0) || usd < 0 {
		return errors.New("guardrails: spend must be a finite non-negative number")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if cs, ok := a.scopes[keyID(apiKey)]; ok {
		cs.spend += usd
	}
	return nil
}

func inPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

func parseAddr(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap().WithZone(""), true
}

// ClientIP returns the client address of r: r.RemoteAddr, or — only when
// RemoteAddr is a configured trusted proxy — the right-most untrusted
// address in X-Forwarded-For.
func (a *APIKeyScoper) ClientIP(r *http.Request) (netip.Addr, bool) {
	remote, ok := parseAddr(r.RemoteAddr)
	if !ok {
		return netip.Addr{}, false
	}
	a.mu.RLock()
	trusted := a.trustedProxies
	a.mu.RUnlock()
	if len(trusted) == 0 || !inPrefixes(remote, trusted) {
		return remote, true
	}
	var hops []string
	for _, v := range r.Header.Values("X-Forwarded-For") {
		hops = append(hops, strings.Split(v, ",")...)
	}
	client := remote
	for i := len(hops) - 1; i >= 0; i-- {
		addr, ok := parseAddr(hops[i])
		if !ok {
			break // malformed entry: stop at the last trustworthy hop
		}
		client = addr
		if !inPrefixes(addr, trusted) {
			break
		}
	}
	return client, true
}

func modelAllowed(allowed []string, model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	for _, m := range allowed {
		m = strings.ToLower(strings.TrimSpace(m))
		switch {
		case m == "*":
			return true
		case strings.HasSuffix(m, "*") && model != "" && strings.HasPrefix(model, strings.TrimSuffix(m, "*")):
			return true
		case m == model && model != "":
			return true
		}
	}
	return false
}

// Validate checks whether a request is allowed by the API key scope. apiKey
// may include a "Bearer " prefix; clientIP may be "ip" or "ip:port".
// Keys without a registered scope are allowed (nil).
func (a *APIKeyScoper) Validate(apiKey, model, clientIP string) error {
	return a.validate(apiKey, model, clientIP, false)
}

// validate implements Validate; skipModel disables the model check for
// requests that cannot name a model (e.g. GET without a body).
func (a *APIKeyScoper) validate(apiKey, model, clientIP string, skipModel bool) error {
	a.mu.RLock()
	cs, ok := a.scopes[keyID(apiKey)]
	var spend float64
	if ok {
		spend = cs.spend
	}
	a.mu.RUnlock()
	if !ok {
		return nil
	}
	sc := cs.scope
	if len(sc.AllowedModels) > 0 && !skipModel {
		if strings.TrimSpace(model) == "" {
			return ErrModelRequired
		}
		if !modelAllowed(sc.AllowedModels, model) {
			return ErrModelNotAllowed
		}
	}
	if len(sc.IPAllowlist) > 0 {
		addr, ok := parseAddr(clientIP)
		if !ok || !inPrefixes(addr, cs.prefixes) {
			return ErrIPNotAllowed
		}
	}
	if sc.MaxBudgetUSD > 0 && spend >= sc.MaxBudgetUSD {
		return ErrBudgetExceeded
	}
	return nil
}

func (a *APIKeyScoper) scopeFor(apiKey string) (APIKeyScope, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	cs, ok := a.scopes[keyID(apiKey)]
	if !ok {
		return APIKeyScope{}, false
	}
	return cs.scope, true
}
