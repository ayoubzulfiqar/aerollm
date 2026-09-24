package compliance

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"
)

// AuditEvent represents a compliance audit event.
type AuditEvent struct {
	Timestamp time.Time
	Policy    string
	Decision  string
	Input     map[string]interface{}
	Reason    string
}

// sensitiveKeys are input/header keys whose values are never written to
// audit output.
var sensitiveKeys = []string{
	"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key",
	"api-key", "api_key", "apikey", "password", "passwd", "secret", "token",
	"access_token", "refresh_token", "private_key", "client_secret",
}

func isSensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	for _, s := range sensitiveKeys {
		if lk == s || strings.HasSuffix(lk, "-"+s) || strings.HasSuffix(lk, "_"+s) {
			return true
		}
	}
	return false
}

// SanitizeInput returns a copy of a policy input with credentials redacted
// (recursively) and the request body replaced by its length, so it can be
// logged safely.
func SanitizeInput(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		switch {
		case isSensitiveKey(k):
			out[k] = "[REDACTED]"
		case k == "body" || k == "data":
			if s, ok := v.(string); ok {
				out[k+"_bytes"] = len(s)
			}
		default:
			out[k] = sanitizeValue(v)
		}
	}
	return out
}

func sanitizeValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return SanitizeInput(t)
	case map[string]string:
		m := make(map[string]interface{}, len(t))
		for k, s := range t {
			m[k] = s
		}
		return SanitizeInput(m)
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = sanitizeValue(e)
		}
		return out
	case []string:
		return append([]string(nil), t...)
	}
	return v
}

// String returns a JSON representation of the audit event with sensitive
// input values redacted.
func (e *AuditEvent) String() string {
	if e == nil {
		return "null"
	}
	out, err := json.Marshal(map[string]interface{}{
		"timestamp": e.Timestamp.UTC().Format(time.RFC3339),
		"policy":    e.Policy,
		"decision":  e.Decision,
		"input":     SanitizeInput(e.Input),
		"reason":    e.Reason,
	})
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, "unserializable audit event")
	}
	return string(out)
}

// AuditLogger logs policy decisions for compliance review.
type AuditLogger interface {
	Log(event *AuditEvent)
}

// DefaultAuditCapacity is the number of events a MemoryAuditLogger keeps.
const DefaultAuditCapacity = 10000

// MemoryAuditLogger stores the most recent audit events in memory. It is
// safe for concurrent use and bounded: once full, the oldest events are
// dropped (Dropped reports how many since the logger was created).
//
// With persistence enabled (NewPersistentAuditLogger) every event is also
// written to a persist.Store and evicted events are deleted from it, so the
// retained window survives restarts. Log cannot return errors: persistence
// failures are counted (PersistErrors) and passed to the handler set with
// SetErrorHandler; use Append to receive them directly.
type MemoryAuditLogger struct {
	mu       sync.Mutex
	events   []*AuditEvent
	capacity int
	dropped  uint64

	disk *auditDisk // non-nil when persistence is enabled
}

// NewMemoryAuditLogger creates a new in-memory audit logger holding up to
// DefaultAuditCapacity events.
func NewMemoryAuditLogger() *MemoryAuditLogger {
	return NewMemoryAuditLoggerWithCapacity(DefaultAuditCapacity)
}

// NewMemoryAuditLoggerWithCapacity creates a logger holding up to capacity
// events (minimum 1).
func NewMemoryAuditLoggerWithCapacity(capacity int) *MemoryAuditLogger {
	if capacity < 1 {
		capacity = 1
	}
	return &MemoryAuditLogger{events: make([]*AuditEvent, 0, min(capacity, 256)), capacity: capacity}
}

// Log appends a copy of the event (with sanitized input). Persistence
// errors are reported through PersistErrors and the error handler.
func (m *MemoryAuditLogger) Log(event *AuditEvent) {
	_ = m.Append(event)
}

// Append is Log returning the persistence error, if any. When the event
// cannot be persisted it is not retained in memory either.
func (m *MemoryAuditLogger) Append(event *AuditEvent) error {
	if m == nil || event == nil {
		return nil
	}
	c := *event
	c.Input = SanitizeInput(event.Input)
	if c.Timestamp.IsZero() {
		c.Timestamp = time.Now().UTC()
	}
	m.mu.Lock()
	err := m.appendLocked(&c)
	handler := m.errorHandlerLocked(err)
	m.mu.Unlock()
	if handler != nil {
		handler(err)
	}
	return err
}

func (m *MemoryAuditLogger) appendLocked(c *AuditEvent) error {
	if m.disk != nil {
		if err := m.disk.put(c); err != nil {
			return err
		}
	}
	var evictErr error
	if len(m.events) >= m.capacity {
		drop := len(m.events) - m.capacity + 1
		clear(m.events[:drop])
		m.events = m.events[drop:]
		m.dropped += uint64(drop)
		if m.disk != nil {
			evictErr = m.disk.evict(drop)
		}
	}
	m.events = append(m.events, c)
	return evictErr
}

// errorHandlerLocked records a persistence failure and returns the handler
// to call (outside the lock), if any.
func (m *MemoryAuditLogger) errorHandlerLocked(err error) func(error) {
	if err == nil || m.disk == nil {
		return nil
	}
	m.disk.failures++
	m.disk.lastErr = err
	return m.disk.onError
}

// Events returns copies of all retained events, oldest first.
func (m *MemoryAuditLogger) Events() []*AuditEvent {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*AuditEvent, len(m.events))
	for i, e := range m.events {
		c := *e
		c.Input = maps.Clone(e.Input)
		out[i] = &c
	}
	return out
}

// Dropped returns how many events were evicted because of the capacity.
func (m *MemoryAuditLogger) Dropped() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropped
}

// Clear removes all logged events (including persisted ones; failures are
// reported like Log's).
func (m *MemoryAuditLogger) Clear() {
	_ = m.Reset()
}

// Reset is Clear returning the persistence error, if any.
func (m *MemoryAuditLogger) Reset() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	clear(m.events)
	m.events = m.events[:0]
	var err error
	if m.disk != nil {
		err = m.disk.evict(len(m.disk.seqs))
	}
	handler := m.errorHandlerLocked(err)
	m.mu.Unlock()
	if handler != nil {
		handler(err)
	}
	return err
}

// PolicyRegistry stores reusable policy rules. It is safe for concurrent use.
type PolicyRegistry struct {
	mu    sync.RWMutex
	rules map[string]Rule
}

// NewPolicyRegistry creates a new policy registry.
func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{rules: make(map[string]Rule)}
}

// AddRule registers a policy rule by ID.
func (p *PolicyRegistry) AddRule(rule Rule) {
	if p == nil || rule.ID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules[rule.ID] = cloneRule(rule)
}

// Rule returns a policy rule by ID.
func (p *PolicyRegistry) Rule(id string) (Rule, bool) {
	if p == nil {
		return Rule{}, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	rule, ok := p.rules[id]
	return cloneRule(rule), ok
}

// RuleIDs returns all registered policy IDs.
func (p *PolicyRegistry) RuleIDs() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := make([]string, 0, len(p.rules))
	for id := range p.rules {
		ids = append(ids, id)
	}
	return ids
}

// EvaluateWithRegistry evaluates a policy by ID from the registry. Input
// without a "policy_id" is allowed by default; an unknown policy ID is
// denied with an error.
func EvaluateWithRegistry(registry *PolicyRegistry, ctx context.Context, input map[string]interface{}) (PolicyResult, error) {
	policyID, _ := input["policy_id"].(string)
	if policyID == "" {
		return PolicyResult{Allowed: true, Policy: "", Reason: "default allow"}, nil
	}
	rule, ok := registry.Rule(policyID)
	if !ok {
		return PolicyResult{Allowed: false, Policy: policyID, Reason: "policy not found"}, fmt.Errorf("policy not found: %s", policyID)
	}
	engine := NewSimplePolicyEngine(policyID)
	engine.AddRule(rule)
	return engine.Evaluate(ctx, input)
}

// auditingEngine wraps a PolicyEngine and logs every decision.
type auditingEngine struct {
	engine PolicyEngine
	logger AuditLogger
}

// WithAudit returns a PolicyEngine that logs each decision (with sanitized
// input) to logger. Evaluation errors are logged with decision "error".
func WithAudit(engine PolicyEngine, logger AuditLogger) PolicyEngine {
	if logger == nil {
		return engine
	}
	return &auditingEngine{engine: engine, logger: logger}
}

func (a *auditingEngine) Evaluate(ctx context.Context, input map[string]interface{}) (PolicyResult, error) {
	res, err := a.engine.Evaluate(ctx, input)
	ev := &AuditEvent{Timestamp: time.Now().UTC(), Policy: res.Policy, Input: SanitizeInput(input), Reason: res.Reason}
	switch {
	case err != nil:
		ev.Decision, ev.Reason = "error", err.Error()
	case res.Allowed:
		ev.Decision = "allow"
	default:
		ev.Decision = "deny"
	}
	a.logger.Log(ev)
	return res, err
}
