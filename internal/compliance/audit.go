package compliance

import (
	"context"
	"encoding/json"
	"fmt"
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

// String returns a JSON representation of the audit event.
func (e *AuditEvent) String() string {
	out, _ := json.Marshal(map[string]interface{}{
		"timestamp": e.Timestamp.UTC().Format(time.RFC3339),
		"policy":    e.Policy,
		"decision":  e.Decision,
		"input":     e.Input,
		"reason":    e.Reason,
	})
	return string(out)
}

// AuditLogger logs policy decisions for compliance review.
type AuditLogger interface {
	Log(event *AuditEvent)
}

// MemoryAuditLogger stores audit events in memory.
type MemoryAuditLogger struct {
	events []*AuditEvent
}

// NewMemoryAuditLogger creates a new in-memory audit logger.
func NewMemoryAuditLogger() *MemoryAuditLogger {
	return &MemoryAuditLogger{events: make([]*AuditEvent, 0, 256)}
}

// Log appends an audit event.
func (m *MemoryAuditLogger) Log(event *AuditEvent) {
	if m == nil || event == nil {
		return
	}
	m.events = append(m.events, event)
}

// Events returns a copy of all logged events.
func (m *MemoryAuditLogger) Events() []*AuditEvent {
	if m == nil {
		return nil
	}
	out := make([]*AuditEvent, len(m.events))
	copy(out, m.events)
	return out
}

// Clear removes all logged events.
func (m *MemoryAuditLogger) Clear() {
	if m == nil {
		return
	}
	m.events = m.events[:0]
}

// PolicyRegistry stores reusable policy rules.
type PolicyRegistry struct {
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
	p.rules[rule.ID] = rule
}

// Rule returns a policy rule by ID.
func (p *PolicyRegistry) Rule(id string) (Rule, bool) {
	if p == nil {
		return Rule{}, false
	}
	rule, ok := p.rules[id]
	return rule, ok
}

// RuleIDs returns all registered policy IDs.
func (p *PolicyRegistry) RuleIDs() []string {
	if p == nil {
		return nil
	}
	ids := make([]string, 0, len(p.rules))
	for id := range p.rules {
		ids = append(ids, id)
	}
	return ids
}

// EvaluateWithRegistry evaluates a policy by ID from the registry.
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
