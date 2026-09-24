package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ayoubzulfiqar/aerollm/internal/cache"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// Approval statuses.
const (
	ApprovalStatusPending  = "pending"
	ApprovalStatusApproved = "approved"
	ApprovalStatusRejected = "rejected"
	ApprovalStatusExpired  = "expired"
)

// DefaultApprovalTTL is how long a pending approval stays valid.
const DefaultApprovalTTL = 24 * time.Hour

var (
	// ErrApprovalNotFound is returned when an approval ID is unknown (or malformed).
	ErrApprovalNotFound = errors.New("approval not found")
	// ErrApprovalProcessed is returned when an approval was already resolved.
	ErrApprovalProcessed = errors.New("approval already processed")
	// ErrApprovalExpired is returned when an approval passed its expiry.
	ErrApprovalExpired = errors.New("approval expired")
	// ErrApprovalRejected is returned when a rejected approval has no stored
	// conversation to continue.
	ErrApprovalRejected = errors.New("user rejected approval")
	// ErrApprovalForbidden is returned when the caller is not the principal
	// that created the approval (see WithPrincipal).
	ErrApprovalForbidden = errors.New("approval belongs to a different principal")
	// ErrApprovalInProgress is returned when the same approval is being
	// resolved concurrently.
	ErrApprovalInProgress = errors.New("approval is being processed")
	// ErrNoApprovalStore is returned when HITL is used without a store.
	ErrNoApprovalStore = errors.New("approval store not configured")
)

// ApprovalRequiredError is returned by ResumeApproval when continuing the
// conversation triggered another tool call that needs approval.
type ApprovalRequiredError struct {
	ApprovalID string
}

func (e *ApprovalRequiredError) Error() string {
	return "further human approval required: " + e.ApprovalID
}

// ApprovalRecord represents a pending human approval for a batch of tool calls.
//
// Besides the tool calls it stores the full conversation state at the moment
// the loop paused (Request already contains the assistant message carrying the
// tool calls), the usage consumed so far and the loop iteration, so that
// ResumeApproval can continue the conversation without the caller resupplying
// anything.
type ApprovalRecord struct {
	ApprovalID string
	RequestID  string
	// ToolCall is the first pending call (kept for backward compatibility).
	ToolCall  models.ToolCall
	Arguments string
	Status    string
	CreatedAt time.Time
	ExpiresAt time.Time
	Result    interface{}
	// Error is the in-memory error; it is not serialized (error values do
	// not round-trip through JSON). ErrorMessage carries its text.
	Error        error  `json:"-"`
	ErrorMessage string `json:",omitempty"`
	// ToolCalls are all the calls of the paused turn.
	ToolCalls []models.ToolCall `json:",omitempty"`
	// Request is the conversation state to resume from.
	Request *models.LLMRequest `json:",omitempty"`
	// Usage is the usage aggregated before the pause.
	Usage *models.Usage `json:",omitempty"`
	// Iteration is the number of LLM calls made before the pause.
	Iteration int `json:",omitempty"`
	// Owner is a SHA-256 fingerprint of the principal that created the
	// approval (empty when no principal was attached to the context).
	Owner      string    `json:",omitempty"`
	ResolvedAt time.Time `json:",omitempty"`
}

// ApprovalStore persists approval records.
type ApprovalStore interface {
	Save(ctx context.Context, record ApprovalRecord) error
	Get(ctx context.Context, approvalID string) (*ApprovalRecord, error)
	Update(ctx context.Context, record ApprovalRecord) error
}

// ApprovalClaimer is optionally implemented by stores that can atomically
// claim an approval so that it is resolved at most once across instances.
type ApprovalClaimer interface {
	Claim(ctx context.Context, approvalID string) (bool, error)
}

// setNXClient is the subset of *redis.Client used for atomic claims.
type setNXClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
}

// RedisApprovalStore stores approvals in Redis.
type RedisApprovalStore struct {
	client cache.RedisClient
	prefix string
	ttl    time.Duration
}

// NewRedisApprovalStore creates a new approval store. A non-positive ttl
// defaults to DefaultApprovalTTL (records must never live forever).
func NewRedisApprovalStore(client cache.RedisClient, prefix string, ttl time.Duration) *RedisApprovalStore {
	if ttl <= 0 {
		ttl = DefaultApprovalTTL
	}
	return &RedisApprovalStore{client: client, prefix: prefix, ttl: ttl}
}

func (r *RedisApprovalStore) key(approvalID string) string {
	return r.prefix + approvalID
}

// Save stores an approval record.
func (r *RedisApprovalStore) Save(ctx context.Context, record ApprovalRecord) error {
	if r == nil || r.client == nil {
		return ErrNoApprovalStore
	}
	if !validApprovalID(record.ApprovalID) {
		return fmt.Errorf("invalid approval id")
	}
	if record.Error != nil && record.ErrorMessage == "" {
		record.ErrorMessage = record.Error.Error()
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode approval: %w", err)
	}
	return r.client.Set(ctx, r.key(record.ApprovalID), string(data), r.ttl).Err()
}

// Get retrieves an approval record. It returns ErrApprovalNotFound when the
// record does not exist (or has expired out of Redis).
func (r *RedisApprovalStore) Get(ctx context.Context, approvalID string) (*ApprovalRecord, error) {
	if r == nil || r.client == nil {
		return nil, ErrNoApprovalStore
	}
	val, err := r.client.Get(ctx, r.key(approvalID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrApprovalNotFound
		}
		return nil, err
	}
	var record ApprovalRecord
	if err := json.Unmarshal([]byte(val), &record); err != nil {
		return nil, fmt.Errorf("decode approval: %w", err)
	}
	if record.ErrorMessage != "" {
		record.Error = errors.New(record.ErrorMessage)
	}
	return &record, nil
}

// Update updates an approval record.
func (r *RedisApprovalStore) Update(ctx context.Context, record ApprovalRecord) error {
	return r.Save(ctx, record)
}

// Claim atomically marks the approval as being resolved (SET NX). It returns
// false when another caller already claimed it. When the underlying client
// does not support SETNX the claim always succeeds and callers rely on the
// engine's in-process locking.
func (r *RedisApprovalStore) Claim(ctx context.Context, approvalID string) (bool, error) {
	if r == nil || r.client == nil {
		return false, ErrNoApprovalStore
	}
	c, ok := r.client.(setNXClient)
	if !ok {
		return true, nil
	}
	return c.SetNX(ctx, r.key(approvalID)+":claim", "1", r.ttl).Result()
}

// AdvancedAgentEngine extends AgentEngine with HITL support.
type AdvancedAgentEngine struct {
	AgentEngine
	Store ApprovalStore
	// ApprovalTTL is the validity of new approvals (default DefaultApprovalTTL).
	ApprovalTTL time.Duration
	mu          sync.RWMutex

	lockMu   sync.Mutex
	inFlight map[string]struct{}
}

// NewAdvancedAgentEngine creates a new advanced agent engine.
func NewAdvancedAgentEngine(provider ToolProvider, registry *ToolRegistry, store ApprovalStore) *AdvancedAgentEngine {
	if registry == nil {
		registry = NewToolRegistry()
	}
	return &AdvancedAgentEngine{
		AgentEngine: AgentEngine{
			Provider:      provider,
			MaxIterations: defaultMaxIterations,
			ToolTimeout:   defaultToolTimeout,
			MaxConcurrent: defaultMaxConcurrent,
			ToolCache:     make(map[string]*ToolResult),
			Registry:      registry,
		},
		Store:       store,
		ApprovalTTL: DefaultApprovalTTL,
	}
}

// principalKey is the context key for the authenticated principal.
type principalKey struct{}

// WithPrincipal attaches the authenticated principal (e.g. the API key or
// user ID) to ctx. Approvals created under a principal can only be resumed by
// a context carrying the same principal. Only a SHA-256 fingerprint of the
// principal is persisted.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// PrincipalFromContext returns the principal attached with WithPrincipal.
func PrincipalFromContext(ctx context.Context) string {
	p, _ := ctx.Value(principalKey{}).(string)
	return p
}

func principalFingerprint(ctx context.Context) string {
	p := PrincipalFromContext(ctx)
	if p == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
}

// RunToolExecutionLoopWithHITL runs the agentic loop with human-in-the-loop
// support. When the model requests a batch of tool calls that includes a tool
// requiring approval, the loop state is persisted and the approval ID is
// returned (with a nil response); resume it with ResumeApproval.
func (e *AdvancedAgentEngine) RunToolExecutionLoopWithHITL(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, string, error) {
	if e.Store == nil {
		return nil, "", ErrNoApprovalStore
	}
	return e.runLoop(ctx, req, e.hitlOptions())
}

func (e *AdvancedAgentEngine) hitlOptions() loopOptions {
	return loopOptions{pause: e.pauseForApproval}
}

// pauseForApproval persists the loop state and returns a new approval ID.
func (e *AdvancedAgentEngine) pauseForApproval(ctx context.Context, st *loopState, calls []models.ToolCall) (string, error) {
	return e.requestApproval(ctx, st, calls)
}

// requestApproval creates an approval record and returns the approval ID.
func (e *AdvancedAgentEngine) requestApproval(ctx context.Context, st *loopState, toolCalls []models.ToolCall) (string, error) {
	if len(toolCalls) == 0 {
		return "", errors.New("no tool calls to approve")
	}
	approvalID, err := generateApprovalID()
	if err != nil {
		return "", fmt.Errorf("generate approval id: %w", err)
	}
	requestID, err := generateApprovalID()
	if err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	ttl := e.ApprovalTTL
	if ttl <= 0 {
		ttl = DefaultApprovalTTL
	}
	now := time.Now().UTC()
	snapshot := *st.req
	snapshot.Messages = append([]models.Message(nil), st.req.Messages...)
	record := ApprovalRecord{
		ApprovalID: approvalID,
		RequestID:  "req-" + requestID,
		ToolCall:   toolCalls[0],
		Arguments:  toolCalls[0].Function.Arguments,
		ToolCalls:  append([]models.ToolCall(nil), toolCalls...),
		Status:     ApprovalStatusPending,
		CreatedAt:  now,
		ExpiresAt:  now.Add(ttl),
		Request:    &snapshot,
		Usage:      st.usageCopy(),
		Iteration:  st.iteration,
		Owner:      principalFingerprint(ctx),
	}
	if err := e.Store.Save(ctx, record); err != nil {
		return "", err
	}
	return approvalID, nil
}

// ResumeApproval resolves a pending approval and continues the paused
// conversation from the state stored with the approval.
//
//   - approved: the pending tool calls are executed and the loop continues
//     until the model produces a final answer.
//   - rejected: the model is told the calls were rejected and the loop
//     continues so it can answer without them. Legacy records without stored
//     state return ErrApprovalRejected.
//
// req is only used for legacy records created without stored state; for
// current records it may be nil or empty. If continuing triggers another
// approval, an *ApprovalRequiredError carrying the new ID is returned (use
// ResumeApprovalWithHITL to receive it as a value instead).
func (e *AdvancedAgentEngine) ResumeApproval(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, error) {
	resp, pendingID, err := e.ResumeApprovalWithHITL(ctx, approvalID, approved, req)
	if err != nil {
		return nil, err
	}
	if pendingID != "" {
		return nil, &ApprovalRequiredError{ApprovalID: pendingID}
	}
	return resp, nil
}

// ResumeApprovalWithHITL is ResumeApproval returning a follow-up approval ID
// (with a nil response) when the continued conversation pauses again.
func (e *AdvancedAgentEngine) ResumeApprovalWithHITL(ctx context.Context, approvalID string, approved bool, req *models.LLMRequest) (*models.LLMResponse, string, error) {
	if e.Store == nil {
		return nil, "", ErrNoApprovalStore
	}
	if !validApprovalID(approvalID) {
		return nil, "", ErrApprovalNotFound
	}
	if !e.lock(approvalID) {
		return nil, "", ErrApprovalInProgress
	}
	record, err := e.resolve(ctx, approvalID, approved)
	e.unlock(approvalID)
	if err != nil {
		return nil, "", err
	}

	calls := record.ToolCalls
	if len(calls) == 0 && record.ToolCall.Function.Name != "" {
		calls = []models.ToolCall{record.ToolCall}
	}

	var st *loopState
	if record.Request != nil {
		st = newLoopState(record.Request, e.Registry)
		st.iteration = record.Iteration
		st.addUsage(record.Usage)
	} else {
		// Legacy record without stored conversation state.
		if !approved {
			return nil, "", ErrApprovalRejected
		}
		base := req
		if base == nil {
			base = &models.LLMRequest{}
		}
		st = newLoopState(base, e.Registry)
		var assistant models.Message
		assistant, calls = assistantMessage(models.Message{}, calls)
		st.req.Messages = append(st.req.Messages, assistant)
		// The legacy record's calls were approved explicitly; treat them as offered.
		for _, tc := range calls {
			st.offered[tc.Function.Name] = true
		}
	}

	var results []*ToolResult
	if approved {
		allow := func(tc models.ToolCall) error {
			if _, ok := e.Registry.Get(tc.Function.Name); !ok || !st.offered[tc.Function.Name] {
				return &UnknownToolError{Name: tc.Function.Name}
			}
			return nil
		}
		results, err = e.executeCalls(ctx, calls, allow, AdvancedLoopOptions{})
		if err != nil {
			return nil, "", err
		}
	} else {
		results = make([]*ToolResult, len(calls))
		for i, tc := range calls {
			results[i] = &ToolResult{ToolCallID: tc.ID, Name: tc.Function.Name, Error: errors.New("the user rejected this tool call")}
		}
	}
	st.req.Messages = append(st.req.Messages, buildToolMessages(calls, results, e.maxToolOutput())...)

	return e.continueLoop(ctx, st, e.hitlOptions())
}

// resolve validates the approval and atomically transitions it out of the
// pending state. It must be called with the in-process lock held.
func (e *AdvancedAgentEngine) resolve(ctx context.Context, approvalID string, approved bool) (*ApprovalRecord, error) {
	record, err := e.Store.Get(ctx, approvalID)
	if err != nil {
		if errors.Is(err, ErrApprovalNotFound) || errors.Is(err, redis.Nil) {
			return nil, ErrApprovalNotFound
		}
		return nil, err
	}
	if record == nil {
		return nil, ErrApprovalNotFound
	}
	if record.Owner != "" {
		caller := principalFingerprint(ctx)
		if subtle.ConstantTimeCompare([]byte(caller), []byte(record.Owner)) != 1 {
			return nil, ErrApprovalForbidden
		}
	}
	if record.Status != ApprovalStatusPending {
		return nil, ErrApprovalProcessed
	}
	now := time.Now().UTC()
	if !record.ExpiresAt.IsZero() && now.After(record.ExpiresAt) {
		record.Status = ApprovalStatusExpired
		record.ResolvedAt = now
		_ = e.Store.Update(ctx, *record)
		return nil, ErrApprovalExpired
	}
	// Continuing requires an LLM; check before consuming the approval.
	if (approved || record.Request != nil) && e.Provider == nil {
		return nil, ErrNoProvider
	}
	if claimer, ok := e.Store.(ApprovalClaimer); ok {
		claimed, err := claimer.Claim(ctx, approvalID)
		if err != nil {
			return nil, err
		}
		if !claimed {
			return nil, ErrApprovalProcessed
		}
	}
	record.ResolvedAt = now
	if approved {
		record.Status = ApprovalStatusApproved
		record.Result = ApprovalStatusApproved
	} else {
		record.Status = ApprovalStatusRejected
		record.Error = ErrApprovalRejected
		record.ErrorMessage = ErrApprovalRejected.Error()
	}
	if err := e.Store.Update(ctx, *record); err != nil {
		return nil, fmt.Errorf("persist approval state: %w", err)
	}
	return record, nil
}

func (e *AdvancedAgentEngine) lock(id string) bool {
	e.lockMu.Lock()
	defer e.lockMu.Unlock()
	if e.inFlight == nil {
		e.inFlight = make(map[string]struct{})
	}
	if _, busy := e.inFlight[id]; busy {
		return false
	}
	e.inFlight[id] = struct{}{}
	return true
}

func (e *AdvancedAgentEngine) unlock(id string) {
	e.lockMu.Lock()
	defer e.lockMu.Unlock()
	delete(e.inFlight, id)
}

var approvalIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func validApprovalID(id string) bool {
	return approvalIDPattern.MatchString(id)
}

// generateApprovalID generates an unguessable 128-bit approval ID.
func generateApprovalID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ToolWithApproval wraps a tool with approval requirement.
type ToolWithApproval struct {
	Tool
}

// RequiresApproval returns true for tools wrapped with approval.
func (t *ToolWithApproval) RequiresApproval() bool {
	return true
}

// NewApprovalTool wraps a tool to require human approval.
func NewApprovalTool(t Tool) Tool {
	return &ToolWithApproval{Tool: t}
}
