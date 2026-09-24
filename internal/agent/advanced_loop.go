package agent

import (
	"context"
	"regexp"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// LoopHook is called at agent loop checkpoints.
type LoopHook interface {
	BeforeLLM(ctx context.Context, req *models.LLMRequest)
	AfterLLM(ctx context.Context, resp *models.LLMResponse)
	BeforeTools(ctx context.Context, calls []models.ToolCall)
	AfterTools(ctx context.Context, results []*ToolResult)
	OnToolDeficit(ctx context.Context, deficit ToolDeficitSignal)
}

// ToolDeficitSignal mirrors synthesis deficit signals for agent loop handling.
type ToolDeficitSignal struct {
	RequestID   string
	MissingTool string
	Reason      string
}

// NoopLoopHook is a zero-value hook implementation.
type NoopLoopHook struct{}

func (n *NoopLoopHook) BeforeLLM(_ context.Context, _ *models.LLMRequest)    {}
func (n *NoopLoopHook) AfterLLM(_ context.Context, _ *models.LLMResponse)    {}
func (n *NoopLoopHook) BeforeTools(_ context.Context, _ []models.ToolCall)   {}
func (n *NoopLoopHook) AfterTools(_ context.Context, _ []*ToolResult)        {}
func (n *NoopLoopHook) OnToolDeficit(_ context.Context, _ ToolDeficitSignal) {}

// AdvancedLoopOptions configures the advanced agent loop.
type AdvancedLoopOptions struct {
	Hooks []LoopHook
	// RetryToolErrors re-executes failed tool calls whose error may be
	// transient (not unknown tools, invalid arguments or approval refusals).
	RetryToolErrors bool
	// MaxToolRetries is the number of retry rounds (default 1).
	MaxToolRetries int
	// ToolRetryDelay is the pause before each retry round (default 200ms).
	ToolRetryDelay time.Duration
}

func (o AdvancedLoopOptions) withDefaults() AdvancedLoopOptions {
	if o.MaxToolRetries <= 0 {
		o.MaxToolRetries = 1
	}
	if o.ToolRetryDelay <= 0 {
		o.ToolRetryDelay = 200 * time.Millisecond
	}
	return o
}

// RunAdvancedExecutionLoop runs the agentic loop (same semantics as
// RunToolExecutionLoop) with hooks, optional tool retries and deficit
// signalling: OnToolDeficit fires when the model calls a tool that is not
// available.
func (e *AgentEngine) RunAdvancedExecutionLoop(ctx context.Context, req *models.LLMRequest, opts AdvancedLoopOptions) (*models.LLMResponse, error) {
	resp, _, err := e.runLoop(ctx, req, loopOptions{hooks: opts.Hooks, retry: opts.withDefaults()})
	return resp, err
}

// ExecuteToolsWithRetry executes tool calls and, when opts.RetryToolErrors is
// set, retries the calls that failed with a potentially transient error up to
// opts.MaxToolRetries times, waiting opts.ToolRetryDelay between rounds.
// Per-call failures remain in the results; the returned error is non-nil only
// when ctx is cancelled.
func (e *AgentEngine) ExecuteToolsWithRetry(ctx context.Context, toolCalls []models.ToolCall, opts AdvancedLoopOptions) ([]*ToolResult, error) {
	if opts.RetryToolErrors {
		opts = opts.withDefaults()
	}
	return e.executeCalls(ctx, toolCalls, nil, opts)
}

// ToolDeficitHandler converts synthesis-style deficit signals into agent loop events.
type ToolDeficitHandler struct {
	mu       sync.RWMutex
	deficits map[string]ToolDeficitSignal
}

// maxRecordedDeficits bounds the memory used by ToolDeficitHandler.
const maxRecordedDeficits = 10000

// unknownToolPattern extracts the tool name from an "unknown tool: X" message.
var unknownToolPattern = regexp.MustCompile(`(?i)unknown tool:\s*([A-Za-z0-9_-]+)`)

// NewToolDeficitHandler creates a new handler.
func NewToolDeficitHandler() *ToolDeficitHandler {
	return &ToolDeficitHandler{deficits: make(map[string]ToolDeficitSignal)}
}

// Record stores a deficit signal by request ID. When the handler is full an
// arbitrary older entry is evicted so memory stays bounded.
func (h *ToolDeficitHandler) Record(reqID string, signal ToolDeficitSignal) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.deficits[reqID]; !exists && len(h.deficits) >= maxRecordedDeficits {
		for k := range h.deficits {
			delete(h.deficits, k)
			break
		}
	}
	h.deficits[reqID] = signal
}

// Get retrieves a deficit signal by request ID.
func (h *ToolDeficitHandler) Get(reqID string) (ToolDeficitSignal, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.deficits[reqID]
	return s, ok
}

// MissingToolFromError extracts the tool name from an "unknown tool: X" error
// message, reporting whether one was found.
func MissingToolFromError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	m := unknownToolPattern.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}
