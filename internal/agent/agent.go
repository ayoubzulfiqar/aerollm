package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

const (
	defaultMaxIterations = 10
	defaultToolTimeout   = 30 * time.Second
	defaultMaxConcurrent = 10

	// DefaultMaxToolOutputBytes caps the size of a single tool result that is
	// fed back to the model, so a misbehaving tool cannot blow up the context.
	DefaultMaxToolOutputBytes = 64 << 10
)

var (
	// ErrNoProvider is returned when the engine has no LLM provider configured.
	ErrNoProvider = errors.New("agent: no LLM provider configured")
	// ErrNilRequest is returned when a nil request is passed to the loop.
	ErrNilRequest = errors.New("agent: nil request")
	// ErrNilResponse is returned when the provider returns neither a response nor an error.
	ErrNilResponse = errors.New("agent: provider returned an empty response")
)

// UnknownToolError is produced (and fed back to the model as tool output) when
// the model calls a tool that is not available to it: not registered on the
// server, or not offered by the client in the request.
type UnknownToolError struct {
	Name string
}

func (e *UnknownToolError) Error() string { return "unknown tool: " + e.Name }

// ApprovalRequiredToolError is fed back to the model when it calls a tool that
// needs human approval from a loop that does not support approvals.
type ApprovalRequiredToolError struct {
	Name string
}

func (e *ApprovalRequiredToolError) Error() string {
	return fmt.Sprintf("tool %q requires human approval and cannot be executed here", e.Name)
}

// ToolResult holds the result of executing a tool call.
type ToolResult struct {
	ToolCallID string
	Name       string
	Content    interface{}
	Error      error
	Duration   time.Duration
	Cached     bool
}

// ContentToString returns the tool result content as a string. Strings are
// returned verbatim, byte slices as text, and any other value is JSON-encoded
// (falling back to fmt formatting when it is not JSON-serializable).
func (t *ToolResult) ContentToString() string {
	if t == nil || t.Content == nil {
		return ""
	}
	switch v := t.Content.(type) {
	case string:
		return v
	case *string:
		if v == nil {
			return ""
		}
		return *v
	case []byte:
		return string(v)
	case json.RawMessage:
		return string(v)
	case fmt.Stringer:
		return v.String()
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// ToolProvider is the interface for calling the LLM during the agent loop.
type ToolProvider interface {
	CallLLM(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error)
}

// Tool is the interface that tool implementations must satisfy.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]interface{}
	Execute(ctx context.Context, args map[string]interface{}) (interface{}, error)
}

// toolNamePattern mirrors the OpenAI function-name constraint.
var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ToolRegistry stores available tools for the agent.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewToolRegistry creates a new ToolRegistry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

// Register adds a tool to the registry. Tool names must match
// ^[A-Za-z0-9_-]{1,64}$ (the OpenAI function-name constraint).
func (r *ToolRegistry) Register(t Tool) error {
	if t == nil {
		return errors.New("tool cannot be nil")
	}
	name := t.Name()
	if !toolNamePattern.MatchString(name) {
		return fmt.Errorf("invalid tool name %q: must match %s", name, toolNamePattern.String())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tools == nil {
		r.tools = make(map[string]Tool)
	}
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("tool %q already registered", name)
	}
	r.tools[name] = t
	return nil
}

// Get returns a tool by name.
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// All returns all registered tool names, sorted.
func (r *ToolRegistry) All() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// List returns all registered tools sorted by name.
func (r *ToolRegistry) List() []Tool {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Tool, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// Definitions returns the model-facing definitions of all registered tools,
// sorted by name. Tools requiring human approval are included; callers that
// must not expose them (e.g. MCP) should filter with RequiresApproval.
func (r *ToolRegistry) Definitions() []models.ToolDefinition {
	tools := r.List()
	out := make([]models.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		out = append(out, ToolDefinitionOf(t))
	}
	return out
}

// Unregister removes a tool by name and reports whether it existed.
func (r *ToolRegistry) Unregister(name string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.tools[name]
	delete(r.tools, name)
	return ok
}

// ToolDefinitionOf converts a Tool into its model-facing definition.
func ToolDefinitionOf(t Tool) models.ToolDefinition {
	return models.ToolDefinition{Name: t.Name(), Description: t.Description(), Parameters: t.Parameters()}
}

// RequiresApproval reports whether a tool demands human approval before it
// may be executed (see ToolWithApproval).
func RequiresApproval(t Tool) bool {
	if t == nil {
		return false
	}
	a, ok := t.(interface{ RequiresApproval() bool })
	return ok && a.RequiresApproval()
}

// Execute runs a tool by name with JSON arguments. Empty arguments are treated
// as an empty object; anything other than a JSON object is rejected. Panics
// raised by the tool are recovered and returned as errors.
func (r *ToolRegistry) Execute(ctx context.Context, name string, argsJSON string) (result interface{}, err error) {
	t, ok := r.Get(name)
	if !ok {
		return nil, &UnknownToolError{Name: name}
	}

	args := map[string]interface{}{}
	if trimmed := strings.TrimSpace(argsJSON); trimmed != "" && trimmed != "null" {
		if err := json.Unmarshal([]byte(trimmed), &args); err != nil {
			return nil, fmt.Errorf("invalid tool arguments: must be a JSON object: %w", err)
		}
		if args == nil {
			args = map[string]interface{}{}
		}
	}

	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("tool %q panicked: %v", name, rec)
		}
	}()
	result, err = t.Execute(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("tool %q failed: %w", name, err)
	}
	return result, nil
}

// AgentEngine executes tool calls from LLM responses concurrently.
//
// Loop semantics (RunToolExecutionLoop):
//   - Only tools that the client offered in req.Tools AND that exist in the
//     server Registry are executed server-side. If the request offers no such
//     tool, the provider is called exactly once and its response is returned
//     unchanged (a transparent proxy).
//   - If the model calls a tool the client offered but the server does not
//     implement, the response is returned to the client (standard OpenAI
//     behaviour: the client executes its own tools).
//   - Calls to tools that were not offered (hallucinated names), tool errors,
//     timeouts and panics are fed back to the model as "error: ..." tool
//     messages instead of aborting the loop.
//   - Tool calls in one turn run concurrently, bounded by MaxConcurrent, each
//     with its own ToolTimeout. The loop is capped at MaxIterations LLM calls.
//   - Usage is aggregated across all LLM calls of the loop.
//   - The caller's request is never mutated.
type AgentEngine struct {
	Provider      ToolProvider
	MaxIterations int
	ToolTimeout   time.Duration
	MaxConcurrent int
	// ToolCache is reserved for tool-result caching; the engine does not
	// read or write it (tool calls are side-effecting and not cacheable by ID).
	ToolCache map[string]*ToolResult
	mu        sync.RWMutex
	Registry  *ToolRegistry
	// ToolBilling, when set, is charged once per successful tool execution.
	ToolBilling ToolCallBiller
	// MaxToolOutputBytes caps each tool result fed back to the model.
	// Zero means DefaultMaxToolOutputBytes; negative disables the cap.
	MaxToolOutputBytes int
}

// ToolCallBiller bills tool executions for the economy layer.
type ToolCallBiller interface {
	BillToolCall(ctx context.Context, toolName string) error
}

// NewAgentEngine creates a new AgentEngine.
func NewAgentEngine(provider ToolProvider, registry *ToolRegistry) *AgentEngine {
	if registry == nil {
		registry = NewToolRegistry()
	}
	return &AgentEngine{
		Provider:      provider,
		MaxIterations: defaultMaxIterations,
		ToolTimeout:   defaultToolTimeout,
		MaxConcurrent: defaultMaxConcurrent,
		ToolCache:     make(map[string]*ToolResult),
		Registry:      registry,
	}
}

// RunToolExecutionLoop runs the agentic loop: call the LLM, execute the
// server-side tool calls it requests, feed the results back, and repeat until
// the model produces a final answer (or hands tool calls back to the client).
// See AgentEngine for the exact semantics.
func (e *AgentEngine) RunToolExecutionLoop(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	resp, _, err := e.runLoop(ctx, req, loopOptions{})
	return resp, err
}

// loopOptions customises the shared loop implementation.
type loopOptions struct {
	hooks []LoopHook
	retry AdvancedLoopOptions
	// pause, when set, is invoked before executing a batch of tool calls that
	// contains at least one approval-required tool. It persists the loop state
	// and returns an approval ID; the loop then stops.
	pause func(ctx context.Context, st *loopState, calls []models.ToolCall) (string, error)
}

// loopState is the mutable state of one agent loop run.
type loopState struct {
	req       *models.LLMRequest
	offered   map[string]bool
	iteration int
	usage     models.Usage
	usageSeen bool
}

// newLoopState copies the caller's request (so it is never mutated) and fills
// in definitions for registry tools that the client requested by name only.
func newLoopState(req *models.LLMRequest, registry *ToolRegistry) *loopState {
	cp := *req
	cp.Messages = append([]models.Message(nil), req.Messages...)
	st := &loopState{req: &cp, offered: make(map[string]bool, len(req.Tools))}
	if len(req.Tools) > 0 {
		cp.Tools = make([]models.ToolDefinition, len(req.Tools))
		for i, def := range req.Tools {
			if t, ok := registry.Get(def.Name); ok {
				if def.Parameters == nil {
					def.Parameters = t.Parameters()
				}
				if def.Description == "" {
					def.Description = t.Description()
				}
			}
			cp.Tools[i] = def
			if def.Name != "" {
				st.offered[def.Name] = true
			}
		}
	}
	return st
}

func (st *loopState) addUsage(u *models.Usage) {
	if u == nil {
		return
	}
	st.usageSeen = true
	st.usage.PromptTokens += u.PromptTokens
	st.usage.CompletionTokens += u.CompletionTokens
	st.usage.TotalTokens += u.TotalTokens
}

func (st *loopState) usageCopy() *models.Usage {
	if !st.usageSeen {
		return nil
	}
	u := st.usage
	return &u
}

// finalize returns the response to hand back to the caller. A single-call loop
// returns the upstream response untouched; otherwise a shallow copy carrying
// the aggregated usage is returned.
func (st *loopState) finalize(resp *models.LLMResponse) *models.LLMResponse {
	if st.iteration <= 1 {
		return resp
	}
	out := *resp
	if u := st.usageCopy(); u != nil {
		out.Usage = u
	}
	return &out
}

// serverToolsOffered reports whether the request offers at least one tool the
// server can execute.
func (st *loopState) serverToolsOffered(registry *ToolRegistry) bool {
	for name := range st.offered {
		if _, ok := registry.Get(name); ok {
			return true
		}
	}
	return false
}

// hasClientSideCall reports whether any call targets a tool that the client
// offered but the server does not implement.
func (st *loopState) hasClientSideCall(calls []models.ToolCall, registry *ToolRegistry) bool {
	for _, tc := range calls {
		if !st.offered[tc.Function.Name] {
			continue
		}
		if _, ok := registry.Get(tc.Function.Name); !ok {
			return true
		}
	}
	return false
}

func (e *AgentEngine) runLoop(ctx context.Context, req *models.LLMRequest, opts loopOptions) (*models.LLMResponse, string, error) {
	if req == nil {
		return nil, "", ErrNilRequest
	}
	if e.Provider == nil {
		return nil, "", ErrNoProvider
	}
	return e.continueLoop(ctx, newLoopState(req, e.Registry), opts)
}

func (e *AgentEngine) continueLoop(ctx context.Context, st *loopState, opts loopOptions) (*models.LLMResponse, string, error) {
	if e.Provider == nil {
		return nil, "", ErrNoProvider
	}
	maxIter := e.maxIterations()
	for {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if st.iteration >= maxIter {
			return nil, "", &MaxIterationsError{Iteration: st.iteration, Usage: st.usageCopy()}
		}
		st.iteration++

		for _, hook := range opts.hooks {
			hook.BeforeLLM(ctx, st.req)
		}
		resp, err := e.Provider.CallLLM(ctx, st.req)
		if err != nil {
			return nil, "", err
		}
		if resp == nil {
			return nil, "", ErrNilResponse
		}
		for _, hook := range opts.hooks {
			hook.AfterLLM(ctx, resp)
		}
		st.addUsage(resp.Usage)

		calls := firstChoiceToolCalls(resp)
		if len(calls) == 0 || !st.serverToolsOffered(e.Registry) || st.hasClientSideCall(calls, e.Registry) {
			return st.finalize(resp), "", nil
		}
		if st.iteration >= maxIter {
			// Executing these tools would be wasted work: no LLM call is left
			// to consume their results.
			return nil, "", &MaxIterationsError{Iteration: st.iteration, Usage: st.usageCopy()}
		}

		assistant, calls := assistantMessage(resp.Choices[0].Message, calls)
		st.req.Messages = append(st.req.Messages, assistant)

		if opts.pause != nil && e.needsApproval(st, calls) {
			id, err := opts.pause(ctx, st, calls)
			if err != nil {
				return nil, "", err
			}
			return nil, id, nil
		}

		for _, hook := range opts.hooks {
			hook.BeforeTools(ctx, calls)
		}
		results, err := e.executeCalls(ctx, calls, e.loopAllow(st, opts.pause != nil), opts.retry)
		if err != nil {
			return nil, "", err
		}
		for _, hook := range opts.hooks {
			hook.AfterTools(ctx, results)
		}
		e.signalDeficits(ctx, resp, results, opts.hooks)

		st.req.Messages = append(st.req.Messages, buildToolMessages(calls, results, e.maxToolOutput())...)
	}
}

// loopAllow returns the per-call admission check used inside the loop: the
// tool must have been offered by the client and exist in the registry, and
// approval-required tools may only run when approvals are supported.
func (e *AgentEngine) loopAllow(st *loopState, approvalsHandled bool) func(models.ToolCall) error {
	return func(tc models.ToolCall) error {
		name := tc.Function.Name
		t, ok := e.Registry.Get(name)
		if !ok || !st.offered[name] {
			return &UnknownToolError{Name: name}
		}
		if !approvalsHandled && RequiresApproval(t) {
			return &ApprovalRequiredToolError{Name: name}
		}
		return nil
	}
}

func (e *AgentEngine) needsApproval(st *loopState, calls []models.ToolCall) bool {
	for _, tc := range calls {
		if !st.offered[tc.Function.Name] {
			continue
		}
		if t, ok := e.Registry.Get(tc.Function.Name); ok && RequiresApproval(t) {
			return true
		}
	}
	return false
}

func (e *AgentEngine) signalDeficits(ctx context.Context, resp *models.LLMResponse, results []*ToolResult, hooks []LoopHook) {
	if len(hooks) == 0 {
		return
	}
	for _, r := range results {
		if r == nil || r.Error == nil {
			continue
		}
		var unknown *UnknownToolError
		if !errors.As(r.Error, &unknown) {
			continue
		}
		signal := ToolDeficitSignal{RequestID: resp.ID, MissingTool: unknown.Name, Reason: r.Error.Error()}
		for _, hook := range hooks {
			hook.OnToolDeficit(ctx, signal)
		}
	}
}

// ExecuteTools runs tool calls concurrently (bounded by MaxConcurrent), each
// with its own ToolTimeout. Per-tool failures (unknown tool, bad arguments,
// tool error, timeout, panic) are reported in the corresponding ToolResult's
// Error field and never abort the other calls. The returned error is non-nil
// only when ctx itself is cancelled or expired. The result slice always has
// one non-nil entry per call, in call order.
func (e *AgentEngine) ExecuteTools(ctx context.Context, toolCalls []models.ToolCall) ([]*ToolResult, error) {
	return e.executeCalls(ctx, toolCalls, nil, AdvancedLoopOptions{})
}

// executeCalls runs the calls, applying allow (when non-nil) as an admission
// check and retrying failed calls according to retry.
func (e *AgentEngine) executeCalls(ctx context.Context, calls []models.ToolCall, allow func(models.ToolCall) error, retry AdvancedLoopOptions) ([]*ToolResult, error) {
	results := e.executeBatch(ctx, calls, allow)
	if retry.RetryToolErrors {
		maxRetries := retry.MaxToolRetries
		if maxRetries <= 0 {
			maxRetries = 1
		}
		delay := retry.ToolRetryDelay
		if delay < 0 {
			delay = 0
		}
		for attempt := 0; attempt < maxRetries; attempt++ {
			var idx []int
			for i, r := range results {
				if r != nil && r.Error != nil && isRetryable(r.Error) {
					idx = append(idx, i)
				}
			}
			if len(idx) == 0 || ctx.Err() != nil {
				break
			}
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return results, ctx.Err()
				case <-timer.C:
				}
			}
			retryCalls := make([]models.ToolCall, len(idx))
			for j, i := range idx {
				retryCalls[j] = calls[i]
			}
			retried := e.executeBatch(ctx, retryCalls, allow)
			for j, i := range idx {
				results[i] = retried[j]
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return results, err
	}
	return results, nil
}

// isRetryable reports whether a tool error may be transient. Unknown tools,
// approval refusals, invalid arguments and cancellations are permanent.
func isRetryable(err error) bool {
	var unknown *UnknownToolError
	var approval *ApprovalRequiredToolError
	if errors.As(err, &unknown) || errors.As(err, &approval) || errors.Is(err, context.Canceled) {
		return false
	}
	return !strings.Contains(err.Error(), "invalid tool arguments")
}

func (e *AgentEngine) executeBatch(ctx context.Context, calls []models.ToolCall, allow func(models.ToolCall) error) []*ToolResult {
	results := make([]*ToolResult, len(calls))
	sem := make(chan struct{}, e.getMaxConcurrent())
	var wg sync.WaitGroup
	for i, tc := range calls {
		if allow != nil {
			if err := allow(tc); err != nil {
				results[i] = &ToolResult{ToolCallID: tc.ID, Name: tc.Function.Name, Error: err}
				continue
			}
		}
		wg.Add(1)
		go func(i int, tc models.ToolCall) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[i] = &ToolResult{ToolCallID: tc.ID, Name: tc.Function.Name, Error: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			results[i] = e.executeSingleTool(ctx, tc)
		}(i, tc)
	}
	wg.Wait()
	return results
}

// executeSingleTool executes one tool call with its own timeout. A tool that
// ignores its context cannot stall the loop: the call is abandoned (and
// reported as timed out) once the deadline passes.
func (e *AgentEngine) executeSingleTool(ctx context.Context, tc models.ToolCall) *ToolResult {
	res := &ToolResult{ToolCallID: tc.ID, Name: tc.Function.Name}
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()

	if err := ctx.Err(); err != nil {
		res.Error = err
		return res
	}
	if e.Registry == nil {
		res.Error = &UnknownToolError{Name: tc.Function.Name}
		return res
	}

	tctx, cancel := context.WithTimeout(ctx, e.toolTimeout())
	defer cancel()

	type outcome struct {
		content interface{}
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		content, err := e.Registry.Execute(tctx, tc.Function.Name, tc.Function.Arguments)
		done <- outcome{content: content, err: err}
	}()

	select {
	case out := <-done:
		res.Content, res.Error = out.content, out.err
	case <-tctx.Done():
		if ctx.Err() != nil {
			res.Error = ctx.Err()
		} else {
			res.Error = fmt.Errorf("tool %q timed out after %s", tc.Function.Name, e.toolTimeout())
		}
		return res
	}

	if res.Error == nil && e.ToolBilling != nil {
		// Billing is best-effort: a billing backend outage must not turn a
		// successful tool execution into a failure visible to the model.
		_ = e.ToolBilling.BillToolCall(ctx, tc.Function.Name)
	}
	return res
}

func (e *AgentEngine) getMaxConcurrent() int {
	if e.MaxConcurrent > 0 {
		return e.MaxConcurrent
	}
	return defaultMaxConcurrent
}

func (e *AgentEngine) maxIterations() int {
	if e.MaxIterations > 0 {
		return e.MaxIterations
	}
	return defaultMaxIterations
}

func (e *AgentEngine) toolTimeout() time.Duration {
	if e.ToolTimeout > 0 {
		return e.ToolTimeout
	}
	return defaultToolTimeout
}

func (e *AgentEngine) maxToolOutput() int {
	switch {
	case e.MaxToolOutputBytes > 0:
		return e.MaxToolOutputBytes
	case e.MaxToolOutputBytes < 0:
		return 0
	default:
		return DefaultMaxToolOutputBytes
	}
}

// MaxIterationsError is returned when the agent exceeds max iterations.
type MaxIterationsError struct {
	Iteration int
	// Usage aggregates the token usage consumed before the cap was hit.
	Usage *models.Usage
}

func (e *MaxIterationsError) Error() string {
	return fmt.Sprintf("max iterations (%d) reached", e.Iteration)
}

// extractToolCalls parses tool_calls from all choices of the LLM response.
func extractToolCalls(resp *models.LLMResponse) []models.ToolCall {
	if resp == nil {
		return nil
	}
	var calls []models.ToolCall
	for _, choice := range resp.Choices {
		calls = append(calls, choice.Message.ToolCalls...)
	}
	return calls
}

// firstChoiceToolCalls returns the tool calls of the first choice; the loop
// continues a single conversation, so only choice 0 drives it.
func firstChoiceToolCalls(resp *models.LLMResponse) []models.ToolCall {
	if resp == nil || len(resp.Choices) == 0 {
		return nil
	}
	return resp.Choices[0].Message.ToolCalls
}

// assistantMessage builds the assistant turn to append to the conversation.
// It copies the tool calls (never mutating the provider response), fills in
// missing call IDs and types so every tool result can reference its call.
func assistantMessage(msg models.Message, calls []models.ToolCall) (models.Message, []models.ToolCall) {
	fixed := make([]models.ToolCall, len(calls))
	for i, tc := range calls {
		if tc.ID == "" {
			tc.ID = newCallID()
		}
		if tc.Type == "" {
			tc.Type = "function"
		}
		fixed[i] = tc
	}
	msg.Role = models.RoleAssistant
	msg.ToolCalls = fixed
	return msg, fixed
}

func newCallID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("call_%d", time.Now().UnixNano())
	}
	return "call_" + hex.EncodeToString(buf)
}

// buildToolMessages formats tool results as role "tool" messages (one per call,
// in call order) carrying the matching tool_call_id. Errors are reported to
// the model as "error: <message>".
func buildToolMessages(calls []models.ToolCall, results []*ToolResult, maxBytes int) []models.Message {
	messages := make([]models.Message, len(calls))
	for i, tc := range calls {
		content := ""
		if i < len(results) && results[i] != nil {
			if results[i].Error != nil {
				content = "error: " + results[i].Error.Error()
			} else {
				content = results[i].ContentToString()
			}
		} else {
			content = "error: tool produced no result"
		}
		content = truncateUTF8(content, maxBytes)
		id := tc.ID
		messages[i] = models.Message{
			Role:       models.RoleTool,
			Content:    &content,
			ToolCallID: &id,
		}
	}
	return messages
}

const truncationMarker = "\n...[truncated]"

// truncateUTF8 limits s to at most maxBytes bytes without splitting a rune.
// maxBytes <= 0 disables truncation.
func truncateUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	cut := maxBytes - len(truncationMarker)
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncationMarker
}

// ToolCallCacheKey generates a cache key for a tool call.
func ToolCallCacheKey(tc models.ToolCall) string {
	return fmt.Sprintf("tool:%s:%s", tc.Function.Name, tc.ID)
}
