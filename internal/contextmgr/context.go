// Package contextmgr estimates token usage and keeps conversations within a
// model's context window, by trimming old turns (TrimToFit) or folding them
// into a summary (MaybeSummarize).
package contextmgr

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

const (
	// messageOverheadTokens approximates the per-message framing cost
	// (role, separators) of chat formats.
	messageOverheadTokens = 4
	// replyPrimingTokens approximates the tokens that prime the assistant reply.
	replyPrimingTokens = 3
	// DefaultContextWindow is used for models missing from the window table.
	DefaultContextWindow = 8192
)

// TokenCounter estimates token count for text.
type TokenCounter interface {
	Count(text string) int
	CountMessages(messages []models.Message) int
}

// simpleTokenizer implements TokenCounter with a character-class heuristic:
// roughly 4 ASCII characters per token, ~1 token per CJK ideograph/kana/hangul
// syllable and ~2 characters per token for other scripts. It errs slightly on
// the high side, which is the safe direction for budgeting.
type simpleTokenizer struct {
	charsPerToken float64
}

// NewSimpleTokenizer creates a basic token counter.
func NewSimpleTokenizer() *simpleTokenizer {
	return &simpleTokenizer{charsPerToken: 4.0}
}

// Count estimates tokens in a string.
func (t *simpleTokenizer) Count(text string) int {
	if strings.TrimSpace(text) == "" {
		return 0
	}
	cpt := t.charsPerToken
	if cpt <= 0 {
		cpt = 4.0
	}
	var units float64
	for _, r := range text {
		switch {
		case r < 128:
			units += 1 / cpt
		case unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul):
			units++
		default:
			units += 0.5
		}
	}
	n := int(math.Ceil(units))
	if n < 1 {
		n = 1
	}
	return n
}

// CountMessages estimates total prompt tokens across messages, including the
// per-message overhead, tool calls, tool results and reply priming.
func (t *simpleTokenizer) CountMessages(messages []models.Message) int {
	if len(messages) == 0 {
		return 0
	}
	total := replyPrimingTokens
	for _, m := range messages {
		total += t.countMessage(m)
	}
	return total
}

func (t *simpleTokenizer) countMessage(m models.Message) int {
	n := messageOverheadTokens
	if m.Content != nil {
		n += t.Count(*m.Content)
	}
	if m.Name != nil {
		n += 1 + t.Count(*m.Name)
	}
	if m.ToolResult != nil {
		n += t.Count(*m.ToolResult)
	}
	if m.ToolCallID != nil {
		n += t.Count(*m.ToolCallID)
	}
	for _, tc := range m.ToolCalls {
		n += 3 + t.Count(tc.Function.Name) + t.Count(tc.Function.Arguments)
	}
	return n
}

var defaultCounter = NewSimpleTokenizer()

// EstimateTokens estimates the number of tokens in text.
func EstimateTokens(text string) int { return defaultCounter.Count(text) }

// EstimateMessagesTokens estimates the prompt tokens of a conversation.
func EstimateMessagesTokens(messages []models.Message) int {
	return defaultCounter.CountMessages(messages)
}

// contextWindows maps model-name prefixes to context window sizes (tokens).
// Lookup uses the longest matching prefix.
var contextWindows = map[string]int{
	// OpenAI
	"gpt-5":         400000,
	"gpt-4.1":       1047576,
	"gpt-4o":        128000,
	"chatgpt-4o":    128000,
	"gpt-4-turbo":   128000,
	"gpt-4-1106":    128000,
	"gpt-4-0125":    128000,
	"gpt-4-32k":     32768,
	"gpt-4":         8192,
	"gpt-3.5-turbo": 16385,
	"o1-mini":       128000,
	"o1":            200000,
	"o3":            200000,
	"o4-mini":       200000,
	// Anthropic
	"claude-2":      100000,
	"claude-":       200000,
	"claude-3":      200000,
	"claude-4":      200000,
	"claude-opus":   200000,
	"claude-sonnet": 200000,
	"claude-haiku":  200000,
	// Google
	"gemini-1.5-pro":   2097152,
	"gemini-1.5-flash": 1048576,
	"gemini-2":         1048576,
	"gemini-pro":       32760,
	"gemini-1.0":       32760,
	"gemini":           1048576,
	// Meta Llama (hyphenated and Ollama-style names)
	"llama-2":    4096,
	"llama2":     4096,
	"llama-3.1":  131072,
	"llama-3.2":  131072,
	"llama-3.3":  131072,
	"llama3.1":   131072,
	"llama3.2":   131072,
	"llama3.3":   131072,
	"llama-3":    8192,
	"llama3":     8192,
	"llama-4":    1048576,
	"llama4":     1048576,
	"meta-llama": 8192,
	// Others
	"mistral-large":  128000,
	"mistral-medium": 128000,
	"mistral-small":  32000,
	"mistral-nemo":   128000,
	"mistral":        32768,
	"mixtral-8x22b":  65536,
	"mixtral":        32768,
	"codestral":      32768,
	"deepseek":       65536,
	"command-r":      128000,
	"qwen2.5":        32768,
	"qwen":           32768,
	"grok":           131072,
}

// windowPrefixes are the table keys sorted longest first.
var windowPrefixes = func() []string {
	keys := make([]string, 0, len(contextWindows))
	for k := range contextWindows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}()

// normalizeModel lowercases the model and strips provider/path prefixes such
// as "openai/", "models/" or "bedrock/anthropic." forms.
func normalizeModel(model string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	// Bedrock-style "anthropic.claude-3-..." / "meta.llama3-...".
	for _, vendor := range []string{"anthropic.", "meta.", "mistral.", "cohere.", "amazon.", "ai21."} {
		m = strings.TrimPrefix(m, vendor)
	}
	return m
}

// ContextWindow returns the context window (in tokens) for a model, matching
// the longest known prefix of the normalised model name. Unknown models get
// DefaultContextWindow.
func ContextWindow(model string) int {
	if w, ok := lookupWindow(model); ok {
		return w
	}
	return DefaultContextWindow
}

func lookupWindow(model string) (int, bool) {
	m := normalizeModel(model)
	if m == "" {
		return 0, false
	}
	for _, prefix := range windowPrefixes {
		if strings.HasPrefix(m, prefix) {
			return contextWindows[prefix], true
		}
	}
	return 0, false
}

// Summarizer compresses older messages into a summary.
type Summarizer interface {
	Summarize(ctx context.Context, messages []models.Message) (models.Message, error)
}

// truncatingSummarizer creates an extractive summary: one clipped line per
// message, bounded overall by maxSummaryTokens.
type truncatingSummarizer struct {
	maxSummaryTokens int
	tokenCounter     TokenCounter
}

// NewTruncatingSummarizer creates a simple summarizer.
func NewTruncatingSummarizer(maxSummaryTokens int) *truncatingSummarizer {
	if maxSummaryTokens <= 0 {
		maxSummaryTokens = 200
	}
	return &truncatingSummarizer{
		maxSummaryTokens: maxSummaryTokens,
		tokenCounter:     NewSimpleTokenizer(),
	}
}

// Summarize condenses messages into a single system message whose estimated
// size stays within the summarizer's token budget.
func (s *truncatingSummarizer) Summarize(ctx context.Context, messages []models.Message) (models.Message, error) {
	_ = ctx
	if len(messages) == 0 {
		return models.Message{Role: models.RoleSystem, Content: strPtr("")}, nil
	}
	const header = "Summary of earlier conversation:"
	budget := s.maxSummaryTokens - s.tokenCounter.Count(header)
	perMsg := budget / len(messages)
	if perMsg < 16 {
		perMsg = 16
	}
	var sb strings.Builder
	sb.WriteString(header)
	used := 0
	for _, m := range messages {
		line := describeMessage(m)
		if line == "" {
			continue
		}
		line = clipToTokens(line, perMsg, s.tokenCounter)
		entry := fmt.Sprintf("\n- [%s] %s", m.Role, line)
		cost := s.tokenCounter.Count(entry)
		if used+cost > budget {
			sb.WriteString("\n- ...")
			break
		}
		sb.WriteString(entry)
		used += cost
	}
	summary := sb.String()
	return models.Message{Role: models.RoleSystem, Content: &summary}, nil
}

// describeMessage renders a message as one line of text for summaries.
func describeMessage(m models.Message) string {
	var parts []string
	if m.Content != nil && strings.TrimSpace(*m.Content) != "" {
		parts = append(parts, strings.Join(strings.Fields(*m.Content), " "))
	}
	if m.ToolResult != nil && strings.TrimSpace(*m.ToolResult) != "" {
		parts = append(parts, "tool result: "+strings.Join(strings.Fields(*m.ToolResult), " "))
	}
	if len(m.ToolCalls) > 0 {
		names := make([]string, len(m.ToolCalls))
		for i, tc := range m.ToolCalls {
			names[i] = tc.Function.Name
		}
		parts = append(parts, "called tools: "+strings.Join(names, ", "))
	}
	return strings.Join(parts, "; ")
}

// clipToTokens shortens text so its estimated token count is <= max.
func clipToTokens(text string, max int, counter TokenCounter) string {
	if counter.Count(text) <= max {
		return text
	}
	runes := []rune(text)
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if counter.Count(string(runes[:mid])+"...") <= max {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return string(runes[:lo]) + "..."
}

// ContextManager handles token budgets and auto-summarization.
type ContextManager struct {
	tokenCounter TokenCounter
	summarizer   Summarizer
	modelLimits  map[string]int
}

// NewContextManager creates a new context manager. modelLimits optionally
// overrides context windows for exact model names; other models use the
// built-in window table (see ContextWindow). A nil map is fine.
func NewContextManager(modelLimits map[string]int) *ContextManager {
	limits := make(map[string]int, len(modelLimits))
	for k, v := range modelLimits {
		limits[k] = v
	}
	return &ContextManager{
		tokenCounter: NewSimpleTokenizer(),
		summarizer:   NewTruncatingSummarizer(200),
		modelLimits:  limits,
	}
}

// SetSummarizer replaces the summarizer (e.g. with an LLM-backed one).
func (c *ContextManager) SetSummarizer(s Summarizer) {
	if s != nil {
		c.summarizer = s
	}
}

// Limit returns the context window used for model.
func (c *ContextManager) Limit(model string) int {
	if v, ok := c.modelLimits[model]; ok && v > 0 {
		return v
	}
	return ContextWindow(model)
}

// SummarizationResult describes the outcome of context compression.
type SummarizationResult struct {
	Summarized bool
	Messages   []models.Message
	Summary    models.Message
}

// MaybeSummarize compresses the conversation when it uses more than 80% of
// the model's context window. System messages are kept verbatim; the most
// recent turns that fit in half of the window are kept (never separating an
// assistant tool_calls message from its tool results); everything older is
// folded into one summary system message placed after the system messages.
// The returned Messages are always usable as a request payload; when no
// compression is needed they are the input unchanged.
func (c *ContextManager) MaybeSummarize(ctx context.Context, model string, messages []models.Message) (SummarizationResult, error) {
	limit := c.Limit(model)
	used := c.tokenCounter.CountMessages(messages)
	if used <= int(float64(limit)*0.8) || len(messages) <= 2 {
		return SummarizationResult{Summarized: false, Messages: messages}, nil
	}

	system, units := splitUnits(messages)
	systemTokens := c.tokenCounter.CountMessages(system)
	recentBudget := limit/2 - systemTokens
	keepFrom := len(units)
	spent := 0
	for i := len(units) - 1; i >= 0; i-- {
		cost := c.unitTokens(units[i])
		if keepFrom < len(units) && spent+cost > recentBudget {
			break
		}
		spent += cost
		keepFrom = i
	}
	if keepFrom == 0 {
		// Everything already fits in the recent budget; only trimming could help.
		return SummarizationResult{Summarized: false, Messages: messages}, nil
	}

	var older []models.Message
	for _, u := range units[:keepFrom] {
		older = append(older, u.messages...)
	}
	summaryMsg, err := c.summarizer.Summarize(ctx, older)
	if err != nil {
		return SummarizationResult{Summarized: false, Messages: messages}, err
	}
	summaryMsg.Role = models.RoleSystem

	compressed := make([]models.Message, 0, len(system)+1+len(messages))
	compressed = append(compressed, system...)
	compressed = append(compressed, summaryMsg)
	for _, u := range units[keepFrom:] {
		compressed = append(compressed, u.messages...)
	}
	if c.tokenCounter.CountMessages(compressed) > limit {
		compressed = c.trim(compressed, limit)
	}
	return SummarizationResult{Summarized: true, Messages: compressed, Summary: summaryMsg}, nil
}

// TrimToFit drops the oldest conversation turns until the estimated prompt
// fits in model's context window minus reserveForOutput tokens, using the
// built-in context window table. See ContextManager.TrimToFit.
func TrimToFit(messages []models.Message, model string, reserveForOutput int) []models.Message {
	return defaultManager.TrimToFit(messages, model, reserveForOutput)
}

var defaultManager = NewContextManager(nil)

// TrimToFit drops the oldest conversation turns until the estimated prompt
// fits in the model's context window minus reserveForOutput tokens.
//
//   - All system messages are kept.
//   - Turns are kept newest-first; an assistant message carrying tool_calls is
//     never separated from the tool messages answering it.
//   - The most recent turn is always kept, even if it alone exceeds the budget.
//   - If trimming would start the conversation with an assistant/tool turn, it
//     advances to the first remaining user message when there is one.
//
// The input slice is not modified.
func (c *ContextManager) TrimToFit(messages []models.Message, model string, reserveForOutput int) []models.Message {
	if reserveForOutput < 0 {
		reserveForOutput = 0
	}
	return c.trim(messages, c.Limit(model)-reserveForOutput)
}

func (c *ContextManager) trim(messages []models.Message, budget int) []models.Message {
	if len(messages) == 0 || c.tokenCounter.CountMessages(messages) <= budget {
		return messages
	}
	system, units := splitUnits(messages)
	if len(units) == 0 {
		return append([]models.Message(nil), messages...)
	}
	remaining := budget - c.tokenCounter.CountMessages(system)
	keepFrom := len(units) - 1
	remaining -= c.unitTokens(units[keepFrom])
	for i := keepFrom - 1; i >= 0; i-- {
		cost := c.unitTokens(units[i])
		if cost > remaining {
			break
		}
		remaining -= cost
		keepFrom = i
	}
	// Prefer starting the kept history with a user turn.
	if units[keepFrom].role() != models.RoleUser {
		for i := keepFrom + 1; i < len(units); i++ {
			if units[i].role() == models.RoleUser {
				keepFrom = i
				break
			}
		}
	}
	kept := make(map[int]bool)
	for _, u := range units[keepFrom:] {
		for _, idx := range u.indexes {
			kept[idx] = true
		}
	}
	out := make([]models.Message, 0, len(messages))
	for i, m := range messages {
		if m.Role == models.RoleSystem || kept[i] {
			out = append(out, m)
		}
	}
	return out
}

// unit is a group of messages that must be kept or dropped together.
type unit struct {
	messages []models.Message
	indexes  []int
}

func (u unit) role() models.MessageRole {
	if len(u.messages) == 0 {
		return ""
	}
	return u.messages[0].Role
}

func (c *ContextManager) unitTokens(u unit) int {
	total := 0
	for _, m := range u.messages {
		if st, ok := c.tokenCounter.(*simpleTokenizer); ok {
			total += st.countMessage(m)
		} else {
			total += messageOverheadTokens
			if m.Content != nil {
				total += c.tokenCounter.Count(*m.Content)
			}
		}
	}
	return total
}

// splitUnits separates system messages from the rest and groups the rest into
// atomic units: an assistant message with tool_calls plus the tool messages
// that follow it form one unit; every other message is its own unit.
func splitUnits(messages []models.Message) (system []models.Message, units []unit) {
	for i := 0; i < len(messages); i++ {
		m := messages[i]
		if m.Role == models.RoleSystem {
			system = append(system, m)
			continue
		}
		u := unit{messages: []models.Message{m}, indexes: []int{i}}
		if m.Role == models.RoleAssistant && len(m.ToolCalls) > 0 {
			for i+1 < len(messages) && messages[i+1].Role == models.RoleTool {
				i++
				u.messages = append(u.messages, messages[i])
				u.indexes = append(u.indexes, i)
			}
		}
		units = append(units, u)
	}
	return system, units
}

func strPtr(s string) *string { return &s }
