package contextpkg

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// TokenCounter estimates token count for text.
type TokenCounter interface {
	Count(text string) int
	CountMessages(messages []models.Message) int
}

// simpleTokenizer implements TokenCounter using a whitespace/character heuristic.
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
	// Rough heuristic: split on whitespace, then add punctuation tokens.
	words := strings.Fields(text)
	tokens := 0
	for _, w := range words {
		// Count contiguous letter/digit runs as one token; punctuation as separate tokens.
		var current strings.Builder
		for _, r := range w {
			if unicode.IsPunct(r) {
				if current.Len() > 0 {
					tokens++
					current.Reset()
				}
				tokens++
			} else {
				current.WriteRune(r)
			}
		}
		if current.Len() > 0 {
			tokens++
		}
	}
	if tokens == 0 && len(text) > 0 {
		// Fallback for unusual strings
		return int(float64(len(text)) / t.charsPerToken)
	}
	return tokens
}

// CountMessages estimates total tokens across messages.
func (t *simpleTokenizer) CountMessages(messages []models.Message) int {
	total := 0
	for _, m := range messages {
		if m.Content != nil {
			total += t.Count(*m.Content)
		}
	}
	return total
}

// Summarizer compresses older messages into a summary.
type Summarizer interface {
	Summarize(ctx context.Context, messages []models.Message) (models.Message, error)
}

// truncatingSummarizer creates a summary by extracting key sentences from older messages.
type truncatingSummarizer struct {
	maxSummaryTokens int
	tokenCounter     TokenCounter
}

// NewTruncatingSummarizer creates a simple summarizer.
func NewTruncatingSummarizer(maxSummaryTokens int) *truncatingSummarizer {
	return &truncatingSummarizer{
		maxSummaryTokens: maxSummaryTokens,
		tokenCounter:     NewSimpleTokenizer(),
	}
}

// Summarize concatenates older messages into a summary string.
func (s *truncatingSummarizer) Summarize(ctx context.Context, messages []models.Message) (models.Message, error) {
	_ = ctx
	if len(messages) == 0 {
		return models.Message{Role: models.RoleSystem, Content: strPtr("")}, nil
	}
	var sb strings.Builder
	sb.WriteString("Summary of conversation: ")
	for _, m := range messages {
		if m.Content != nil && strings.TrimSpace(*m.Content) != "" {
			content := strings.TrimSpace(*m.Content)
			// Truncate long messages to fit summary budget.
			approxTokens := s.tokenCounter.Count(content)
			if approxTokens > s.maxSummaryTokens {
				// Keep first N words roughly.
				words := strings.Fields(content)
				keep := s.maxSummaryTokens * 2
				if keep > len(words) {
					keep = len(words)
				}
				content = strings.Join(words[:keep], " ") + "..."
			}
			sb.WriteString(fmt.Sprintf("[%s] %s ", m.Role, content))
		}
	}
	summary := sb.String()
	return models.Message{Role: models.RoleSystem, Content: &summary}, nil
}

// ContextManager handles token budgets and auto-summarization.
type ContextManager struct {
	tokenCounter TokenCounter
	summarizer   Summarizer
	modelLimits  map[string]int
}

// NewContextManager creates a new context manager.
func NewContextManager(modelLimits map[string]int) *ContextManager {
	return &ContextManager{
		tokenCounter: NewSimpleTokenizer(),
		summarizer:   NewTruncatingSummarizer(200),
		modelLimits:  modelLimits,
	}
}

// SummarizationResult describes the outcome of context compression.
type SummarizationResult struct {
	Summarized bool
	Messages   []models.Message
	Summary    models.Message
}

// MaybeSummarize checks token usage and compresses if needed.
func (c *ContextManager) MaybeSummarize(ctx context.Context, model string, messages []models.Message) (SummarizationResult, error) {
	limit, ok := c.modelLimits[model]
	if !ok {
		limit = 8000
	}
	used := c.tokenCounter.CountMessages(messages)
	if used <= int(float64(limit)*0.8) {
		return SummarizationResult{Summarized: false, Messages: messages}, nil
	}
	if len(messages) <= 2 {
		return SummarizationResult{Summarized: false, Messages: messages}, nil
	}
	// Keep first system message + last few messages; summarize middle.
	keep := 2
	if keep > len(messages)-1 {
		keep = len(messages) - 1
	}
	older := messages[:len(messages)-keep]
	recent := messages[len(messages)-keep:]
	summaryMsg, err := c.summarizer.Summarize(ctx, older)
	if err != nil {
		return SummarizationResult{Summarized: false, Messages: messages}, err
	}
	compressed := append([]models.Message{summaryMsg}, recent...)
	return SummarizationResult{Summarized: true, Messages: compressed, Summary: summaryMsg}, nil
}

func strPtr(s string) *string { return &s }
