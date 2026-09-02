package intelligence

import (
	"context"
	"math"
	"strings"
	"unicode"
)

// Intent represents a classified request intent.
type Intent string

const (
	IntentCode       Intent = "code"
	IntentQA         Intent = "qa"
	IntentSummarize  Intent = "summarization"
	IntentCreative   Intent = "creative"
	IntentExtraction Intent = "extraction"
	IntentGeneral    Intent = "general"
)

// IntentClassifyRequest holds request data for intent classification.
type IntentClassifyRequest struct {
	Prompt string
}

// IntentClassifyResult is the classification result.
type IntentClassifyResult struct {
	Intent Intent
}

// Classifier classifies request intents.
type Classifier interface {
	Classify(ctx context.Context, req IntentClassifyRequest) (IntentClassifyResult, error)
}

// HeuristicClassifier uses simple keyword heuristics.
type HeuristicClassifier struct{}

// NewHeuristicClassifier returns a new heuristic classifier.
func NewHeuristicClassifier() *HeuristicClassifier {
	return &HeuristicClassifier{}
}

// Classify classifies intent from prompt heuristics.
func (c *HeuristicClassifier) Classify(ctx context.Context, req IntentClassifyRequest) (IntentClassifyResult, error) {
	_ = ctx
	prompt := strings.ToLower(req.Prompt)
	if containsAny(prompt, []string{"```", "function", "class ", "bug", "refactor", "typescript", "python", "golang", "rust"}) {
		return IntentClassifyResult{Intent: IntentCode}, nil
	}
	if containsAny(prompt, []string{"why", "how does", "explain", "what is", "define", "difference between"}) {
		return IntentClassifyResult{Intent: IntentQA}, nil
	}
	if containsAny(prompt, []string{"summarize", "summary", "tl;dr", "brief", "condense"}) {
		return IntentClassifyResult{Intent: IntentSummarize}, nil
	}
	if containsAny(prompt, []string{"story", "poem", "creative", "write a", "compose", "lyrics"}) {
		return IntentClassifyResult{Intent: IntentCreative}, nil
	}
	if containsAny(prompt, []string{"extract", "json", "entity", "list", "parse", "regex", "field"}) {
		return IntentClassifyResult{Intent: IntentExtraction}, nil
	}
	return IntentClassifyResult{Intent: IntentGeneral}, nil
}

func containsAny(s string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}

// TokenCount approximates token count from prompt.
func TokenCount(prompt string) int {
	count := 0
	inWord := false
	for _, r := range prompt {
		if unicode.IsSpace(r) {
			inWord = false
			continue
		}
		if !inWord {
			count++
			inWord = true
		}
	}
	return int(math.Ceil(float64(count) * 1.3))
}
