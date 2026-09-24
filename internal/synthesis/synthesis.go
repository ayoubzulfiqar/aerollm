package synthesis

import (
	"context"
	"regexp"
	"strings"
)

// ToolDeficitSignal represents a detected missing-tool event.
type ToolDeficitSignal struct {
	RequestID     string
	Prompt        string
	MissingTool   string
	Reason        string
	SuggestedArgs map[string]interface{}
}

// MaxAnalyzeBytes caps the text inspected by DeficitDetector.Analyze so huge
// responses cannot make detection expensive.
const MaxAnalyzeBytes = 16 * 1024

var (
	defaultToolPattern = regexp.MustCompile(`(?i)(\b[A-Z][A-Za-z0-9_]*\b)\s+(?:tool|function|api)\s+(?:missing|not found|unavailable|needed)`)
	wordPattern        = regexp.MustCompile(`[A-Za-z0-9_]+`)
)

// DeficitDetector inspects provider/tool execution results for missing-tool signals.
type DeficitDetector struct {
	toolPattern *regexp.Regexp
}

// NewDeficitDetector creates a detector with heuristic patterns.
func NewDeficitDetector() *DeficitDetector {
	return &DeficitDetector{toolPattern: defaultToolPattern}
}

// Analyze scans an LLM response or tool error for a deficit signal. Only the
// first MaxAnalyzeBytes of text are inspected.
func (d *DeficitDetector) Analyze(ctx context.Context, requestID, text string, err error) (ToolDeficitSignal, bool) {
	_ = ctx
	signal := ToolDeficitSignal{RequestID: requestID, SuggestedArgs: map[string]interface{}{}}
	if err != nil {
		signal.Reason = err.Error()
	}
	if len(text) > MaxAnalyzeBytes {
		text = text[:MaxAnalyzeBytes]
	}
	pattern := defaultToolPattern
	if d != nil && d.toolPattern != nil {
		pattern = d.toolPattern
	}
	if matches := pattern.FindStringSubmatch(text); len(matches) > 1 {
		signal.MissingTool = strings.ToLower(matches[1])
		signal.Reason = text
		return signal, true
	}
	lower := strings.ToLower(text)
	if strings.Contains(lower, "i need a tool") || strings.Contains(lower, "i cannot do that without") {
		if words := wordPattern.FindAllString(text, -1); len(words) > 0 {
			signal.MissingTool = strings.ToLower(words[len(words)-1])
		}
		signal.Reason = text
		return signal, true
	}
	return signal, false
}
