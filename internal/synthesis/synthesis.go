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

// DeficitDetector inspects provider/tool execution results for missing-tool signals.
type DeficitDetector struct {
	toolPattern *regexp.Regexp
}

// NewDeficitDetector creates a detector with heuristic patterns.
func NewDeficitDetector() *DeficitDetector {
	return &DeficitDetector{
		toolPattern: regexp.MustCompile(`(?i)(\b[A-Z][A-Za-z0-9_]*\b)\s+(?:tool|function|api)\s+(?:missing|not found|unavailable|needed)`),
	}
}

// Analyze scans an LLM response or tool error for a deficit signal.
func (d *DeficitDetector) Analyze(ctx context.Context, requestID, text string, err error) (ToolDeficitSignal, bool) {
	_ = ctx
	signal := ToolDeficitSignal{RequestID: requestID, SuggestedArgs: map[string]interface{}{}}
	if err != nil {
		signal.Reason = err.Error()
	}
	matches := d.toolPattern.FindStringSubmatch(text)
	if len(matches) > 1 {
		signal.MissingTool = strings.ToLower(matches[1])
		signal.Reason = text
		return signal, true
	}
	if strings.Contains(strings.ToLower(text), "i need a tool") || strings.Contains(strings.ToLower(text), "i cannot do that without") {
		words := regexp.MustCompile(`[A-Za-z0-9_]+`).FindAllString(text, -1)
		if len(words) > 0 {
			signal.MissingTool = strings.ToLower(words[len(words)-1])
		}
		signal.Reason = text
		return signal, true
	}
	return signal, false
}
