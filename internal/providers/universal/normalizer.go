package universal

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// StreamNormalizer converts provider-specific stream events into AeroStreamChunk.
type StreamNormalizer struct{}

// NewStreamNormalizer creates a new normalizer.
func NewStreamNormalizer() *StreamNormalizer {
	return &StreamNormalizer{}
}

// Normalize converts a provider-specific chunk into an AeroStreamChunk.
func (n *StreamNormalizer) Normalize(provider string, data []byte) (AeroStreamChunk, error) {
	var chunk AeroStreamChunk
	chunk.Provider = provider
	chunk.Data = data
	if len(data) == 0 {
		chunk.Finish = true
		return chunk, nil
	}
	var payload struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return chunk, err
	}
	if len(payload.Choices) > 0 {
		chunk.Delta = payload.Choices[0].Delta.Content
		if payload.Choices[0].FinishReason != nil {
			chunk.Finish = *payload.Choices[0].FinishReason == "stop"
		}
	}
	chunk.ContentType = "text/event-stream"
	return chunk, nil
}

// HealthByIntent classifies the likely intent from an LLM request using a lightweight heuristic.
func HealthByIntent(ctx context.Context, req *models.LLMRequest) (string, error) {
	_ = ctx
	if req == nil || len(req.Messages) == 0 {
		return "unknown", nil
	}
	last := req.Messages[len(req.Messages)-1]
	if last.Role == "tool" || len(last.ToolCalls) > 0 {
		return "tool_use", nil
	}
	content := ""
	if last.Content != nil {
		content = *last.Content
	}
	lower := strings.ToLower(content)
	switch {
	case strings.Contains(lower, "code") || strings.Contains(lower, "function") || strings.Contains(lower, "implement"):
		return "coding", nil
	case strings.Contains(lower, "summarize") || strings.Contains(lower, "summary") || strings.Contains(lower, "tl;dr"):
		return "summarization", nil
	case strings.Contains(lower, "translate") || strings.Contains(lower, "language"):
		return "translation", nil
	default:
		return "chat", nil
	}
}
