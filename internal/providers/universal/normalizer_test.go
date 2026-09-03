package universal

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestHealthByIntentCoding(t *testing.T) {
	intent, err := HealthByIntent(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: "user", Content: strPtr("write some code now")}},
	})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if intent != "coding" { t.Fatalf("expected coding, got %s", intent) }
}

func TestHealthByIntentSummarization(t *testing.T) {
	intent, err := HealthByIntent(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: "user", Content: strPtr("please summarize this")}},
	})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if intent != "summarization" { t.Fatalf("expected summarization, got %s", intent) }
}

func TestHealthByIntentTranslation(t *testing.T) {
	intent, err := HealthByIntent(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: "user", Content: strPtr("translate to spanish")}},
	})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if intent != "translation" { t.Fatalf("expected translation, got %s", intent) }
}

func TestHealthByIntentToolUse(t *testing.T) {
	intent, err := HealthByIntent(context.Background(), &models.LLMRequest{
		Messages: []models.Message{{Role: "tool", ToolCallID: strPtr("1"), ToolCalls: []models.ToolCall{{Function: models.ToolFunction{Name: "weather"}}}}},
	})
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if intent != "tool_use" { t.Fatalf("expected tool_use, got %s", intent) }
}

func TestHealthByIntentUnknown(t *testing.T) {
	intent, err := HealthByIntent(context.Background(), nil)
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if intent != "unknown" { t.Fatalf("expected unknown, got %s", intent) }
}

func strPtr(s string) *string { return &s }
