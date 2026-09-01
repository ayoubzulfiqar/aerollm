package agent

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestMessageMemoryRememberRecall(t *testing.T) {
	m := NewMessageMemory()
	content := "hello"
	msg := models.Message{Role: models.RoleUser, Content: &content}
	if err := m.Remember(context.Background(), "conv1", msg); err != nil {
		t.Fatalf("remember failed: %v", err)
	}
	recalled, err := m.Recall(context.Background(), "conv1", 10)
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(recalled) != 1 || recalled[0].Content == nil || *recalled[0].Content != "hello" {
		t.Fatalf("unexpected recall: %+v", recalled)
	}
}

func TestMessageMemorySummarize(t *testing.T) {
	m := NewMessageMemory()
	if _, err := m.Summarize(context.Background(), "missing"); err != nil {
		t.Fatalf("summarize failed: %v", err)
	}
	content := "hi"
	if err := m.Remember(context.Background(), "conv1", models.Message{Role: models.RoleUser, Content: &content}); err != nil {
		t.Fatalf("remember failed: %v", err)
	}
	sum, err := m.Summarize(context.Background(), "conv1")
	if err != nil {
		t.Fatalf("summarize failed: %v", err)
	}
	if sum == "" {
		t.Fatal("expected non-empty summary")
	}
}
