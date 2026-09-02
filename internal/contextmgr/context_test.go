package contextmgr

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestSimpleTokenizerCount(t *testing.T) {
	tok := NewSimpleTokenizer()
	if tok.Count("") != 0 {
		t.Fatalf("expected 0 tokens for empty string")
	}
	if tok.Count("hello world") <= 0 {
		t.Fatalf("expected positive token count")
	}
}

func TestSimpleTokenizerCountMessages(t *testing.T) {
	tok := NewSimpleTokenizer()
	messages := []models.Message{
		{Role: models.RoleUser, Content: strPtr("hello")},
		{Role: models.RoleAssistant, Content: strPtr("world")},
	}
	if tok.CountMessages(messages) <= 0 {
		t.Fatalf("expected positive token count for messages")
	}
}

func TestTruncatingSummarizerSummarize(t *testing.T) {
	s := NewTruncatingSummarizer(50)
	messages := []models.Message{
		{Role: models.RoleUser, Content: strPtr("hello")},
		{Role: models.RoleAssistant, Content: strPtr("world")},
	}
	msg, err := s.Summarize(context.Background(), messages)
	if err != nil {
		t.Fatalf("summarize error: %v", err)
	}
	if msg.Role != models.RoleSystem {
		t.Fatalf("expected system role summary, got %s", msg.Role)
	}
	if msg.Content == nil || *msg.Content == "" {
		t.Fatal("expected non-empty summary content")
	}
}

func TestContextManagerMaybeSummarizeNoOp(t *testing.T) {
	cm := NewContextManager(map[string]int{"gpt-4": 1000})
	messages := []models.Message{
		{Role: models.RoleUser, Content: strPtr("hi")},
	}
	res, err := cm.MaybeSummarize(context.Background(), "gpt-4", messages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Summarized {
		t.Fatal("expected no summarization for small input")
	}
	if len(res.Messages) != 1 {
		t.Fatalf("expected original messages, got %d", len(res.Messages))
	}
}

func TestContextManagerMaybeSummarizeTriggers(t *testing.T) {
	cm := NewContextManager(map[string]int{"gpt-4": 10})
	var messages []models.Message
	for i := 0; i < 20; i++ {
		messages = append(messages, models.Message{Role: models.RoleUser, Content: strPtr("some longer message that should cause summarization to trigger because token limit is exceeded")})
	}
	res, err := cm.MaybeSummarize(context.Background(), "gpt-4", messages)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Summarized {
		t.Fatal("expected summarization to trigger")
	}
	if len(res.Messages) != 3 {
		t.Fatalf("expected compressed message list, got %d", len(res.Messages))
	}
}
