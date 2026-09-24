package agent

import (
	"context"
	"math"
	"sync"
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

func TestInMemoryVectorMemorySearch(t *testing.T) {
	vm := NewInMemoryVectorMemory()
	_ = vm.Upsert(context.Background(), "conv-1", models.Message{Role: "user", Content: strPtrMem("hello world")})
	_ = vm.Upsert(context.Background(), "conv-1", models.Message{Role: "assistant", Content: strPtrMem("world hello")})
	_ = vm.Upsert(context.Background(), "conv-2", models.Message{Role: "user", Content: strPtrMem("other")})

	results, err := vm.Search(context.Background(), "conv-1", "hello", 10)
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func strPtrMem(s string) *string { return &s }

func TestMessageMemoryBoundsAndCopies(t *testing.T) {
	m := NewMessageMemory()
	m.MaxMessagesPerConversation = 3
	m.MaxConversations = 2
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s := string(rune('a' + i))
		_ = m.Remember(ctx, "c1", models.Message{Role: models.RoleUser, Content: &s})
	}
	got, _ := m.Recall(ctx, "c1", 0)
	if len(got) != 3 || *got[0].Content != "c" || *got[2].Content != "e" {
		t.Fatalf("per-conversation cap not applied: %+v", got)
	}
	*got[0].Content = "mutated"
	again, _ := m.Recall(ctx, "c1", 1)
	if *again[0].Content != "e" {
		t.Fatalf("unexpected recall %q", *again[0].Content)
	}
	all, _ := m.Recall(ctx, "c1", 0)
	if *all[0].Content != "c" {
		t.Fatal("recall must return copies")
	}
	_ = m.Remember(ctx, "c2", models.Message{Role: models.RoleUser, Content: strPtrMem("x")})
	_ = m.Remember(ctx, "c3", models.Message{Role: models.RoleUser, Content: strPtrMem("y")})
	if old, _ := m.Recall(ctx, "c1", 0); len(old) != 0 {
		t.Fatal("least recently updated conversation should be evicted")
	}
	if err := m.Remember(ctx, "", models.Message{}); err == nil {
		t.Fatal("empty conversation id must be rejected")
	}
}

func TestMessageMemoryConcurrent(t *testing.T) {
	m := NewMessageMemory()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = m.Remember(ctx, "c", models.Message{Role: models.RoleUser, Content: strPtrMem("m")})
				_, _ = m.Recall(ctx, "c", 5)
				_, _ = m.Summarize(ctx, "c")
			}
		}(i)
	}
	wg.Wait()
	if got, _ := m.Recall(ctx, "c", 0); len(got) != 1000 {
		t.Fatalf("expected 1000 messages, got %d", len(got))
	}
}

func TestVectorMemoryRanksBySimilarityAndKeepsDistinctMessages(t *testing.T) {
	vm := NewInMemoryVectorMemory()
	ctx := context.Background()
	// Same role and same length: previously these overwrote each other.
	_ = vm.Upsert(ctx, "c", models.Message{Role: "user", Content: strPtrMem("cats purr loudly")})
	_ = vm.Upsert(ctx, "c", models.Message{Role: "user", Content: strPtrMem("dogs bark loudly")})
	_ = vm.Upsert(ctx, "c", models.Message{Role: "user", Content: strPtrMem("stock markets rose")})
	res, err := vm.Search(ctx, "c", "why do cats purr", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) < 1 || *res[0].Content != "cats purr loudly" {
		t.Fatalf("most similar message should rank first, got %+v", res)
	}
	for _, r := range res {
		if *r.Content == "stock markets rose" {
			t.Fatal("unrelated message (zero similarity) should not be returned")
		}
	}
	all, _ := vm.Search(ctx, "c", "loudly", 0)
	if len(all) != 2 {
		t.Fatalf("expected both 'loudly' messages to be stored, got %d", len(all))
	}
}

func TestCosineSimilarityEdgeCases(t *testing.T) {
	if cosineSimilarity([]float64{0, 0}, []float64{1, 1}) != 0 {
		t.Fatal("zero vector must give 0")
	}
	if cosineSimilarity([]float64{1}, []float64{1, 2}) != 0 {
		t.Fatal("dimension mismatch must give 0")
	}
	if got := cosineSimilarity([]float64{1e308, 1e308}, []float64{1e308, 1e308}); got != 0 && (got < 0.99 || got > 1.01) {
		t.Fatalf("overflow must not produce garbage, got %v", got)
	}
	s := NewInMemoryVectorStore()
	if err := s.Upsert(context.Background(), "x", []float64{math.NaN()}, nil); err == nil {
		t.Fatal("NaN vectors must be rejected")
	}
}
