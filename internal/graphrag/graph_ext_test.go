package graphrag

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

func TestNodeIDDeterministicWithProps(t *testing.T) {
	props := map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
	first := nodeID("x", "y", props)
	for i := 0; i < 50; i++ {
		if nodeID("x", "y", props) != first {
			t.Fatal("node IDs must not depend on map iteration order")
		}
	}
}

func TestUpsertEdgeNoIndexDuplicatesAndMove(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := s.UpsertEdge(ctx, Edge{ID: "e1", Source: "a", Target: "b", Label: "l"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.sourceIdx["a"]) != 1 {
		t.Fatalf("index accumulated duplicates: %d", len(s.sourceIdx["a"]))
	}
	_, _ = s.UpsertEdge(ctx, Edge{ID: "e1", Source: "a", Target: "c", Label: "l"})
	if _, ok := s.targetIdx["b"]; ok {
		t.Fatal("stale target index entry after moving the edge")
	}
	edges, _ := s.Neighbors(ctx, "c", 1)
	if len(edges) != 1 {
		t.Fatalf("moved edge not reachable from new target: %d", len(edges))
	}
	if _, err := s.UpsertEdge(ctx, Edge{Source: "a"}); err == nil {
		t.Fatal("edges without target must be rejected")
	}
	if _, err := s.UpsertNode(ctx, Node{}); err == nil {
		t.Fatal("nodes without id and label must be rejected")
	}
}

func TestNeighborsTemporalValidity(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	_, _ = s.UpsertEdge(ctx, Edge{Source: "a", Target: "b", Label: "expired", ValidFrom: past.Add(-time.Hour), ValidTo: &past})
	_, _ = s.UpsertEdge(ctx, Edge{Source: "a", Target: "c", Label: "future", ValidFrom: future})
	_, _ = s.UpsertEdge(ctx, Edge{Source: "a", Target: "d", Label: "current"})
	edges, _ := s.Neighbors(ctx, "a", 1)
	if len(edges) != 1 || edges[0].Label != "current" {
		t.Fatalf("only the currently valid edge should be returned, got %+v", edges)
	}
	then, _ := s.NeighborsAt(ctx, "a", 1, past.Add(-30*time.Minute))
	if len(then) != 1 || then[0].Label != "expired" {
		t.Fatalf("temporal query wrong: %+v", then)
	}
}

func TestUpsertNodePreservesCreatedAt(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	id, _ := s.UpsertNode(ctx, Node{Label: "A", Type: "t"})
	n1, _ := s.GetNode(ctx, id)
	time.Sleep(2 * time.Millisecond)
	_, _ = s.UpsertNode(ctx, Node{ID: id, Label: "A2", Type: "t"})
	n2, _ := s.GetNode(ctx, id)
	if !n2.CreatedAt.Equal(n1.CreatedAt) || !n2.UpdatedAt.After(n1.UpdatedAt) || n2.Label != "A2" {
		t.Fatalf("created/updated timestamps wrong: %+v -> %+v", n1, n2)
	}
}

func TestQueryIgnoresStopwordsAndPunctuation(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	_, _ = s.UpsertNode(ctx, Node{Label: "Weather", Type: "topic"})
	_, _ = s.UpsertNode(ctx, Node{Label: "Tea", Type: "drink"})
	nodes, _ := s.Query(ctx, "What is the weather?", 5)
	if len(nodes) != 1 || nodes[0].Label != "Weather" {
		t.Fatalf("expected only Weather, got %+v", nodes)
	}
	if nodes, _ := s.Query(ctx, "the a of", 5); len(nodes) != 0 {
		t.Fatal("stopword-only queries must not match everything")
	}
}

func TestGraphMiddlewareInjectsRelationsAndPreservesFields(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	paris, _ := s.UpsertNode(ctx, Node{Label: "Paris", Type: "city"})
	france, _ := s.UpsertNode(ctx, Node{Label: "France", Type: "country"})
	_, _ = s.UpsertEdge(ctx, Edge{Source: paris, Target: france, Label: "capital_of"})
	var got []byte
	var gotLen int64
	h := NewGraphRAGMiddleware(s).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		gotLen = r.ContentLength
	}))
	body := `{"model":"m","seed":7,"rag_enabled":true,"messages":[{"role":"system","content":"sys"},{"role":"user","content":"Tell me about Paris"}]}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if int64(len(got)) != gotLen {
		t.Fatalf("content length mismatch %d vs %d", gotLen, len(got))
	}
	var parsed struct {
		Seed     int `json:"seed"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Seed != 7 || len(parsed.Messages) != 3 || parsed.Messages[0].Content != "sys" || parsed.Messages[1].Role != "system" {
		t.Fatalf("unexpected rewrite: %s", got)
	}
	if !strings.Contains(parsed.Messages[1].Content, "Paris --capital_of--> France") {
		t.Fatalf("relations missing from graph context: %q", parsed.Messages[1].Content)
	}
}

func TestGraphMiddlewarePassThroughAndStreaming(t *testing.T) {
	s := NewBboltGraphStore()
	_, _ = s.UpsertNode(context.Background(), Node{Label: "weather"})
	mw := NewGraphRAGMiddleware(s)
	mw.MaxBodyBytes = 32
	for name, body := range map[string]string{
		"not opted in": `{"messages":[{"role":"user","content":"weather"}]}`,
		"too large":    `{"rag_enabled":true,"messages":[{"role":"user","content":"weather ` + strings.Repeat("x", 100) + `"}]}`,
		"invalid":      `{"rag_enabled":tru`,
	} {
		var got []byte
		flushed := false
		h := mw.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, _ = io.ReadAll(r.Body)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
				flushed = true
			}
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		if string(got) != body {
			t.Errorf("%s: body changed: %q", name, got)
		}
		if !flushed {
			t.Errorf("%s: response writer must stay a Flusher", name)
		}
	}
}

type fakeLedger struct{ records []ledger.LedgerRecord }

func (f fakeLedger) All(context.Context) ([]ledger.LedgerRecord, error) { return f.records, nil }

func TestAutoOntologyWorkerIngestsLedger(t *testing.T) {
	s := NewBboltGraphStore()
	w := NewAutoOntologyWorker(s, nil)
	if err := w.Run(context.Background(), "not a ledger"); err != ErrUnsupportedLedger {
		t.Fatalf("expected ErrUnsupportedLedger, got %v", err)
	}
	led := fakeLedger{records: []ledger.LedgerRecord{{
		Timestamp:       time.Now(),
		RequestPayload:  `{"model":"m","messages":[{"role":"user","content":"Is Acme Corp based in Berlin? The weather is nice."}]}`,
		ResponsePayload: `{"choices":[{"message":{"role":"assistant","content":"Yes, Acme Corp is headquartered in Berlin."}}]}`,
	}}}
	if err := w.Run(context.Background(), led); err != nil {
		t.Fatal(err)
	}
	nodes, _ := s.Query(context.Background(), "acme", 5)
	if len(nodes) != 1 || nodes[0].Label != "Acme Corp" {
		t.Fatalf("expected Acme Corp entity, got %+v", nodes)
	}
	edges, _ := s.Neighbors(context.Background(), nodes[0].ID, 1)
	if len(edges) != 1 {
		t.Fatalf("expected one deduplicated co-occurrence edge, got %d", len(edges))
	}
	if weather, _ := s.Query(context.Background(), "The", 5); len(weather) != 0 {
		t.Fatal("sentence-initial stopwords must not become entities")
	}
	// Re-running is idempotent and skips already processed records.
	if err := w.Run(context.Background(), led); err != nil {
		t.Fatal(err)
	}
	if len(s.nodes) != 2 || len(s.edges) != 1 {
		t.Fatalf("ingestion not idempotent: %d nodes %d edges", len(s.nodes), len(s.edges))
	}
}

func TestGraphStoreConcurrent(t *testing.T) {
	s := NewBboltGraphStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := NewAutoOntologyWorker(s, nil)
			for j := 0; j < 20; j++ {
				_, _ = w.IngestText(ctx, "Alice met Bob in London. Carol visited Paris.")
				_, _ = s.Query(ctx, "alice", 3)
				_, _ = NewGraphRAGMiddleware(s).BuildContext(ctx, "Where did Alice go?")
			}
		}(i)
	}
	wg.Wait()
}
