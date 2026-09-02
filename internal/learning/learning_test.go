package learning

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

func TestEnqueueAndStatus(t *testing.T) {
	store := ledger.NewInMemoryLedgerStore()
	_ = store.Append(context.Background(), ledger.LedgerRecord{
		RequestPayload:  `{"prompt":"hello"}`,
		ResponsePayload: `{"text":"world"}`,
	})
	trainer := NewTrainer(&flywheel.DatasetExporter{Ledger: store}, store, t.TempDir())
	job, err := trainer.Enqueue(context.Background(), "model-a", "up")
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if job.Model != "model-a" || job.Status != "queued" {
		t.Fatalf("unexpected job: %+v", job)
	}
	got, ok := trainer.Status(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatal("expected job to be retrievable")
	}
}

func TestWriteDataset(t *testing.T) {
	store := ledger.NewInMemoryLedgerStore()
	_ = store.Append(context.Background(), ledger.LedgerRecord{
		RequestPayload:  `{"prompt":"hello"}`,
		ResponsePayload: `{"text":"world"}`,
	})
	trainer := NewTrainer(&flywheel.DatasetExporter{Ledger: store}, store, t.TempDir())
	path, err := trainer.WriteDataset(context.Background(), "up", "ds.jsonl")
	if err != nil {
		t.Fatalf("write dataset failed: %v", err)
	}
	if path == "" {
		t.Fatal("expected dataset path")
	}
}
