package keymanager

import (
	"context"
	"testing"
	"time"
)

// TestGenerateAndValidateKey tests the full key lifecycle: generate, validate, info, delete.
func TestGenerateAndValidateKey(t *testing.T) {
	store := NewInMemoryKeyStore()
	mgr := NewManager(store, "test-master-key")
	ctx := context.Background()

	// Generate a new key.
	req := &GenerateRequest{
		Models:    []string{"gpt-4o", "claude-3-sonnet"},
		Duration:  "24h",
		MaxBudget: 100.0,
		Metadata:  map[string]interface{}{"app": "test"},
		UserID:    "user_123",
		TeamID:    "team_abc",
	}
	resp, err := mgr.Generate(ctx, req)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if resp.Key == "" {
		t.Fatal("expected non-empty key")
	}
	if resp.KeyHash == "" {
		t.Fatal("expected non-empty key hash")
	}
	if !IsVirtualKey(resp.Key) {
		t.Fatal("expected virtual key prefix sk-")
	}

	// Validate the key.
	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if vk.Status != StatusActive {
		t.Fatalf("expected active status, got %s", vk.Status)
	}
	if len(vk.Models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(vk.Models))
	}

	// Info should return the same data.
	info, err := mgr.Info(ctx, resp.KeyHash)
	if err != nil {
		t.Fatalf("Info failed: %v", err)
	}
	if info.MaxBudget != 100.0 {
		t.Fatalf("expected max_budget 100, got %f", info.MaxBudget)
	}

	// Delete (soft delete).
	if err := mgr.Delete(ctx, resp.KeyHash); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// After deletion, validation should fail.
	_, err = mgr.Validate(ctx, resp.Key)
	if err == nil {
		t.Fatal("expected validation error after deletion")
	}
}

// TestKeyExpiry tests that expired keys are rejected.
func TestKeyExpiry(t *testing.T) {
	store := NewInMemoryKeyStore()
	mgr := NewManager(store, "test-master-key")
	ctx := context.Background()

	req := &GenerateRequest{
		Models:   []string{"gpt-4o"},
		Duration: "1ms", // expires almost immediately
	}
	resp, err := mgr.Generate(ctx, req)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Wait for expiry.
	time.Sleep(2 * time.Millisecond)

	_, err = mgr.Validate(ctx, resp.Key)
	if err == nil {
		t.Fatal("expected expiry error")
	}
}
