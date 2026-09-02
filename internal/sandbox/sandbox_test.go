package sandbox

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestWasmExecutorExecuteStub(t *testing.T) {
	e := NewWasmExecutor()
	out, err := e.Execute(context.Background(), "echo", map[string]interface{}{"text": "hi"})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestExecuteAgentToolNilExecutor(t *testing.T) {
	_, err := ExecuteAgentTool(context.Background(), nil, models.ToolDefinition{Name: "echo"}, nil)
	if err == nil {
		t.Fatal("expected error for nil executor")
	}
}
