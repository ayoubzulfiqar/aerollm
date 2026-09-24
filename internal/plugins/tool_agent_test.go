package plugins_test

import (
	"context"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

// WasmTool must satisfy the agent tool contract without plugins importing
// the agent package.
var _ agent.Tool = (*plugins.WasmTool)(nil)

func TestWasmToolRegistersWithAgent(t *testing.T) {
	rt, err := wasmrt.New(wasmrt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	tool, err := plugins.NewWasmTool("answer", "Returns the answer", nil, wasmrttest.Hello(`{"answer":42}`), rt)
	if err != nil {
		t.Fatal(err)
	}
	reg := agent.NewToolRegistry()
	if err := reg.Register(tool); err != nil {
		t.Fatal(err)
	}
	out, err := reg.Execute(context.Background(), "answer", `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if out.(map[string]interface{})["answer"] != float64(42) {
		t.Fatalf("unexpected output %v", out)
	}
	if defs := reg.Definitions(); len(defs) != 1 || defs[0].Name != "answer" {
		t.Fatalf("unexpected definitions %+v", defs)
	}
}
