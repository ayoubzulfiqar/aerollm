package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/billing"
)

func captureOutput(t *testing.T, args []string) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newRootCmd()
	cmd.SetArgs(args)
	_ = cmd.Execute()

	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	return strings.TrimSpace(buf.String())
}

func TestInitCmd(t *testing.T) {
	dir := t.TempDir()
	absDir, _ := filepath.Abs(dir)
	oldWd, _ := os.Getwd()
	_ = os.Chdir(absDir)
	defer os.Chdir(oldWd)
	output := captureOutput(t, []string{"init", "--dir", absDir})
	if !strings.Contains(output, "created config.yaml, docker-compose.yml, and plugin.go") {
		t.Fatalf("unexpected output: %s", output)
	}
	if _, err := os.Stat(filepath.Join(absDir, "config.yaml")); err != nil {
		t.Fatalf("config.yaml missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(absDir, "docker-compose.yml")); err != nil {
		t.Fatalf("docker-compose.yml missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(absDir, "plugin.go")); err != nil {
		t.Fatalf("plugin.go missing: %v", err)
	}
}

func TestPluginBuildMissingFile(t *testing.T) {
	output := captureOutput(t, []string{"plugin", "build", "missing.go", "-o", "out.wasm"})
	if !strings.Contains(output, "build failed") {
		t.Fatalf("expected build failure output, got: %s", output)
	}
}

func TestPluginPublishMissingKey(t *testing.T) {
	output := captureOutput(t, []string{"plugin", "publish", "plugin.wasm"})
	if !strings.Contains(output, "missing private key") {
		t.Fatalf("expected missing key output, got: %s", output)
	}
}

func TestPluginPublishPreparesManifest(t *testing.T) {
	_ = os.Setenv("AEROLLM_PLUGIN_PRIVATE_KEY", "fake-key")
	defer os.Unsetenv("AEROLLM_PLUGIN_PRIVATE_KEY")
	output := captureOutput(t, []string{"plugin", "publish", "plugin.wasm"})
	if !strings.Contains(output, "prepared manifest") {
		t.Fatalf("expected prepared manifest output, got: %s", output)
	}
}

func TestGitOpsSync(t *testing.T) {
	output := captureOutput(t, []string{"sync"})
	if !strings.Contains(output, "gitops sync triggered") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestBillingGenerate(t *testing.T) {
	output := captureOutput(t, []string{"billing", "generate"})
	if !strings.Contains(output, "generated invoice") {
		t.Fatalf("unexpected output: %s", output)
	}
}

type testEdgeTransport struct {
	base *http.ServeMux
}

func newTestEdgeTransport() *testEdgeTransport {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/marketplace/openstandard/capability/self", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"version":"1.0"}`))
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("/v1/marketplace/openstandard/capability", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var m map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil { http.Error(w, "invalid request", http.StatusBadRequest); return }
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("/v1/marketplace/openstandard/receipt", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var rec map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil { http.Error(w, "invalid request", http.StatusBadRequest); return }
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(rec)
	})
	return &testEdgeTransport{base: mux}
}

func (t *testEdgeTransport) Start() *httptest.Server { return httptest.NewServer(t.base) }

func TestEdgeStatus(t *testing.T) {
	transport := newTestEdgeTransport()
	server := transport.Start()
	defer server.Close()

	_ = os.Setenv("EDGE_LISTEN", strings.TrimPrefix(server.URL, "http://"))
	defer os.Unsetenv("EDGE_LISTEN")
	output := captureOutput(t, []string{"edge", "status"})
	if !strings.Contains(output, `"version":"1.0"`) {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestEdgeCapability(t *testing.T) {
	transport := newTestEdgeTransport()
	server := transport.Start()
	defer server.Close()

	_ = os.Setenv("EDGE_LISTEN", strings.TrimPrefix(server.URL, "http://"))
	defer os.Unsetenv("EDGE_LISTEN")
	output := captureOutput(t, []string{"edge", "capability"})
	if !strings.Contains(output, `"version":"1.0"`) {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestEdgeReceipt(t *testing.T) {
	transport := newTestEdgeTransport()
	server := transport.Start()
	defer server.Close()

	_ = os.Setenv("EDGE_LISTEN", strings.TrimPrefix(server.URL, "http://"))
	defer os.Unsetenv("EDGE_LISTEN")
	output := captureOutput(t, []string{"edge", "receipt", "--customer", "c1", "--event", "token", "--value", "1"})
	if !strings.Contains(output, `"event_name":"token"`) {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestOpenStandardCapability(t *testing.T) {
	cmd := newOpenStandardCmd()
	_, _, _ = cmd.Find([]string{"capability"})
}

func TestServerBillingWorkerEmitsUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping short-mode ticker test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		provider := billing.NewInMemoryProvider()
		gen := billing.NewInvoiceGenerator(provider)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = gen.Generate(ctx, []billing.MeterEntry{{CustomerID: "worker", EventName: "token", Value: 1}})
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
}

func TestPqcKeysListsAlgorithms(t *testing.T) {
	output := captureOutput(t, []string{"pqc", "keys"})
	if !strings.Contains(output, "hybrid-ed25519+mldsa-65") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestSpatialParse(t *testing.T) {
	output := captureOutput(t, []string{"spatial", "parse", "-t", `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`})
	if !strings.Contains(output, `"x"`) {
		t.Fatalf("unexpected output: %s", output)
	}
	if !strings.Contains(output, `"anchors"`) {
		t.Fatalf("expected WebXR translation output, got: %s", output)
	}
}

func TestFederatedList(t *testing.T) {
	output := captureOutput(t, []string{"federated", "list"})
	if !strings.Contains(output, "fedavg") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestFederatedAggregate(t *testing.T) {
	updates := `[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`
	output := captureOutput(t, []string{"federated", "aggregate", "-i", updates})
	if !strings.Contains(output, "aggregated") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestFederatedVerify(t *testing.T) {
	matrix := `{"Rows":1,"Cols":1,"Data":[1],"Owner":"e1"}`
	output := captureOutput(t, []string{"federated", "verify", "-m", matrix, "-s", "abc"})
	if !strings.Contains(output, "error") {
		t.Fatalf("expected signature verification error, got: %s", output)
	}
}

func TestEdgeSpatialStream(t *testing.T) {
	output := captureOutput(t, []string{"edge", "spatial", "stream", "--anchor", `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`})
	if !strings.Contains(output, "parsed anchors=1") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestEdgeFederatedAggregate(t *testing.T) {
	updates := `[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`
	output := captureOutput(t, []string{"edge", "federated", "aggregate", "--input", updates})
	if !strings.Contains(output, "received 2 updates") {
		t.Fatalf("unexpected output: %s", output)
	}
}

func TestTraceMetricsOutput(t *testing.T) {
	output := captureOutput(t, []string{"trace", "metrics"})
	if !strings.Contains(output, "service") {
		t.Fatalf("expected service metric key, got: %s", output)
	}
}

func TestHealthOutput(t *testing.T) {
	output := captureOutput(t, []string{"health"})
	if !strings.Contains(output, "status") {
		t.Fatalf("expected health output, got: %s", output)
	}
}

func TestResilienceOutput(t *testing.T) {
	output := captureOutput(t, []string{"resilience"})
	if !strings.Contains(output, "state") {
		t.Fatalf("expected resilience output, got: %s", output)
	}
}

func TestChaosFaultOutput(t *testing.T) {
	output := captureOutput(t, []string{"chaos", "fault"})
	if !strings.Contains(output, "type") {
		t.Fatalf("expected chaos output, got: %s", output)
	}
}

func TestBackpressureOutput(t *testing.T) {
	output := captureOutput(t, []string{"backpressure"})
	if !strings.Contains(output, "inflight") {
		t.Fatalf("expected backpressure output, got: %s", output)
	}
}

func TestQuotaOutput(t *testing.T) {
	output := captureOutput(t, []string{"quota"})
	if !strings.Contains(output, "Limit") {
		t.Fatalf("expected quota output, got: %s", output)
	}
}

func TestTrafficShadowOutput(t *testing.T) {
	output := captureOutput(t, []string{"traffic", "shadow"})
	if !strings.Contains(output, "shadow") {
		t.Fatalf("expected shadow output, got: %s", output)
	}
}

func TestSloBudgetOutput(t *testing.T) {
	output := captureOutput(t, []string{"slo", "budget"})
	if !strings.Contains(output, "target") {
		t.Fatalf("expected slo output, got: %s", output)
	}
}
