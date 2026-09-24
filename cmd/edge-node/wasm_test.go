package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt/wasmrttest"
)

func manifestCaps(t *testing.T, url string) []string {
	t.Helper()
	resp, body := doReq(t, http.MethodGet, url+"/v1/marketplace/openstandard/capability/self", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("capability/self: %d %s", resp.StatusCode, body)
	}
	var m marketplace.CapabilityManifest
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatal(err)
	}
	return m.Capabilities
}

func TestEdgeCapabilityManifestReflectsWasmRuntime(t *testing.T) {
	rt, err := newWasmRuntime()
	if err != nil {
		t.Fatalf("wasm runtime: %v", err)
	}
	defer rt.Close()
	withRT, srvRT := newTestEdgeRT(t, nil, rt)
	if withRT.wasm == nil || !slices.Contains(manifestCaps(t, srvRT.URL), wasmCapability) {
		t.Fatalf("runtime available: manifest must advertise wasm, got %v", manifestCaps(t, srvRT.URL))
	}
	_, body := doReq(t, http.MethodGet, srvRT.URL+"/v1/edge/capabilities", "", nil)
	if !strings.Contains(body, `"wasm":true`) {
		t.Fatalf("capabilities must report the runtime: %s", body)
	}

	without, srv := newTestEdge(t, nil)
	if without.wasm != nil || slices.Contains(manifestCaps(t, srv.URL), wasmCapability) {
		t.Fatalf("runtime unavailable: manifest must not advertise wasm, got %v", manifestCaps(t, srv.URL))
	}
	if !slices.Contains(manifestCaps(t, srv.URL), "mesh") {
		t.Fatal("other capabilities must stay advertised")
	}
	// An operator-supplied manifest cannot advertise wasm either.
	withWasm := strings.Replace(capabilityBody, `"capabilities":["mesh"]`, `"capabilities":["mesh","wasm"]`, 1)
	if resp, body := doReq(t, http.MethodPut, srv.URL+"/v1/marketplace/openstandard/capability/self", withWasm, nil); resp.StatusCode != http.StatusAccepted || strings.Contains(body, `"wasm"`) {
		t.Fatalf("PUT: %d %s", resp.StatusCode, body)
	}
	if slices.Contains(manifestCaps(t, srv.URL), wasmCapability) {
		t.Fatal("stored manifest advertises wasm without a runtime")
	}
	// Jobs are refused without a runtime.
	resp, _ := doReq(t, http.MethodPost, srv.URL+"/v1/edge/wasm/run", `{"module":"AGFzbQEAAAA="}`, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without runtime, got %d", resp.StatusCode)
	}
}

func wasmJob(module []byte, extra string) string {
	return `{"module":"` + base64.StdEncoding.EncodeToString(module) + `"` + extra + `}`
}

func TestEdgeWasmRunJobs(t *testing.T) {
	rt, err := newWasmRuntime()
	if err != nil {
		t.Fatalf("wasm runtime: %v", err)
	}
	defer rt.Close()
	_, srv := newTestEdgeRT(t, nil, rt)
	run := func(body string) (int, wasmRunResponse, string) {
		t.Helper()
		resp, raw := doReq(t, http.MethodPost, srv.URL+"/v1/edge/wasm/run", body, nil)
		var out wasmRunResponse
		if resp.StatusCode == http.StatusOK {
			if err := json.Unmarshal([]byte(raw), &out); err != nil {
				t.Fatalf("decode %s: %v", raw, err)
			}
		}
		return resp.StatusCode, out, raw
	}

	code, out, raw := run(wasmJob(wasmrttest.Hello("hello edge"), ""))
	if code != http.StatusOK || out.ExitCode != 0 || string(out.Stdout) != "hello edge" {
		t.Fatalf("hello job: %d %+v %s", code, out, raw)
	}
	code, out, raw = run(wasmJob(wasmrttest.Exit(3), ""))
	if code != http.StatusOK || out.ExitCode != 3 {
		t.Fatalf("exit job: %d %+v %s", code, out, raw)
	}
	if code, _, raw := run(wasmJob(wasmrttest.Loop(), `,"timeout_ms":100`)); code != http.StatusUnprocessableEntity || !strings.Contains(raw, "job failed") {
		t.Fatalf("runaway job must be stopped by its timeout: %d %s", code, raw)
	}
	if code, _, raw := run(wasmJob(wasmrttest.Import("env", "evil"), "")); code != http.StatusBadRequest {
		t.Fatalf("module with host imports: %d %s", code, raw)
	}
	for name, body := range map[string]string{
		"not wasm":      wasmJob([]byte("definitely not wasm"), ""),
		"bad timeout":   wasmJob(wasmrttest.Nop(), `,"timeout_ms":999999999`),
		"neg timeout":   wasmJob(wasmrttest.Nop(), `,"timeout_ms":-1`),
		"unknown field": wasmJob(wasmrttest.Nop(), `,"env":{"A":"b"}`),
		"trailing":      wasmJob(wasmrttest.Nop(), "") + "{}",
		"bad base64":    `{"module":"!!!"}`,
		"too much mem":  wasmJob(wasmrttest.Nop(), `,"max_memory_bytes":1099511627776`),
	} {
		if code, _, raw := run(body); code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d %s", name, code, raw)
		}
	}
	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/edge/wasm/run", "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: expected 405, got %d", resp.StatusCode)
	}
}
