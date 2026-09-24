package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/sandbox"
	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

const (
	// wasmCapability is advertised in the capability manifest only when the
	// WASM runtime started.
	wasmCapability = "wasm"
	// maxWasmRunBody bounds a /v1/edge/wasm/run request: a base64 module of
	// up to sandbox.MaxWasmModuleBytes, an input of up to the default
	// argument cap (JSON escaping can inflate it up to 6x) and slack for the
	// envelope.
	maxWasmRunBody = int64(sandbox.MaxWasmModuleBytes+2)/3*4 + 6*(256<<10) + 64<<10
	// wasmRunReadTimeout bounds uploading a WASM job (modules can be large).
	wasmRunReadTimeout = 2 * time.Minute
	// maxWasmErrorText bounds guest-controlled text quoted in errors.
	maxWasmErrorText = 512
)

// newWasmRuntime creates the sandboxed WebAssembly runtime for WASM jobs
// (no filesystem, network, host environment, real clock or randomness;
// memory, time, input, output and concurrency caps). Its limits mirror
// sandbox.DefaultLimits; concurrency defaults to GOMAXPROCS.
func newWasmRuntime() (*wasmrt.Runtime, error) {
	limits := sandbox.DefaultLimits()
	return wasmrt.New(wasmrt.Config{
		MaxModuleBytes: sandbox.MaxWasmModuleBytes,
		MaxMemoryPages: uint32(limits.MaxMemoryBytes / wasmrt.PageSize),
		Timeout:        limits.Timeout,
		MaxInputBytes:  limits.MaxArgBytes,
		MaxOutputBytes: limits.MaxOutputBytes,
		QueueTimeout:   limits.QueueTimeout,
	})
}

// wasmRunRequest is the body of POST /v1/edge/wasm/run. Module is the WASI
// preview1 command module, base64-encoded; Input is written to its stdin.
type wasmRunRequest struct {
	Module         []byte `json:"module"`
	Input          string `json:"input,omitempty"`
	TimeoutMS      int64  `json:"timeout_ms,omitempty"`
	MaxMemoryBytes uint64 `json:"max_memory_bytes,omitempty"`
}

// wasmRunResponse reports a finished job. Stdout is base64-encoded (it may
// be binary); a non-zero ExitCode means the guest exited with an error.
type wasmRunResponse struct {
	ExitCode        uint32 `json:"exit_code"`
	Stdout          []byte `json:"stdout"`
	Stderr          string `json:"stderr,omitempty"`
	StderrTruncated bool   `json:"stderr_truncated,omitempty"`
	DurationMS      int64  `json:"duration_ms"`
	PeakMemoryBytes uint64 `json:"peak_memory_bytes,omitempty"`
}

// handleWasmRun runs one WASM job in the sandbox and returns its output.
func (s *edgeServer) handleWasmRun(w http.ResponseWriter, r *http.Request) {
	if s.wasm == nil {
		respondErr(w, "wasm runtime unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Body == nil || r.Body == http.NoBody {
		respondErr(w, "missing request body", http.StatusBadRequest)
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxWasmRunBody)
	defer body.Close()
	var req wasmRunRequest
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			respondErr(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		respondErr(w, "invalid request: "+wasmrt.SanitizeText(err.Error(), maxWasmErrorText), http.StatusBadRequest)
		return
	}
	if _, err := dec.Token(); err != io.EOF {
		respondErr(w, "invalid request: trailing data", http.StatusBadRequest)
		return
	}
	if req.TimeoutMS < 0 || req.TimeoutMS > sandbox.MaxWasmTimeout.Milliseconds() {
		respondErr(w, fmt.Sprintf("invalid timeout_ms: must be between 0 and %d", sandbox.MaxWasmTimeout.Milliseconds()), http.StatusBadRequest)
		return
	}
	payload := sandbox.WasmToolPayload{
		Module:    req.Module,
		Input:     req.Input,
		Timeout:   time.Duration(req.TimeoutMS) * time.Millisecond,
		MaxMemory: req.MaxMemoryBytes,
	}
	if len(payload.Module) > sandbox.MaxWasmModuleBytes {
		respondErr(w, "module too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := payload.Validate(); err != nil {
		respondErr(w, "invalid job: "+err.Error(), http.StatusBadRequest)
		return
	}
	res, err := s.wasm.RunPayload(r.Context(), payload)
	var exitErr *wasmrt.ExitError
	switch {
	case err == nil, errors.As(err, &exitErr):
		// A guest that exits non-zero ran to completion: report its result.
		respondJSON(w, http.StatusOK, wasmRunResponse{
			ExitCode: res.ExitCode,
			Stdout:   append([]byte{}, res.Stdout...),
			// JSON encoding escapes control characters; only invalid UTF-8
			// needs replacing.
			Stderr:          strings.ToValidUTF8(string(res.Stderr), "\uFFFD"),
			StderrTruncated: res.StderrTruncated,
			DurationMS:      res.Duration.Milliseconds(),
			PeakMemoryBytes: res.PeakMemoryBytes,
		})
	default:
		msg, code := wasmRunError(err)
		if code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		respondErr(w, msg, code)
	}
}

// wasmRunError maps a failed run to a client-safe message and status.
func wasmRunError(err error) (string, int) {
	text := wasmrt.SanitizeText(err.Error(), maxWasmErrorText)
	switch {
	case errors.Is(err, wasmrt.ErrInvalidModule), errors.Is(err, wasmrt.ErrInvalidOptions):
		return "invalid job: " + text, http.StatusBadRequest
	case errors.Is(err, wasmrt.ErrModuleTooLarge), errors.Is(err, sandbox.ErrArgumentsTooLarge):
		return "job too large: " + text, http.StatusRequestEntityTooLarge
	case errors.Is(err, sandbox.ErrBusy), errors.Is(err, sandbox.ErrExecutorClosed), errors.Is(err, sandbox.ErrWasmRuntimeUnavailable):
		return "wasm runtime busy or unavailable", http.StatusServiceUnavailable
	case errors.Is(err, sandbox.ErrTimeout), errors.Is(err, wasmrt.ErrMemoryLimit), errors.Is(err, wasmrt.ErrTrap),
		errors.Is(err, sandbox.ErrOutputTooLarge), errors.Is(err, sandbox.ErrToolPanicked):
		return "job failed: " + text, http.StatusUnprocessableEntity
	default:
		return "job failed", http.StatusInternalServerError
	}
}

// servableCapabilities drops advertised capabilities this node cannot serve
// (currently "wasm" when the runtime is unavailable).
func (s *edgeServer) servableCapabilities(in []string) []string {
	if s.wasm != nil || in == nil {
		return in
	}
	out := make([]string, 0, len(in))
	for _, c := range in {
		if c != wasmCapability {
			out = append(out, c)
		}
	}
	return out
}
