# Performance Benchmark & Profiling Suite

This directory contains the benchmark and profiling tools for the AeroLLM Go backend.

## Contents

| File | Description |
|------|-------------|
| `load_test.js` | k6 load testing script targeting `/v1/chat/completions` with tool-calling payloads |
| `run.sh` | Automation script: build, start, load test, profile, flame graph |
| `README.md` | This file |

## Prerequisites

1. **Go** (1.26+): For building the server binary and running `go tool pprof`.
2. **k6**: Install from https://k6.io/docs/getting-started/installation
   - Linux: `sudo apt install k6` or use the official package repository
   - macOS: `brew install k6`
3. **FlameGraph** (optional): For SVG flame graph generation
   - Clone: `git clone https://github.com/brendangregg/FlameGraph /opt/FlameGraph`
   - Or set `FLAMEGRAPH_DIR` env var to your local clone path

## Quick Start

### Full automated run (build + test + profile):

```bash
./benchmark/run.sh
```

### Test-only mode (server already running):

```bash
./benchmark/run.sh --test-only
```

### Skip flame graph generation:

```bash
./benchmark/run.sh --no-flamegraph
```

### Custom API URL or key:

```bash
AEROLLM_API_URL=http://localhost:8080 AEROLLM_API_KEY=sk-demo ./benchmark/run.sh
```

## What the load test covers

The `load_test.js` k6 script targets the `/v1/chat/completions` endpoint with:

- A payload that includes a `tools` array (two function tools: `get_weather` and `get_current_time`)
- This forces the **Advanced Agent Loop** execution path in the handler:
  1. Request enters the middleware chain (auth → rate limit → PII redaction → injection shielding)
  2. Router selects a provider (round-robin by default)
  3. Agent engine's `RunToolExecutionLoop` calls `CallLLM` → receives tool_calls → executes tools → feeds results back → loops
- Simulates realistic concurrent traffic: ramp up to 200 VUs, sustain for 1 minute, ramp down

### Thresholds

| Metric | Threshold |
|--------|-----------|
| `http_req_duration` p(95) | < 150ms |
| `http_req_failed` rate | < 1% |
| `chat_latency` p(99) | < 200ms |
| `chat_success_rate` rate | > 99% |

## Profiling

### pprof

The server starts a dedicated pprof HTTP server on `localhost:6060` (separate from the main `:8080` gateway). Available endpoints:

- `http://localhost:6060/debug/pprof/` — Index page
- `http://localhost:6060/debug/pprof/profile` — CPU profile (30s default)
- `http://localhost:6060/debug/pprof/heap` — Heap allocation profile
- `http://localhost:6060/debug/pprof/trace` — Execution trace
- `http://localhost:6060/debug/pprof/goroutine` — Goroutine blocking profile

### Analyzing profiles

```bash
# View top CPU-consuming functions
go tool pprof -top profiles/cpu_profile.prof

# View top heap allocations
go tool pprof -top profiles/heap_profile.prof

# Start interactive web UI
go tool pprof -http=:8081 profiles/cpu_profile.prof

# Generate text report with call graph
go tool pprof -list main profiles/cpu_profile.prof

# Generate flame graph (requires FlameGraph)
go tool pprof -raw -output profiles/cpu_raw.txt ./aerollm profiles/cpu_profile.prof
/opt/FlameGraph/stackcollapse-go.pl profiles/cpu_raw.txt \
    | /opt/FlameGraph/flamegraph.pl --title="AeroLLM CPU Flame Graph" \
    > profiles/cpu_flamegraph.svg
```

## Project Goals

This benchmark suite is designed to prove:

1. **Sub-2ms overhead**: Aerollm's middleware chain adds minimal latency compared to routing directly to backend providers.
2. **High concurrency**: The server can handle 200+ concurrent VUs with < 150ms p95 latency.
3. **Agent loop efficiency**: The Advanced Agent Loop (tool calling + iteration) doesn't add excessive overhead compared to Python agent frameworks.
