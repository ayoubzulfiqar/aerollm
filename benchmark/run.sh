#!/usr/bin/env bash
#
# benchmark/run.sh
#
# Automation script for the AeroLLM Performance Benchmark & Profiling Suite.
#
# This script:
#   1. Builds the Go binary from source.
#   2. Starts the server in the background (with pprof on :6060).
#   3. Runs the k6 load test against /v1/chat/completions.
#   4. Collects CPU and Memory profile samples.
#   5. Generates flame graphs from the collected profiles.
#
# Prerequisites:
#   - Go 1.26+
#   - k6 (https://k6.io/docs/getting-started/installation)
#   - go tool pprof (bundled with Go)
#   - FlameGraph: https://github.com/brendangregg/FlameGraph (optional, for
#     SVG flame graph output)
#
# Usage:
#   ./benchmark/run.sh           # Full run: build, start, test, profile, graph
#   ./benchmark/run.sh --test-only  # Skip build/start, just run k6
#   ./benchmark/run.sh --no-flamegraph  # Skip flame graph generation
#
# Environment variables:
#   AEROLLM_API_URL  - API URL (default: http://localhost:8080)
#   AEROLLM_API_KEY  - API key (default: sk-demo)
#   AEROLLM_BINARY   - Path to pre-built binary (default: ./aerollm)
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${AEROLLM_BINARY:-$PROJECT_ROOT/aerollm}"
BINARY_NAME="$(basename "$BINARY")"

# pprof endpoints
PPROF_HOST="localhost"
PPROF_PORT="6060"
PPROF_URL="http://${PPROF_HOST}:${PPROF_PORT}"

# API endpoint
API_URL="${AEROLLM_API_URL:-http://localhost:8080}"
API_KEY="${AEROLLM_API_KEY:-sk-demo}"

# Profile output directory
PROFILE_DIR="$PROJECT_ROOT/profiles"
mkdir -p "$PROFILE_DIR"

# FlameGraph path (optional)
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/opt/FlameGraph}"

# k6 script path
K6_SCRIPT="$SCRIPT_DIR/load_test.js"

# Build flags
GO_BUILD_FLAGS="-ldflags=-s -ldflags=-w"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

log() {
    echo -e "\033[1;36m[benchmark]\033[0m $*"
}

log_error() {
    echo -e "\033[1;31m[benchmark:ERROR]\033[0m $*" >&2
}

cleanup() {
    local exit_code=$?
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log "Stopping server (PID: $SERVER_PID)..."
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        log "Server stopped."
    fi
    exit $exit_code
}

trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------

TEST_ONLY=false
NO_FLAMEGRAPH=false

for arg in "$@"; do
    case $arg in
        --test-only)
            TEST_ONLY=true
            shift
            ;;
        --no-flamegraph)
            NO_FLAMEGRAPH=true
            shift
            ;;
        *)
            log_error "Unknown argument: $arg"
            echo "Usage: $0 [--test-only] [--no-flamegraph]"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Step 1: Build the Go binary
# ---------------------------------------------------------------------------

if [ "$TEST_ONLY" = false ]; then
    log "Step 1: Building the Go binary..."
    cd "$PROJECT_ROOT"
    go build -o "$BINARY" ./cmd/server/
    log "Binary built: $BINARY"
else
    log "Skipping build (--test-only mode)"
fi

# ---------------------------------------------------------------------------
# Step 2: Start the server
# ---------------------------------------------------------------------------

if [ "$TEST_ONLY" = false ]; then
    log "Step 2: Starting the AeroLLM server..."
    cd "$PROJECT_ROOT"

    # Start the server in the background.
    # The server prints "server starting on port 8080" and
    # "pprof listening on :6060" when ready.
    "$BINARY" > "$PROFILE_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    log "Server started (PID: $SERVER_PID)"

    # Wait for the server to be ready (up to 30 seconds).
    log "Waiting for server to be ready..."
    for i in $(seq 1 60); do
        if curl -sf "${API_URL}/healthz" > /dev/null 2>&1; then
            log "Server is healthy at ${API_URL}"
            break
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            log_error "Server process died unexpectedly."
            log_error "Server log:"
            cat "$PROFILE_DIR/server.log"
            exit 1
        fi
        sleep 0.5
    done

    # Verify readiness.
    if ! curl -sf "${API_URL}/healthz" > /dev/null 2>&1; then
        log_error "Server did not become healthy within the timeout."
        log_error "Server log:"
        cat "$PROFILE_DIR/server.log"
        exit 1
    fi

    # Verify pprof is available.
    if curl -sf "${PPROF_URL}/debug/pprof/" > /dev/null 2>&1; then
        log "pprof is available at ${PPROF_URL}/debug/pprof/"
    else
        log "Warning: pprof endpoint not reachable at ${PPROF_URL}"
    fi

    log "Server is ready. Starting load test..."
    # Give the server a moment to settle.
    sleep 2
else
    log "Test-only mode: assuming server is already running at ${API_URL}"
fi

# ---------------------------------------------------------------------------
# Step 3: Run the k6 load test
# ---------------------------------------------------------------------------

log "Step 3: Running k6 load test..."

# Pass environment variables to k6.
export AEROLLM_API_URL="$API_URL"
export AEROLLM_API_KEY="$API_KEY"

# Count CPU profiles: take a baseline and after the test.
log "Collecting baseline CPU profile..."
curl -s "${PPROF_URL}/debug/pprof/profile?seconds=15" -o "$PROFILE_DIR/cpu_baseline.prof" 2>/dev/null || true

# Run the k6 test.
# k6 runs synchronously and exits when the test completes.
if command -v k6 &> /dev/null; then
    log "k6 found. Running load test: k6 run $K6_SCRIPT"
    k6 run "$K6_SCRIPT"
    K6_STATUS=$?
else
    log_error "k6 is not installed. Please install it from https://k6.io"
    log_error "Attempting to continue with profiling only..."
    K6_STATUS=0
fi

# Collect post-test CPU profile.
log "Collecting post-test CPU profile..."
curl -s "${PPROF_URL}/debug/pprof/profile?seconds=15" -o "$PROFILE_DIR/cpu_profile.prof" 2>/dev/null || true

# Collect heap profile.
log "Collecting heap profile..."
curl -s "${PPROF_URL}/debug/pprof/heap" -o "$PROFILE_DIR/heap_profile.prof" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Step 4: Generate flame graphs (optional)
# ---------------------------------------------------------------------------

if [ "$NO_FLAMEGRAPH" = false ] && [ -d "$FLAMEGRAPH_DIR" ]; then
    log "Step 4: Generating flame graphs..."

    if [ -f "$PROFILE_DIR/cpu_profile.prof" ]; then
        log "Generating CPU flame graph..."

        # Convert pprof binary to folded stack format.
        go tool pprof -raw -output "$PROFILE_DIR/cpu_raw.txt" "$BINARY" "$PROFILE_DIR/cpu_profile.prof" 2>/dev/null || true

        if [ -f "$PROFILE_DIR/cpu_raw.txt" ]; then
            "$FLAMEGRAPH_DIR/stackcollapse-go.pl" "$PROFILE_DIR/cpu_raw.txt" \
                | "$FLAMEGRAPH_DIR/flamegraph.pl" --title="AeroLLM CPU Flame Graph" \
                > "$PROFILE_DIR/cpu_flamegraph.svg" 2>/dev/null || true
            log "CPU flame graph: $PROFILE_DIR/cpu_flamegraph.svg"
        fi
    fi

    if [ -f "$PROFILE_DIR/heap_profile.prof" ]; then
        log "Generating memory flame graph..."

        go tool pprof -raw -output "$PROFILE_DIR/heap_raw.txt" "$BINARY" "$PROFILE_DIR/heap_profile.prof" 2>/dev/null || true

        if [ -f "$PROFILE_DIR/heap_raw.txt" ]; then
            "$FLAMEGRAPH_DIR/stackcollapse-go.pl" "$PROFILE_DIR/heap_raw.txt" \
                | "$FLAMEGRAPH_DIR/flamegraph.pl" --title="AeroLLM Memory Flame Graph" \
                > "$PROFILE_DIR/heap_flamegraph.svg" 2>/dev/null || true
            log "Memory flame graph: $PROFILE_DIR/heap_flamegraph.svg"
        fi
    fi

    log "Flame graphs generated in: $PROFILE_DIR"
else
    if [ "$NO_FLAMEGRAPH" = true ]; then
        log "Skipping flame graph generation (--no-flamegraph)"
    else
        log "Skipping flame graph generation (FlameGraph not found at $FLAMEGRAPH_DIR)"
        log "To enable: install https://github.com/brendangregg/FlameGraph and set FLAMEGRAPH_DIR"
    fi
fi

# ---------------------------------------------------------------------------
# Step 5: Summary
# ---------------------------------------------------------------------------

log "Benchmark complete!"
log ""
log "=== Results Summary ==="
log "Profiles saved to:     $PROFILE_DIR/"
log "  CPU profile:          $PROFILE_DIR/cpu_profile.prof"
log "  Heap profile:         $PROFILE_DIR/heap_profile.prof"
log "  Server log:           $PROFILE_DIR/server.log"
if [ -f "$PROFILE_DIR/cpu_flamegraph.svg" ]; then
    log "  CPU flame graph:      $PROFILE_DIR/cpu_flamegraph.svg"
fi
if [ -f "$PROFILE_DIR/heap_flamegraph.svg" ]; then
    log "  Memory flame graph:   $PROFILE_DIR/heap_flamegraph.svg"
fi
log ""
log "=== Analysis Commands ==="
log "View CPU top functions:"
log "  go tool pprof -top $PROFILE_DIR/cpu_profile.prof"
log ""
log "View heap top allocations:"
log "  go tool pprof -top $PROFILE_DIR/heap_profile.prof"
log ""
log "Start interactive pprof UI:"
log "  go tool pprof -http=:8081 $PROFILE_DIR/cpu_profile.prof"
log ""

if [ $K6_STATUS -ne 0 ]; then
    log_error "k6 test exited with non-zero status: $K6_STATUS"
    exit $K6_STATUS
fi

exit 0
