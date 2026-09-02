# AeroLLM

AeroLLM is a high-performance, intelligent LLM routing and proxy server written in Go. It provides intelligent routing between multiple LLM providers, agentic tool execution, Redis caching, rate limiting, and OpenTelemetry observability.

## Features

### Core Features
- **Multi-Provider Routing**: Route requests to OpenAI, Anthropic, or local providers
- **Intelligent Routing Strategies**: Round-robin, latency-based, cost-based, and fallback routing
- **Circuit Breaker**: Automatic failover with circuit breaker pattern
- **Agentic Tool Execution**: Built-in agent engine for tool use and multi-step reasoning
- **Redis Caching**: Exact-match and semantic caching for responses
- **Rate Limiting**: Token bucket-based rate limiting per API key
- **OpenTelemetry**: Distributed tracing with OTLP exporter support
- **Structured Logging**: JSON-formatted structured logging
- **Graceful Shutdown**: Proper signal handling and resource cleanup

### Enterprise Extensions
- **Guardrails**: PII redaction, prompt injection shield, API key scoping
- **FinOps**: Per-model cost tracking, budget enforcement, and `budget_exceeded` webhooks
- **Advanced Agent**: HITL approval flows with Redis-backed state and `POST /v1/agents/approvals/{id}`
- **Memory**: Short-term message memory plus long-term vector memory interfaces
- **Shadow Traffic**: Async shadow routing for provider comparison
- **Webhook Dispatcher**: Async retry-capable webhook delivery with exponential backoff

### Phase 3: Autonomous AI Control Plane
- **Graph Orchestrator**: DAG-based execution engine with dependency-aware concurrency
- **MCP Hub**: Native Model Context Protocol server for external tool integration
- **Hybrid RAG**: Dense + keyword retrieval with Reciprocal Rank Fusion
- **Context Manager**: Token counting and auto-summarization for long conversations
- **GitOps**: Git-backed prompt template versioning and delivery
- **Immutable Ledger**: Cryptographic audit chain for request/response integrity
- **WASM Sandbox**: Zero-trust isolated tool execution runtime

### Provider Support
- **OpenAI**: GPT-4, GPT-3.5-turbo, and compatible APIs
- **Anthropic**: Claude 3 models (Sonnet, Opus, Haiku)
- **Local**: Local LLM providers (Ollama, llama.cpp)

## Quick Start

### Prerequisites
- Go 1.22+
- Redis 7+ (optional, for caching/state)
- Docker and Docker Compose (optional)

### Installation

```bash
git clone https://github.com/ayoubzulfiqar/aerollm.git
cd aerollm
go mod download
go build -o aerollm ./cmd/server
```

### Configuration

AeroLLM uses Viper for configuration via `config.yaml` or environment variables with `AEROLLM_` prefix.

Key env vars:
- `AEROLLM_WEBHOOK_URL`
- `AEROLLM_WEBHOOK_SECRET`
- `AEROLLM_BUDGET_WEBHOOK_URL`
- `AEROLLM_BUDGET_WEBHOOK_SECRET`

### Running

```bash
./aerollm
```

### Docker

```bash
docker-compose up -d
```

## Architecture

Request path for `/v1/chat/completions`:

1. Recovery
2. Logging
3. Authentication
4. Rate limiting
5. Injection shield
6. PII redaction
7. API key scoping
8. Budget pre-check
9. Exact-match cache lookup
10. Provider routing
11. Agent tool execution loop
12. Usage recording + webhook dispatch on failure

Key packages:
- `internal/middleware` — HTTP middleware primitives
- `internal/guardrails` — PII, injection shield, API key scoping
- `internal/finops` — cost tracking and budget enforcement
- `internal/traffic` — shadow testing
- `internal/webhooks` — async webhook dispatch with retry/backoff
- `internal/agent` — agent engine, memory, approvals
- `internal/router` — round-robin, latency, cost, fallback + circuit breaker
- `internal/orchestrator` — DAG execution with `errgroup` concurrency
- `internal/mcp` — Model Context Protocol HTTP/SSE server
- `internal/rag` — hybrid retrieval, RRF fusion, context injection
- `internal/context` — token counting, auto-summarization
- `internal/gitops` — git-backed prompt template polling store
- `internal/ledger` — chained-hash append-only audit log
- `internal/sandbox` — WASM tool execution interface

## API

### POST /v1/chat/completions
Send an OpenAI-compatible chat completion request.

Example:
```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-dev-1" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}'
```

### GET /health
Liveness probe.

### GET /ready
Readiness probe.

### POST /v1/agents/approvals/{id}
Resume a paused HITL approval flow.

```bash
curl -X POST http://localhost:8080/v1/agents/approvals/approval-123 \
  -H "Content-Type: application/json" \
  -d '{"approved":true}'
```

### MCP Server
AeroLLM exposes a native MCP endpoint at `/mcp`.

Supported methods:
- `POST /mcp` with JSON-RPC `initialize`
- `POST /mcp` with JSON-RPC `tools/list`
- `POST /mcp` with JSON-RPC `tools/call`
- `GET /mcp` for SSE event stream

Example JSON-RPC calls:

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
```

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## Middleware Chain

`/v1/chat/completions` is composed as:

1. Recovery
2. Logging
3. Authentication
4. Rate limiting
5. Injection shield
6. PII redaction
7. API key scoping
8. Budget pre-check
9. Exact-match cache lookup
10. Provider routing
11. Agent tool execution loop
12. Usage recording + webhook dispatch on failure

## FinOps

- Budget checks happen before routing.
- If `BudgetChecker.CheckBudget` returns an error, the handler returns `HTTP 402` with body `{"error":"budget exceeded"}`.
- Set `AEROLLM_BUDGET_WEBHOOK_URL` and `AEROLLM_BUDGET_WEBHOOK_SECRET` to receive `budget_exceeded` events.

## Guardrails

- Injection shield returns `HTTP 403`.
- PII redaction rewrites the request body with placeholders; original body is preserved in request context for downstream handlers that need restoration.
- API key scoping enforces `AllowedModels` and `IPAllowlist`.

### Phase 4: AI-Native Edge Fabric & Data Flywheel
- **Realtime WebSocket**: Bidirectional streaming via `/ws`
  - Text control: `{"type":"ping"}` -> `{"event":"pong"}`, `{"type":"barge-in"}` or `{"action":"cancel"}` cancels generation and returns `{"event":"barge-in"}`
  - Binary frames: non-empty binary WebSocket messages are treated as barge-in/cancel by default
- **Barge-in/VAD**: Cancel provider generation on user voice activity or explicit cancel event
- **Multimodal**: Audio/image preprocessing with transcription/vision hooks
- **Kubernetes Operator**: Control-plane reconciliation via `AeroRoute`/`AeroBudget`/`AeroAgentPipeline`
- **Flywheel**: Feedback ingestion (`POST /v1/feedback`), dataset export, and fine-tuning pipeline
