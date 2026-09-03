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
- **Advanced Agent Loop**: Hookable execution loop with retry, deficit detection, and tool error recovery

### Phase 3: Autonomous AI Control Plane
- **Graph Orchestrator**: DAG-based execution engine with dependency-aware concurrency
- **MCP Hub**: Native Model Context Protocol server for external tool integration
- **Hybrid RAG**: Dense + keyword retrieval with Reciprocal Rank Fusion
- **Context Manager**: Token counting and auto-summarization for long conversations
- **GitOps**: Git-backed prompt template versioning and delivery
- **Immutable Ledger**: Cryptographic audit chain for request/response integrity
- **WASM Sandbox**: Zero-trust isolated tool execution runtime

### Phase 4: AI-Native Edge Fabric & Data Flywheel
- **Realtime WebSocket**: Bidirectional streaming via `/ws`
  - Text control: `{"type":"ping"}` -> `{"event":"pong"}`, `{"type":"barge-in"}` or `{"action":"cancel"}` cancels generation and returns `{"event":"barge-in"}`
  - Binary frames: non-empty binary WebSocket messages are treated as barge-in/cancel by default
- **Barge-in/VAD**: Cancel provider generation on user voice activity or explicit cancel event
- **Multimodal**: Audio/image preprocessing with transcription/vision hooks
- **Kubernetes Operator**: Control-plane reconciliation via `AeroRoute`/`AeroBudget`/`AeroAgentPipeline`
- **Flywheel**: Feedback ingestion (`POST /v1/feedback`), dataset export, and fine-tuning pipeline

### Phase 5: Cognitive OS & Decentralized Action Fabric
- **Embedded State Store**: bbolt-backed KV with flat vector index for zero-latency agent memory
- **Dynamic Agent Swarms**: Sub-agent spawning with shared hive-mind context and lifecycle orchestration
- **Red-Teaming & Self-Healing**: Adversarial prompt generation from the ledger, automatic guardrail patch proposal and local patch emission

### Phase 6: The Definitive AI Platform
- **Universal Protocol Fabric**: Dynamic provider registry with adapters for Google Gemini, AWS Bedrock, Azure OpenAI, Groq, Cohere, DeepSeek, plus OpenAI-compatible stream normalization
- **Adaptive Intelligence**: Heuristic intent classifier, auto model selector, and multi-armed bandit router with Thompson Sampling
- **Multi-Tenant SaaS Core**: Hierarchical tenant model, tenant middleware, and tenant-scoped service wrappers
- **Plugin Ecosystem**: WASM-compatible plugin interface with lifecycle hooks, in-memory registry, and plugin host
- **Evaluation Engine**: Judge pipeline, regression detector, and benchmark runner for quality assurance
- **Compliance-as-Code**: OPA-backed policy engine with HTTP 451 enforcement middleware

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
11. Synthesis deficit detection + GraphRAG context injection
12. Advanced agent loop with hooks, retry, and tool error recovery
13. AIOps self-optimization
14. Usage recording + webhook dispatch on failure

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
- `internal/contextmgr` — token counting, auto-summarization
- `internal/gitops` — git-backed prompt template polling store
- `internal/ledger` — chained-hash append-only audit log
- `internal/sandbox` — WASM tool execution interface
- `internal/realtime` — WebSocket hub with barge-in support
- `internal/multimodal` — audio/image transcription and vision preprocessing
- `internal/flywheel` — feedback ingestion and dataset export
- `internal/k8s` — lightweight K8s reconciler interfaces
- `internal/state` — embedded bbolt state store with vector index
- `internal/swarm` — dynamic sub-agent orchestration, consensus, federated learning
- `internal/redteam` — adversarial worker and self-healing patch proposal
- `internal/evolution` — self-evolution proposal queue and scoring
- `internal/learning` — autonomous fine-tuning pipeline from flywheel datasets
- `internal/providers/universal` — provider registry and unified adapter/stream normalizer
- `internal/intelligence` — intent classification, model selection, bandit routing
- `internal/tenant` — multi-tenant models and context propagation
- `internal/plugins` — plugin hooks, registry, and WASM host
- `internal/eval` — judge pipeline, regression detection, benchmark runner
- `internal/compliance` — policy engine and HTTP 451 middleware
- `internal/synthesis` — deficit detection, LLM code generation stub, tool promoter
- `internal/graphrag` — temporal graph store, BFS neighbors, token query, GraphRAG middleware
- `internal/aiops` — self-optimizing tuner with metrics source and cooldown actions
- `internal/providers/universal` — universal model registry for capability cards
- `internal/mesh` — CRDT-backed state, peer discovery, gossip/sync workers, and an in-memory transport stub used for local tests
- `internal/marketplace` — signed manifest verification, registry client, micro-royalty tracking
- `internal/economy` — agent wallets, micro-transaction billing for tool calls, and SLA-aware selection
- `internal/zk` — zero-knowledge encrypted payload middleware and confidential compute stubs

## API

### POST /v1/chat/completions
Send an OpenAI-compatible chat completion request.

Example:
```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $AEROLLM_API_KEY" \
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

### POST /v1/feedback
Submit feedback for the data flywheel.

```bash
curl -X POST http://localhost:8080/v1/feedback \
  -H "Content-Type: application/json" \
  -d '{"request_id":"req-123","score":0.9,"metadata":{"model":"gpt-4"}}'
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

## Phases

### Phase 1: Foundation
Multi-provider routing, agentic tool execution, Redis caching, rate limiting, and OpenTelemetry observability.

### Phase 2: Enterprise Extensions
Guardrails, FinOps, HITL approvals, memory, shadow traffic, and webhooks.

### Phase 3: Autonomous AI Control Plane
DAG orchestration, MCP Hub, Hybrid RAG, context manager, GitOps, immutable ledger, and WASM sandbox.

### Phase 4: AI-Native Edge Fabric & Data Flywheel
Realtime WebSocket, barge-in, multimodal preprocessing, K8s operator, flywheel feedback, and dataset export.

### Phase 5: Cognitive OS & Decentralized Action Fabric
Embedded state store, dynamic agent swarms, consensus, federated learning, red-team self-healing, and autonomous evolution.

### Phase 6: The Definitive AI Platform
Universal protocol fabric, adaptive intelligence, multi-tenant SaaS core, plugin ecosystem, evaluation engine, and compliance-as-code.

### Phase 7: The Sentient Mesh
- **Generative API Synthesis**: `internal/synthesis` with tool deficit detection, LLM-backed code generation, WASM placeholder compilation, and tool promotion into the plugin registry
- **Temporal GraphRAG**: `internal/graphrag` with temporal nodes/edges, BFS neighbor traversal, tokenized query ranking, and HTTP middleware that injects graph context when `rag_enabled=true`
- **Self-Optimizing AIOps**: `internal/aiops` with `MetaAgentTuner`, configurable `MetricsSource`, and tuner actions with cooldown semantics; wired to live telemetry in `cmd/server/main.go`
- **Advanced Agent Loop**: `internal/agent/advanced_loop.go` adds `LoopHook` lifecycle hooks, `ExecuteToolsWithRetry`, unknown-tool deficit detection, and `ToolDeficitHandler`
- **Universal Model Registry**: `internal/providers/universal/model_registry.go` for model capability cards, provider-indexed lookup, and registration validation

### Phase 8: The Global Cognitive Mesh & Zero-Knowledge Fabric
- **Global Edge Mesh**: `internal/mesh` with CRDT-backed state, peer discovery, and gossip sync for bandit weights and plugin registries
- **Marketplace & Royalties**: `internal/marketplace` with signed manifest verification, registry client, and micro-royalty tracking via webhook dispatch
- **Zero-Knowledge Guardrails**: `internal/zk` with `ConfidentialCompute` middleware stubs for encrypted payload handling

### Phase 9: Visual Control Plane, Developer CLI & Agent Economy
- **AeroLLM Studio Backend**: `internal/studio` exposes `/v1/studio/topology`, `/v1/studio/analytics/cost`, and `/v1/studio/dags` under existing licensing middleware
- **Developer CLI**: `cmd/cli` uses Cobra; `aerollm init` scaffolds `config.yaml`, `docker-compose.yml`, and `plugin.go`
- **Plugin Build/Publish**: `aerollm plugin build` compiles Go/WASM plugins; `aerollm plugin publish` prepares signed marketplace manifests and POSTs to `/v1/marketplace/plugins`
- **Marketplace Registry API**: `internal/marketplace/registry_api.go` adds durable registry store interfaces plus HTTP handlers for `/v1/marketplace/plugins` list/publish and `/v1/marketplace/plugins/{id}` get
- **Redis Registry Persistence**: `internal/marketplace/redis_store.go` adds a `RedisStore` backed by `go-redis/v9`; server uses Redis for registry state when available, otherwise falls back to in-memory store
- **Marketplace Auth Middleware**: `/v1/marketplace/*` routes now require API key auth via existing middleware; `internal/marketplace/middleware.go` provides reusable marketplace requester middleware
- **Local Redis Compose**: `docker-compose.yml` defines a local Redis service with healthchecks and AeroLLM wiring
- **Agent Economy**: `internal/economy` provides wallet interfaces, in-memory ledger store, tool-call billing interceptor, and SLA-aware selector extension in `internal/intelligence`
- **Generative UI Streaming**: `internal/genui` intercepts `aerollm_ui` JSON schemas in LLM output and normalizes them into SSE-friendly UI events; chat responses now opt into GenUI when requested

### Phase 10: The Edge Companion, Hardware Fabric & Open Standard
- **Edge Companion**: `cmd/edge-node` is a local-first binary using bbolt for offline state, in-memory mesh discovery/transport, hardware detection, wallet initialization, and WASM sandbox execution
- **Hardware Routing**: `internal/hardware` adds silicon detection for CUDA/Metal/ROCm/Vulkan/Ollama/CPU and `HardwareAwareSelector` for privacy-first/cost-zero routing
- **SaaS Billing**: `internal/billing` adds `InMemoryProvider` + `StripeProvider` using `stripe-go/v80` `billing/meterevent`, plus `InvoiceGenerator` and CLI `aerollm billing generate`
- **Server Billing Worker**: `cmd/server/main.go` starts a background invoice worker, using Stripe when `AEROLLM_STRIPE_SECRET_KEY` is set
- **Open Standard Spec**: `internal/marketplace/openstandard.go` defines `CapabilityManifest` and `BillingReceipt` structures, with validation and canonical JSON; server + edge expose `/v1/marketplace/openstandard/capability` and `/v1/marketplace/openstandard/receipt`, plus edge self endpoints at `/v1/marketplace/openstandard/capability/self` and `/v1/marketplace/openstandard/receipt/self`
- **Edge CLI**: `aerollm edge status/capability/receipt` commands interact with local edge-node endpoints; set `EDGE_LISTEN` if edge runs on a different host/port

### Phase 11: The Spatial Reality, Autonomous Cloud & Post-Quantum Fabric
- **Spatial Reality Fabric**: `internal/spatial` adds chunked video/3D streaming via zero-buffer HTTP chunk writers, plus a WebXR spatial translator that scans LLM outputs for spatial anchors and converts them into standardized AR/VR payloads
- **Post-Quantum Cryptography**: `internal/pqc` adds `QuantumSafeKeyManager` with ML-KEM-768 encapsulation and ML-DSA-65 signatures, plus hybrid Ed25519/ML-DSA fallback for mesh peer attestation; includes stream encryptors for secure weight/channel transport
- **Integration Points**: PQC key management is additive to existing `internal/ledger` and `internal/marketplace` flows; spatial middleware can wrap existing provider handlers without changing upstream routing

### Open Standard Examples

```bash
curl -X POST http://localhost:7910/v1/marketplace/openstandard/capability \
  -H "Content-Type: application/json" \
  -d '{"version":"1.0","hardware":{"has_local_gpu":true,"os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh","wasm"]}'
```

```bash
curl -X POST http://localhost:7910/v1/marketplace/openstandard/receipt \
  -H "Content-Type: application/json" \
  -d '{"receipt_id":"r-1","customer_id":"c1","provider_id":"p1","event_name":"token","value":1,"currency":"USD"}'
```

## FinOps

- Budget checks happen before routing.
- If `BudgetChecker.CheckBudget` returns an error, the handler returns `HTTP 402` with body `{"error":"budget exceeded"}`.
- Set `AEROLLM_BUDGET_WEBHOOK_URL` and `AEROLLM_BUDGET_WEBHOOK_SECRET` to receive `budget_exceeded` events.

## Guardrails

- Injection shield returns `HTTP 403`.
- PII redaction rewrites the request body with placeholders; original body is preserved in request context for downstream handlers that need restoration.
- API key scoping enforces `AllowedModels` and `IPAllowlist`.
