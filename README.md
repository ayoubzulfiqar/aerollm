# AeroLLM

![Logo](assets/aero.png)

AeroLLM is a high-performance, intelligent LLM routing and proxy server written in Go. It provides intelligent routing between multiple LLM providers, agentic tool execution, Redis caching, rate limiting, and OpenTelemetry observability.

## Features

### Core
- **Multi-Provider Routing**: OpenAI, Anthropic, Google Gemini, AWS Bedrock, Azure OpenAI, Groq, Cohere, DeepSeek, and OpenAI-compatible providers
- **Routing Strategies**: Round-robin, latency-based, cost-based, fallback, and circuit breaker
- **Agentic Tool Execution**: Built-in agent engine for tool use and multi-step reasoning
- **Caching**: Exact-match and semantic caching with Redis
- **Rate Limiting**: Token bucket-based rate limiting per API key
- **Observability**: OpenTelemetry distributed tracing with OTLP exporter
- **Structured Logging**: JSON-formatted structured logging
- **Graceful Shutdown**: Proper signal handling and resource cleanup

### Enterprise & Security
- **Guardrails**: PII redaction, prompt injection shield, API key scoping
- **FinOps**: Per-model cost tracking, budget enforcement, and webhook alerts
- **HITL Approvals**: Human-in-the-loop approval flows with Redis-backed state
- **Memory**: Short-term message memory plus long-term vector memory interfaces
- **Shadow Traffic**: Async shadow routing for provider comparison
- **Webhooks**: Async retry-capable webhook delivery with exponential backoff

### Advanced
|- **Graph Orchestrator**: DAG-based execution engine with dependency-aware concurrency
|- **MCP Hub**: Native Model Context Protocol server for external tool integration
|- **Hybrid RAG**: Dense + keyword retrieval with Reciprocal Rank Fusion
|- **Context Manager**: Token counting and auto-summarization for long conversations
|- **GitOps**: Git-backed prompt template versioning and delivery
|- **Immutable Ledger**: Cryptographic audit chain for request/response integrity
|- **WASM Sandbox**: Zero-trust isolated tool execution runtime
|- **Realtime**: Bidirectional WebSocket streaming with barge-in support
|- **Multimodal**: Audio/image preprocessing with transcription/vision hooks
|- **Kubernetes Operator**: Control-plane reconciliation for routes, budgets, and agent pipelines
|- **Flywheel**: Feedback ingestion, dataset export, and fine-tuning pipeline
|- **Embedded State**: bbolt-backed KV with flat vector index for zero-latency agent memory
|- **Agent Swarms**: Dynamic sub-agent spawning with shared hive-mind context
|- **Red-Teaming**: Adversarial prompt generation and self-healing patch proposal
|- **Evaluation Engine**: Judge pipeline, regression detector, and benchmark runner
|- **Compliance-as-Code**: Policy engine with HTTP 451 enforcement
|- **Multi-Tenant**: Hierarchical tenant model with tenant-scoped service wrappers
|- **Plugins**: WASM-compatible plugin interface with lifecycle hooks and registry
|- **Marketplace**: Signed manifest verification, registry client, micro-royalty tracking
|- **Post-Quantum Crypto**: ML-KEM/ML-DSA hybrid key management and stream encryptors
|- **Spatial Fabric**: Chunked video/3D streaming and WebXR spatial translation
|- **Federated Learning**: FedAvg aggregation for secure LoRA weight averaging
|- **Edge Companion**: Local-first binary with bbolt, hardware detection, and WASM sandbox
|- **Policy Engine**: HTTP policy evaluation with rule-based access control
|- **Data Retention**: TTL and max-items retention policies
|- **Incident Management**: Incident lifecycle with severity and status tracking
|- **Notifications**: Multi-channel notification routing (webhook, email, Slack, SMS)
|- **Scheduled Tasks**: Cron, interval, and onetime automation tasks
|- **Secrets Management**: In-memory secret storage with metadata
|- **Multi-Region**: Region-aware routing and data residency controls

### Enterprise & Migration
|- **Virtual Keys & Agencies**: Per-key rate limits, budget tracking, and agency-level RBAC
|- **LiteLLM Migration CLI**: Convert LiteLLM `litellm_config.yaml` to AeroLLM `config.yaml`
|- **OpenAI-Compatible Batch API**: Asynchronous batch processing for large-scale LLM workloads
|- **OpenAPI/Swagger Docs**: Auto-generated API spec with interactive Swagger UI
|- **Standardized Rate Limit Headers**: OpenAI-compatible `X-RateLimit-*` headers for SDK backpressure
|- **Cache Management APIs**: Inspect, stats, and clear endpoints for DevOps (admin-auth protected)
|- **Advanced Provider Features**: OpenAI structured outputs (JSON schema) and Anthropic prompt caching

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

### Running

```bash
./aerollm
```

### Docker

```bash
docker-compose up -d
```

## Usage Guides

### Chat Completions

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}'
```

### Health Checks

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

### Resilience Status

```bash
curl http://localhost:8080/resilience/status
```

### Shadow Traffic

```bash
curl -X POST http://localhost:8080/v1/shadow \
  -H "Content-Type: application/json" \
  -d '{"provider":"openai","model":"gpt-4"}'
```

### SLO Budgets

```bash
curl http://localhost:8080/v1/slo/budget
```

### Chaos Fault Injection

```bash
curl -X POST http://localhost:8080/v1/chaos/fault \
  -H "Content-Type: application/json" \
  -d '{"type":"latency","percent":50}'
```

### Backpressure

```bash
curl http://localhost:8080/backpressure/status
```

### Quota Enforcement

```bash
curl -X POST http://localhost:8080/v1/quota \
  -H "Content-Type: application/json" \
  -d '{"scope":"api_key","key":"k1","limit":1000}'
```

### Audit Events

```bash
curl http://localhost:8080/v1/audit/events
```

### Admission Control

```bash
curl -X POST http://localhost:8080/v1/admission/validate \
  -H "Content-Type: application/json" \
  -d '{"resource":"models","path":"/v1/models","method":"POST"}'
```

### Usage Metering

```bash
curl -X POST http://localhost:8080/v1/meter/usage \
  -H "Content-Type: application/json" \
  -d '{"api_key":"k1","provider":"p1","model":"m1","tokens_in":10,"tokens_out":20,"latency_ms":100}'
```

### Feature Flags

```bash
# Create/update flag
curl -X POST http://localhost:8080/v1/flags \
  -H "Content-Type: application/json" \
  -d '{"key":"darkmode","enabled":true,"strategy":"global"}'

# Get flag
curl "http://localhost:8080/v1/flags?key=darkmode"

# List flags
curl http://localhost:8080/v1/flags
```

### Evaluation Engine

```bash
# Judge scoring
curl -X POST http://localhost:8080/v1/eval/judge \
  -H "Content-Type: application/json" \
  -d '{"prompt":"hi","response":"hello","model":"m1","provider":"p1","prompt_version":"v1"}'

# Regression detection
curl http://localhost:8080/v1/eval/regression

# Benchmark
curl -X POST http://localhost:8080/v1/eval/benchmark \
  -H "Content-Type: application/json" \
  -d '{"dataset":"{\"prompt\":\"hello\"}\n","model":"m1","provider":"p1","rubric":"general"}'
```

### Policy Engine

```bash
# Create policy
curl -X POST http://localhost:8080/v1/policy \
  -H "Content-Type: application/json" \
  -d '{"id":"deny-post","expression":"deny-post","severity":"high"}'

# List policies
curl http://localhost:8080/v1/policy

# Block middleware
curl -X POST http://localhost:8080/v1/policy/block \
  -H "Content-Type: application/json" \
  -d '{"method":"POST"}'
```

### Data Retention

```bash
curl -X POST http://localhost:8080/v1/retention \
  -H "Content-Type: application/json" \
  -d '{"id":"logs","resource":"logs","ttl":24,"max_items":1000}'
```

### Incidents

```bash
# Create incident
curl -X POST http://localhost:8080/v1/incidents \
  -H "Content-Type: application/json" \
  -d '{"title":"outage","severity":"high","status":"open"}'

# List incidents
curl http://localhost:8080/v1/incidents

# Update incident
curl -X PUT "http://localhost:8080/v1/incidents?id=inc_123" \
  -H "Content-Type: application/json" \
  -d '{"status":"resolved"}'
```

### Notifications

```bash
# Create channel
curl -X POST http://localhost:8080/v1/notification/channels \
  -H "Content-Type: application/json" \
  -d '{"id":"c1","name":"ops","type":"webhook","target":"https://example.com/alerts","enabled":true}'

# List channels
curl http://localhost:8080/v1/notification/channels

# Create subscription
curl -X POST http://localhost:8080/v1/notification/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"id":"s1","alert_id":"a1","channel_id":"c1","enabled":true}'
```

### Scheduled Tasks

```bash
# Create task
curl -X POST http://localhost:8080/v1/schedule \
  -H "Content-Type: application/json" \
  -d '{"name":"backup","type":"cron","schedule":"0 0 * * *","payload":"{}"}'

# List tasks
curl http://localhost:8080/v1/schedule

# Update status
curl -X PUT "http://localhost:8080/v1/schedule?id=task_123" \
  -H "Content-Type: application/json" \
  -d '{"status":"running"}'
```

### Secrets

```bash
# Create secret
curl -X POST http://localhost:8080/v1/secrets \
  -H "Content-Type: application/json" \
  -d '{"name":"api-key","value":"secret123","type":"token"}'

# List secrets
curl http://localhost:8080/v1/secrets

# Delete secret
curl -X DELETE "http://localhost:8080/v1/secrets?id=sec_api-key"
```

### Multi-Region

```bash
# Create region
curl -X POST http://localhost:8080/v1/region/regions \
  -H "Content-Type: application/json" \
  -d '{"id":"us-east-1","name":"US East","endpoint":"https://us.example.com","primary":true}'

# Create residency policy
curl -X POST http://localhost:8080/v1/region/residency \
  -H "Content-Type: application/json" \
  -d '{"id":"p1","region":"us-east-1","data_type":"pii","required":true}'

# Create route rule
curl -X POST http://localhost:8080/v1/region/routes \
  -H "Content-Type: application/json" \
  -d '{"id":"r1","region":"us-east-1","providers":["openai"],"priority":1,"enabled":true}'
```

### MCP Server

```bash
# Initialize
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'

# List tools
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## CLI

AeroLLM includes a CLI for common operations:

```bash
# Initialize config
aerollm init

# Build plugin
aerollm plugin build plugin.go -o plugin.wasm

# Publish plugin
aerollm plugin publish plugin.wasm

# GitOps sync
aerollm sync

# Health checks
aerollm health

# Resilience status
aerollm resilience

# Shadow traffic
aerollm traffic shadow

# SLO budget
aerollm slo budget

# Chaos fault
aerollm chaos fault

# Backpressure
aerollm backpressure

# Quota
aerollm quota

# Audit events
aerollm audit events

# Admission validate
aerollm admission validate

# Meter usage
aerollm meter usage

# Feature flags
aerollm flags --key darkmode
aerollm flags --set '{"key":"darkmode","enabled":true,"strategy":"global"}' --key darkmode

# Evaluation
aerollm eval --kind judge --prompt hi --response hello --model m1 --provider p1 --prompt-version v1
aerollm eval --kind regression
aerollm eval --kind benchmark --dataset '{"prompt":"hello"}\n' --model m1 --provider p1

# Policy
aerollm policy --id allow --expr allow --severity low

# Retention
aerollm retention --id logs --resource logs --ttl 24 --max-items 1000

# Incidents
aerollm incident --title "outage" --severity high

# Notifications
aerollm notification --resource channel
aerollm notification --resource subscription

# Schedule
aerollm schedule --name "backup" --schedule "0 0 * * *"

# Secrets
aerollm secrets --name "api-key" --value "secret123" --type token

|# Region
aerollm region --resource region --name "us-east-1" --endpoint "https://us.example.com" --primary
```

## Migration CLI

AeroLLM includes a CLI tool for migrating LiteLLM configurations to AeroLLM format:

```bash
# Install the CLI
go build -o aerollm ./cmd/cli

# Migrate a LiteLLM config to AeroLLM config
aerollm migrate litellm --input litellm_config.yaml --output config.yaml
```

The migration tool translates:
- `model_list` entries into AeroLLM `providers` list
- `litellm_params.api_key` into `api_key` (with `${ENV_VAR}` references)
- `litellm_params.api_base` into `base_url`
- `litellm_params.model` into `models` list
- `router_settings.routing_strategy` into `router.strategy`

Supported strategy mappings:
- `simple-rotation` / `round robin` → `round_robin`
- `lowest-latency` → `latency`
- `cost` / `cost-optimized` → `cost`
- `least-busy` → `least-busy`
- `usage-based` → `usage-based`
- `sequential` / `fallback` → `fallback`

Supported model classification (prefix-based):
- `bedrock/` → bedrock provider
- `vertex_ai/` → gemini provider
- `claude-*` → anthropic provider
- `gpt-*` → openai-compatible provider
- `gemini-*` → gemini provider
- `llama-*` → openai-compatible (Groq) provider
- `command-*` → openai-compatible (Cohere) provider

## API Documentation

AeroLLM provides auto-generated OpenAPI/Swagger documentation. Once the server is running, access:

- **Swagger UI**: http://localhost:8080/swagger/index.html
- **JSON spec**: http://localhost:8080/swagger/doc.json
- **YAML spec**: http://localhost:8080/swagger/doc.yaml

## Rate Limit Headers

All responses include OpenAI-compatible rate limit headers for SDK backpressure handling:

| Header | Description |
|---|---|
| `X-RateLimit-Limit-Requests` | Max requests allowed in the window |
| `X-RateLimit-Limit-Tokens` | Max tokens allowed in the window |
| `X-RateLimit-Remaining-Requests` | Remaining requests in the current window |
| `X-RateLimit-Remaining-Tokens` | Remaining tokens in the current window |
| `X-RateLimit-Reset-Requests` | Seconds until the request limit resets |
| `X-RateLimit-Reset-Tokens` | Seconds until the token limit resets |

Headers reflect per-key limits from Virtual Key or Tenant configuration. On 429 responses, remaining counts are zeroed.

## Cache Management API

DevOps endpoints for cache inspection and management. These require Master/Admin API key authentication (not standard virtual keys).

```bash
# Get cache statistics (exact + semantic)
curl -H "Authorization: Bearer <master-key>" http://localhost:8080/v1/cache/stats

# Clear cache (all, or specific type via ?type=semantic|exact)
curl -X DELETE -H "Authorization: Bearer <master-key>" http://localhost:8080/v1/cache
curl -X DELETE -H "Authorization: Bearer <master-key>" "http://localhost:8080/v1/cache?type=semantic"

# Inspect cached entries (paginated, metadata-only for security)
curl -H "Authorization: Bearer <master-key>" "http://localhost:8080/v1/cache/inspect?cursor=0&page_size=50"
```

## Batch API

AeroLLM supports the OpenAI-compatible Batch API for asynchronous, large-scale processing.

```bash
# Create a batch job
curl -X POST -H "Authorization: Bearer <master-key>" \
  -F "file=@batch_input.jsonl" \
  http://localhost:8080/v1/batches

# Check batch status
curl -H "Authorization: Bearer <master-key>" http://localhost:8080/v1/batches/batch_abc123

# Download batch results (available when status is "completed")
curl -H "Authorization: Bearer <master-key>" http://localhost:8080/v1/batches/batch_abc123/results
```

The input JSONL file format matches the OpenAI Batch API spec:
```jsonl
{"custom_id": "request-1", "method": "POST", "endpoint": "/v1/chat/completions", "body": {"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]}}
```

Batch statuses: `validating` → `in_progress` → `completed` (or `failed`)

## Advanced Provider Features

### OpenAI Structured Outputs

Send a `response_format` with `json_schema` type to get guaranteed structured JSON:

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "What is the capital of France?"}],
    "response_format": {
      "type": "json_schema",
      "json_schema": {
        "name": "capital_response",
        "schema": {
          "type": "object",
          "properties": {"capital": {"type": "string"}, "population": {"type": "number"}},
          "required": ["capital", "population"]
        }
      }
    }
  }'
```

### Anthropic Prompt Caching

Use `cache_control` on messages to leverage Anthropic's prompt caching (up to 90% cost reduction for long prompts):

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "messages": [
      {"role": "user", "content": "You are an expert analyst. Here is a 100-page report...", "cache_control": {"type": "ephemeral"}},
      {"role": "user", "content": "Summarize the key findings."}
    ]
  }'
```

The adapter automatically:
1. Sets the `anthropic-beta: prompt-caching-2024-02-15` header
2. Inserts `cache_control` on the annotated message
3. Uses Anthropic's native `/v1/messages` endpoint format

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

## Key Packages

- `internal/middleware` — HTTP middleware primitives
- `internal/guardrails` — PII, injection shield, API key scoping
- `internal/finops` — cost tracking and budget enforcement
- `internal/traffic` — shadow testing
- `internal/webhooks` — async webhook dispatch with retry/backoff
- `internal/agent` — agent engine, memory, approvals
- `internal/router` — round-robin, latency, cost, fallback, least-busy, usage-based + circuit breaker
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
- `internal/batch` — async batch processing engine for OpenAI-compatible Batch API
- `internal/middleware/ratelimit.go` — standardized rate limit header middleware
- `internal/intelligence` — intent classification, model selection, bandit routing
- `internal/tenant` — multi-tenant models and context propagation
- `internal/plugins` — plugin hooks, registry, and WASM host
- `internal/eval` — judge pipeline, regression detection, benchmark runner
- `internal/compliance` — policy engine and HTTP 451 middleware
- `internal/synthesis` — deficit detection, LLM code generation stub, tool promoter
- `internal/graphrag` — temporal graph store, BFS neighbors, token query, GraphRAG middleware
- `internal/aiops` — self-optimizing tuner with metrics source and cooldown actions
- `internal/providers/universal` — universal model registry for capability cards
- `internal/mesh` — CRDT-backed state, peer discovery, gossip/sync workers
- `internal/keymanager` — virtual key generation, validation, and agency/RBAC management
- `internal/callbacks` — native observability callback dispatch (Langfuse, Datadog, webhook)
- `internal/config` — Viper config loading, hot-reload with atomic pointer, model info
- `internal/marketplace` — signed manifest verification, registry client, micro-royalty tracking
- `internal/economy` — agent wallets, micro-transaction billing for tool calls, and SLA-aware selection
- `internal/zk` — zero-knowledge encrypted payload middleware and confidential compute stubs
- `internal/pqc` — post-quantum key management and hybrid attestation
- `internal/spatial` — chunked media streaming and WebXR spatial translation
- `internal/federated` — federated learning aggregation and verification
- `internal/hardware` — silicon detection and hardware-aware routing
- `internal/billing` — provider billing, invoice generation, and Stripe integration
- `internal/licensing` — license validation and feature gating
- `internal/studio` — topology, analytics, and DAG visualization APIs
- `internal/genui` — generative UI SSE streaming and normalization

## Community

- **Code of Conduct**: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)
- **Security Policy**: [SECURITY.md](SECURITY.md)
- **Issue Templates**: [.github/ISSUE_TEMPLATE/bug_report.md](.github/ISSUE_TEMPLATE/bug_report.md), [.github/ISSUE_TEMPLATE/feature_request.md](.github/ISSUE_TEMPLATE/feature_request.md)
- **Pull Request Template**: [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md)

## Author

Ayoub Zulfiqar  
Website: https://ayoubzulfiqar.com  
Contact: contact@ayoubzulfiqar.com  
GitHub: https://github.com/ayoubzulfiqar

## License

Copyright (c) 2026 Ayoub Zulfiqar. All rights reserved.

This repository and its contents are the intellectual property of Ayoub Zulfiqar (https://ayoubzulfiqar.com, contact@ayoubzulfiqar.com).

Permission is NOT granted for personal use, reproduction, modification, distribution, or any other use of this work without explicit written permission from the author.

Unauthorized use is prohibited.
