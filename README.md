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
- **Graph Orchestrator**: DAG-based execution engine with dependency-aware concurrency
- **MCP Hub**: Native Model Context Protocol server for external tool integration
- **Hybrid RAG**: Dense + keyword retrieval with Reciprocal Rank Fusion
- **Context Manager**: Token counting and auto-summarization for long conversations
- **GitOps**: Git-backed prompt template versioning and delivery
- **Immutable Ledger**: Cryptographic audit chain for request/response integrity
- **WASM Sandbox**: Zero-trust isolated tool execution runtime
- **Realtime**: Bidirectional WebSocket streaming with barge-in support
- **Multimodal**: Audio/image preprocessing with transcription/vision hooks
- **Kubernetes Operator**: Control-plane reconciliation for routes, budgets, and agent pipelines
- **Flywheel**: Feedback ingestion, dataset export, and fine-tuning pipeline
- **Embedded State**: bbolt-backed KV with flat vector index for zero-latency agent memory
- **Agent Swarms**: Dynamic sub-agent spawning with shared hive-mind context
- **Red-Teaming**: Adversarial prompt generation and self-healing patch proposal
- **Evaluation Engine**: Judge pipeline, regression detector, and benchmark runner
- **Compliance-as-Code**: Policy engine with HTTP 451 enforcement
- **Multi-Tenant**: Hierarchical tenant model with tenant-scoped service wrappers
- **Plugins**: WASM-compatible plugin interface with lifecycle hooks and registry
- **Marketplace**: Signed manifest verification, registry client, micro-royalty tracking
- **Post-Quantum Crypto**: ML-KEM/ML-DSA hybrid key management and stream encryptors
- **Spatial Fabric**: Chunked video/3D streaming and WebXR spatial translation
- **Federated Learning**: FedAvg aggregation for secure LoRA weight averaging
- **Edge Companion**: Local-first binary with bbolt, hardware detection, and WASM sandbox
- **Policy Engine**: HTTP policy evaluation with rule-based access control
- **Data Retention**: TTL and max-items retention policies
- **Incident Management**: Incident lifecycle with severity and status tracking
- **Notifications**: Multi-channel notification routing (webhook, email, Slack, SMS)
- **Scheduled Tasks**: Cron, interval, and onetime automation tasks
- **Secrets Management**: In-memory secret storage with metadata
- **Multi-Region**: Region-aware routing and data residency controls

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

# Region
aerollm region --resource region --name "us-east-1" --endpoint "https://us.example.com" --primary
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

## Key Packages

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
- `internal/mesh` — CRDT-backed state, peer discovery, gossip/sync workers
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
- `internal/marketplace` — plugin marketplace, registry, and royalty tracking
- `internal/zk` — zero-knowledge guardrails and confidential compute stubs

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
