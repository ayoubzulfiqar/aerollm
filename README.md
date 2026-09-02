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

### Provider Support
- **OpenAI**: GPT-4, GPT-3.5-turbo, and compatible APIs
- **Anthropic**: Claude 3 models (Sonnet, Opus, Haiku)
- **Local**: Local LLM providers (Ollama, llama.cpp)

## Quick Start

### Prerequisites
- Go 1.22+
- Redis 7+ (optional, for caching)
- Docker and Docker Compose (optional)

### Installation

```bash
git clone https://github.com/ayoubzulfiqar/aerollm.git
cd aerollm
go mod download
go build -o aerollm ./cmd/server
```

### Configuration

AeroLLM uses Viper for configuration via config.yaml or environment variables with AEROLLM_ prefix.

### Running

```bash
./aerollm
```

### Docker

```bash
docker-compose up -d
```

## API

POST /v1/chat/completions
GET /health
GET /ready

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

### FinOps

- Budget checks happen before routing.
- If `BudgetChecker.CheckBudget` returns an error, the handler returns `HTTP 402` with body `{"error":"budget exceeded"}`.
- Set `AEROLLM_BUDGET_WEBHOOK_URL` and `AEROLLM_BUDGET_WEBHOOK_SECRET` to receive `budget_exceeded` events.

### Guardrails

- Injection shield returns `HTTP 403`.
- PII redaction rewrites the request body with placeholders; original body is preserved in request context for downstream handlers that need restoration.
- API key scoping enforces `AllowedModels` and `IPAllowlist`.

## Env Vars

- `AEROLLM_WEBHOOK_URL`
- `AEROLLM_WEBHOOK_SECRET`
- `AEROLLM_BUDGET_WEBHOOK_URL`
- `AEROLLM_BUDGET_WEBHOOK_SECRET`
