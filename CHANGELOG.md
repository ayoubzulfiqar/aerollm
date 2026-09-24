# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2026-09-24 — Security & correctness audit

### Security
- All control-plane endpoints (secrets, config, keys, policy, flags, chaos, RSI, cache, spend, incidents, …) now require an admin key; inference endpoints require a client, admin or virtual key. Previously most were unauthenticated and the `NewAuthMiddleware(h).Next` pattern bypassed auth entirely.
- No more well-known defaults (`sk-demo`, `default-master-key`): a random admin key is generated and printed once if none is configured.
- Virtual keys: 256-bit random, stored as SHA-256 only, constant-time lookup, budgets, expiry, model allow-lists, per-key rate limits.
- Secrets encrypted with AES-256-GCM; webhook payloads HMAC-signed with replay protection; SSRF protection for notifications and shadow traffic; git argument-injection and path-traversal fixes; WebSocket/MCP origin checks; request body caps everywhere; per-key cache namespaces (no cross-tenant cache hits).
- Real Ed25519 manifest signing for the marketplace, real ML-KEM-768 / X-Wing KEM, honest labelling of simulated subsystems (WASM, autoscale, mesh transport).

### Fixed
- Config defaults and `AEROLLM_*` env overrides never applied (viper key delimiter); `${VAR}` placeholders never expanded; `/config/yaml` wiped live API keys.
- Rate limiter was a no-op; spend was never recorded; cost was overstated ~1000×; budget webhooks never fired.
- Graceful shutdown hung (root context never cancelled); streaming cut off by a 15s write timeout; Swagger UI broken; data races across telemetry, router, webhooks, keymanager, ledger, cache and more.
- Dockerfile could not build (Go 1.22 image vs go 1.26 module); release workflow overwrote per-platform assets.

### Added
- OpenAI-compatible SSE streaming (native per provider, with fallback), `/v1/models`, provider fallback with circuit breakers, OpenAI error envelope, typed upstream errors with `Retry-After`.
- Anthropic-compatible `/v1/messages` (content blocks, tools, streaming events).
- Redis-backed distributed rate limiting with in-memory fallback; Prometheus `/metrics`; request IDs, structured JSON logs, CORS, security headers.
- Batch API: list, cancel, owner scoping, JSONL results/errors, per-owner billing.
- Server-side tool loop semantics, HITL approvals that store the paused conversation, MCP JSON-RPC server, realtime WebSocket bridged to real providers, context-window trimming, BM25 + hashed-embedding RAG.
- RSI deploy gate on held-out data with a paired t-test, dry-run by default, rollback; statistically sound eval judge wired to a real model.
- CLI: `chat --stream`, `models list`, `keys`, `metrics`, `health`, shared HTTP client; LiteLLM migration rewritten against the real format.

## [0.x] - 2026-09-01

### Added
- Full project initialization: Go module, Docker, Docker Compose, config system
- Multi-provider routing: OpenAI, Anthropic, local/vLLM providers
- Circuit breaker with closed/half-open/open states
- Agent engine with concurrent tool execution via errgroup
- Redis exact-match caching via go-redis/v9
- Token bucket rate limiting interface
- OpenTelemetry OTLP gRPC exporter with context-aware spans
- Comprehensive README with API docs and architecture
- RSI Engine (Phase 32): Recursive Self-Improvement package at `internal/rsi/`
  - HCI (Headroom-Closed Index) engine assessing headroom across 6 dimensions: routing, cache, guardrails, agent tools, cost, latency
  - Dream Simulator for offline policy evaluation via ledger replay (no real LLM calls)
  - Pluggable Policy interface for routing/caching/guardrail/agent-tool evolution
  - Thread-safe, cached ledger record loading with Refresh support
  - Integration-ready with internal/ledger, internal/trace, internal/finops, internal/aiops
- RSI Engine Step 2: ModularRSI benchmark-disjoint evaluation + Autonomous Explorer
  - ModularEvaluator with k-fold partitioning and diversity-based interleaving
  - EvaluateDisjoint returns train/test scores and generalization gap
  - AutonomousExplorer with broad-then-deep policy search (ExploreBroad, ExploreDeep, Explore)
  - Concrete policies: RoutingPolicy, CachePolicy, GuardrailPolicy, AgentWorkflowPolicy
  - Composite scoreMetrics scoring in [0, 1] across latency, cost, errors, cache-hit-rate
  - 54 new tests: partition correctness, policy apply/mutate/clone, exploration improvement
- RSI Engine Step 3: Orchestrator + API/CLI integration
  - RSIOrchestrator with full lifecycle: assess → explore → evaluate → deploy
  - RSIConfig with improvement threshold, broad/deep iterations, cycle interval, k-fold
  - RSICycle history tracking with JSON serialization
  - OnDeploy callback for policy deployment via AIOps
  - API handler (internal/api/rsi_handler.go): GET /v1/rsi/headroom, GET /v1/rsi/cycles, POST /v1/rsi/cycle, GET /v1/rsi/current
  - CLI subcommands (cmd/cli/rsi.go): aerollm rsi headroom, aerollm rsi cycles, aerollm rsi trigger
  - CLI command registered in root command tree (cmd/cli/main.go)
  - routerProviderLister adapter wires router.Providers() to rsi.ProviderLister
- RSI Engine: End-to-end test suite
  - 27 orchestrator tests: RunCycle lifecycle, deploy trigger/not-triggered/error, headroom assessment, cycle history, JSON serialization, background Run loop, integration full lifecycle
  - Total: 128 tests across 6 test files in internal/rsi/
- RSI Engine: Full project integration
  - Server wiring: RSIOrchestrator started as background goroutine in cmd/server/main.go
  - Four HTTP endpoints: GET /v1/rsi/headroom, GET /v1/rsi/cycles, POST /v1/rsi/cycle, GET /v1/rsi/current
  - Runtime config endpoints: GET /v1/rsi/config, PUT /v1/rsi/config/
  - CLI subcommands: aerollm rsi headroom, aerollm rsi cycles, aerollm rsi trigger, aerollm rsi config get/set
  - Full project: go build ./..., go vet ./..., go test ./... — all passing
- RSI Engine: API handler test suite (12 tests)
  - internal/api/rsi_handler_test.go: headroom, cycles, current cycle, config get/update/invalid, trigger cycle, nil orchestrator handling

## [1.0.0] - 2026-09-01

### Added
- Tool registry with registration, lookup, and JSON argument execution
- Built-in tools: echo, calculator, current_time
- Semantic cache with cosine-similarity search
- Metrics middleware: `/metrics` endpoint for requests/cache hits/errors/latency
- Telemetry in-memory stats: request count, cache hits, error count, avg latency
- Proper environment prefix: `AEROLLM_` instead of legacy `GOCONDUIT_`
- Router public `Providers()` accessor for health/status endpoints
- Handler provider-aware routing with model metadata propagation
- Expanded test coverage for router and agent max-iteration scenarios

### Fixed
- Agent max-iteration test now uses registered tool to avoid premature unknown-tool error
- Removed accidental nested git repo from project tracking
- Cleaned unused imports across cache, agent tools, middleware metrics

## [0.1.0] - 2026-09-01

### Added
- Core HTTP server with `/health`, `/ready`, `/v1/chat/completions`
- DI wiring in `cmd/server/main.go`
- Viper-based config with sensible defaults
- Graceful shutdown with signal handling
- Structured logger adapter

## Git Commit History

```
a2e218c chore: remove accidental embedded git repo from project tracking
6f89dd2 feat: extend features - tool registry, semantic cache, metrics middleware, telemetry stats, built-in tools, proper env prefix
b20cef5 test: fix max iterations test with registered tool; add tool registry and built-in tools coverage
90bdfca chore: initialize aerollm project with core routing and telemetry
```
