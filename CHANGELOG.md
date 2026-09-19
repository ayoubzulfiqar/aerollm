# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2026-09-01

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
