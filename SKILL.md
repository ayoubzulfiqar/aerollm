---
name: go-gateway-evolution
description: Class-level skill for extending production Go services/gateways incrementally. Covers middleware injection, interface-based extensions, Phase 4 edge/flywheel patterns, Phase 5 state/swarm/redteam, Phase 6 platform extensions, Phase 7 sentient mesh, Phase 8 global cognitive mesh, Phase 9 developer experience/commercial ecosystem, verification discipline, and git history hygiene.
tags:
  - go
  - gateway
  - middleware
  - phase7
  - phase8
  - phase9
  - aiops
  - agent-loop
  - studio
  - cli
  - licensing
---

# Go Gateway Evolution

Class-level skill for extending production Go services/gateways incrementally. Covers middleware injection, interface-based extensions, Phase 4 edge/flywheel patterns, Phase 5 state/swarm/redteam, Phase 6 platform extensions, Phase 7 sentient mesh, Phase 8 global cognitive mesh, Phase 9 developer experience/commercial ecosystem, verification discipline, and git history hygiene.

## Trigger
Use when extending a Go HTTP gateway by adding middleware, new provider adapters, tenant scoping, plugin hooks, evaluation pipelines, compliance middleware, Phase 6 platform capabilities, Phase 7 sentient-mesh capabilities, Phase 8 global mesh/marketplace/zk capabilities, or Phase 9 studio/CLI/licensing capabilities.

## Principles
- Do NOT modify existing Phase 1-6 code unless explicitly required; extend via interfaces, middleware, and new packages.
- Keep `cmd/server/main.go` lean; wire packages via constructors and middleware chains.
- Use `context.Context` strictly and goroutines only with bounded concurrency (`errgroup`, `semaphore`, channels).
- Maintain strict API backward compatibility for `/v1/chat/completions`, `/mcp`, and existing routes.
- Env prefix: `AEROLLM_`. Secrets come from env/config, never hardcoded or as `***` placeholders.
- Prefer composition over inheritance; add optional swarm/intelligence fields via interfaces instead of replacing core engines.
- Prefer targeted `patch` edits; if `patch` fails twice, rewrite the enclosing file/function with `write_file`.
- When editing `internal/api/handler.go`, keep imports minimal and do not add `fmt` unless the file actually uses it.

## Phase 6 Universal Protocol Fabric
- Package: `internal/providers/universal`
- Define unified `ProviderAdapter` interface: `ChatCompletions`, `Stream`, `Health`, `Close`, `Name`, `Type`.
- Implement `ProviderRegistry` with thread-safe registration and lookup.
- Implement `OpenAICompatibleAdapter` for Groq/DeepSeek/Azure-style APIs.
- Add stubbed adapter constructors for Gemini, Bedrock, Azure OpenAI, Cohere.
- Implement `StreamNormalizer` that converts provider SSE JSON into unified `AeroStreamChunk`.
- Provide `LegacyProviderAdapter` to bridge existing `providers.Provider` into the registry without changing router behavior.
- Add `ModelRegistry` with `Register`, `Get`, `ByProvider`, `List`, and `Models`; `ModelCard` captures ID, provider, type, capabilities, pricing, and timestamps.

## Phase 6 Adaptive Intelligence Layer
- Package: `internal/intelligence`
- `intent.go`: `Classifier` interface + `HeuristicClassifier` using lightweight keyword heuristics; include `TokenCount` approximation.
- `selector.go`: `ModelSelector` interface + `HeuristicSelector` filtering by cost/latency/quality policy.
- `bandit.go`: `BanditRouter` with per-provider/model state (`Alpha`, `Beta`), exploration/exploitation via sampled scoring, and `Update(latency, cost, success)`.

## Phase 6 Multi-Tenant SaaS Core
- Package: `internal/tenant`
- Models: `Organization`, `Team`, `User`, `APIKey` with hierarchical IDs.
- `TenantResolver` interface for API-key/token resolution.
- Context helpers: `WithTenantContext`, `APIKeyFromContext`, `TenantIDFromContext`.
- Tenant-scoped middleware should resolve and inject tenant before routing.

## Phase 6 Plugin Ecosystem
- Package: `internal/plugins`
- Interfaces: `Plugin`, `Metadata`, `Registry`, `Host`.
- Hooks: `OnRequest`, `OnResponse`, `OnToolCall`, `OnToolResult`.
- Implement `InMemoryRegistry` with enable/disable lifecycle.
- Implement `WasmHost` stub for `.wasm` plugin loading and hook execution; real wazero execution can be wired later behind the same interface.

## Phase 6 Evaluation Engine
- Package: `internal/eval`
- `JudgePipeline`: score ledger responses with a judge model and persist `ScoreRecord`.
- `RegressionDetector`: group by prompt version, compare averages, flag >10% drops.
- `BenchmarkRunner`: consume JSONL prompts, score against rubric, return aggregates.
- `ScoreStore` interface with `AppendScore` and `ListScores`; `InMemoryScoreStore` for development.

## Phase 6 Compliance-as-Code
- Package: `internal/compliance`
- `PolicyEngine` interface; `RegoPolicyEngine` stub for OPA-backed evaluation.
- `ComplianceMiddleware` returning HTTP 451 on policy denial.
- Keep middleware additive; do not remove existing auth/logging/recovery/rate-limit chain.

## Phase 7: The Sentient Mesh
- Package: `internal/synthesis`
  - `ToolDeficitSignal`, `DeficitDetector` with heuristic missing-tool detection from text/errors.
  - `LLMCodeGenerator` stub for future SLM-based tool generation.
  - `WasmCompiler` stub for hot-loadable WASM promotion.
  - `ToolPromoter` + `InMemoryManifestStore` for tool manifest lifecycle.
  - `ToolPromoter.Promote` must tolerate a nil registry in tests; return nil rather than panic.
- Package: `internal/graphrag`
  - Temporal graph store with `Node`, `Edge`, temporal validity.
  - `GraphStore` interface: `UpsertNode`, `UpsertEdge`, `Neighbors`, `Query`.
  - `GraphRAGMiddleware` with `Middleware(next http.Handler) http.HandlerFunc` plus `MaybeInject(ctx, req)`.
  - `AutoOntologyWorker` stub for background entity/relationship extraction.
- Package: `internal/aiops`
  - `MetaAgentTuner` with `Run(ctx)` control loop, cooldown-aware evaluation, `RegisterAction`.
  - `MetricsSource` interface + `DefaultMetricsSource` using Go runtime stats.
  - Wire real telemetry into `DefaultMetricsSource`: use `telemetry.RequestCount`, `telemetry.ErrorCount`, and `telemetry.AvgLatency` from production rather than stub `0` returning closures.
- Package: `internal/providers/universal`
  - `ModelRegistry` with `Register`, `Get`, `ByProvider`, `List`, and `Models`.
  - `ModelCard` captures ID, provider, type, capabilities, pricing, and timestamps.

## Phase 7 Wiring Pattern
- In `cmd/server/main.go`, create a single `deficitDetector := synthesis.NewDeficitDetector()` and use it in a small `http.HandlerFunc` wrapper around the chat handler.
- `DeficitDetector.Analyze` returns `(ToolDeficitSignal, bool)`. Do NOT call `len()` on the signal struct; check the boolean and use `signal.MissingTool`.
- Wrap `chat` with `http.HandlerFunc(chat)` before passing to middleware that expects `http.Handler`.
- Start AIOps tuner with `go tuner.Run(ctx)`, not `Start(ctx)`.
- Keep imports exact; unused imports fail the build immediately.
- Do not reference undeclared identifiers inside `internal/api/handler.go` unless they are imported or declared in that file.

## GraphRAG Middleware Body Preservation
- `GraphRAGMiddleware.Middleware` reads `req.Body`; preserve downstream handlers by buffering with `io.ReadAll`, resetting `req.Body = io.NopCloser(bytes.NewReader(body))`, and after `MaybeInject`, re-marshal the mutated request and reset `req.Body` again.
- Add stdlib imports `io` and `bytes` when using this pattern.
- Do NOT return a `http.HandlerFunc` that calls itself recursively; wrap the existing chat handler variable or use a separate named wrapper.

## Phase 8: Global Cognitive Mesh & Zero-Knowledge Fabric
- Package: `internal/mesh`
  - `MeshState` interface with `Merge`, `LocalSnapshot`, `Type`; pair with custom CRDTs rather than undeclared external libs.
  - CRDTs: `PNCounter`, `LWWElementSet`, `LWWRWRegister`, `VectorClock`.
  - Transport interfaces: `SecureTransport`, `PeerConn`, `PeerListener`; provide an in-memory transport stub first and only add libp2p when its dependencies are vendored.
  - `Discovery` + `SyncWorker` for peer tracking and gossip; keep it optional via `AEROLLM_MESH_ENABLED=true`.
- Package: `internal/marketplace`
  - `Client` for manifest and WASM artifact download with `ParseAndVerifyManifest` as a signature-verification stub.
  - `SignedRegistry` wraps `plugins.Registry`; preserve `Register(meta)` for interface compatibility and add `RegisterVerified(ctx, meta)` for verified flows.
  - `RoyaltyRecorder` records usage events with `CreatorID` and dispatches royalty webhooks.
  - If `webhooks.EventDispatcher` is not available, define a minimal internal `eventDispatcher` interface with `DispatchAsync` instead of referencing a missing type.
- Package: `internal/zk`
  - `ConfidentialCompute` interface with `Evaluate(ctx, ciphertext) ([]byte, error)`.
  - `Middleware` returns `func(http.Handler) http.Handler` and short-circuits only when `X-Encrypted-Payload` is present.
  - Default no-op compute implementation.

## Phase 8 Wiring Pattern
- In `cmd/server/main.go`, gate mesh with `AEROLLM_MESH_ENABLED=true` and use `mesh.DefaultMeshConfig()`.
- Instantiate marketplace client and royalty recorder even if registry integration is deferred; keep them assigned to `_` only when intentionally unused.
- ZK middleware: assign `var chatHandler http.Handler = http.HandlerFunc(chat)`, then wrap with middleware returning `http.Handler`.
- Do NOT pass `*agent.ToolRegistry` into `marketplace.NewSignedRegistry` unless its `Get` signature matches `plugins.Registry`; otherwise defer marketplace registry integration.
- Do NOT add unused imports like `math/rand`; remove them immediately.

## Phase 9: Developer Experience & Commercial Ecosystem
- Packages: `internal/studio`, `internal/licensing`, `cmd/cli`.
- `internal/licensing`: env-based `LicenseChecker` with `IsEnterprise()` and `IsFeatureEnabled(feature)`; use `GateFeature(checker, feature)` at initialization or middleware boundaries.
- `internal/studio`:
  - `Topology` endpoint aggregates router provider health, swarm active count, and mesh status for frontend visualization.
  - `AnalyticsCost` endpoint returns cost breakdown structure from pricing/model registry data.
  - `DAGStore` interface plus `InMemoryDAGStore`; `DAGHandler` with `ServeDAGs` routing `GET` to `ListDAGs` and `POST` to `SaveDAG`.
  - Use `studio.NewHandler(router, swarmOrchestrator, pricingMap)` and `studio.NewDAGHandler(dagStore)`.
- `cmd/cli`:
  - Use `github.com/spf13/cobra`.
  - Root command: `aerollm`.
  - Subcommands: `init`, `plugin build`, `plugin publish`, `gitops sync`.
  - `init` should emit starter `config.yaml` and `plugin.go` only; keep outputs minimal and non-interactive.

## Phase 9 Server Wiring Pattern
- Register studio routes under `/v1/studio/*` in `cmd/server/main.go`.
- Create studio handler after router/swarm/pricing are initialized; reuse existing objects instead of introducing new config structs.
- For DAG management, instantiate `studio.NewInMemoryDAGStore()` and register `dagHandler.ServeDAGs` at `/v1/studio/dags`.
- CLI wiring is a separate binary under `cmd/cli`; do not mix CLI command definitions into `cmd/server/main.go`.

## Package-Naming Pitfalls
- Do not declare duplicate JSON helper names in the same package across files; if multiple files need a helper, use one shared helper or a uniquely named function.
- When `studio.go` already defines `writeJSON`, do not add another `writeJSON` in `dag.go`. Either use a shared helper in one file, or change one helper name.

## Router/Provider Access Pattern
- `router.Router` exposes `Providers() []*CircuitBreaker`; use this for topology/health enumeration.
- Do NOT call `ListProviders()` on the router; that method does not exist.

## Pricing/Finops Access Pattern
- Use `finops.PricingMap.Models()` to enumerate known models for analytics responses.
- Do NOT call `CostTracker.ListModels()` unless that method exists; it does not in this codebase.

## Synthesis Test Pattern
- `TestLLMCodeGeneratorStub`: assert generated code contains `Execute(ctx context.Context` and the original description text.
- `TestWasmCompilerStub`: assert stub output contains `wasm-stub:<module>` and cast byte slice to string before `strings.Contains`.
- `TestToolPromoterStub`: pass `plugins.Metadata{ID: "1", Name: "weather"}` for the success case; for nil registry, `Promote` should return nil rather than panic.

## Deeper Agent Loop Extensions
- Package: `internal/agent/advanced_loop.go`
  - `LoopHook` interface: `BeforeLLM`, `AfterLLM`, `BeforeTools`, `AfterTools`, `OnToolDeficit`.
  - `RunAdvancedExecutionLoop(ctx, req, AdvancedLoopOptions)` adds hook checkpoints around the existing tool loop.
  - `ExecuteToolsWithRetry` retries only previously failed tool calls with bounded delay.
  - `ToolDeficitHandler` stores/retrieves `ToolDeficitSignal` by request ID.
  - Unknown-tool detection: after tool execution, if `r.Error != nil`, match `r.Error.Error()` with `unknownToolPattern` and emit `OnToolDeficit` hooks.
- HTTP wiring: add `AdvancedLoopHandler` in `internal/api/handler.go` with `Run(ctx, req)` delegating to `RunAdvancedExecutionLoop`.
- `AdvancedLoopHandler.Run` should return `(nil, nil)` when `Agent` is nil to keep handler flow simple.
- Test pattern: build fake provider responses with `models.ToolCall` + `models.ToolFunction`, then assert hook counts and final text response.
- Reuse existing test tools like `EchoTool`; if an `errorTool` is needed, declare it locally in the test file with `errors.New` instead of referencing undeclared types.

## Universal Model Registry Tests
- `TestModelRegistryLifecycle`: register a `ModelCard`, assert `Get`, `ByProvider`, `List`, and empty-ID rejection.

## Verification Discipline
- Run after each package addition: `go build ./... && go vet ./... && go test ./...`.
- If build fails, inspect the exact error line and fix imports/types before adding more packages.
- Common new-package fixes: unused imports (`context`, `time`, `sync`, `encoding/json`, `fmt`), wrong package names causing import path mismatches, and missing stdlib imports after interface changes.
- When a `patch` target changes between read and apply, rewrite the target file with `write_file` instead of retrying `patch`.
- After fixing test compilation, re-run `go test ./...` instead of narrowing to a subset unless diagnosing a single package.

## Git History Hygiene
- Use conventional commits: `feat:`, `fix:`, `docs:`, `chore:`.
- Group related new packages into one focused commit per step; avoid interleaving docs with large feature commits unless explicitly requested.
- Before finalizing a commit, ensure no `***` credential placeholders remain in non-test code.

## Docs Discipline
- After each Phase commit, update `README.md` to include a one-line description under `## Phases`.
- Remove `***` credential placeholders and `TODO` comments from non-test code before commit.

## Verification Discipline
- Run after each package addition: `go build ./... && go vet ./... && go test ./...`.
- Prefer full-file rewrite with `write_file` after 2 failed `patch` attempts on the same region.