# Callback Execution Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Callback 默认执行改为同步直调，精简公开配置并隔离高频 Chunk 观测，同时保持 Agent、Graph、LLM 和 Tool 的生命周期数据流正确。

**Architecture:** 用无队列的 `SyncDispatcher` 取代现有混合异步 `Dispatcher`；`Dispatch` 在当前 Goroutine 按注册顺序执行 Handler，返回聚合的 `CallbackError`，并通过独立 `ErrorSink` 旁路报告。`FilterChunks` 在统一入口过滤 Chunk，父子 Dispatcher 共享原子 `Sequence`，事件对每个 Handler 使用递归深拷贝。框架不提供异步管理器，异步包装完全由用户负责。

**Tech Stack:** Go、标准库 `context`/`errors`/`sync/atomic`，现有 `pkg/callback`、`pkg/agent`、`pkg/graph` 和 `internal/toolruntime` 测试工具链。

## Global Constraints

- `callback.Config` 只保留 `Timeout time.Duration` 和 `FilterChunks bool`。
- `Config.Timeout` 为协作式 deadline；Handler 必须主动响应 `ctx.Done()`，框架不强行终止阻塞 Handler。
- Agent 默认路径不创建异步队列、worker、背压或 Flush 生命周期。
- Handler 必须可被多个 Fan-Out Goroutine 并发调用；事件序号使用原子递增。
- 每个 Handler 接收独立的递归 Event 深拷贝，禁止 Handler 间共享嵌套 Metadata/Metrics 容器。
- Middleware 继续拥有授权、转换、重试、工具超时和业务拒绝职责；Callback 错误不得改变业务结果。
- 现有工作区中与本任务无关的用户修改必须保留，不得加入暂存区。

---

### Task 1: Replace the callback dispatcher with synchronous execution

**Files:**
- Modify: `pkg/callback/callback.go`
- Modify: `pkg/callback/dispatcher.go`
- Modify: `pkg/callback/types.go`
- Create: `pkg/callback/async_observer.go`
- Test: `pkg/callback/dispatcher_test.go`
- Test: `pkg/callback/async_observer_test.go`

**Interfaces:**
- Consumes: existing `CallbackFunc`, `Funcs`, `Event`, `CallbackError`, `CloneMetadata`.
- Produces: `Handler`, `SyncDispatcher`, `ErrorSink`, `New([]Handler, Config, ...ErrorSink)`, `NewChild(*SyncDispatcher, []Handler, ...ErrorSink)` and `Dispatch(context.Context, Event) error`.

- [ ] **Step 1: Write failing synchronous-dispatch tests.** Replace the old non-blocking/drop assertions with tests that assert a blocking Handler prevents `Dispatch` from returning until it releases, Handlers execute in registration order, errors and panics are returned as aggregated `CallbackError` values, and later Handlers still run.

- [ ] **Step 2: Add failing tests for configuration and event isolation.** Assert `Config{}` accepts unlimited cooperative timeout, positive timeout passes a deadline, `FilterChunks` skips only Agent/LLM Chunk events, sequence numbers remain unique under concurrent `Dispatch`, and nested Metadata/Metrics map/slice mutation in Handler 1 is invisible to Handler 2.

- [ ] **Step 3: Run the focused callback tests to capture RED.**

Run: `go test ./pkg/callback -run 'Test(Sync|Dispatcher|ErrorSink)' -count=1`

Expected: FAIL because the synchronous dispatcher API and deep per-Handler snapshots are not implemented yet.

- [ ] **Step 4: Implement `Handler` naming and synchronous dispatcher.** Define `Handler` as the primary interface; retain `type Callback = Handler` only as a behavior-free source alias if needed by untouched consumers. Replace queue/worker/mutex/Wait/Stats/Dropped code with immutable Handler entries, an `atomic.Uint64` sequence, `FilterChunks`, and inline invocation. `Dispatch` must clone the event for every Handler, recover panics, wrap timeout/error as `CallbackError`, continue through all Handlers, and return `errors.Join` of failures. `Timeout == 0` must not create a deadline; positive timeout uses `context.WithTimeout` cooperatively and does not disable a Handler.

- [ ] **Step 5: Implement the independent error sink.** Notify `ErrorSink.OnError` synchronously for every isolated Handler error/panic; contain sink panics and inherit sinks across child dispatchers. Do not put ErrorSink in `callback.Config` or create framework Goroutines.

- [ ] **Step 6: Run focused tests to capture GREEN.**

Run: `go test ./pkg/callback -run 'Test(Sync|Dispatcher|ErrorSink)' -count=1`

Expected: PASS, including race-safe sequence allocation and deep-copy isolation.

- [ ] **Step 7: Commit the dispatcher unit.**

Run: `git add pkg/callback/callback.go pkg/callback/dispatcher.go pkg/callback/types.go pkg/callback/async_observer.go pkg/callback/dispatcher_test.go pkg/callback/async_observer_test.go && git commit -m "refactor(callback): make dispatch synchronous"`

### Task 2: Update context and graph callback scopes

**Files:**
- Modify: `pkg/callback/context.go`
- Modify: `pkg/callback/graph.go`
- Test: `pkg/callback/context_test.go`
- Test: `pkg/callback/graph_scope_test.go`

**Interfaces:**
- Consumes: `SyncDispatcher` and `Dispatch(... ) error` from Task 1.
- Produces: Context helpers storing `*SyncDispatcher`, child graph scopes that preserve parent forwarding and trace parentage without queues.

- [ ] **Step 1: Update failing scope tests.** Replace `*callback.Dispatcher` references with `*callback.SyncDispatcher`, remove `Wait` calls, and assert graph events are already present immediately after `Invoke` returns. Add a child/parent assertion that `Dispatch` errors are not returned by graph execution.

- [ ] **Step 2: Run the scope tests to capture RED.**

Run: `go test ./pkg/callback -run 'Test(NestedScopes|NestedGraph)' -count=1`

Expected: FAIL while Context and graph bridge still use the removed dispatcher type and wait semantics.

- [ ] **Step 3: Switch Context and graph bridge to `SyncDispatcher`.** Update `WithDispatcher`, `DispatcherFromContext`, `NewChild`, graph sink fields, and all dispatch statements. Parent forwarding must call the parent synchronously before local Handlers; every returned callback error is intentionally ignored at the graph boundary.

- [ ] **Step 4: Run scope tests to capture GREEN.**

Run: `go test ./pkg/callback -run 'Test(NestedScopes|NestedGraph)' -count=1`

Expected: PASS with deterministic event ordering and no flush/wait requirement.

- [ ] **Step 5: Commit the scope unit.**

Run: `git add pkg/callback/context.go pkg/callback/graph.go pkg/callback/context_test.go pkg/callback/graph_scope_test.go && git commit -m "refactor(callback): update synchronous scopes"`

### Task 3: Migrate Agent, run options, and tool lifecycle integration

**Files:**
- Modify: `pkg/agent/base.go`
- Modify: `pkg/agent/config_options.go`
- Modify: `pkg/agent/options.go`
- Modify: `pkg/agent/callback.go`
- Modify: `internal/toolruntime/runtime.go`
- Modify: `pkg/agent/callback_lifecycle_internal_test.go`
- Modify: `pkg/agent/callback_test.go`
- Modify: `pkg/agent/tool_concurrency_test.go`

**Interfaces:**
- Consumes: `Handler`, `SyncDispatcher`, `Config{Timeout, FilterChunks}` and Context helpers from Tasks 1-2.
- Produces: `BaseConfig.Callbacks []callback.Handler`, `RunOptions.Callbacks []callback.Handler`, and `WithCallbacks`/`WithCallback`/`WithRunCallbacks`/`WithRunCallback` signatures that express synchronous Handler semantics.

- [ ] **Step 1: Add failing Agent integration assertions.** Change lifecycle tests to assert callback events are available before `Invoke` returns, add a `FilterChunks: true` run that still receives `llm.end`/`agent.end` usage metrics, and add a Handler that mutates nested metadata to verify later Handlers see the original snapshot.

- [ ] **Step 2: Run the focused Agent tests to capture RED.**

Run: `go test ./pkg/agent -run 'Test(Callback|Agent.*Callback|LLMUsage|NestedAgent)' -count=1`

Expected: FAIL because the Agent still stores asynchronous callback types and tests wait on channels.

- [ ] **Step 3: Migrate configuration and lifecycle types.** Change Base/Run callback slices and option signatures to `callback.Handler`; validate with `callback.New`; update comments to say synchronous observation. Update `runLifecycle` and `beginLLMCall` dispatcher types, and explicitly assign `_ = dispatcher.Dispatch(...)` at business boundaries so callback errors cannot replace business errors.

- [ ] **Step 4: Preserve lifecycle metrics while filtering only Chunk events.** Leave stream forwarding and Usage accumulation unchanged; rely on `SyncDispatcher`'s `FilterChunks` gate so `llm.end` and `agent.end` retain final metrics. Remove asynchronous waiting helpers from tests and assert event order directly after invocation.

- [ ] **Step 5: Run focused Agent and tool tests to capture GREEN.**

Run: `go test ./pkg/agent ./internal/toolruntime -run 'Test(Callback|Agent.*Callback|LLMUsage|NestedAgent|ReactAgentRunsMiddleware)' -count=1`

Expected: PASS with synchronous callback delivery, unchanged middleware behavior, and no callback-induced business failure.

- [ ] **Step 6: Commit the Agent integration unit.**

Run: `git add pkg/agent/base.go pkg/agent/config_options.go pkg/agent/options.go pkg/agent/callback.go pkg/agent/callback_lifecycle_internal_test.go pkg/agent/callback_test.go pkg/agent/tool_concurrency_test.go internal/toolruntime/runtime.go && git commit -m "refactor(agent): use synchronous callback handlers"`

### Task 4: Update callback documentation and examples

**Files:**
- Modify: `docs/tool-runtime.md`
- Modify: `docs/superpowers/specs/2026-08-12-callback-execution-design.md`

**Interfaces:**
- Consumes: final synchronous API from Tasks 1-3.
- Produces: Chinese documentation and examples that no longer advertise queue, worker, drop, or Flush behavior.

- [ ] **Step 1: Replace the callback example.** Use `callback.Handler`/`callback.Funcs`, `callback.Config{FilterChunks: true}`, and show that `OnEnd` carries token totals. Remove `QueueSize`, `MaxConcurrency`, and `ErrorHandler` from callback configuration; retain Agent tool `WithMaxConcurrency` as a separate setting.

- [ ] **Step 2: Document the execution and context contracts.** State synchronous inline delivery, thread-safe Handler requirement under Fan-Out, atomic event sequence, recursive event snapshots, cooperative timeout limitation, and the correct `WithoutCancel` pattern with an independent persistence timeout.

- [ ] **Step 3: Add a ten-line user-owned async wrapper example.** Show cloning the event and starting a user Goroutine, with an explicit note that users own buffering, retries and shutdown. Do not present it as a lossless billing/audit path.

- [ ] **Step 4: Verify documentation contains no stale callback queue fields.**

Run: `rg -n "QueueSize|callback.*MaxConcurrency|ErrorHandler|Dispatcher\.Wait|Dropped" docs/tool-runtime.md`

Expected: no matches except the separate Agent/tool concurrency explanation where applicable.

- [ ] **Step 5: Commit documentation.**

Run: `git add docs/tool-runtime.md docs/superpowers/specs/2026-08-12-callback-execution-design.md && git commit -m "docs(callback): document inline handlers"`

### Task 5: Full verification and final review

**Files:**
- Test-only changes allowed if a focused failure exposes a migration gap.

**Interfaces:**
- Consumes: all callback implementation and documentation changes from Tasks 1-4.
- Produces: passing repository verification and a clean reviewable diff containing no unrelated files.

- [ ] **Step 1: Run the root unit suite.**

Run: `go test -count=1 ./...`

Expected: PASS.

- [ ] **Step 2: Run static analysis.**

Run: `go vet ./...`

Expected: PASS.

- [ ] **Step 3: Run the root race suite with MinGW-w64 GCC.**

Run in PowerShell: `$env:PATH='D:\Program Files\msys64\ucrt64\bin;' + $env:PATH; go test -race -count=1 ./...`

Expected: PASS with no data races in concurrent sequence allocation or Handler event snapshots.

- [ ] **Step 4: Verify extensions.**

Run from `extensions`: `go test -count=1 ./...`; `go vet ./...`; then prepend the same MinGW-w64 path and run `go test -race -count=1 ./...`.

Expected: PASS.

- [ ] **Step 5: Inspect the final diff and status.**

Run: `git diff --check`; `git status --short`; `git diff --stat HEAD~4..HEAD`

Expected: no whitespace errors; unrelated pre-existing user files remain unstaged; only callback implementation, tests, docs, and their commits are included.

- [ ] **Step 6: Commit any final test-only correction and report exact verification output.**
