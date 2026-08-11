# Runtime Boundary Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent delayed nil-Agent panics, isolate concurrent tool metadata, and reject incomplete Agent streams before checkpoint commit.

**Architecture:** Keep the existing Graph, Agent, and callback boundaries. Add validation at the Compose Agent registration boundary, reuse the existing recursive metadata clone at the Agent tool boundary, and make the Agent lifecycle wrapper enforce the stream terminal-event contract.

**Tech Stack:** Go 1.24, standard testing, existing `graph`, `compose`, `agent`, `callback`, and `runnable` packages.

## Global Constraints

- Only modify files required by these three fixes and their focused tests.
- Every production change must follow a failing regression test.
- Preserve the existing Graph data/control plane separation and checkpoint semantics.
- Do not add compatibility adapters or redesign public Runnable/Agent contracts.

---

### Task 1: Reject Nil Agents in Compose Graphs

**Files:**
- Modify: `pkg/compose/graph.go:38-52`
- Test: `pkg/compose/graph_test.go`

**Interfaces:**
- Consumes: `runnable.NamedRunnable[[]schema.Message, []schema.Message]` supplied to `GraphBuilder.Agent`.
- Produces: a builder error for nil and typed-nil Agent values before Graph execution.

- [ ] **Step 1: Write the failing test**

Add a test that passes a typed-nil Agent to `NewGraph("nil-agent").Agent("agent", value).Edge(graph.Start, "agent").Edge("agent", graph.End).Build()` and asserts `Build` returns an error containing `agent cannot be nil`.

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
go test -count=1 ./pkg/compose -run TestGraphBuilderRejectsNilAgent
```

Expected: the test fails because the current adapter captures the nil value and Graph build succeeds.

- [ ] **Step 3: Implement the smallest validation**

Use the same typed-nil reflection pattern as `graph.isNilRunnable` locally in the Compose package, reject nil before creating the closure, and store the error in `GraphBuilder.err`.

- [ ] **Step 4: Run focused and package tests**

```powershell
go test -count=1 ./pkg/compose -run TestGraphBuilderRejectsNilAgent
go test -count=1 ./pkg/compose
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add -- pkg/compose/graph.go pkg/compose/graph_test.go
git commit -m "fix: reject nil agents in compose graphs"
```

### Task 2: Isolate Concurrent Tool Metadata

**Files:**
- Modify: `pkg/agent/base.go:274-304`
- Test: `pkg/agent/tool_concurrency_test.go`

**Interfaces:**
- Consumes: `RunOptions.Metadata` passed to parallel `toolTaskBatch` calls.
- Produces: one recursively cloned `tool.CallInput.Metadata` map per tool invocation.

- [ ] **Step 1: Write the failing test**

Add a parallel-tool test with a nested `map[string]any` Metadata value. Each handler changes its own nested value and synchronizes with the other handler; assert both handlers observe their own value and the original run metadata remains unchanged.

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
go test -count=1 ./pkg/agent -run TestReactAgentIsolatesNestedToolMetadata
```

Expected: the test fails because `Base.cloneMetadata` only copies the top-level map.

- [ ] **Step 3: Reuse the canonical recursive clone**

Replace `cloneMetadata(opts.Metadata)` with `callback.CloneMetadata(opts.Metadata)` in `Base.callTool`, remove the local shallow helper, and keep the rest of the tool execution path unchanged.

- [ ] **Step 4: Run focused and Agent tests**

```powershell
go test -count=1 ./pkg/agent -run TestReactAgentIsolatesNestedToolMetadata
go test -count=1 ./pkg/agent
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add -- pkg/agent/base.go pkg/agent/tool_concurrency_test.go
git commit -m "fix: isolate concurrent tool metadata"
```

### Task 3: Require Agent Stream Completion Events

**Files:**
- Modify: `pkg/agent/callback.go:112-166`
- Test: `pkg/agent/callback_test.go`

**Interfaces:**
- Consumes: `runnable.Stream[schema.MessageEvent]` wrapped by `runLifecycle.wrap`.
- Produces: an Agent error and no checkpoint commit when the stream closes without `MessageEventDone`.

- [ ] **Step 1: Write the failing test**

Add an internal-package lifecycle test that creates a `runLifecycle` with a nil session, wraps a stream containing one snapshot and then closes it without Done, drains the wrapper, and asserts it emits an error containing `done event` and never emits a successful Done event.

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
go test -count=1 ./pkg/agent -run TestRunLifecycleRejectsStreamWithoutDone
```

Expected: the test fails because the current wrapper commits and emits Done after any clean channel close.

- [ ] **Step 3: Enforce the terminal event**

Track `sawDone` while consuming the stream. If the channel closes with `sawDone == false`, call `l.fail` with an explicit incomplete-stream error, emit that error, and return before breakpoint/checkpoint commit. Preserve existing context cancellation and item-error behavior.

- [ ] **Step 4: Run focused and full verification**

```powershell
go test -count=1 ./pkg/agent -run TestRunLifecycleRejectsStreamWithoutDone
go test -count=1 ./...
go vet ./...
$env:Path='D:\Program Files\msys64\ucrt64\bin;' + $env:Path
go test -race -count=1 ./...
```

Expected: PASS with no race reports.

- [ ] **Step 5: Commit**

```powershell
git add -- pkg/agent/callback.go pkg/agent/callback_test.go
git commit -m "fix: reject incomplete agent streams"
```
