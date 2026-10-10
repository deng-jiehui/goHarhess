# Graph Interrupt and Resume Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Graph-owned portable interruption/resume, whole-batch PreTool approval, and a minimal byte CheckpointStore while removing the Agent-owned continuation state machine.

**Architecture:** `pkg/control` defines the typed cancellation cause and byte store SPI. `pkg/graph` owns checkpoint options, revisioned Continuations, safe-point persistence and Resume. `pkg/tool` owns immutable batch hooks; built-in Agents move all durable recipe control state into Graph State and become adapters over Graph recovery.

**Tech Stack:** Go 1.24, `context.WithCancelCause`, standard `encoding/json` and `crypto/sha256`, existing Runnable OptionPlan, Graph StateCodec, Go tests/race detector.

## Global Constraints

- Recovery is at-least-once; do not add locks, leases, fencing, CAS, checkpoint deletion or one-time consumption.
- Only typed `control.InterruptRequest` causes persistence; ordinary cancellation and deadlines do not Save.
- A successful interruption releases the execution stack and returns only after a bounded Save succeeds.
- Resumable Graphs require State and a PreHandler for every possible resume target.
- A committed node that routes directly to `END` completes normally; the engine does not create an END Continuation without a portable final edge value.
- PreTool evaluates the complete parsed batch before any scheduler task starts.
- Do not preserve compatibility branches for Agent `Breakpointer`, `InterruptSignal` or the old Agent Continuation format.
- Keep conversation history storage independent from Graph execution checkpoints.
- Windows race commands prepend `D:\Program Files\msys64\ucrt64\bin` to `PATH` and set `CGO_ENABLED=1`.

## File Structure

- Create `pkg/control/control.go`: typed interruption request, defensive metadata cloning, byte CheckpointStore.
- Create `pkg/control/control_test.go`: error-chain and defensive-copy contract.
- Create `pkg/graph/continuation.go`: public continuation format, errors, checkpoint option resolution, JSON validation and persistence.
- Create `pkg/graph/revision.go`: deterministic compiled recovery revision and resumability report.
- Create `pkg/graph/continuation_test.go`: sequential interruption/resume and failure semantics.
- Modify `pkg/graph/runtime_graph.go`: freeze revision/resumability metadata and expose `Resume`.
- Modify `pkg/graph/runtime_execute.go`: execute from a Continuation and enforce Prepare/Invoke/Commit safe points.
- Modify `pkg/graph/parallel.go`: make one parallel node an atomic recovery unit.
- Modify `pkg/graph/execute.go`: add message-oriented resume streaming.
- Create `pkg/tool/batch_hook.go`: immutable batch snapshots and synchronous hook dispatch.
- Modify `pkg/tool/node.go`: run hooks after all calls parse and before worker creation.
- Modify `pkg/agent/tool_batch.go`: run the same whole-batch hook before managed Agent Tool scheduling.
- Modify `internal/agentrecipe/*.go`: move durable counters, responses and routing cursors into `RecipeState`.
- Modify `pkg/agent/{base,config_options,options,runnable,interrupt,state}.go`: expose Graph checkpoint and PreTool options; remove Agent continuation logic.
- Modify built-in Agent files and tests to delegate Resume to recipe Graphs.
- Modify `README.md`, `docs/execution-model.md`, `docs/context-management.md`, `docs/tool-runtime.md`, and `docs/使用手册.md`: document the new contracts and removed APIs.

---

### Task 1: Shared Control Protocol

**Files:**
- Create: `pkg/control/control.go`
- Create: `pkg/control/control_test.go`

**Interfaces:**
- Produces: `control.InterruptRequest`, `control.RequestInterrupt(string, map[string]any) error`, `control.AsInterrupt(error) (InterruptRequest, bool)`, and `control.CheckpointStore`.
- Consumes: only the Go standard library.

- [ ] **Step 1: Write failing protocol tests**

Cover `errors.Is`/`errors.As` through wrapped errors, trimmed default reason, mutation of source Metadata after construction, mutation of returned Metadata after `AsInterrupt`, and nil-safe byte Store test doubles.

```go
func TestInterruptRequestSurvivesWrappingAndClonesMetadata(t *testing.T) {
    source := map[string]any{"approval": map[string]any{"id": "A-1"}}
    err := fmt.Errorf("hook: %w", control.RequestInterrupt(" approval ", source))
    source["approval"].(map[string]any)["id"] = "changed"
    request, ok := control.AsInterrupt(err)
    if !ok || request.Reason != "approval" {
        t.Fatalf("request=%#v ok=%t", request, ok)
    }
    request.Metadata["approval"].(map[string]any)["id"] = "mutated"
    again, _ := control.AsInterrupt(err)
    if again.Metadata["approval"].(map[string]any)["id"] != "A-1" {
        t.Fatal("interrupt metadata was not isolated")
    }
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `go test -count=1 ./pkg/control`

Expected: FAIL because `pkg/control` does not exist.

- [ ] **Step 3: Implement the minimal protocol**

Use an unexported error wrapper that owns a recursively cloned JSON-compatible Metadata map. `AsInterrupt` must use `errors.As`, return an owned copy and never require Graph or Tool imports.

- [ ] **Step 4: Run the tests and verify GREEN**

Run: `go test -count=1 ./pkg/control`

Expected: PASS.

- [ ] **Step 5: Commit**

```text
feat(control): 添加挂起控制协议
```

---

### Task 2: Graph Continuation, Revision and Sequential Safe Points

**Files:**
- Create: `pkg/graph/continuation.go`
- Create: `pkg/graph/revision.go`
- Create: `pkg/graph/continuation_test.go`
- Modify: `pkg/graph/runtime_graph.go`
- Modify: `pkg/graph/runtime_execute.go`
- Modify: `pkg/graph/execute.go`
- Modify: `pkg/graph/errors.go`

**Interfaces:**
- Consumes: `control.CheckpointStore`, `control.AsInterrupt`, existing `StateCodec`, `runnable.OptionPlan`, `state.GraphStateCheckpoint`, and `state.CursorStack`.
- Produces: `graph.WithCheckpoint`, `(*Graph[I,O]).Resume`, `graph.ResumeMessages`, `Continuation`, `InterruptedError`, `InterruptPoint`, and stable sentinel errors.

- [ ] **Step 1: Write failing Continuation and option tests**

Add a byte memory Store test double that counts Load/Save. Test that ordinary Invoke with no interruption performs no Store operation; malformed option values fail before State Factory; missing Store/key returns `ErrCheckpointRequired`; Continuation JSON clones maps/bytes and rejects bad schema/counters.

- [ ] **Step 2: Write failing sequential safe-point tests**

Build a stateful `first -> second -> END` Graph where every node has Pre/Post handlers. Cover:

- a typed cause present before `first` Saves `ResumeNode == "first"`;
- cancellation during `first.Invoke` skips its PostHandler and Resume replays `first`;
- interruption returned by a node is recognized through wrapping;
- an interruption observed after Post/route saves the committed State and resumes at `second`;
- plain cancel/deadline does not Save;
- Save failure does not satisfy `errors.Is(err, ErrInterrupted)`;
- repeated Resume replays the same checkpoint, documenting at-least-once;
- missing checkpoint, graph name, revision, node, Codec or schema mismatch fails before any user callback.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `go test -count=1 ./pkg/graph -run 'Test(Continuation|GraphInterrupt|GraphResume|GraphCheckpoint|GraphRevision)'`

Expected: FAIL because the public APIs and safe-point engine do not exist.

- [ ] **Step 4: Implement checkpoint option and portable format**

Use a private `checkpointConfig{Store control.CheckpointStore, Key string, Timeout time.Duration}` stored in `runnable.OptionSet.Values`. `WithCheckpoint` applies it once. Resolve it from the Graph OptionPlan without forwarding it to nodes. Default Save timeout is five seconds.

Implement `Continuation.Clone`, `Validate`, `JSON` and decode with `CurrentContinuationSchemaVersion = 1`. Persist only State snapshot, `ResumeNode`, Step, Visits, CursorStack, Point, reason, metadata, graph name and revision.

- [ ] **Step 5: Implement deterministic revision and resumability report**

After normal compile validation succeeds, hash a canonical JSON structure with SHA-256. Sort maps; preserve declared node and edge order. Include the exact fields from the design spec. Store `revision string` and `resumableErr error` on the frozen Graph. `WithCheckpoint` Invoke and every Resume reject `resumableErr` before State Factory or node code; ordinary Invoke remains valid.

- [ ] **Step 6: Implement Resume entry and sequential safe points**

Refactor `invokeRuntime` into a common entry accepting an optional decoded Continuation. Resume Loads and validates before entering Graph execution. Restore State exactly once and initialize `resumeNode`, Step, Visits and cursor from the Continuation.

Before Pre, after node Invoke, and after Commit, classify `context.Cause(originalCtx)` and returned errors with `control.AsInterrupt`. Persist with `context.WithTimeout(context.WithoutCancel(ctx), timeout)`. PostHandler and route selection use `context.WithoutCancel(nodeCtx)`. Only a successful Save returns `*InterruptedError`.

- [ ] **Step 7: Add message resume streaming**

Factor the existing `StreamMessages` relay so `ResumeMessages(ctx, g, opts...)` calls a small interface exposing `Resume`. It must emit Snapshot/Done exactly like first-run streaming and preserve terminal errors.

- [ ] **Step 8: Run package tests and verify GREEN**

Run: `go test -count=1 ./pkg/control ./pkg/graph`

Expected: PASS.

- [ ] **Step 9: Commit**

```text
feat(graph): 添加原生挂起与恢复
```

---

### Task 3: Atomic Parallel Recovery

**Files:**
- Modify: `pkg/graph/parallel.go`
- Modify: `pkg/graph/runtime_execute.go`
- Create: `pkg/graph/continuation_parallel_test.go`

**Interfaces:**
- Consumes: Task 2 safe-point classifier and persistence helper.
- Produces: parallel nodes that either fully merge or persist their base State and replay as one node.

- [ ] **Step 1: Write failing parallel interruption tests**

Use two branches sharing a cancel-cause Context. One branch requests interruption while the sibling blocks. Assert sibling cancellation, zero merger calls, persisted base State unchanged, `ResumeNode` equal to the parallel node, and successful Resume re-executes both branches before one declaration-order merge.

- [ ] **Step 2: Run the tests and verify RED**

Run: `go test -count=1 ./pkg/graph -run 'TestParallelInterrupt'`

Expected: FAIL because parallel errors are currently ordinary node errors.

- [ ] **Step 3: Implement atomic parallel classification**

Use `context.WithCancelCause` for sibling control. Preserve the first typed interrupt cause, skip every branch Post after interruption, never run the merger after failure/interruption, and let the outer node safe point persist the original base State at the parallel node boundary.

- [ ] **Step 4: Run Graph tests and race tests**

```powershell
go test -count=1 ./pkg/graph
$env:Path = 'D:\Program Files\msys64\ucrt64\bin;' + $env:Path
$env:CGO_ENABLED = '1'
go test -race -count=1 ./pkg/graph
```

Expected: PASS.

- [ ] **Step 5: Commit**

```text
feat(graph): 原子化并行节点恢复
```

---

### Task 4: Whole-Batch PreTool Hook

**Files:**
- Create: `pkg/tool/batch_hook.go`
- Create: `pkg/tool/batch_hook_test.go`
- Modify: `pkg/tool/node.go`
- Modify: `pkg/tool/node_test.go`
- Modify: `pkg/agent/tool_batch.go`
- Modify: `pkg/agent/recipe_adapter.go`
- Modify: `pkg/agent/tool_concurrency_test.go`

**Interfaces:**
- Consumes: `control.InterruptRequest`, Tool `Spec`, `Subject`, strict argument parsing and existing schedulers.
- Produces: `tool.BatchCall`, `tool.Batch`, `tool.BeforeBatchHook`, `tool.BeforeBatchFunc`, `ToolNodeConfig.BeforeBatch`, and Agent-managed batch admission.

- [ ] **Step 1: Write failing immutable hook tests**

Test Hook order, panic-to-error conversion, source/result mutation isolation, Subject copying, ordinary error propagation and wrapped interrupt propagation. Test both standalone ToolsNode and Agent-managed Tool batches.

- [ ] **Step 2: Write failing zero-execution batch tests**

Generate two valid Tool Calls where the first is read-only and the second mutating. The Hook returns `control.RequestInterrupt`. Assert neither Tool starts, no scheduler semaphore is acquired, and the returned error retains the typed request. Add a success case proving both execute only after the Hook returns nil.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `go test -count=1 ./pkg/tool ./pkg/agent -run 'Test(BeforeBatch|PreTool|ToolBatchInterrupt)'`

Expected: FAIL because hooks do not exist and Agent tools currently start during the LLM stream.

- [ ] **Step 4: Implement immutable batch types and dispatch**

Parse every call first. Build BatchCall with copied Spec, RawArgs, Args, call ID and metadata; copy Subject from Context. Execute hooks synchronously in order with panic recovery. Hooks cannot return rewritten calls.

- [ ] **Step 5: Gate standalone ToolsNode before workers**

Add `BeforeBatch []BeforeBatchHook` to `ToolNodeConfig`. Run dispatch after all calls validate and before creating the results channel, WaitGroup or goroutines. Unknown tools and malformed arguments remain normal Tool result errors unless the batch itself cannot be constructed safely.

- [ ] **Step 6: Gate Agent-managed tools and remove streaming prestart**

Add Hooks to Base configuration. Replace `Observe`/early `Submit` with a Resolve path that validates the complete final batch, runs Hooks once, then submits all valid calls. Remove `TestReactAgentStartsToolBeforeLLMStreamEnds`; replace it with a test proving no Tool starts before model Done and Hook approval.

- [ ] **Step 7: Run Tool and Agent tests**

Run: `go test -count=1 ./pkg/tool ./internal/toolruntime ./pkg/agent`

Expected: PASS.

- [ ] **Step 8: Commit**

```text
feat(tool): 添加执行前整批挂起切面
```

---

### Task 5: Migrate Built-In Agents to Graph Continuations

**Files:**
- Modify: `internal/agentrecipe/recipe.go`
- Modify: `internal/agentrecipe/routing.go`
- Modify: `internal/agentrecipe/group.go`
- Modify: `pkg/agent/base.go`
- Modify: `pkg/agent/config_options.go`
- Modify: `pkg/agent/options.go`
- Modify: `pkg/agent/runnable.go`
- Replace: `pkg/agent/interrupt.go`
- Modify: `pkg/agent/state.go`
- Modify: `pkg/agent/basic.go`
- Modify: `pkg/agent/react_agent.go`
- Modify: `pkg/agent/routing.go`
- Modify: `pkg/agent/hub.go`
- Modify: `pkg/agent/callback.go`
- Modify: `pkg/agent/recipe_adapter.go`
- Modify: `pkg/state/checkpoint.go`
- Modify: `pkg/state/context.go`
- Modify tests under `pkg/agent`, `pkg/state`, and `extensions/state/redis`.

**Interfaces:**
- Consumes: `graph.WithCheckpoint`, Graph Resume APIs and Tool BeforeBatch hooks.
- Produces: `agent.WithCheckpoint(store,key)`, `agent.WithBeforeToolBatch`, and `agent.Resume(ctx, agent, options...)`/`ResumeStream`; no Breakpointer or InterruptSignal API.

- [ ] **Step 1: Write failing Agent Graph-recovery tests**

Cover Basic external cancel-cause resume, ReAct PreTool interruption with zero tool calls then approved Resume, Routing resume without repeating a completed child, GroupChat resume from the saved round, caller System message preservation, callback terminal error semantics, and separation from `WithConversationID` history storage.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./pkg/agent -run 'TestAgent(GraphResume|PreToolResume|RoutingResume|GroupResume|CheckpointSeparation)'`

Expected: FAIL because Agent still owns its Continuation.

- [ ] **Step 3: Move durable recipe control into Graph State**

Expand `RecipeState` into JSON-safe fields for Messages, Response, LLMCalls, pending Calls, dispatch input, routing steps, round and last agent. Replace Context-owned mutable run state with immutable runtime limits/config plus Pre/Post mappings. Do not persist ToolBatch, model, callbacks, hooks or clients; rebuild batches on each Tools-node invocation.

- [ ] **Step 4: Expose recipe Resume and forward checkpoint options**

Add `Recipe.Resume` and `Recipe.ResumeStream` calling its Graph. Agent `RunOptions` stores the checkpoint Option and forwards it only to the recipe Graph. `agent.Resume` selects an internal resumable method implemented by built-in Agents and passes no input messages.

- [ ] **Step 5: Remove Agent-owned continuation APIs**

Delete Breakpointer, InterruptDecision, InterruptSignal, AgentKind continuation validation, EngineCheckpoint and old GraphState Context handoff. Remove `state.Continuation` from conversation Checkpoint. Keep conversation Messages/Values/versioning and Store unchanged.

- [ ] **Step 6: Preserve conversation commit semantics around Graph recovery**

Conversation history still Loads before first Invoke and commits only after Graph Done. An execution checkpoint key is independent from ConversationID. Graph Resume reconstructs recipe State, then the Agent lifecycle commits the completed response to the optional conversation Store.

- [ ] **Step 7: Run Agent, State and extension tests**

```powershell
go test -count=1 ./pkg/agent ./pkg/state ./pkg/graph
Push-Location extensions
try { go test -count=1 ./state/redis ./mcp } finally { Pop-Location }
```

Expected: PASS.

- [ ] **Step 8: Commit**

```text
refactor(agent)!: 下沉挂起状态到图引擎
```

Include a body explaining removal of Breakpointer/InterruptSignal and the new independent Checkpoint Store.

---

### Task 6: Documentation, Architecture Gates and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/execution-model.md`
- Modify: `docs/context-management.md`
- Modify: `docs/tool-runtime.md`
- Modify: `docs/使用手册.md`
- Modify: `pkg/graph/architecture_test.go`
- Modify: `examples/basic/main.go`
- Modify: `examples/graph/main.go`

**Interfaces:**
- Consumes: final public APIs from Tasks 1-5.
- Produces: user-facing Chinese guidance and source scans that forbid removed APIs.

- [ ] **Step 1: Add architecture source scans**

Reject product-code references to `Breakpointer`, `InterruptSignal`, old Agent Continuation fields, Lease/Fencing types and Graph checkpoint Context handoff. Assert `pkg/control` imports no project package and `pkg/graph` does not import `pkg/tool`.

- [ ] **Step 2: Update examples and Chinese documentation**

Document typed Cancel Cause, `WithCheckpoint`, `Graph.Resume`, `agent.Resume`, PreTool approval, whole-batch semantics, resumable State requirements and at-least-once limitations. Include an application-layer Redis SETNX sketch only as non-framework guidance.

- [ ] **Step 3: Run formatting and stale API scans**

```powershell
gofmt -w pkg internal examples extensions
rg -n "Breakpointer|InterruptSignal|WithRunBreakpointer|WithInterruptSignal|EngineCheckpoint" --glob '!docs/superpowers/**' .
git diff --check
```

Expected: source scan finds no removed product API; historical design/plan files are excluded; diff check exits zero.

- [ ] **Step 4: Run complete verification**

```powershell
go test -count=1 ./...
go vet ./...

Push-Location extensions
try {
  go test -count=1 ./...
  go vet ./...
} finally {
  Pop-Location
}

$env:Path = 'D:\Program Files\msys64\ucrt64\bin;' + $env:Path
$env:CGO_ENABLED = '1'
go test -race -count=1 ./...

Push-Location extensions
try { go test -race -count=1 ./... } finally { Pop-Location }
```

Expected: every command exits zero.

- [ ] **Step 5: Commit**

```text
docs(graph): 更新挂起与恢复手册
```

## Stop Condition

Stop when all six tasks pass their focused tests, the complete root and extension test/vet/race gates exit zero, stale product APIs are absent, and the worktree contains only the intended committed migration.
