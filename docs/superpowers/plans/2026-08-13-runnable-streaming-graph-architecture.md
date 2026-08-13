# Runnable, Streaming, and Graph Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the duplicate execution and observation paths with one complete Runnable contract, immutable compiled Graph plans, strongly typed Context-only lifecycle callbacks, and a separate typed EventStream.

**Architecture:** Build the new stream and callback kernels additively, migrate every first-party component onto them, then perform the breaking public-interface cutover and delete the compatibility surface. Graph compilation binds an invoke plan and a stream plan; Agent recipes become public Graph clients and use the public ToolNode runtime.

**Tech Stack:** Go 1.24, standard-library generics/reflection/context/io/sync, `mark3labs/mcp-go` in the extensions module, Go test/vet/race.

## Global Constraints

- Follow red-green-refactor: every production behavior starts with a focused failing test.
- Callback and EventStream registration are accepted only through `context.Context`.
- Lifecycle Callback is synchronous, ordered, lossless, and observational; its failures never replace business results.
- LLM chunks, Tool progress, MCP notifications, and custom live data use EventStream, never lifecycle Callback.
- Graph `Stream` and `Transform` expose only the complete END result `O`.
- Graph runtime never stores an invocation Context on a struct and never invokes user code while holding an engine mutex.
- Sequential State reuses one pointer; Codec snapshots occur only at Fan-Out, retry, checkpoint, and resume boundaries.
- The completed migration retains no compatibility aliases or temporary exported adapters.
- Each task ends with a compiling repository, focused tests, and one Chinese Conventional Commit.
- Root and `extensions` modules must pass test, vet, and race verification at every stage boundary.

---

## Stage 1: Execution and Observation Kernel

### Task 1: Add StreamReader, StreamWriter, and Concat

**Files:**
- Create: `pkg/runnable/stream_reader.go`
- Create: `pkg/runnable/concat.go`
- Create: `pkg/runnable/stream_reader_test.go`
- Create: `pkg/runnable/concat_test.go`
- Modify: `pkg/runnable/runnable.go`

**Interfaces:**
- Produces: `Pipe[T](context.Context, ...PipeOption[T]) (*StreamReader[T], *StreamWriter[T])`
- Produces: `(*StreamReader[T]).Recv() (T, error)` and `Close() error`
- Produces: `(*StreamWriter[T]).Send(T) error`, `Close() error`, and `CloseWithError(error) error`
- Produces: `Concat[T](context.Context, *StreamReader[T]) (T, error)` and internal Reader Concat binding

- [x] **Step 1: Write failing StreamReader lifecycle tests**

```go
func TestPipePreservesOrderAndEndsWithEOF(t *testing.T) {
    reader, writer := runnable.Pipe[int](context.Background())
    go func() { _ = writer.Send(1); _ = writer.Send(2); _ = writer.Close() }()
    first, err := reader.Recv()
    if err != nil || first != 1 { t.Fatalf("first = %d, %v", first, err) }
    second, err := reader.Recv()
    if err != nil || second != 2 { t.Fatalf("second = %d, %v", second, err) }
    if _, err = reader.Recv(); !errors.Is(err, io.EOF) { t.Fatalf("end = %v", err) }
}

func TestReaderCloseCancelsBlockedWriter(t *testing.T) {
    reader, writer := runnable.Pipe[int](context.Background())
    done := make(chan error, 1)
    go func() { done <- writer.Send(1) }()
    if err := reader.Close(); err != nil { t.Fatal(err) }
    if err := <-done; !errors.Is(err, context.Canceled) { t.Fatalf("send = %v", err) }
}
```

- [x] **Step 2: Run the tests and verify missing APIs fail compilation**

Run: `go test ./pkg/runnable -run 'TestPipe' -count=1`

Expected: FAIL because `Pipe` and `StreamReader` are undefined.

- [x] **Step 3: Implement the bounded, single-consumer pipe**

Use one unbuffered value channel, one terminal state guarded by `sync.Once`, and
one child cancellation Context. `Recv` returns the stored terminal error once,
then `io.EOF`; early Reader close sets `context.Canceled` and cancels the writer.

- [x] **Step 4: Add failing Concat tests**

Cover `string`, `[]byte`, `[]int`, last-value fallback, empty stream zero value,
and upstream error. Each test creates a Pipe and calls `runnable.Concat`.

- [x] **Step 5: Implement default Concat selection**

Bind a `func(T, T) (T, error)` when the Reader is created. Use direct typed
paths for string and bytes, reflection only once for arbitrary slices, and
last-value semantics otherwise.

- [x] **Step 6: Verify and commit**

Run: `go test -race -count=1 ./pkg/runnable`

Commit: `feat(runnable): 添加背压流读取器与自动归约`

### Task 2: Add the Four-Mode Adapter and Lambda Constructors

**Files:**
- Create: `pkg/runnable/adapter.go`
- Create: `pkg/runnable/adapter_test.go`
- Rewrite: `pkg/runnable/lambda.go`
- Modify: `pkg/runnable/runnable.go`
- Modify: `pkg/runnable/runnable_test.go`

**Interfaces:**
- Produces `Adapter[I, O]` with `Invoke`, `Stream`, `Collect`, and `Transform`
- Produces: `Adapt[I, O](any) (*Adapter[I, O], error)`
- Produces: `InvokableLambda`, `StreamableLambda`, and `TransformableLambda`
- Removes now: `AnyRunnable`, `InvokeAny`, and `Erase`
- Defers until Task 9 cutover: broadening `Runnable` and removing `Stream[T]`/`Item[T]`

- [x] **Step 1: Write the adapter matrix tests**

Create one native type per capability and assert every cell of this matrix:

```text
Invoke native:    direct | Box | Concat->Invoke | Concat->Invoke->Box
Stream native:    Concat output | direct | Concat input/output | Concat input->Stream
Transform native: Box->Transform->Concat | Box->Transform | Transform->Concat | direct
```

Also assert native methods win when a value implements more than one method.

- [x] **Step 2: Verify tests fail because Adapt and TransformableLambda are absent**

Run: `go test ./pkg/runnable -run 'TestAdapt|TestTransformableLambda' -count=1`

- [x] **Step 3: Implement Adapter with closures bound once**

```go
type Runnable[I, O any] interface {
    Invoke(context.Context, I, ...Option) (O, error)
    Stream(context.Context, I, ...Option) (*StreamReader[O], error)
    Collect(context.Context, *StreamReader[I], ...Option) (O, error)
    Transform(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}
```

Keep the existing Invoke-only `Runnable` constraint until Task 9 so existing
core components remain assignable between commits. `Adapter` already exposes
the final four methods. `Adapt` detects exact typed method interfaces once and fills missing closures
from Box and Concat. It rejects nil/typed-nil values and a value with no native
capability.

- [x] **Step 4: Rewrite Lambda constructors on Adapter**

Each constructor wraps its function in a private native type, calls the typed
adapter constructor without reflection, and returns a complete Runnable.

- [x] **Step 5: Migrate package tests and remove old stream APIs**

Update Lambda tests to `StreamReader.Recv`; remove type-erasure tests and
assert the removed symbols have no repository consumers with:

Run: `rg -n 'AnyRunnable|InvokeAny|Erase' --glob '*.go'`

Task 9 migrates remaining channel streams and then broadens `Runnable` to the
four-method interface in the same compiling commit.

- [x] **Step 6: Verify and commit**

Run: `go test -race -count=1 ./pkg/runnable`

Commit: `refactor(runnable)!: 统一四种执行范式`

### Task 3: Replace the Generic Callback Dispatcher Vertically

**Files:**
- Rewrite: `pkg/callback/callback.go`
- Rewrite: `pkg/callback/context.go`
- Rewrite: `pkg/callback/dispatcher.go`
- Rewrite: `pkg/callback/types.go`
- Modify: `pkg/callback/error_sink.go`
- Create: `pkg/callback/manager_test.go`
- Remove after migration: `pkg/callback/graph.go`
- Modify: Callback call sites and tests under `pkg/agent`, `pkg/graph`, `pkg/llm`, and `pkg/tool`

**Interfaces:**
- Produces: `Handler{Name() string}`, `Manager`, `Invocation`, `Phase`, and `Dispatch[T Handler]`
- Produces: `WithManager` and `ManagerFromContext`
- Retains: synchronous `ErrorSink` with typed `CallbackError`
- Removes: generic `Event`, `Funcs`, component/event string constants, and `FilterChunks`

- [ ] **Step 1: Write failing typed dispatch tests**

```go
type auditHandler interface {
    callback.Handler
    OnAudit(context.Context, string) error
}

func TestDispatchSelectsTypedHandlersInRegistrationOrder(t *testing.T) {
    manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{first, unrelated, second})
    ctx := callback.WithManager(context.Background(), manager)
    err := callback.Dispatch[auditHandler](ctx, invocation, func(ctx context.Context, h auditHandler) error {
        return h.OnAudit(ctx, "value")
    })
    // assert first, second and no unrelated call
}
```

Add cases for one Handler implementing two domain interfaces, panic isolation,
aggregate errors, ErrorSink panic, metadata cloning, caller cancellation versus
manager timeout, and concurrent dispatch sequence safety.

- [ ] **Step 2: Verify tests fail because Manager and generic Dispatch are absent**

Run: `go test ./pkg/callback -run 'TestDispatch|TestManager' -count=1`

- [ ] **Step 3: Implement Context-only Manager**

Store immutable registrations. `Dispatch[T]` filters with a type assertion,
binds a cloned `Invocation` to the current Context, invokes synchronously, and
reports isolated failures. Do not use a queue or goroutine.

- [ ] **Step 4: Implement timeout classification**

Use `context.WithTimeoutCause` per Handler. Only the Manager's own timeout cause
produces `TimeoutError`; parent cancellation retains its original cause.

- [ ] **Step 5: Migrate every old Handler call site in the same task**

Because old `callback.Handler` means `Handle(Event)` and the final Handler means
`Name() string`, they cannot coexist under one exported name. Replace Agent
configuration callbacks with runtime Context Manager lookup, route current
Graph/LLM/Tool lifecycle calls through temporary domain-local typed closures,
and rewrite their tests before deleting `SyncDispatcher`, `Event`, `Funcs`,
`FilterChunks`, and `pkg/callback/graph.go`. Tasks 5 and 9 then replace the
temporary domain-local closures with the final public domain interfaces without
reintroducing a generic event.

- [ ] **Step 6: Verify and commit the vertical Manager migration**

Run: `go test -race -count=1 ./pkg/callback`

Commit: `refactor(callback)!: 改用上下文强类型回调管理器`

### Task 4: Add Typed EventStream

**Files:**
- Create: `pkg/callback/event_stream.go`
- Create: `pkg/callback/event_stream_test.go`

**Interfaces:**
- Produces: `StreamCallbackOf[T](string, func(context.Context, T) error) StreamCallback`
- Produces: `WithStreamCallbacks(context.Context, ...StreamCallback) context.Context`
- Produces: `EmitStream[T](context.Context, T)`

- [ ] **Step 1: Write failing routing and isolation tests**

Test exact-type routing, multiple sinks of one type in order, unrelated types,
typed nil payloads, panic/error to ErrorSink, and parent cancellation.

- [ ] **Step 2: Verify the API is absent**

Run: `go test ./pkg/callback -run TestEventStream -count=1`

- [ ] **Step 3: Implement opaque registrations**

Use an unexported type key derived from `reflect.TypeFor[T]`. Store cloned
immutable registrations in Context. `EmitStream[T]` synchronously invokes exact
type matches through the Manager's common isolation helper.

- [ ] **Step 4: Verify and commit**

Run: `go test -race -count=1 ./pkg/callback`

Commit: `feat(callback): 添加强类型实时事件流`

### Task 5: Add Domain Callback Interfaces

**Files:**
- Create: `pkg/llm/callback.go`
- Create: `pkg/llm/callback_test.go`
- Create: `pkg/tool/callback.go`
- Create: `pkg/tool/callback_test.go`
- Create: `pkg/retrieval/callback.go`
- Create: `pkg/retrieval/callback_test.go`
- Create: `pkg/graph/callback.go`
- Create: `pkg/graph/callback_test.go`
- Create: `extensions/mcp/callback.go`
- Create: `extensions/mcp/callback_test.go`

**Interfaces:**
- Produces exact interfaces and tagged values from the approved design spec
- Produces `CallbackFuncs` in each domain package
- Consumes: `callback.Dispatch[T]`, `Invocation`, and Context Manager

- [ ] **Step 1: Write compile-time interface and clone-isolation tests**

For each domain, construct a `CallbackFuncs`, register it once in a Manager,
dispatch Start/End/Error, and assert typed input, invocation phase, call order,
and defensive cloning.

- [ ] **Step 2: Verify all domain tests fail on missing interfaces**

Run: `go test ./pkg/llm ./pkg/tool ./pkg/retrieval ./pkg/graph -run Callback -count=1`

Run: `cd extensions; go test ./mcp -run Callback -count=1`

- [ ] **Step 3: Implement ChatModel and Tool callbacks**

Use the exact three-method interfaces in the design. A returned model-visible
Tool error is End; a Go/transport error is Error.

- [ ] **Step 4: Implement Retriever and MCP tagged values**

Constructors validate exactly one active field for the operation. Deep clone
maps, slices, messages, specs, arguments, hits, and results per Handler. MCP
types contain no `mark3labs/mcp-go` value.

- [ ] **Step 5: Implement Graph callback values**

Expose node ID and step as direct fields. `CallbackFuncs.OnNode` follows the
phase-specific function; remove magic phase strings.

- [ ] **Step 6: Verify and commit**

Run: `go test -race -count=1 ./pkg/callback ./pkg/llm ./pkg/tool ./pkg/retrieval ./pkg/graph`

Run: `cd extensions; go test -race -count=1 ./mcp`

Commit: `feat(callback): 添加领域生命周期接口`

## Stage 2: Compiled Graph and Core Components

### Task 6: Split Graph Builder from CompiledGraph

**Files:**
- Rewrite: `pkg/graph/runtime_graph.go`
- Create: `pkg/graph/compiled.go`
- Create: `pkg/graph/compile_plan.go`
- Modify: `pkg/graph/runtime_execute.go`
- Modify: `pkg/graph/revision.go`
- Create: `pkg/graph/compiled_test.go`

**Interfaces:**
- Changes: `(*Graph[I,O]).Compile() (*CompiledGraph[I,O], error)`
- Produces: immutable `CompiledGraph[I,O]` implementing complete Runnable
- Moves: `Invoke`, `Stream`, `Collect`, `Transform`, `Resume`, and `ResumeStream` to CompiledGraph

- [ ] **Step 1: Write failing immutable-snapshot tests**

Compile a graph, mutate the builder, compile again, and prove the first compiled
graph retains its original nodes and revision. Run both compiled values
concurrently under `-race`.

- [ ] **Step 2: Verify current Compile signature and builder execution fail the test**

Run: `go test ./pkg/graph -run 'TestCompiledGraph' -count=1`

- [ ] **Step 3: Extract immutable topology and runtime metadata**

Copy nodes, edges, options, types, resumability report, and revision into
CompiledGraph. Builder locks protect only mutation and snapshot creation.

- [ ] **Step 4: Move execution and resume methods**

Delete builder execution methods and update Graph nesting to compile/mount an
immutable child. Resume validates against the compiled snapshot revision.

- [ ] **Step 5: Verify and commit**

Run: `go test -race -count=1 ./pkg/graph`

Commit: `refactor(graph)!: 分离构建器与不可变执行图`

### Task 7: Compile Invoke and Stream Plans

**Files:**
- Modify: `pkg/graph/compile_plan.go`
- Create: `pkg/graph/stream_execute.go`
- Modify: `pkg/graph/runtime_execute.go`
- Create: `pkg/graph/stream_plan_test.go`
- Modify: `pkg/graph/continuation_test.go`

**Interfaces:**
- Produces internal `invokePlan` and `streamPlan`
- Binds Box, Concat, Transform chains, Pre/Post, Branch, Fan-Out, and checkpoint barriers
- Guarantees Graph Stream emits only final END `O`

- [ ] **Step 1: Write failing direct-pipeline and barrier tests**

Use instrumented native Transform nodes. Assert two sequential transforms
receive one Reader chain with no intermediate Concat. Add a branch and assert
one Concat occurs before predicate evaluation.

- [ ] **Step 2: Write failing final-output test**

Emit three internal chunks and assert `CompiledGraph.Stream` yields exactly one
complete END output followed by EOF.

- [ ] **Step 3: Bind both plans during Compile**

Select native capabilities once. Store closed-over execution functions in the
immutable node plan. Runtime branch selection chooses only among already-bound
targets.

- [ ] **Step 4: Integrate terminal lifecycle and continuation boundaries**

End node spans at Reader EOF/error/close. Never checkpoint a partial Reader;
resume the first uncommitted segment boundary.

- [ ] **Step 5: Verify and commit**

Run: `go test -race -count=1 ./pkg/graph`

Commit: `feat(graph): 编译离散与流式执行计划`

### Task 8: Purify Graph and Remove Wrapper Packages

**Files:**
- Remove: `pkg/graph/data.go`
- Remove message-specific sections: `pkg/graph/types.go`, `pkg/graph/execute.go`
- Move Agent adapters to: `internal/agentrecipe/message_graph.go`
- Remove: `pkg/framework/`
- Remove: `pkg/compose/`
- Modify: all examples, tests, and docs importing removed packages
- Create: `pkg/graph/dependency_test.go`

**Interfaces:**
- Removes: MessageGraph, MessageState, StreamMessages, ResumeMessages, and DataSink
- Preserves generic Graph EventStream forwarding and control lifecycle

- [ ] **Step 1: Add a failing Graph dependency boundary test**

Parse imports under `pkg/graph` and reject `schema`, `llm`, `tool`, `retrieval`,
`agent`, and `extensions/mcp`.

- [ ] **Step 2: Move message adapters to Agent recipe ownership**

Translate Agent message streaming to EventStream and final Graph output. Update
Agent tests to import no message helpers from Graph.

- [ ] **Step 3: Delete framework and compose**

Replace examples with direct Graph compilation and application-owned maps where
registration is needed.

- [ ] **Step 4: Verify and commit**

Run: `go test -count=1 ./...`

Commit: `refactor(graph)!: 移除消息领域与重复包装层`

### Task 9: Migrate ChatModel, ToolNode, Retriever, MCP, and Cache

**Files:**
- Modify: `pkg/llm/model.go`, provider files, retry/rate-limit decorators
- Modify: `pkg/tool/tool.go`, `pkg/tool/node.go`, scheduler and middleware files
- Modify: `pkg/retrieval/*.go`
- Modify: `extensions/mcp/mcp.go`
- Modify: `extensions/cache/llm/cache.go`, `extensions/cache/retrieval/cache.go`
- Modify corresponding tests

**Interfaces:**
- Every public component implements complete Runnable
- ChatModel Reader carries domain Concat and emits chunks through EventStream
- ToolNode owns the full public Tool execution runtime

- [ ] **Step 1: Migrate ChatModel streaming tests to Response Reader**

Remove `llm.Event` and explicit Done events. EOF is transport completion;
malformed/truncated providers close with an error. Verify indexed Tool deltas
and final Usage through ChatModel Concat.

- [ ] **Step 2: Instrument ChatModel lifecycle**

Start before provider work; End at reader EOF with concatenated Response;
Error on provider failure, cancellation, or early close. Emit each incremental
Response through `EmitStream` only.

- [ ] **Step 3: Merge internal Tool runtime into public ToolNode**

Move Middleware, timeout, retry, concurrency, recursion, authorization, batch
hook, Callback, and progress EventStream into `pkg/tool`. Delete duplicate
execution code after Agent migration.

- [ ] **Step 4: Instrument retrieval pipelines and MCP protocol operations**

Nested retrieval operations and MCP Initialize/ListTools/CallTool create child
spans. MCP Tool call emits both Tool and MCP lifecycles with correct parentage.

- [ ] **Step 5: Migrate cache decorators without losing stream lifetime**

Followers receive incremental Reader values. Subscriber close cancels only its
subscription; upstream cancels after all subscribers leave. Cached replay ends
ChatModel lifecycle at replay EOF.

- [ ] **Step 6: Verify and commit**

Run: `go test -race -count=1 ./...`

Run: `cd extensions; go test -race -count=1 ./...`

Commit: `refactor(runtime)!: 统一核心组件执行与观测契约`

### Task 10: Rebuild Agents as Public Compiled Graphs

**Files:**
- Rewrite: `internal/agentrecipe/recipe.go`
- Rewrite: `internal/agentrecipe/routing.go`
- Rewrite: `internal/agentrecipe/group.go`
- Modify: `pkg/agent/basic_agent.go`, `react_agent.go`, `routing_agent.go`, `group_chat_agent.go`
- Modify: Agent tests and examples
- Remove: `internal/toolruntime/` after all consumers migrate

**Interfaces:**
- Agent holds and delegates to public `CompiledGraph`
- Model, ToolNode, Retriever, and child Agent are actual public Graph nodes
- Agent has no callback configuration; runtime Context owns the Manager

- [ ] **Step 1: Add architecture tests for Graph-native Agents**

Assert recipe structs contain a CompiledGraph, no `internal/toolruntime`
reference exists, and model/tool calls appear as named nodes in Graph Snapshot.

- [ ] **Step 2: Rebuild Basic and ReAct**

Use Pre/Post handlers for message State and public ToolNode for the tool loop.
Remove manual model stream consumption and `graph.EmitData`.

- [ ] **Step 3: Rebuild Routing and GroupChat**

Mount child Agents as Graph nodes. Preserve strict complete-stream consumption,
valid child history, supervisor Tool result pairing, and append-only commits.

- [ ] **Step 4: Remove Agent callback options and private runtime**

Delete `WithCallbacks`, `WithCallbackConfig`, run callback options, BaseConfig
callback fields, and `internal/toolruntime`.

- [ ] **Step 5: Verify and commit**

Run: `go test -race -count=1 ./pkg/agent ./internal/agentrecipe/...`

Run: `go test -count=1 ./...`

Commit: `refactor(agent)!: 使用公开编译图装配内置代理`

## Stage 3: Public Surface Contraction

### Task 11: Separate State and Collapse Context Engine

**Files:**
- Move graph recovery types from `pkg/state/checkpoint.go` to `pkg/graph/checkpoint_state.go`
- Modify Graph continuation/runtime state files
- Rewrite: `pkg/contextengine/engine.go`
- Rewrite: `pkg/contextengine/builder.go`
- Modify Agent Context Engine injection
- Modify tests and docs

- [ ] **Step 1: Add package ownership tests**

Assert `pkg/state` exports only conversation Checkpoint/Store contracts and
Graph owns CursorStack/GraphStateCheckpoint.

- [ ] **Step 2: Move recovery types without changing wire JSON**

Round-trip a pre-move fixture and compare exact JSON bytes and resume behavior.

- [ ] **Step 3: Retain one Context Engine Runnable**

Delete synonymous builders/options and expose one constructor plus one Agent
injection option. Use Retriever Callback through runtime Context.

- [ ] **Step 4: Verify and commit**

Run: `go test -race -count=1 ./pkg/state ./pkg/graph ./pkg/contextengine ./pkg/agent`

Commit: `refactor(state)!: 收敛图恢复与上下文构建边界`

### Task 12: Remove Dead APIs and Canonicalize Names

**Files:**
- Modify: `pkg/tool/tool.go`
- Modify: `pkg/llm/model.go`
- Modify: `pkg/agent/*.go`
- Remove: `pkg/agent/react/`
- Move retained memory functionality into `pkg/retrieval/`
- Remove or rewrite `pkg/memory/`
- Modify tests, examples, and docs

- [ ] **Step 1: Add a public-surface symbol test**

Use `go doc`/AST inspection to reject removed aliases and constructors:
`BaseTool`, `Model`, `NewAgent`, `NewBase`, `AddTool`, duplicate
`RequestInterrupt`, and legacy Graph node events.

- [ ] **Step 2: Remove Tool registry and Agent internals**

Delete Factory/Register/MustRegister/RegisteredNames/New registry paths and
public Agent base assembly. Keep only constructor-injected Tools.

- [ ] **Step 3: Canonicalize names and memory ownership**

Retain `tool.Tool`, `llm.ChatModel`, domain `Callback`, one Agent constructor
per type, and `control.RequestInterrupt`. Move useful retrieval implementation;
delete unused ParserPageSize, KeygenStrategy, and Searchers.

- [ ] **Step 4: Verify and commit**

Run: `go test -count=1 ./...`

Commit: `refactor(api)!: 删除重复入口与无效设计`

### Task 13: Documentation and Final Verification

**Files:**
- Rewrite: `README.md`
- Rewrite: `docs/使用手册.md`
- Rewrite: `docs/tool-runtime.md`
- Modify: `docs/mcp-tools.md`
- Rewrite examples under `examples/basic` and `examples/graph`

- [ ] **Step 1: Update examples to the final API**

Show direct ChatModel/ToolNode/MCP/Graph construction, Compile, complete
Runnable methods, Context Manager registration, typed lifecycle Callback,
EventStream token rendering, and Graph interrupt/resume.

- [ ] **Step 2: Remove every deleted symbol from user-facing docs**

Run:

```text
rg -n 'pkg/framework|pkg/compose|AnyRunnable|InvokeAny|Erase|FilterChunks|WithCallbacks|CallbackConfig|MessageGraph|StreamMessages' README.md docs examples
```

Expected: no references outside historical design/plan documents.

- [ ] **Step 3: Run complete root verification**

```powershell
go test -count=1 ./...
go vet ./...
$env:PATH = 'D:\Program Files\msys64\ucrt64\bin;' + $env:PATH
$env:CGO_ENABLED = '1'
go test -race -count=1 ./...
```

- [ ] **Step 4: Run complete extensions verification**

```powershell
Set-Location extensions
go test -count=1 ./...
go vet ./...
$env:PATH = 'D:\Program Files\msys64\ucrt64\bin;' + $env:PATH
$env:CGO_ENABLED = '1'
go test -race -count=1 ./...
```

- [ ] **Step 5: Verify public architecture acceptance criteria**

Run dependency and removed-symbol tests, inspect Agent Graph snapshots, and
manually run both examples. Confirm Graph Stream emits only END output while UI
tokens arrive only through EventStream.

- [ ] **Step 6: Commit documentation**

Commit: `docs: 更新统一图编排使用手册`
