# Runtime State Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `StateGraph[I,O,S]` with a uniform `Graph[I,O]` API whose pure reflected nodes use Graph-owned Pre/Post handlers to access one run-exclusive pointer State.

**Architecture:** `Graph[I,O]` records reflected node input/output types and compiles all topology checks before execution. A reflected State Factory creates one pointer State per run; a strict default JSON Codec snapshots it only at Fan-Out and durable checkpoint boundaries, while sequential nodes reuse the same pointer without serialization.

**Tech Stack:** Go 1.24 generics, `reflect`, `encoding/json`, `context.Context`, existing Runnable/Graph/Agent/Checkpoint contracts.

## Global Constraints

- This is a breaking migration; remove `StateGraph[I,O,S]`, `Cloner`, package-level `AddNode`, package-level `AddConditionalEdge`, and legacy compatibility aliases.
- Public Graph types retain only `[I,O]`; the State type is a runtime pointer-to-struct created once per root invocation.
- ChatModel, ToolNode, MCP, Retriever and application nodes remain pure Runnables and never read Graph State directly.
- Pre/Post handlers are the only node-level State mapping mechanism.
- Default JSON snapshots reject unexported or ignored State fields and runtime `any` type degradation.
- Sequential nodes, ordinary branches and successful completion perform no State encoding or decoding.
- Codec work occurs only for Fan-Out, future Graph-owned retry boundaries, durable checkpoint/interrupt, and resume.
- Do not revert or overwrite the existing uncommitted Agent stream, Graph snapshot and typed-nil fixes.
- Race verification on Windows prepends `D:\Program Files\msys64\ucrt64\bin` to `PATH` and sets `CGO_ENABLED=1`.

---

### Task 1: Introduce the Uniform Graph Registration API

**Files:**
- Replace: `pkg/graph/types.go`
- Replace: `pkg/graph/graph.go`
- Replace: `pkg/graph/node.go`
- Test: `pkg/graph/graph_test.go`

**Interfaces:**
- Produces: `Graph[I,O]`, `NewGraph[I,O]`, `(*Graph[I,O]).SetStateFactory(any)`, `NodeOption`, `WithPreHandler(any)`, `WithPostHandler(any)`, `WithNodeOptions(...runnable.Option)`, `(*Graph[I,O]).AddNode(string, any, ...NodeOption)`, and `(*Graph[I,O]).AddBranch(string,string,string,any)`.
- Consumes: existing `runnable.Option`, `runnable.Runnable` method shape, Graph errors and snapshots.

- [ ] **Step 1: Add failing public-API tests.** Replace initial Graph tests with calls such as:

```go
g := graph.NewGraph[string, string]("uniform")
if err := g.SetStateFactory(func(_ context.Context, input string) (*testState, error) {
    return &testState{Text: input}, nil
}); err != nil { t.Fatal(err) }
if err := g.AddNode("length", runnable.Func[string, int](...)); err != nil { t.Fatal(err) }
if err := g.AddNode("render", runnable.Func[int, string](...)); err != nil { t.Fatal(err) }
if err := g.AddEdge(graph.Start, "length"); err != nil { t.Fatal(err) }
if err := g.AddEdge("length", "render"); err != nil { t.Fatal(err) }
if err := g.AddEdge("render", graph.End); err != nil { t.Fatal(err) }
```

Add tests that direct `g.AddNode` accepts ChatModel-style and ToolsNode-style concrete values, rejects malformed `Invoke` methods and typed nils, and that `Compile` rejects adjacent type mismatches.

- [ ] **Step 2: Run the focused tests and verify RED.**

```powershell
go test -count=1 ./pkg/graph -run 'Test(GraphUniformAPI|GraphRejectsMalformedNode|CompileRejectsMismatchedNodeEdgeTypes)$'
```

Expected: compile failures because `NewGraph`, instance `AddNode` and instance `AddBranch` do not exist.

- [ ] **Step 3: Implement reflected node registration.** Build one erased invocation closure per node using its bound `Invoke` method. Validate the exact shape `(context.Context, NI, ...runnable.Option) (NO, error)`, record `NI/NO`, and preserve nested Graph compilation and node metadata.

- [ ] **Step 4: Implement State Factory and reflected Pre/Post bindings.** Require factory shape `func(context.Context, I) (*S, error)`, Pre shape `func(context.Context,*S)(NI,error)`, and Post shape `func(context.Context,*S,NO) error`. Store types for Compile; do not invoke user code under Graph locks.

- [ ] **Step 5: Implement instance branches and Compile checks.** Resolve predicate input as source output or exact State pointer. Reject missing fallback, ambiguity and type mismatch.

- [ ] **Step 6: Run package tests and verify GREEN.**

```powershell
go test -count=1 ./pkg/graph
```

---

### Task 2: Add Strict Runtime State Codecs and Lazy Snapshots

**Files:**
- Replace: `pkg/graph/checkpoint.go`
- Create: `pkg/graph/state.go`
- Create: `pkg/graph/state_codec.go`
- Test: `pkg/graph/state_test.go`
- Test: `pkg/graph/architecture_test.go`

**Interfaces:**
- Produces: `StateCodec`, `NewStateCodec[S any]`, `(*Graph[I,O]).SetStateCodec(StateCodec)`, private `stateScope`, `StateCheckpointFromContext(context.Context)`, and strict default JSON Codec.
- Consumes: Task 1's reflected State Factory and handler metadata; `state.GraphStateCheckpoint`.

- [ ] **Step 1: Write failing strict-codec tests.** Cover exported pointer State success, unexported field rejection, `json:"-"` rejection, unsupported runtime resources, custom marshaler allowance, `map[string]any` integer degradation rejection, and custom Codec success.

- [ ] **Step 2: Write a zero-sequential-snapshot test.** Install a counting custom Codec, invoke a sequential graph without checkpointing, and assert encode/decode counts remain zero.

- [ ] **Step 3: Run focused tests and verify RED.**

```powershell
go test -count=1 ./pkg/graph -run 'Test(DefaultStateCodec|CustomStateCodec|SequentialGraphDoesNotSnapshotState)$'
```

Expected: compile failures because the runtime Codec and lazy State Scope do not exist.

- [ ] **Step 4: Implement Codec construction and compile validation.** The default Codec recursively rejects unsafe State shapes while allowing types with explicit JSON marshal/unmarshal behavior.

- [ ] **Step 5: Implement strict JSON snapshot.** Encode, allocate exact State pointer, decode, recursively compare all interface dynamic types, re-encode and require stable bytes. Return an error recommending a custom Codec on loss.

- [ ] **Step 6: Implement lazy snapshot Context provider.** Provider encoding occurs only when `StateCheckpointFromContext` is called. Resume still reads `state.GraphStateCheckpoint` from invocation Context and decodes once.

- [ ] **Step 7: Run focused and package tests.**

```powershell
go test -count=1 ./pkg/graph
```

---

### Task 3: Replace Graph Execution with Runtime Context State

**Files:**
- Replace: `pkg/graph/execute.go`
- Modify: `pkg/graph/data.go`
- Modify: `pkg/graph/events.go`
- Test: `pkg/graph/graph_test.go`
- Test: `pkg/graph/architecture_test.go`

**Interfaces:**
- Consumes: `Graph[I,O]`, reflected nodes, runtime `stateScope`, lazy snapshot provider.
- Produces: `(*Graph[I,O]).Invoke`, `StreamMessages[I]`, sequential State reuse, cursor resume and nested Graph isolation.

- [ ] **Step 1: Add failing execution tests.** Assert Factory runs once per invocation, sequential Pre/Post receive the same pointer, pure node input/output travel over edges, handler errors become node errors, and no user function runs while a Graph lock is held.

- [ ] **Step 2: Add failing resume and streaming tests.** Resume a State-backed node through a stored checkpoint, reject resume into a node without Pre state reconstruction, preserve empty final snapshots and require message `Done`.

- [ ] **Step 3: Run focused tests and verify RED.**

```powershell
go test -count=1 ./pkg/graph -run 'Test(RuntimeState|CursorResume|StreamMessages)'
```

- [ ] **Step 4: Implement root execution.** Resolve options, Compile, create/decode State once, bind Scope/provider, start edge value as public input, and execute nodes without cloning or Codec work.

- [ ] **Step 5: Implement node execution and routing.** Pre maps State to input, Invoke receives forwarded node options, Post mutates State, source output remains the outgoing edge value, and branches read output or State as compiled.

- [ ] **Step 6: Implement END, cursor and nested Graph behavior.** END requires the current edge value assignable to `O`; cursor resume requires a Pre handler; nested graphs isolate State checkpoints and data/control sinks as before.

- [ ] **Step 7: Run package tests and verify GREEN.**

```powershell
go test -count=1 ./pkg/graph
```

---

### Task 4: Migrate Parallel State and Durable Checkpoints

**Files:**
- Replace: `pkg/graph/parallel.go`
- Modify: `pkg/agent/interrupt.go`
- Modify: `pkg/agent/state.go`
- Test: `pkg/graph/graph_test.go`
- Test: `pkg/agent/interrupt_test.go`

**Interfaces:**
- Produces: non-generic `ParallelBranch`, instance `AddParallelNode`, reflected State merger, and Agent use of `graph.StateCheckpointFromContext`.
- Consumes: Task 2 snapshot Codec/provider and Task 3 runtime State Scope.

- [ ] **Step 1: Add failing Fan-Out tests.** Use a counting Codec to assert one base Encode and one Decode per branch, unique branch State pointers, declaration-order merge, sibling cancellation and no merge after branch failure.

- [ ] **Step 2: Add failing interruption tests.** Assert ordinary sequential execution performs no encode, interrupt requests one lazy Graph checkpoint, and resume decodes once.

- [ ] **Step 3: Run focused tests and verify RED.**

```powershell
go test -count=1 ./pkg/graph ./pkg/agent -run 'Test(Parallel|Interrupt.*GraphState|Resume.*GraphState)'
```

- [ ] **Step 4: Implement reflected parallel branches.** Require branch Pre/Post handlers, snapshot base once, decode independent pointers, execute branches concurrently and merge into the base State after all succeed.

- [ ] **Step 5: Migrate interruption checkpoint capture.** Request the Graph snapshot provider only when constructing a durable continuation; preserve an already supplied portable checkpoint as fallback.

- [ ] **Step 6: Run Graph and Agent package tests.**

```powershell
go test -count=1 ./pkg/graph ./pkg/agent
```

---

### Task 5: Migrate Built-In Agent Recipes and Compose

**Files:**
- Modify: `internal/agentrecipe/recipe.go`
- Modify: `internal/agentrecipe/routing.go`
- Modify: `internal/agentrecipe/group.go`
- Replace: `pkg/compose/graph.go`
- Modify: `pkg/callback/graph.go`
- Modify: all Graph/Agent/compose/callback tests using old APIs

**Interfaces:**
- Consumes: `Graph[I,O]`, State Factory, instance node/branch methods, Pre/Post handlers.
- Produces: Basic/ReAct/Routing/GroupChat Graph recipes and message-oriented compose builder on the new API.

- [ ] **Step 1: Add/adjust failing Agent graph-shape tests.** Preserve Basic one-model flow, ReAct `llm -> tools -> llm`, Routing supervisor/router, GroupChat bounded loop, node spans and strict `Done` behavior.

- [ ] **Step 2: Run targeted packages and verify RED.**

```powershell
go test -count=1 ./internal/agentrecipe ./pkg/agent ./pkg/compose ./pkg/callback
```

Expected: build failures from removal of `StateGraph`, package `AddNode`, and package `AddConditionalEdge`.

- [ ] **Step 3: Migrate Agent recipes.** Factory creates `*RecipeState`; pure recipe nodes accept message slices or dedicated request values; Pre reads messages and Post appends results. Use instance branches and Compile.

- [ ] **Step 4: Migrate compose.** `NewGraph` owns a `*graph.MessageState` factory; `Agent` and `Node` adapters use Pre/Post handlers; Builder delegates to instance methods.

- [ ] **Step 5: Migrate callback graph ownership and all tests.** Preserve node spans, runtime Context propagation, no callbacks under locks, and existing stream data plane.

- [ ] **Step 6: Run all root-module tests.**

```powershell
go test -count=1 ./...
```

---

### Task 6: Remove Legacy API and Update Documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/execution-model.md`
- Modify: `docs/mcp-tools.md`
- Modify: `docs/使用手册.md`
- Modify: `examples/basic/main.go`
- Modify: `examples/graph/main.go`
- Modify: extension tests or code referring to Graph APIs

**Interfaces:**
- Consumes: final `Graph[I,O]` API.
- Produces: no references to `StateGraph`, `Cloner`, package `AddNode`, package `AddConditionalEdge`, or user-written State `Clone` methods.

- [ ] **Step 1: Search for legacy API references.**

```powershell
rg -n 'StateGraph|NewStateGraph|Cloner|graph\.AddNode|graph\.AddConditionalEdge|WithStatePreHandler|WithStatePostHandler' . -g '*.go' -g '*.md'
```

- [ ] **Step 2: Rewrite examples and docs.** Lead with ChatModel/ToolNode/MCP nodes, `Graph[I,O]`, pointer State Factory, Pre/Post handlers and Compile-time reflected validation. Document strict exported-field/default-Codec rules and snapshot timing.

- [ ] **Step 3: Confirm legacy search is empty outside historical specs/plans.** Historical design records may retain old names; production code, current docs and examples may not.

- [ ] **Step 4: Run formatting and complete verification.**

```powershell
gofmt -w pkg/graph internal/agentrecipe pkg/compose pkg/agent pkg/callback examples
go test -count=1 ./...
go vet ./...
$env:Path = 'D:\Program Files\msys64\ucrt64\bin;' + $env:Path
$env:CGO_ENABLED = '1'
go test -race -count=1 ./...

Push-Location extensions
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
Pop-Location
```

- [ ] **Step 5: Review the final diff.** Verify no unrelated user changes were reverted and the existing runtime correctness fixes remain present.
