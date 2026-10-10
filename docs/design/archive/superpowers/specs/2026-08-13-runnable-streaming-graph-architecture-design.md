# Runnable, Streaming, and Graph Architecture Design

## Goal

Make Graph the framework's single orchestration core and make every executable
component composable through one complete Runnable contract. The migration must
let users assemble Agents from ChatModel, ToolNode, Retriever, MCP, and Graph
components without private execution paths or message-specific behavior in the
Graph engine.

The target architecture has four properties:

1. `Runnable[I, O]` expresses discrete and streaming execution uniformly.
2. `Graph[I, O]` is a mutable builder, while `CompiledGraph[I, O]` is an
   immutable, concurrently reusable Runnable.
3. Graph compilation binds separate discrete and streaming execution plans;
   runtime execution does not inspect whether the current carrier is a value or
   a stream.
4. Lifecycle Callback and real-time EventStream are separate Context-scoped
   observation channels with domain-specific, strongly typed interfaces.

This is a breaking architecture migration. Removed APIs are not retained as
compatibility aliases.

## Non-Goals

- Graph does not expose intermediate node values as its data output.
- Callback does not authorize, transform, retry, or otherwise control business
  execution.
- Lifecycle Callback does not transport LLM tokens, Tool progress, or custom
  real-time data.
- The framework does not provide an asynchronous callback queue, worker pool,
  flush lifecycle, or `AsyncObserverManager`.
- This change does not add distributed locks, leases, exactly-once side effects,
  or a process-wide run registry.
- The migration does not preserve `pkg/framework`, `pkg/compose`, the current
  channel-based `runnable.Stream`, or the current generic `callback.Event` API.

## Dependency Direction

Dependencies remain one-way:

```text
pkg/runnable        pkg/callback        pkg/control
      ^                  ^                  ^
      |                  |                  |
      +----------- pkg/graph --------------+
      |                  ^
      |                  |
      +-- pkg/llm -------+
      +-- pkg/tool ------+
      +-- pkg/retrieval -+
      |                  ^
      +-- extensions/mcp-+
                         ^
                         |
                    pkg/agent
```

`pkg/graph` must not import `pkg/schema`, `pkg/llm`, `pkg/tool`,
`pkg/retrieval`, `extensions/mcp`, or `pkg/agent`. Agent remains the final
assembly layer and is the first-party client used to prove that the public
Graph, Runnable, ToolNode, and Callback APIs are sufficient.

## Runnable Contract

### Public Interface

The public interface contains all four execution forms directly:

```go
type Runnable[I, O any] interface {
    Invoke(context.Context, I, ...Option) (O, error)
    Stream(context.Context, I, ...Option) (*StreamReader[O], error)
    Collect(context.Context, *StreamReader[I], ...Option) (O, error)
    Transform(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}
```

The methods are not embedded through four smaller public interfaces. Keeping
them directly on `Runnable` makes the framework's executable contract visible
in one place.

Users normally implement only the execution form native to their component.
The framework wraps that implementation in an `Adapter[I, O]`, which supplies
the other forms. Core framework components and compiled graphs expose the full
`Runnable` interface.

`Adapter` is constructed once and stores four bound closures. It does not
inspect the data carrier on every call:

| Native capability | Invoke | Stream | Collect | Transform |
| --- | --- | --- | --- | --- |
| Invoke | direct | Invoke then Box | Concat input then Invoke | Concat input, Invoke, then Box |
| Stream | Stream then Concat | direct | Concat input, Stream, then Concat | Concat input then Stream |
| Transform | Box input, Transform, then Concat | Box input then Transform | Transform then Concat | direct |

When a component supplies more than one native capability, the matching native
method wins and only missing methods use the table. Adapter construction
rejects a value with no supported method and rejects a method whose exact
input, output, Context, or Option signature does not match `I` and `O`.

Outside Graph, users can explicitly adapt a component:

```go
component, err := runnable.Adapt[Input, Output](native)
```

Inside Graph, `AddNode` accepts the native value and Compile performs the same
adaptation while it binds the node's types. Lambda constructors are the concise
path for ordinary functions. Core component constructors return an already
adapted complete Runnable.

### Lambda Constructors

The package exposes three constructors:

```go
runnable.InvokableLambda(fn)
runnable.StreamableLambda(fn)
runnable.TransformableLambda(fn)
```

There is no public `CollectableLambda`. `Collect` is primarily an engine
reduction operation and does not need a fourth user-facing constructor.

Each constructor returns a complete `Runnable` backed by `Adapter`. A user can
therefore add the returned value directly to a Graph without writing a struct
or manually implementing the other three methods.

### Removed Runnable APIs

The following APIs are removed:

- channel-based `Stream[T]` and `Item[T]`;
- `AnyRunnable`, `InvokeAny`, and `Erase`;
- package-level Lambda forwarding in `pkg/compose`;
- public partial `Invokable`, `Streamable`, `Collectable`, and `Transformable`
  interface hierarchy.

Graph has one internal type-erased node binding produced during compilation.
Runnable does not expose a competing type-erasure abstraction.

## StreamReader Lifecycle

### Public API

`StreamReader` and its paired writer belong to `pkg/runnable`, not
`pkg/schema`:

```go
type StreamReader[T any] struct { /* private */ }
type StreamWriter[T any] struct { /* private */ }

func Pipe[T any](ctx context.Context, options ...PipeOption[T])
    (*StreamReader[T], *StreamWriter[T])

func (r *StreamReader[T]) Recv() (T, error)
func (r *StreamReader[T]) Close() error

func (w *StreamWriter[T]) Send(T) error
func (w *StreamWriter[T]) Close() error
func (w *StreamWriter[T]) CloseWithError(error) error
```

The contract is:

- one Reader has one consumer;
- normal completion is reported as `io.EOF`;
- `Close` is idempotent;
- closing a downstream Reader cancels its upstream producer chain;
- a Transform takes ownership of its input Reader and closes it;
- delivery is ordered and backpressured;
- no unbounded queue is created by the framework;
- terminal errors are delivered exactly once before subsequent `Recv` calls
  return the closed state.

### Stream Completion Hooks

Adapters and Graph plans wrap readers with terminal hooks. A streaming
component's lifecycle does not end when its `Stream` or `Transform` method
returns a Reader. It ends only when one of these events occurs:

- the Reader reaches `io.EOF`;
- the upstream returns an error;
- the consumer closes the Reader early;
- the runtime Context is canceled.

This keeps callback duration and tracing spans aligned with actual stream
consumption. Terminal hooks are idempotent so concurrent cancellation and EOF
cannot emit two terminal lifecycle calls.

## Automatic Concat

Every Reader carries an internal Concat strategy selected by the framework
when the Reader is created. Users do not register global Concat functions.

Default strategies are:

| Output type | Concat behavior |
| --- | --- |
| `string` | concatenate in receive order |
| `[]byte` | append bytes in receive order |
| any slice type | append elements in receive order |
| all other types | retain the last received value |
| empty stream | return the output type's zero value |

Any stream error fails Concat immediately. Domain components can attach a
domain Concat when creating a Reader. ChatModel uses this facility to merge
message deltas, reasoning content, indexed Tool calls, usage, and provider
metadata without requiring application registration.

Concat is an execution adapter, not an observation mechanism. It never emits
intermediate Graph output.

## Graph Builder and Compiled Graph

### Builder

`Graph[I, O]` only builds topology and captures node contracts:

```go
g := graph.NewGraph[Input, Output]("support-agent")
g.SetStateFactory(stateFactory)
g.AddNode("chat", chatModel, graph.WithPreHandler(chatInput), graph.WithPostHandler(chatOutput))
g.AddNode("tools", toolsNode, graph.WithPreHandler(toolInput), graph.WithPostHandler(toolOutput))
g.AddBranch("chat", "tools", "has_tool_calls", hasToolCalls)
g.AddEdge("chat", "finish")
g.AddEdge("tools", "chat")

compiled, err := g.Compile()
```

The builder may be modified after a successful Compile and compiled again.
Each call returns a new immutable snapshot. Previously compiled values do not
observe later builder changes.

The builder no longer exposes `Build`, `IsBuilt`, `Invoke`, `Stream`, `Resume`,
or `ResumeStream`.

### Compiled Graph

```go
type CompiledGraph[I, O any] struct { /* immutable */ }
```

`CompiledGraph[I, O]` implements all four Runnable methods and owns
`Resume`/`ResumeStream`. It contains immutable topology, reflected type
bindings, state and checkpoint metadata, and two executable plans. A compiled
graph is safe for concurrent calls because per-run state, cursors, callbacks,
and streams live only in the invocation Context or run-local values.

### Compile Validation

Compile rejects the graph before any run when it finds:

- invalid topology, unreachable nodes, missing fallback branches, or cycles
  unsupported by the scheduler;
- START input or END output mismatch;
- adjacent node type mismatch after accounting for Pre/Post handlers;
- an unsupported node execution shape;
- a streaming connection for which neither direct Transform nor a valid
  Box/Concat adapter can be bound;
- invalid State Factory, Codec, branch, fan-out, fan-in, checkpoint, or resume
  contracts;
- nested Graph dependency cycles.

Compilation invokes no user Handler, node, Middleware, Codec serialization,
or network operation while holding a Graph mutex.

## Static Execution Plans

Compile creates two plans:

```text
invokePlan: I -> O
streamPlan: I -> Stream[O]
```

`Invoke` executes `invokePlan`. Native discrete nodes call Invoke directly;
native streaming nodes run through Stream or Transform followed by a
compile-bound Concat.

`Stream` executes `streamPlan`. Consecutive streaming nodes are connected by
Transform without intermediate collection. A native discrete node is adapted
with a compile-bound Box operation. `Collect` reuses `invokePlan` after
concatenating its input, while `Transform` reuses `streamPlan` with its input
Reader.

The runtime does not repeatedly inspect whether the current carrier is a value
or a stream. Conditional predicates still evaluate runtime data, but each
branch target already has a compiled execution shape.

## Streaming Segments and Barriers

Consecutive nodes that can exchange Readers form a streaming segment. A Reader
passes directly between their Transform operations with backpressure and
reverse cancellation.

The following constructs are explicit reduction or commit barriers:

- a PreHandler or PostHandler that requires a complete value;
- a content-dependent Branch;
- Fan-Out or Fan-In;
- a checkpoint or continuation boundary;
- an Invoke-only node in `invokePlan` or a node whose output must be committed
  to runtime State.

Compile inserts Concat at a barrier. It does not collect after every sequential
node.

Graph data output remains pure in both plans. `CompiledGraph.Stream` and
`Transform` return a Reader that produces only the final complete value emitted
by END, normally one `O`. LLM tokens, Tool progress, and intermediate node
state do not appear in this Reader.

## Runtime State, Commit, and Continuation

Sequential nodes reuse one run-owned State pointer. JSON or a custom StateCodec
is used only at Fan-Out, Graph-owned retry, checkpoint, and resume boundaries.
State requirements and strict default JSON behavior remain as specified by
`2026-08-13-runtime-state-graph-design.md`.

Streaming segments use node-boundary durability:

- a half-consumed Reader is never serialized;
- a segment is committed only after its output has been fully reduced where a
  barrier requires reduction;
- interruption or failure before commit resumes from the first uncommitted
  node or segment boundary;
- the entire uncommitted segment may run again, so external side effects must
  remain idempotent;
- Graph's existing safe-point and whole-batch PreTool interruption semantics
  remain authoritative.

`Resume` is owned by `CompiledGraph`, because the graph revision and execution
plan in a Continuation must be checked against the exact immutable compilation
snapshot.

## Callback Architecture

### Core Manager

`pkg/callback` becomes domain-neutral. It owns only Context registration,
handler naming, synchronous dispatch, trace/span identity, cooperative Handler
timeouts, panic isolation, and ErrorSink reporting.

```go
type Handler interface {
    Name() string
}

type Config struct {
    Timeout time.Duration
}

type Manager struct { /* private immutable registrations */ }

func NewManager(Config, []Handler, ...ErrorSink) (*Manager, error)
func WithManager(context.Context, *Manager) context.Context
func ManagerFromContext(context.Context) (*Manager, bool)

func Dispatch[T Handler](
    context.Context,
    Invocation,
    func(context.Context, T) error,
) error
```

Framework components accept no callback fields, callback constructor options,
or callback setters. The only registration path is the runtime Context:

```go
manager, err := callback.NewManager(
    callback.Config{Timeout: time.Second},
    []callback.Handler{modelAudit, toolAudit, graphTrace},
    alertSink,
)
ctx = callback.WithManager(ctx, manager)
result, err := compiled.Invoke(ctx, input)
```

Nested Graphs, Agents, models, and tools use the same Manager and derive child
operations from the runtime Context. They do not store a construction-time
Context or create a second Manager.

Manager dispatch is synchronous, registration-ordered, and lossless for every
accepted lifecycle call. It recovers each Handler panic, records each returned
error, continues with later Handlers, and synchronously reports failures to
ErrorSink. Handler or ErrorSink failure never replaces a business result or
business error.

Go does not support methods with their own type parameters, so strongly typed
domain dispatch is the package function `Dispatch[T]`, not a generic Manager
method. It resolves the Manager from Context, selects registered Handlers that
implement `T`, and invokes the supplied typed closure. A Handler may implement
several domain Callback interfaces and is registered only once. The component
closure clones its domain input for each invocation. Framework business paths
deliberately ignore the aggregate error returned by `Dispatch`; ErrorSink still
receives every failure.

`Config.Timeout` is a cooperative Handler deadline. A caller cancellation
remains caller cancellation and is not relabeled as a Handler timeout. A
Handler that must persist audit data after caller cancellation explicitly uses
`context.WithTimeout(context.WithoutCancel(ctx), limit)`.

No user Handler, ErrorSink, Middleware, or other user function runs while an
engine mutex is held.

### Shared Lifecycle Metadata

The core supplies a payload-free operation descriptor used for tracing and
ErrorSink diagnostics:

```go
type Phase string

const (
    PhaseStart Phase = "start"
    PhaseEnd   Phase = "end"
    PhaseError Phase = "error"
)

type Invocation struct {
    Component string
    Operation string
    Phase     Phase
    Trace     Trace
    StartedAt time.Time
    Timestamp time.Time
    Duration  time.Duration
    Metadata  map[string]any
}
```

`Invocation` has no `Payload any`, prompts, tool arguments, model output, or
Graph State. Domain data is delivered only through the typed interfaces below.
Metadata is cloned per Handler. ErrorSink receives the Handler name,
`Invocation`, and isolated error.

`callback.InvocationFromContext` exposes the current cloned descriptor inside a
domain method without adding tracing parameters to every interface. Dispatch
binds the phase-specific descriptor to the exact runtime Context before it
calls the Handler.

### ChatModel Callback

`pkg/llm` owns its lifecycle interface:

```go
type Callback interface {
    callback.Handler
    OnChatModelStart(context.Context, Request) error
    OnChatModelEnd(context.Context, Response) error
    OnChatModelError(context.Context, error) error
}
```

Every framework ChatModel implementation, decorator, and cache dispatches this
contract at its public Runnable boundary. Nested provider decorators use child
operations so duplicate logical spans are distinguishable rather than silently
collapsed.

For Stream and Transform, `OnChatModelStart` runs before the provider call.
`OnChatModelEnd` runs only after EOF and receives the fully concatenated
Response, including final Usage. Upstream failure, cancellation, or early
consumer close calls `OnChatModelError`. Incremental Response chunks never call
this interface; they use EventStream.

Requests and Responses are cloned for each Handler so one observer cannot
mutate data observed by another Handler or the business path.

`llm.CallbackFuncs` implements the interface for users who prefer functions
over a struct. Its fields have the same three typed signatures; it is not an
alias for the removed cross-domain `callback.Funcs`.

### Tool Callback

`pkg/tool` owns Tool execution lifecycle:

```go
type Callback interface {
    callback.Handler
    OnToolStart(context.Context, Spec, CallInput) error
    OnToolEnd(context.Context, Spec, CallInput, Result) error
    OnToolError(context.Context, Spec, CallInput, error) error
}
```

The public ToolNode execution runtime dispatches Tool callbacks around the
actual Tool invocation after scheduling admission and before PostHandler
commit. Middleware remains business control and is not replaced by Callback.

A transport or Go error invokes `OnToolError`. A valid `Result` marked as a
model-visible Tool failure invokes `OnToolEnd`, because the Tool successfully
returned a domain result. Specs, arguments, and Results are cloned for each
Handler. Tool progress and application-defined status updates use EventStream.

`tool.CallbackFuncs` implements the interface with the same three typed
function fields.

### Retriever Callback

`pkg/retrieval` owns one lifecycle interface covering its core component
operations without an `any` payload:

```go
type Callback interface {
    callback.Handler
    OnRetrieverStart(context.Context, CallbackInput) error
    OnRetrieverEnd(context.Context, CallbackOutput) error
    OnRetrieverError(context.Context, CallbackFailure) error
}
```

The provider-neutral tagged values have explicit fields:

```go
type Operation string

const (
    OperationChunk         Operation = "chunk"
    OperationEmbedding     Operation = "embedding"
    OperationVectorStore   Operation = "vector_store"
    OperationTextSearch    Operation = "text_search"
    OperationRerank        Operation = "rerank"
    OperationHybridSearch  Operation = "hybrid_search"
    OperationDocumentIndex Operation = "document_index"
)

type CallbackInput struct {
    Operation  Operation
    Document   *Document
    Embedding  *EmbeddingRequest
    Vector     *VectorRequest
    TextSearch *TextSearchRequest
    Rerank     *RerankRequest
    Hybrid     *HybridQuery
    Documents  []Document
}

type CallbackOutput struct {
    Operation Operation
    Chunks    []Chunk
    Embedding *EmbeddingResponse
    Vector    *VectorResponse
    Hits      []SearchHit
    Reranked  []RerankResult
    Index     *IndexStats
}

type CallbackFailure struct {
    Input CallbackInput
    Err   error
}
```

Exactly one operation-specific field is active. Package constructors build and
validate these values; callback dispatch never exposes a partially initialized
union. `CallbackFailure` contains the cloned input and the business error. The
operation tag uses exported constants, so users do not compare magic strings.

The tagged shape is preferred to seven unrelated callback interfaces because a
single retrieval observer commonly measures latency, item counts, scores, and
embedding usage across the entire retrieval pipeline. It still exposes no
untyped payload. Pipeline components create child operations for their internal
Embedder, VectorStore, search, and rerank calls.

Streaming retrieval chunks or index progress use EventStream. End is delivered
after the final result has been collected.

`retrieval.CallbackFuncs` provides the corresponding typed function adapter.

### MCP Callback

`extensions/mcp` owns MCP protocol lifecycle independently from Tool
execution:

```go
type Callback interface {
    callback.Handler
    OnMCPStart(context.Context, CallbackRequest) error
    OnMCPEnd(context.Context, CallbackResponse) error
    OnMCPError(context.Context, CallbackFailure) error
}
```

MCP does not expose SDK request or response types through this API:

```go
type Operation string

const (
    OperationInitialize Operation = "initialize"
    OperationListTools  Operation = "list_tools"
    OperationCallTool   Operation = "call_tool"
)

type InitializeRequest struct {
    ClientName    string
    ClientVersion string
}

type InitializeResponse struct {
    ProtocolVersion string
}

type ListToolsRequest struct {
    Refresh bool
}

type ListToolsResponse struct {
    Tools []tool.Spec
}

type CallToolRequest struct {
    Name string
    Args map[string]any
}

type CallToolResponse struct {
    Result tool.Result
}

type CallbackRequest struct {
    Operation  Operation
    Initialize *InitializeRequest
    ListTools  *ListToolsRequest
    CallTool   *CallToolRequest
}

type CallbackResponse struct {
    Operation  Operation
    Initialize *InitializeResponse
    ListTools  *ListToolsResponse
    CallTool   *CallToolResponse
}

type CallbackFailure struct {
    Request CallbackRequest
    Err     error
}
```

Exactly one operation-specific field is active and package constructors
validate it. Requests and responses are cloned per Handler. The extension
translates SDK values at its boundary, so applications are not coupled to
`mark3labs/mcp-go` callback types.

An `MCPTool.Invoke` emits an MCP CallTool child operation for the remote
protocol request and a Tool lifecycle operation for the framework Tool
execution. This is intentional: Tool Callback measures policy and executable
component behavior, while MCP Callback measures transport/protocol behavior.
Their trace parentage prevents ambiguity in APM systems.

Tool list-change notifications are real-time protocol signals and use
EventStream when applications need them. The subsequent refresh itself emits
the ListTools lifecycle. `Client.Close` is not observed because it has no
runtime Context; adding a Context-free callback path is explicitly rejected.

`mcp.CallbackFuncs` provides the corresponding typed function adapter in the
extension package.

### Graph Callback

`pkg/graph` owns Graph and node scheduling lifecycle:

```go
type Callback interface {
    callback.Handler
    OnGraphStart(context.Context, RunInfo) error
    OnNodeStart(context.Context, NodeExecution) error
    OnNodeOutput(context.Context, NodeExecution) error
    OnNodeEnd(context.Context, NodeResult) error
    OnGraphEnd(context.Context, RunResult) error
    OnGraphError(context.Context, RunFailure) error
}
```

`RunInfo` contains graph name, revision, boundary type names, and resume mode.
`NodeExecution` contains node ID, declared kind, step, visit count, and node
input/output type names. `NodeResult` adds duration and terminal status but not
the node output. `RunResult` contains step counts and completion metadata.
`RunFailure` contains scheduling location and the business error. Node IDs and
steps are direct typed fields, never entries in numeric Metrics maps.

Each node creates a child span under the Graph span. `OnNodeOutput` marks that
the node produced a complete output but carries no output payload. A failed
node still receives exactly one `OnNodeEnd` with error status before
`OnGraphError`, ensuring the node span closes and APM waterfall timing remains
correct.

Graph invokes callbacks with the current node execution Context so OpenTelemetry
values, trace IDs, and node cancellation signals remain intact. It never
substitutes a Context stored on a struct.

`graph.CallbackFuncs` provides phase-specific typed functions plus an optional
`OnNode` convenience function that is called after each phase-specific node
function. It does not retain `OnGraphNode` or accept string phase names.

Agent does not define a sixth, duplicate lifecycle Callback. Every built-in
Agent is a named `CompiledGraph`, so its run and node lifecycle is observed
through `graph.Callback`. ChatModel, Tool, Retriever, and MCP child operations
remain visible through their own domain interfaces under the same trace.

## EventStream

Real-time data uses a separate Context-scoped channel:

```go
sink := callback.StreamCallbackOf[llm.Response](
    "web-ui",
    func(ctx context.Context, chunk llm.Response) error {
        return render(chunk)
    },
)

ctx = callback.WithStreamCallbacks(ctx, sink)
result, err := compiled.Invoke(ctx, input)
```

Components publish with `callback.EmitStream(ctx, value)`. The user-facing API
is generic and strongly typed. Internal type erasure may be used to store mixed
sinks, but `Payload any` is never exposed to Handler implementations.

`StreamCallbackOf[T]` accepts the typed function and returns an opaque
`callback.StreamCallback` registration. `WithStreamCallbacks` accepts those
registrations, and the generic `EmitStream[T]` selects matching registrations
by exact Go type. Applications can register several payload types on the same
Context and several sinks for the same type; dispatch preserves registration
order.

EventStream delivery is synchronous, ordered, and backpressured. Sink errors,
panic, and cooperative timeouts go to the same ErrorSink isolation path and do
not affect business execution. Users who need asynchronous UI or metrics
delivery wrap their own sink and own its queue, loss policy, and shutdown.

EventStream is observation only. It is not the Graph data output and does not
participate in Concat, branch selection, State mutation, checkpointing, or
Continuation replay. Events emitted before an uncommitted segment fails may be
seen again after resume; consumers that require deduplication use trace,
operation, and application identifiers.

## Core Component Migration

Every core executable becomes a full Runnable through a native implementation
plus Adapter:

| Component | Native form | Full contract supplied by |
| --- | --- | --- |
| ChatModel | Stream or Invoke | Adapter and model Concat |
| FunctionTool | Invoke | Adapter |
| ToolsNode | Invoke, later Transform where useful | Adapter |
| Retriever components | Invoke or Transform | Adapter |
| MCP Tool and DynamicToolsNode | Invoke | Adapter |
| Context Engine | Invoke | Adapter |
| CompiledGraph | compiled Invoke and Stream plans | Graph compiler |
| Agent | wrapped public CompiledGraph | CompiledGraph |

Fan-Out and Fan-In are Graph topology primitives, not user-visible executable
nodes. Every branch payload must still be a Runnable. Compiled topology
primitives implement one private execution contract so the scheduler has no
special public node hierarchy.

The public ToolNode absorbs the complete execution behavior currently held by
`internal/toolruntime`: Middleware, timeout, retry, concurrency admission,
recursion protection, authorization integration, PreTool batch hook, Tool
Callback, and EventStream. Agent uses this public node and no longer owns a
second Tool execution route.

Basic, ReAct, Routing, and GroupChat are rebuilt only from public Graph,
Runnable, ChatModel, ToolNode, and Callback APIs. Their recipes hold an
immutable `CompiledGraph`; model and Tool calls are actual graph nodes rather
than hidden inside `runnable.Func` hooks.

## Package Cleanup

### Graph Purity

Move `MessageState`, `MessageGraph`, `NewMessageGraph`, `StreamMessages`,
`ResumeMessages`, `schema.MessageEvent`, and message-specific data forwarding
out of `pkg/graph`. Agent-owned message adapters belong under the Agent recipe
layer. Generic Graph retains topology, execution, runtime State, Callback,
EventStream forwarding, Continuation, and control events.

Add an architecture test that fails if `pkg/graph` imports Agent, LLM, Tool,
Retriever, MCP, or Schema packages.

### Removed Wrapper Packages

Delete `pkg/framework`. It is a second Runnable registry with no production
consumer and its named wrapper loses streaming capabilities. Application
routing and registration belong to the user's service or gateway.

Delete `pkg/compose`. Its Graph wrapper duplicates the public builder, inserts
a hidden finish node, and forwards Lambda APIs without adding an execution
contract.

### State Ownership

Move `GraphStateCheckpoint` and `CursorStack` from `pkg/state` to `pkg/graph`.
Keep Agent cross-turn conversation `Checkpoint` and `Store` in `pkg/state`.
Conversation persistence and Graph Continuation persistence remain separate
interfaces because their ownership and lifecycle differ.

### Context Engine

Collapse `Assembler`, `DefaultContextBuilder`, `ContextBuilder`,
`ContextBuilderFunc`, and overlapping configuration options into one public
Runnable component and one Agent injection option. The retained component is a
normal graph node and observes Retriever lifecycle through Context.

### Dead and Duplicate APIs

Remove:

- the process-global Tool Factory registry, which has no production consumer;
- public `agent.Base`, `NewBase`, and `AddTool` after recipes use public nodes;
- the `pkg/agent/react` constructor wrapper;
- unused `pkg/memory` configuration fields and move any retained retrieval
  implementation into `pkg/retrieval`;
- compatibility aliases `Tool/BaseTool`, `Model/ChatModel`,
  `Handler/Callback`, `New/NewAgent`, duplicate `RequestInterrupt`, and legacy
  `EventGraphNode`.

One canonical name is selected at each migration site; aliases are not kept.

## Error Handling

- Compile errors identify the node, edge, plan, source type, target type, and
  missing adapter or barrier.
- Stream errors preserve their cause and close upstream resources exactly once.
- A consumer closing early cancels upstream and ends lifecycle spans without
  reporting successful completion.
- Business errors remain unchanged by Callback or EventStream failures.
- Callback failures identify Handler name, component, operation, phase, and
  trace, and are sent to ErrorSink.
- Parent Context cancellation is never rewritten as framework callback timeout.
- Continuation persistence only happens for typed interrupt requests; ordinary
  cancellation remains an ordinary error.
- Resume rejects a Graph revision mismatch before invoking user code.

## Staged Migration

The migration is delivered in three independently buildable, testable, and
committable stages.

### Stage 1: Execution Kernel

- add the complete Runnable, Adapter, StreamReader/Writer, and Concat contracts;
- compile Graph into immutable `CompiledGraph` with invoke and stream plans;
- bind barriers and streaming segments at Compile;
- remove message-domain data types from Graph;
- add Context-only callback Manager, the five domain Callback interfaces, and
  EventStream;
- remove the old generic `callback.Event`, `callback.Funcs`, Chunk lifecycle
  events, `FilterChunks`, and Agent callback configuration;
- remove `pkg/framework`, `pkg/compose`, `AnyRunnable`, `InvokeAny`, and `Erase`.

Stage 1 ends with all remaining packages migrated enough to compile against the
new kernel; temporary internal adapters may exist only inside the same stage's
commit history and are absent from the stage completion commit.

### Stage 2: Core Components and Agent Assembly

- migrate ChatModel, Tool, ToolNode, Retriever, Context Engine, MCP, cache, and
  Agent to complete Runnable;
- move all lifecycle instrumentation to the domain Callback interfaces;
- move all real-time token and progress observation to EventStream;
- merge `internal/toolruntime` behavior into public ToolNode;
- rebuild every Agent from public compiled Graph nodes;
- define Fan-Out/Fan-In exclusively as topology primitives.

At Stage 2 completion, Agent is a Graph-native first-party client, not merely a
Graph-backed wrapper.

### Stage 3: Public API Contraction

- relocate Graph checkpoint types and keep session storage separate;
- collapse Context Engine entry points;
- privatize Agent assembly internals;
- remove the Tool registry, Agent wrappers, dead memory fields, duplicate
  aliases, and migration-only helpers;
- update README, examples, Chinese user guide, and package documentation to the
  final API only;
- add dependency and public-surface architecture tests.

## Verification

Each stage follows red-green TDD and ends with no skipped migration tests. At a
minimum, focused tests cover:

- all Adapter promotion and reduction combinations;
- StreamReader EOF, error, early close, backpressure, and reverse cancellation;
- default and ChatModel Concat behavior;
- Compile-time type failures and immutable compiled snapshots;
- direct Transform chaining and barrier placement;
- Graph Stream exposing only final END output;
- interruption and resume at streaming segment boundaries;
- Context-only callback discovery and nested trace propagation;
- each ChatModel, Tool, Retriever, MCP, and Graph typed lifecycle method;
- callback ordering, timeout classification, panic isolation, ErrorSink, and
  execution outside locks;
- EventStream typed routing, error isolation, duplicate-type sinks, and
  cancellation;
- Agent assembly using only public Graph and ToolNode APIs;
- architecture dependency rules and removal of legacy symbols.

Repository verification after each stage is:

```text
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...

cd extensions
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
```

Windows race runs prepend `D:\Program Files\msys64\ucrt64\bin` to `PATH` and
set `CGO_ENABLED=1`.

## Acceptance Criteria

The architecture is complete when:

1. A user can wrap one ordinary Go function as a complete Runnable and add it
   directly to a Graph.
2. Compile produces immutable discrete and streaming plans and reports all
   known type/adapter mismatches before execution.
3. Consecutive streaming nodes exchange Readers without intermediate Collect,
   while Graph callers receive only the final END result.
4. ChatModel, ToolNode, Retriever, MCP, nested Graph, and child Agent execute as
   ordinary public Graph nodes.
5. The five domain Callback interfaces are discoverable only through Context,
   receive strongly typed lifecycle values, and cannot affect business output.
6. LLM tokens, Tool progress, MCP notifications, and custom live state use only
   EventStream.
7. Agent recipes contain no private Tool runner or manual model stream loop.
8. `pkg/graph` contains no Message, Agent, LLM, Tool, Retriever, MCP, or Schema
   dependency.
9. `pkg/framework`, `pkg/compose`, legacy callback events, duplicate aliases,
   and listed dead APIs are absent.
10. Every migration stage passes normal, vet, and race verification in both the
    root and extensions modules.
