# Runtime State Graph Design

## Goal

Replace `StateGraph[I, O, S]` with a graph API that exposes only the public
input and output types while keeping ChatModel, Tool, MCP, Retriever and other
nodes independent from workflow state.

The target API is uniform:

```go
g := graph.NewGraph[Input, Output]("agent")
g.SetStateFactory(factory)
g.AddNode("chat", chatModel, pre, post)
g.AddNode("tools", toolsNode, pre, post)
g.AddBranch("chat", "tools", "has_tool_calls", condition)
g.AddEdge("tools", "chat")
g.Compile()
```

This is a breaking migration. `StateGraph[I, O, S]`, `Cloner`, package-level
`AddNode`, package-level `AddConditionalEdge`, and their compatibility adapters
will be removed.

## Public Model

### Graph Boundary

```go
type Graph[I, O any] struct { /* private runtime metadata */ }

func NewGraph[I, O any](name string) *Graph[I, O]
```

Only the application input and output remain compile-time generic. Internal
node types and the runtime State type are captured through reflection when
components and handlers are registered.

`Graph[I, O]` implements `runnable.Runnable[I, O]`. A graph whose output is
`[]schema.Message` can also use `graph.StreamMessages` for the existing message
data plane. Graph lifecycle events remain separate in `graph.EventSink`.

### Runtime State

State is optional. A stateful graph registers one factory:

```go
g.SetStateFactory(func(ctx context.Context, input Input) (*AgentState, error) {
    return &AgentState{Messages: schema.CloneMessages(input.Messages)}, nil
})
```

The factory must return a non-nil pointer to a struct. Every root Graph
invocation creates one exclusive State pointer. The pointer is stored inside a
private State Scope carried by the invocation Context. Users do not call
`GetState` or `UpdateState`; Graph-owned handlers are the only mapping layer.

Nested graphs create their own State Scope. Parent Pre/Post handlers may map
parent State to a child graph's public input and map the child output back.

### Pure Nodes

`AddNode` accepts any concrete value whose `Invoke` method matches the Runnable
shape:

```go
Invoke(context.Context, NI, ...runnable.Option) (NO, error)
```

```go
func (g *Graph[I, O]) AddNode(
    name string,
    node any,
    options ...NodeOption,
) error
```

Registration builds an erased invocation closure once and records `NI` and
`NO` as `reflect.Type`. ChatModel, ToolNode, MCP, Retriever and application
Runnables remain ordinary typed components and never import or inspect Graph
State.

### Pre/Post Handlers

Pre/Post handlers are NodeOptions owned by Graph:

```go
graph.WithPreHandler(func(ctx context.Context, state *AgentState) (llm.Request, error) {
    return llm.Request{Messages: schema.CloneMessages(state.Messages)}, nil
})

graph.WithPostHandler(func(ctx context.Context, state *AgentState, output llm.Response) error {
    state.Messages = append(state.Messages, schema.CloneMessages(output.Messages)...)
    return nil
})
```

The required signatures are:

```text
Pre:  func(context.Context, *S) (NI, error)
Post: func(context.Context, *S, NO) error
```

Compile validates that every handler uses the factory's exact State pointer
type and the node's exact input/output type. Handler errors are node execution
errors. User code is never called while a Graph mutex is held.

Without a Pre handler, the incoming edge value must be assignable to the node
input. Without a Post handler, the node output simply becomes the outgoing edge
value. A Post handler updates runtime State but does not replace the outgoing
node value.

### Edges and Branches

Ordinary edges remain instance methods:

```go
g.AddEdge(graph.Start, "chat")
g.AddEdge("tools", "chat")
g.AddEdge("finish", graph.End)
```

Branches also become instance methods:

```go
g.AddBranch("chat", "tools", "has_tool_calls",
    func(ctx context.Context, state *AgentState) (bool, error) {
        return state.HasToolCalls, nil
    },
)
```

A branch predicate may receive either the source node output or the runtime
State pointer. Compile selects the mode from the predicate input type. If that
type is assignable from both values, compilation rejects the ambiguous branch.
Every source with conditional branches must have one unconditional fallback.

### Compile-Time Runtime Validation

`Compile` freezes the Graph and validates:

- graph name, nodes, edges, reachability and maximum steps;
- node `Invoke` method shape and nested graph cycles;
- START input and END output compatibility;
- adjacent node types when the target has no Pre handler;
- State Factory input and pointer result type;
- Pre/Post handler State and node types;
- branch predicate source-output or State type;
- parallel branch and merger types;
- default JSON Codec eligibility.

Type mismatches fail during initialization before a Graph can run.

## State Snapshot Codec

### Public Codec

Graph owns an erased internal codec. Users can replace it with a typed
constructor:

```go
codec, err := graph.NewStateCodec[*AgentState](
    "agent-state",
    1,
    encodeAgentState,
    decodeAgentState,
)
g.SetStateCodec(codec)
```

Custom codecs can use protobuf, msgpack or domain-specific cloning.

### Strict Default JSON Codec

A stateful Graph without an explicit Codec uses a strict JSON Codec. It is not
a general Go object copier.

Compile rejects State definitions that cannot be preserved safely:

- State must be a pointer to a struct;
- ordinary State fields must be exported;
- `json:"-"` State fields are rejected;
- nested structs with unexported fields are rejected unless the nested type
  supplies both JSON marshal and unmarshal behavior;
- channels, functions, unsafe pointers and other unsupported runtime resources
  are rejected.

At every real snapshot, the default Codec performs:

1. Encode the current State.
2. Allocate a new pointer of the exact State type.
3. Decode into it.
4. Recursively compare interface/`any` dynamic types between source and copy.
5. Encode the copy again and require a stable JSON round trip.

If `map[string]any{"count": int(1)}` becomes `float64(1)`, or another dynamic
type degrades, snapshot creation fails and instructs the caller to install a
custom State Codec. It never silently accepts lossy State.

`schema.Message` remains usable because its `Content` type provides explicit
JSON behavior. Dynamic values inside `Message.Extra`, audio or video payloads
must still survive strict runtime type validation.

## Snapshot Timing

Sequential execution never encodes or decodes State. Pre, Node and Post execute
against the same run-exclusive State pointer.

Codec work is limited to ownership or durability boundaries:

| Boundary | Codec behavior |
| --- | --- |
| Sequential node | None |
| Ordinary branch | None |
| Fan-Out | Encode base once, Decode once per branch |
| Fan-In | Explicit merger updates the base State |
| Graph-owned retry | Snapshot before retry and restore before a later attempt |
| Durable interrupt/checkpoint | Encode lazily when the checkpoint is requested |
| Resume | Decode once at Graph entry |
| Successful ordinary completion | None |

The current behavior that encodes State at Graph entry and after every node is
removed. The Graph Context carries a lazy snapshot provider rather than an
eager `GraphStateCheckpoint` value.

The initial migration does not add a new Graph retry policy. Existing Tool
Runtime retries happen inside the pure Tool node before PostHandler commits the
result, so they do not require a Graph State restore. Any future Graph-owned
node retry must use the snapshot boundary above.

## Parallel Execution

Parallel branches use independent State pointers:

```go
g.AddParallelNode("research", []graph.ParallelBranch{
    {Name: "web", Node: webRetriever, Pre: ..., Post: ...},
    {Name: "memory", Node: memoryRetriever, Pre: ..., Post: ...},
}, graph.WithStateMerger(func(ctx context.Context, base *AgentState, branches []*AgentState) error {
    // deterministic declaration-order merge
    return nil
}))
```

The base State is encoded once. Each branch decodes its own pointer, executes
without sharing mutable State, and returns its branch State to the merger in
declaration order. The merger mutates the base pointer. A branch error cancels
siblings and skips the merge.

No Handler, node, Codec or merger runs while an internal mutex is held.

## Checkpoint and Resume

The State Scope exposes a private lazy snapshot provider through Context.
Agent interruption code requests a `state.GraphStateCheckpoint` only when a
durable continuation is actually created. The provider returns Codec ID,
version and owned bytes.

Resume supplies the stored checkpoint to Graph entry. The configured Codec
must match ID, version and State type. Graph decodes once, creates the runtime
State Scope and resumes at the stored cursor.

Nested graphs do not consume the parent's State checkpoint. Parent and child
State ownership remain isolated.

## Migration

This is a full contract replacement:

- remove `StateGraph[I, O, S]` and `Cloner`;
- replace `NewStateGraph` with `NewGraph[I, O]` plus `SetStateFactory`;
- replace package `AddNode` with `g.AddNode`;
- replace package `AddConditionalEdge` with `g.AddBranch`;
- replace generic State Codec interfaces with runtime typed codecs;
- replace generic parallel types with reflected branch adapters;
- migrate Basic, ReAct, Routing and GroupChat recipes;
- migrate compose, callbacks, Graph streaming, checkpoint/resume, examples,
  README and all Graph documentation;
- do not retain legacy compatibility aliases.

## Verification

The migration follows red-green TDD for each contract. Final verification is:

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
