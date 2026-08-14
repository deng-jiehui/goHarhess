# Execution Model

## One execution contract

All executable components implement the complete `runnable.Runnable[I, O]`
contract: `Invoke`, `Stream`, `Collect`, and `Transform`.

- Models: `Runnable[llm.Request, llm.Response]`.
- ToolsNode: `Runnable[[]schema.Message, []schema.Message]`.
- Agents and message graphs: `Runnable[[]schema.Message, []schema.Message]`.
- General graphs compile to `Runnable[I, O]`.

`Stream` returns `*runnable.StreamReader[O]`; a compiled Graph exposes only its
complete END result. Live model chunks, tool progress, and Agent snapshots use
typed Context EventStream callbacks. Graph lifecycle events use the typed Graph
Callback interface and never enter the business data Reader.

## Graph boundary and pure nodes

Only the public graph input and output are generic:

```go
g := graph.NewGraph[Request, Response]("research")
```

`g.AddNode` accepts any component with this method shape:

```go
Invoke(context.Context, NI, ...runnable.Option) (NO, error)
```

ChatModelNode, ToolsNode, MCP, Retriever and child Agent nodes remain ordinary
Runnables. They never import or inspect Graph State. Wrap a provider model with
`llm.NewChatModelNode` before mounting it so model lifecycle callbacks and the
`chat_model` node kind are attached at the component boundary. `AddNode` records
the component kind and input/output `reflect.Type` once; `Compile` validates
every edge and END output before execution.

```go
_ = g.AddNode("collect", collector)
_ = g.AddNode("render", renderer)
_ = g.AddEdge(graph.Start, "collect")
_ = g.AddEdge("collect", "render")
_ = g.AddEdge("render", graph.End)
_ = g.Compile()
```

Branches are also instance methods. A predicate may consume the source node
output or the exact runtime State pointer:

```go
_ = g.AddBranch("chat", "tools", "has_tool_calls",
    func(_ context.Context, state *AgentState) (bool, error) {
        return state.HasToolCalls, nil
    },
)
_ = g.AddEdge("chat", "finish") // unconditional fallback
```

## Runtime State

State is optional. Stateful graphs register one factory whose result is a
non-nil pointer to a struct:

```go
type ResearchState struct {
    Topic string
    Notes []string
}

_ = g.SetStateFactory(func(_ context.Context, input Request) (*ResearchState, error) {
    return &ResearchState{Topic: input.Topic}, nil
})
```

Every invocation owns one concrete `*ResearchState`. Sequential nodes reuse
that exact pointer. Users do not implement `Clone`, call `GetState`, or mutate
State from pure components. Graph-owned handlers are the mapping boundary:

```go
_ = g.AddNode("search", retriever,
    graph.WithPreHandler(func(
        _ context.Context, state *ResearchState,
    ) (retrieval.Query, error) {
        return retrieval.Query{Text: state.Topic}, nil
    }),
    graph.WithPostHandler(func(
        _ context.Context, state *ResearchState, output retrieval.Result,
    ) error {
        state.Notes = append(state.Notes, output.Text)
        return nil
    }),
)
```

Without a PreHandler, the incoming edge value must be assignable to the node
input. A PostHandler mutates State but does not replace the outgoing edge value;
the node output remains available to downstream nodes and output-based branches.

No user function runs while Graph holds its topology lock. Nodes and handlers
receive the node execution Context, including cancellation, deadline, Trace and
Callback span values.

## State snapshots

Sequential execution performs no State serialization. Snapshot work happens
only at ownership or durability boundaries:

| Boundary | Codec work |
| --- | --- |
| Sequential node or branch | none |
| Fan-Out | encode base once, decode one State per branch |
| Durable interrupt | encode lazily when the checkpoint is requested |
| Resume | decode once at Graph entry |
| Ordinary completion | none |

The default State Codec is strict JSON. State fields must be exported and may
not use `json:"-"`; channels, functions, unsafe pointers and other runtime
resources are rejected. Dynamic values stored in `any` must retain their exact
Go type across a snapshot. Install a typed custom Codec for protobuf, msgpack,
private fields or domain-specific resources:

```go
codec, err := graph.NewStateCodec[*ResearchState](
    "research-state", 1, encodeResearchState, decodeResearchState,
)
if err == nil {
    err = g.SetStateCodec(codec)
}
```

Fan-Out branches receive independent State pointers and merge in declaration
order through an explicit `WithStateMerger`. A failed branch cancels siblings
and skips the merge.

## Durable cursors

Every executing node pushes a JSON-clean `graph.CursorFrame`. Interrupted
checkpoints persist the complete `CursorStack`; resume consumes the matching
top frame without rebuilding a Go call stack. Runtime objects such as contexts,
goroutines, clients and tool batches are never stored.

A resumable Graph requires a State Codec and a PreHandler for every possible
resume target. Arbitrary heterogeneous edge values are not serialized. Graph
validates the checkpoint schema, graph name, deterministic revision, Codec and
resume node before invoking user code.

Only a typed `control.InterruptRequest` returned by a component or installed as
the cause of `context.WithCancelCause` creates a Continuation. Plain cancellation
and deadlines never save one. Configure persistence per invocation and resume
with the same application-owned key:

```go
output, err := g.Invoke(ctx, input, graph.WithCheckpoint(store, key))
if errors.Is(err, graph.ErrInterrupted) {
    output, err = g.Resume(context.Background(), graph.WithCheckpoint(store, key))
}
```

Recovery is at-least-once. A node interrupted during execution and every branch
of an interrupted parallel node may run again. The framework deliberately does
not provide leases, distributed locks, fencing or one-time checkpoint deletion;
multi-replica applications must serialize Resume before entering Graph.

Built-in Agent recipes use this same Graph Continuation. Normal sequential runs
perform no State encoding. Agent prompts are injected immediately before each
model call, so durable State contains caller-owned conversation history rather
than an old Agent instance's prompt or credentials.

## Options and Context

Framework options such as metadata cascade through `runnable.OptionPlan`.
Provider- or node-specific options use `runnable.NodeOption`; graph-local node
options use `graph.WithNodeOptions`. State handlers are Graph options and are
never forwarded to the pure node.

Cancellation, deadlines, Trace, Callback and identity remain in
`context.Context`. Graph checkpoint configuration is an invocation option and
is consumed by Graph rather than forwarded to pure nodes.

## Agent recipes

`pkg/agent` is a fast assembly layer over the same Graph engine:

- Basic: one `chat_model` node.
- ReAct: `chat_model -> tools -> chat_model`, bounded by model-call and Graph
  step limits.
- Routing: `decide(chat_model) -> agent:<name> -> decide` in Supervisor mode;
  Router mode executes one selected Agent node and finishes.
- GroupChat: `select -> agent:<name> -> select`, bounded by round count.

`agent.AgentNode` is the public child-node adapter. It returns a typed
`runnable.Outcome` so Supervisor mode can turn a child failure into an
observation while Router and GroupChat can propagate the same failure. Child
execution is still a real Graph node, so snapshots, callbacks and Continuation
resume points expose the selected Agent name instead of an opaque dispatch step.

Nested Agents and Graphs create independent lifecycle scopes. Graph nodes have
their own child spans, so APM systems retain node-level waterfall timing.
`graph.NodeExecution` carries the compiled node kind and input/output types in
addition to node ID and step.

## Compilation

`Compile` freezes the graph and validates topology, reachability, conditional
fallbacks, node and handler types, END output, State Factory, State Codec,
parallel branches and nested graph cycles. Reflection is used to bind and
validate types and to invoke erased node adapters; application nodes still
receive their declared Go types.
