# Execution Model

## One execution contract

All executable components implement `runnable.Runnable[I, O]`. Optional
streaming uses `runnable.Streamer[I, E]`; the stream event type is independent
from the final output type.

- Models: `Runnable[llm.Request, llm.Response]` and
  `Streamer[llm.Request, llm.Event]`.
- ToolsNode: `Runnable[[]schema.Message, []schema.Message]`.
- Agents and message graphs: `Runnable[[]schema.Message, []schema.Message]`.
- General graphs: `graph.Graph[I, O]`.

`graph.StreamMessages` exposes the message data plane. Graph lifecycle events
never enter that stream; they are published to `graph.EventSink`, and
`callback.BindGraph` translates them into callback events.

## Graph boundary and pure nodes

Only the public graph input and output are generic:

```go
g := graph.NewGraph[Request, Response]("research")
```

`g.AddNode` accepts any component with this method shape:

```go
Invoke(context.Context, NI, ...runnable.Option) (NO, error)
```

ChatModel, ToolsNode, MCP, Retriever and child Agent nodes remain ordinary
Runnables. They never import or inspect Graph State. `AddNode` records their
input/output `reflect.Type` once; `Compile` validates every edge and END output
before execution.

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

Every executing node pushes a JSON-clean `state.CursorFrame`. Interrupted
checkpoints persist the complete `CursorStack`; resume consumes the matching
top frame without rebuilding a Go call stack. Runtime objects such as contexts,
goroutines, clients and tool batches are never stored.

A resumed runtime State can enter only a node with a PreHandler. Arbitrary
heterogeneous edge values are not serialized. Attempting to resume another
node fails with `graph.resume.type_mismatch`.

Built-in Agent recipes request `graph.StateCheckpointFromContext` only after a
Breakpointer chooses to interrupt. Normal sequential runs therefore perform no
State encoding. Agent prompts are injected immediately before each model call;
the durable State contains caller-owned conversation history, not an old Agent
instance's system prompt.

## Options and Context

Framework options such as metadata cascade through `runnable.OptionPlan`.
Provider- or node-specific options use `runnable.NodeOption`; graph-local node
options use `graph.WithNodeOptions`. State handlers are Graph options and are
never forwarded to the pure node.

Cancellation, deadlines, Trace, Callback, identity and Checkpoint values remain
in `context.Context`. They are not copied into `OptionSet` or persisted as
runtime State.

## Agent recipes

`pkg/agent` is a fast assembly layer over the same Graph engine:

- Basic: one model node.
- ReAct: `llm -> tools -> llm`, bounded by model-call and Graph step limits.
- Routing: `decide -> dispatch -> decide` in Supervisor mode, or one dispatch
  in Router mode.
- GroupChat: one fixed dispatch node with a bounded self-edge.

Nested Agents and Graphs create independent lifecycle scopes. Graph nodes have
their own child spans, so APM systems retain node-level waterfall timing.

## Compilation

`Compile` freezes the graph and validates topology, reachability, conditional
fallbacks, node and handler types, END output, State Factory, State Codec,
parallel branches and nested graph cycles. Reflection is used to bind and
validate types and to invoke erased node adapters; application nodes still
receive their declared Go types.
