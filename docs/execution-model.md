# Execution Model

## One execution contract

All executable components implement `runnable.Runnable[I, O]`. Optional
streaming uses `runnable.Streamer[I, E]`; the stream event type is independent
from the final output type.

- Models: `Runnable[llm.Request, llm.Response]` and
  `Streamer[llm.Request, llm.Event]`.
- Agents and message graphs: `Runnable[[]schema.Message, []schema.Message]`.
  `graph.StreamMessages` exposes their `schema.MessageEvent` data plane.
- General graphs: `graph.StateGraph[I, O, S]`, where `S` implements
  `graph.Cloner[S]`.

Graph control events never appear in either data stream. They are published to
`graph.EventSink`, and `callback.BindGraph` translates them into callback
events when observability is enabled.

## Agent recipes

`pkg/agent` contains configuration, lifecycle and provider/tool adapters. The
actual Basic, ReAct, Routing and GroupChat workflows live in
`internal/agentrecipe` and execute through `pkg/graph`.

- ReAct: `llm -> tools -> llm`, bounded by the configured model-call limit and
  Graph step budget.
- RoutingAgent: `decide -> sub-agent -> decide` in Supervisor mode, or one
  `decide -> sub-agent` transition in Router mode. Sub-agents are exposed to
  the supervisor model as standard function definitions. Use
  `RoutingModeSupervisor` or `RoutingModeRouter` when constructing a config
  from zero values; the explicit mode removes the ambiguity of a Go `bool`.
- GroupChat: one fixed `dispatch` node with a bounded self-edge. Agent
  selection is state data, not dynamically generated edges.

## Durable cursors

Every executing node pushes a JSON-clean `state.CursorFrame`. Interrupted
checkpoints persist the complete `CursorStack`; resume consumes the matching
top frame without recursively rebuilding a call stack. Runtime objects such as
contexts, goroutines, clients and tool batches are never stored.

A portable cursor can resume only at a node whose input accepts `S`, because
arbitrary heterogeneous intermediate values are intentionally not serialized.
Attempting to resume directly at another `NI` fails with
`graph.resume.type_mismatch`. Place durable breakpoints on state-typed nodes or
model the intermediate value as part of the concrete state.

Invocation options have two strictly isolated routing layers. Framework
options such as `runnable.WithMetadata` and other valid GlobalOptions cascade
into framework components such as the Context Engine. Provider- or
node-specific options must be wrapped with `runnable.NodeOption` and are the
only options passed to Model, Tool, and Graph node invocation interfaces.
Request lifecycle values such as cancellation,
deadlines, Trace, Callback and Checkpoint state remain in `context.Context`;
they are never encoded as OptionSet values. Only a nil option or an option
whose `Apply` returns an error fails the invocation.

ReAct resumes at `before_llm` after a completed tool batch is in the message
history and can rehydrate its model-call count from messages. Routing and
GroupChat also persist a typed JSON-safe `state.EngineCheckpoint`: Routing
stores the completed supervisor-call count, while GroupChat stores the round
and last-agent cursor. This prevents a resumed graph loop from dispatching a
completed child Agent twice.

## Callback ownership

One root run owns one asynchronous dispatcher. Nested Agents reuse it and
create child spans. Graph has one child operation whose node events are
progress events, preventing duplicate Graph/Agent lifecycle ownership.

## Compilation

Graph `Compile` validates synchronously before every canonical `Invoke` or
`StreamMessages` execution. Every edge is checked with `reflect.Type`: the
source node's output must be assignable to the target node's input, typed
conditions must accept the source output, and all END inputs must be assignable
to `O`. Nested graphs are compiled recursively with indirect containment-cycle
detection. Their lifecycle and data sinks remain isolated from the parent.

Reflection is used only while binding and compiling type tokens. Node
execution and State cloning do not use reflective value traversal.

## Strongly Typed State

Graph execution state is never represented as `map[string]any`. Applications
define one concrete state type and implement a native `Clone() S` method:

```go
type Request struct{ Topic string }
type Response struct{ Summary string }

type ResearchState struct {
    Topic string
    Notes []string
}

func (s ResearchState) Clone() ResearchState {
    s.Notes = append([]string(nil), s.Notes...)
    return s
}
```

`genState` converts public input `I` into the state value placed on START.
Package-level generic functions bind heterogeneous nodes because Go methods
cannot declare additional type parameters:

```go
g := graph.NewStateGraph[Request, Response, ResearchState](
    "research",
    func(_ context.Context, input Request) ResearchState {
        return ResearchState{Topic: input.Topic}
    },
)

collect := runnable.Func[ResearchState, int](func(
    _ context.Context, state ResearchState, _ ...runnable.Option,
) (int, error) {
    return len(state.Notes), nil
})

render := runnable.Func[int, Response](func(
    _ context.Context, count int, _ ...runnable.Option,
) (Response, error) {
    return Response{Summary: fmt.Sprintf("%d notes", count)}, nil
})

_ = graph.AddNode(g, "collect", collect)
_ = graph.AddNode(g, "render", render)
_ = g.AddEdge(graph.Start, "collect")
_ = g.AddEdge("collect", "render")
_ = g.AddEdge("render", graph.End)
```

The public API remains strongly typed. Internally, `AddNode` creates a private
`anyNode` containing input/output type tokens and an invocation closure. A
runtime value is erased only inside the Graph engine and is restored to the
node's generic input before user code runs. Type mismatches therefore fail in
`Compile`, rather than surfacing as application-level type assertions.

`S.Clone()` owns every pointer, slice, map, or custom resource copy policy.
The Graph calls native Clone code before handing S to a node and before
returning S as O. Clone panics become execution errors; there is no reflective
or shallow-copy fallback.
