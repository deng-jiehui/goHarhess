# Graph Interrupt and Resume Design

## Goal

Move durable interruption and recovery into the Graph engine while keeping the
runtime local, lightweight and provider-neutral. The public design has three
primitives:

1. Graph-owned portable Continuations and a native resume lifecycle.
2. A synchronous PreTool batch hook that can stop all tool calls before any
   side effect starts.
3. A byte-oriented CheckpointStore contract with no distributed coordination
   policy.

The framework provides at-least-once recovery. Cross-process serialization,
leases, distributed locks, task ownership and exactly-once side effects remain
application responsibilities.

## Scope

This change covers:

- cooperative Graph interruption through a typed `context.CancelCause`;
- inline interruption requests returned by user components such as PreTool
  hooks;
- persistent Graph State and scheduler cursor snapshots;
- `Graph.Resume` and message-oriented `graph.ResumeMessages`;
- whole-batch interruption before Tool scheduling;
- migration of built-in Agent recipes to Graph-owned Continuations;
- separation of conversation history storage from execution checkpoints.

This change does not add:

- a RunID registry or process-wide active-run table;
- blocked goroutines that wait for a later resume call;
- Redis locks, leases, fencing tokens, renewal workers or CAS semantics;
- automatic deletion or one-time consumption of checkpoints;
- exactly-once execution for arbitrary nodes or tools;
- recovery of arbitrary stateless heterogeneous Graphs;
- compatibility with the previous Agent Continuation wire format.

## Package Boundaries

### Control Protocol

`pkg/control` contains only cross-component control-plane contracts:

```go
package control

type InterruptRequest struct {
    Reason   string         `json:"reason,omitempty"`
    Metadata map[string]any `json:"metadata,omitempty"`
}

func RequestInterrupt(reason string, metadata map[string]any) error
func AsInterrupt(err error) (InterruptRequest, bool)

type CheckpointStore interface {
    Save(ctx context.Context, key string, data []byte) error
    Load(ctx context.Context, key string) (data []byte, found bool, err error)
}
```

`InterruptRequest` implements `error`. Constructors and accessors defensively
copy Metadata. The package contains no Graph, Tool or Agent types.

The dependency direction is:

```text
pkg/control
   ^          ^
pkg/graph   pkg/tool
   ^          ^
   +-- pkg/agent --+
```

### Graph Ownership

`pkg/graph` owns the Continuation wire format, safe-point algorithm, checkpoint
validation and resume API. It does not import `pkg/tool`.

### Tool Ownership

`pkg/tool` owns the PreTool batch description and hook. It imports only the
shared control protocol and does not know how Graph persists a Continuation.

### Agent Ownership

`pkg/agent` adapts built-in Basic, ReAct, Routing and GroupChat recipes to the
Graph lifecycle. It does not retain a second execution-cursor state machine.

## Public Graph API

Checkpoint configuration is one invocation option:

```go
output, err := g.Invoke(ctx, input,
    graph.WithCheckpoint(store, "tenant-a:task-42"),
)

output, err = g.Resume(ctx,
    graph.WithCheckpoint(store, "tenant-a:task-42"),
)

stream, err := graph.ResumeMessages(ctx, g,
    graph.WithCheckpoint(store, "tenant-a:task-42"),
)
```

`WithCheckpoint` requires a non-nil Store and a non-empty application-owned
key. Graph treats the key as opaque and never derives tenant, user, session or
task identity from it.

For a typed external interruption, applications retain cancellation ownership:

```go
ctx, cancel := context.WithCancelCause(parent)

go func() {
    <-pauseRequested
    cancel(control.RequestInterrupt(
        "user paused",
        map[string]any{"operator": "user-42"},
    ))
}()

output, err := g.Invoke(ctx, input, graph.WithCheckpoint(store, key))
```

`graph.RequestInterrupt` may be provided as a convenience alias that creates
the same `control.InterruptRequest`. Graph does not expose a mutable
`g.Interrupt()` registry API because one Graph instance may serve concurrent
invocations.

## Portable Continuation

The Graph wire format is provider-neutral and contains no goroutine, Context,
channel, function, node instance, model, client, callback or credential:

```go
type Continuation struct {
    SchemaVersion int                        `json:"schema_version"`
    GraphName     string                     `json:"graph_name"`
    GraphRevision string                     `json:"graph_revision"`
    ResumeNode    string                     `json:"resume_node"`
    Step          int                        `json:"step"`
    Visits        map[string]int             `json:"visits,omitempty"`
    State         state.GraphStateCheckpoint `json:"state"`
    CursorStack   state.CursorStack          `json:"cursor_stack,omitempty"`
    Point         InterruptPoint             `json:"point"`
    Reason        string                     `json:"reason,omitempty"`
    Metadata      map[string]any             `json:"metadata,omitempty"`
    CreatedAt     time.Time                  `json:"created_at"`
}
```

The Continuation records the next node boundary and Graph State, not an
arbitrary heterogeneous edge value. `ResumeNode` is always a real node ID.
`Visits` and `Step` preserve maximum-step and loop accounting across resumes.

`GraphRevision` is a deterministic digest of the compiled recovery contract:

- Graph name;
- ordered node IDs and their input/output type names;
- ordered edges, branch labels and fallback shape;
- parallel-node declaration shape;
- State type, State Codec ID and State Codec version.

It deliberately excludes function addresses, closure captures, model
credentials and runtime configuration. Applications making an incompatible
behavior change without changing topology must bump the State Codec version or
use a new Graph name. A revision mismatch is rejected before any user node is
called.

The existing strict State Codec contract remains authoritative. A resumable
Graph must have a State Factory and State Codec. Every node that can be a
resume target must have a PreHandler capable of rebuilding its input from
State. Compile always derives an immutable resumability report from the frozen
Graph. An Invoke carrying `WithCheckpoint`, and every Resume, validates that
report before State Factory, PreHandler or node execution. Ordinary Invoke is
not rejected merely because a Graph is not resumable.

Stateless Graphs continue to support ordinary Invoke and Stream. If they
receive an interruption request, they return `ErrGraphNotResumable` and do not
claim that a durable interruption was created.

## Interruption State Machine

Graph uses release-the-stack semantics:

```text
Invoke
  |
  v
Running ---- ordinary cancel/deadline ----> Failed
  |
  +---- InterruptRequest ----> Persisting ----> Interrupted
                                      |
                                      +-------> Failed

Interrupted ---- Resume + Load ----> Running ----> Completed
```

No execution stack remains alive after `ErrInterrupted`. Resume creates a new
runtime Context and Graph State instance from portable data.

Only a typed `control.InterruptRequest` returned by a user component or found
through `context.Cause(ctx)` requests persistence. Plain `context.Canceled` and
`context.DeadlineExceeded` preserve their original meaning and never create a
Continuation. If a node returns an error after its Context was canceled, Graph
checks the typed cancellation cause before classifying the node failure.

Checkpoint saving uses `context.WithoutCancel` to retain Context values and a
bounded Graph checkpoint timeout. `ErrInterrupted` is returned only after Save
succeeds. Encoding or Save failures are ordinary execution failures and remain
in the returned error chain.

## Node Safe-Point Algorithm

Each node invocation has three logical phases:

```text
Prepare (replayable) -> Invoke (replayable) -> Commit (non-interruptible)
```

The engine executes these steps:

1. Before Prepare, inspect the Context. A typed interrupt persists a
   Continuation targeting the current node. An ordinary cancellation returns
   immediately.
2. Run PreHandler and the node with the original runtime Context.
3. After the node returns, inspect the Context again. A typed interrupt
   discards the node output, skips PostHandler and persists a Continuation
   targeting the current node.
4. Enter Commit with a Context that preserves values but no longer observes
   cancellation. Run PostHandler and select the next edge. These functions
   must be short, deterministic State reducers without external I/O.
5. After Commit, inspect the original Context. A typed interrupt persists the
   committed State and targets the selected next node. Otherwise scheduling
   continues.

PreHandlers are read-only projections from State. PostHandlers, branch
predicates and State mergers must not perform external side effects. A custom
node interrupted during Invoke may run again after Resume, so arbitrary node
side effects are at-least-once and require application idempotency.

### Parallel Nodes

A parallel node is one recovery unit. If an interruption reaches any branch,
the engine cancels siblings, skips State merge and persists the base State with
the parallel node as `ResumeNode`. Resume re-executes all branches. External
branch side effects remain at-least-once.

## PreTool Batch Hook

The Tool API exposes an immutable batch view:

```go
type BatchCall struct {
    ID       string
    Spec     Spec
    RawArgs  string
    Args     map[string]any
    Metadata map[string]any
}

type Batch struct {
    Calls   []BatchCall
    Subject Subject
}

type BeforeBatchHook interface {
    BeforeBatch(context.Context, Batch) error
}

type BeforeBatchFunc func(context.Context, Batch) error
```

The Agent construction API is:

```go
assistant, err := agent.New(config,
    agent.WithBeforeToolBatch(approval),
)
```

The runtime sequence is strict:

```text
parse and validate every call
        |
build a defensive whole-batch snapshot
        |
run BeforeBatch hooks in registration order
        |
        +-- InterruptRequest -> persist Graph Continuation; execute zero tools
        +-- ordinary error   -> fail the batch; execute zero tools
        +-- nil              -> admit the whole batch to the scheduler
```

Batch Calls, Specs, Args, Metadata and Subject are defensively copied. Hooks
may allow, interrupt or reject a batch but cannot rewrite calls. The first
error stops later hooks. Hook execution is synchronous and receives the
runtime Context.

PreTool is a control-plane hook, not a Callback and not ordinary Tool
Middleware. Middleware retains execution-time authorization, transformation,
audit and wrapping responsibilities. On Resume, PreTool hooks run again; an
approval implementation should inspect an application-provided approval token
and return nil after approval.

The whole batch is evaluated before scheduler tasks are created. One
interruption therefore guarantees zero Tool executions from that batch before
the durable checkpoint is saved.

## Checkpoint Store Contract

CheckpointStore is intentionally byte-oriented:

```go
type CheckpointStore interface {
    Save(ctx context.Context, key string, data []byte) error
    Load(ctx context.Context, key string) ([]byte, bool, error)
}
```

Graph owns encoding, validation and defensive byte copies. Store owns only
storage and retrieval.

Behavior is:

- an ordinary first Invoke does not Load or Save;
- an explicit interruption encodes and Saves once;
- Resume Loads once and validates before executing user code;
- missing data returns `ErrCheckpointNotFound`;
- successful completion does not delete or overwrite the checkpoint;
- the same checkpoint may be resumed repeatedly.

This is explicitly at-least-once recovery. Applications running multiple
replicas must serialize Resume calls before entering the framework, for
example with a gateway lock, Redis SETNX, a unique queue task or a database
transaction. The framework does not provide distributed ownership.

## Errors

Graph exposes stable sentinel errors:

```go
var (
    ErrInterrupted          = errors.New("graph execution interrupted")
    ErrCheckpointRequired   = errors.New("graph interruption requires a checkpoint")
    ErrCheckpointNotFound   = errors.New("graph checkpoint not found")
    ErrInvalidContinuation  = errors.New("invalid graph continuation")
    ErrGraphRevisionMismatch = errors.New("graph revision mismatch")
    ErrGraphNotResumable    = errors.New("graph is not resumable")
)
```

Successful persistence returns:

```go
type InterruptedError struct {
    Point        InterruptPoint
    Reason       string
    Continuation Continuation
}
```

`InterruptedError` unwraps to `ErrInterrupted` and returns defensive copies.
Store and Codec failures preserve their causes with `%w`. A malformed schema,
unknown resume node, invalid counters, Codec mismatch or Graph revision
mismatch is rejected before State Factory, PreHandler or node execution.

## Agent Migration

Built-in Agents use the Graph engine as their only execution Continuation:

- Basic, ReAct, Routing and GroupChat recipes expose all recovery-safe state
  through their Graph State and Codec;
- Agent-owned Breakpoint cursor reconstruction is removed;
- `InterruptSignal` is removed in favor of typed Context cancellation causes;
- `agent.Resume` and `agent.ResumeStream` remain convenience wrappers over
  `Graph.Resume` and `graph.ResumeMessages`;
- Agent `WithCheckpoint` forwards the Graph checkpoint option;
- `WithBeforeToolBatch` installs the Tool batch control hook;
- conversation-history Store behavior remains separate and unchanged.

The previous Agent Continuation format is not accepted by the new Graph
engine. There is no compatibility branch for removed Breakpointer or
InterruptSignal APIs.

## Verification

Focused tests must prove:

- interruption before a sequential node resumes at that node;
- interruption during node Invoke skips PostHandler and replays the node;
- interruption after Commit resumes at the selected next node;
- plain cancellation and deadlines never Save;
- Save failure never returns `ErrInterrupted`;
- schema, revision, Codec and node mismatches fail before user code;
- stateless or missing-PreHandler Graphs cannot enable checkpoints;
- PreTool interruption executes zero calls from the entire batch;
- approved Resume re-runs the hook and then admits the whole batch;
- parallel interruption skips merge and replays the complete parallel node;
- ordinary Invoke does not touch CheckpointStore;
- repeated Resume demonstrates documented at-least-once behavior;
- nested Graph State and cursor ownership remain isolated;
- Checkpoint and hook data are defensively copied;
- no user Hook, Handler, Codec or node runs under an engine mutex.

Final verification is:

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
