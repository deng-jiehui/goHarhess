# Runtime Correctness Fixes Design

## Goal

Repair the seven runtime correctness defects identified during review without
restoring removed legacy APIs or redesigning the framework's public execution
contracts.

## Scope

This change covers:

1. Indexed OpenAI-compatible streaming tool calls.
2. Valid child-agent history in routing workflows.
3. `ToolsNode` enforcement of `tool.Spec.Concurrency`.
4. Exact `OptionPlan` forwarding for deletions and non-JSON values.
5. Durable preservation of caller-owned system messages.
6. Singleflight cache lifetime independent of the first caller.
7. Empty tool arguments normalized as an empty JSON object.

Legacy compatibility adapters for removed APIs such as `FnCallAgent`,
`Assistant`, and `RunSync` are explicitly out of scope.

## Design

### OpenAI-Compatible Streams

The public model contract represents one completion sequence, so the adapter
will reject a response containing a non-zero choice index instead of silently
mixing multiple alternatives. Within choice zero, decoded tool-call deltas will
be placed by `tool_calls[].index`. Sparse frames will preserve empty positions
so `DeltaAccumulator` continues merging each tool call into its stable slot.
Tool call index zero may share the first assistant message with text or
reasoning content, matching the existing single-call representation.

Malformed frames with duplicate or negative indexes will return a provider
decode error. Regression tests will cover interleaved sparse tool-call deltas,
multiple tool calls in one frame, and unsupported multiple choices.

### Routing History

The routing recipe will retain a snapshot of the conversation immediately
before the supervisor decision. Child agents will receive that snapshot, not
the assistant tool-call message used to select them. The supervisor workflow
will still retain the decision message and append matching function
observations before its next model call.

This keeps every provider-bound history valid: an assistant tool call is never
sent to an unrelated child model without its matching tool result.

### ToolsNode Concurrency

Each `ToolsNode` will own a persistent concurrency scheduler shared by all of
its invocations. Registration validates every tool's normalized group and
requires a consistent `MaxParallel` value inside a group. Execution acquires
both the node-wide semaphore and the tool's group semaphore.

The zero-value policy therefore serializes undeclared tools across concurrent
calls to the same node. Agent-only middleware, retry, timeout, recursion, and
authorization controls remain owned by `internal/toolruntime`; the standalone
node documentation will state this boundary rather than duplicating the Agent
runtime.

### OptionPlan Forwarding

An option delta will contain both upserts and explicit deleted keys. Applying a
forwarded delta will process deletions before upserts. Snapshot comparison will
distinguish unchanged non-JSON values from values actually changed by the
current option, so a later option cannot claim a value owned by an earlier
scope.

Tests will verify deletion replay, Global/Node isolation with function-backed
values, and the existing apply-once guarantee.

### Durable System Messages

Checkpoint input will preserve system messages supplied by the caller because
it is captured before `Base.prepareMessages` injects the Agent prompt.
Ordinary commits will store that caller input unchanged while continuing to
exclude system messages produced in model output.

Interrupted working histories will be rebuilt from caller-owned checkpoint
and current input system messages plus the non-system working conversation.
Resume then injects the current Agent prompt exactly once. Tests will cover a
two-turn conversation and interrupt/resume with both caller and Agent system
instructions.

### Cache Singleflight Lifetime

A cache flight will own an internal cancelable context derived with
`context.WithoutCancel` from the initiating call, preserving values but not the
leader's cancellation. Every caller subscribes independently to an incremental
flight history and releases its subscription on completion or cancellation.
The upstream request is canceled only when all active subscribers leave before
the flight finishes.

The first caller can therefore cancel without failing a live follower. Event
delivery remains ordered and streaming; followers do not wait for the entire
response before receiving already available events.

### Empty Tool Arguments

Agent tool-call completeness will use `tool.ParseArgsStrict` as the canonical
validator. Blank or whitespace arguments resolve to an empty object, while
arrays, `null`, malformed JSON, and incomplete JSON remain invalid.

## Error Handling

New provider validation errors will identify the unsupported choice or invalid
tool-call index. Scheduler admission remains context-aware. Cache subscribers
receive their own context error without poisoning other subscribers, while
real upstream failures remain shared with every active subscriber.

## Verification

Each defect will follow a red-green TDD cycle with a focused regression test.
After all focused tests pass, verification will run:

```text
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...

cd extensions
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
```

Race runs will prepend `D:\Program Files\msys64\ucrt64\bin` to `PATH` so Go
can use the installed MinGW-w64 GCC toolchain.
