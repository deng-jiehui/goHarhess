# Runtime Correctness Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair the seven reviewed runtime correctness defects while preserving the current public execution contracts.

**Architecture:** Apply focused fixes inside the owning provider, recipe, node, option, persistence, and cache boundaries. Each task begins with a behavior-level regression test, makes the smallest production change that restores the documented contract, and is independently reviewable.

**Tech Stack:** Go 1.24, standard library concurrency/context primitives, `net/http/httptest`, existing `runnable`, `graph`, `agent`, `tool`, and nested `extensions` modules.

## Global Constraints

- Do not restore or add compatibility adapters for removed APIs such as `FnCallAgent`, `Assistant`, or `RunSync`.
- Do not redesign the public `runnable.Runnable`, `llm.Model`, `agent.Agent`, or `tool.BaseTool` contracts.
- Keep edits scoped to the files named by each task; do not stage unrelated dirty-worktree files.
- Every production change must be preceded by a focused test that is run and observed failing for the expected behavior.
- OpenAI-compatible responses represent one completion sequence; reject any non-zero choice index.
- `ToolsNode` enforces `Spec.Concurrency`; Agent-only middleware, retry, timeout, recursion, and authorization remain in `internal/toolruntime`.
- Race verification prepends `D:\Program Files\msys64\ucrt64\bin` to `PATH`.

---

### Task 1: Preserve Indexed OpenAI Tool-Call Deltas

**Files:**
- Modify: `pkg/llm/openai_compatible.go:338-405`
- Test: `pkg/llm/openai_compatible_test.go`

**Interfaces:**
- Consumes: OpenAI `choices[].index` and streaming `delta.tool_calls[].index` wire fields.
- Produces: `decodeOpenAIResponse([]byte, bool) ([]schema.Message, *Usage, string, error)` with stable message slots for tool deltas and explicit multi-choice errors.

- [ ] **Step 1: Write the failing sparse-delta regression test**

Add a test server that emits two interleaved tool calls where later frames contain only one sparse index:

```go
func TestOpenAICompatibleStreamingPreservesSparseToolCallIndexes(t *testing.T) {
    frames := []string{
        `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-0","function":{"name":"first","arguments":"{\"value\":\""}},{"index":1,"id":"call-1","function":{"name":"second","arguments":"{\"value\":\""}}]}}]}`,
        `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"two\"}"}}]}}]}`,
        `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"one\"}"}}]}}]}`,
    }
    server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
        writer.Header().Set("Content-Type", "text/event-stream")
        for _, frame := range frames {
            _, _ = fmt.Fprintf(writer, "data: %s\n\n", frame)
        }
        _, _ = writer.Write([]byte("data: [DONE]\n\n"))
    }))
    defer server.Close()

    model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
        Model: "indexed", BaseURL: server.URL, Client: server.Client(),
    })
    if err != nil { t.Fatal(err) }
    stream, err := model.Stream(context.Background(), llm.Request{Options: llm.ChatOptions{DeltaStream: true}})
    if err != nil { t.Fatal(err) }
    accumulator := llm.NewDeltaAccumulator(true)
    for item := range stream {
        if item.Err != nil { t.Fatal(item.Err) }
        if item.Value.Kind == llm.EventDelta {
            if _, err := accumulator.Apply(item.Value); err != nil { t.Fatal(err) }
        }
    }
    messages := accumulator.Messages()
    if len(messages) != 2 { t.Fatalf("messages=%#v", messages) }
    if messages[0].FunctionCall == nil || messages[0].FunctionCall.Name != "first" || messages[0].FunctionCall.Arguments != `{"value":"one"}` || messages[0].FunctionID("") != "call-0" {
        t.Fatalf("first call=%#v", messages[0])
    }
    if messages[1].FunctionCall == nil || messages[1].FunctionCall.Name != "second" || messages[1].FunctionCall.Arguments != `{"value":"two"}` || messages[1].FunctionID("") != "call-1" {
        t.Fatalf("second call=%#v", messages[1])
    }
}
```

Add `fmt` to the test imports.

- [ ] **Step 2: Write the failing unsupported-choice test**

```go
func TestOpenAICompatibleRejectsMultipleChoices(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
        writer.Header().Set("Content-Type", "application/json")
        _, _ = writer.Write([]byte(`{"choices":[{"index":1,"message":{"role":"assistant","content":"alternate"}}]}`))
    }))
    defer server.Close()
    model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
        Model: "single-choice", BaseURL: server.URL, Client: server.Client(),
    })
    if err != nil { t.Fatal(err) }
    _, err = model.Invoke(context.Background(), llm.Request{})
    if err == nil || !strings.Contains(err.Error(), "choice index 1") {
        t.Fatalf("choice error=%v", err)
    }
}
```

Add a table-driven streaming test for malformed tool indexes:

```go
func TestOpenAICompatibleRejectsInvalidToolCallIndexes(t *testing.T) {
    cases := map[string]string{
        "negative":  `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":-1,"function":{"name":"bad","arguments":"{}"}}]}}]}`,
        "duplicate": `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"first","arguments":"{}"}},{"index":0,"function":{"name":"second","arguments":"{}"}}]}}]}`,
    }
    for name, frame := range cases {
        t.Run(name, func(t *testing.T) {
            server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
                writer.Header().Set("Content-Type", "text/event-stream")
                _, _ = fmt.Fprintf(writer, "data: %s\n\ndata: [DONE]\n\n", frame)
            }))
            defer server.Close()
            model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
                Model: "invalid-index", BaseURL: server.URL, Client: server.Client(),
            })
            if err != nil { t.Fatal(err) }
            stream, err := model.Stream(context.Background(), llm.Request{Options: llm.ChatOptions{DeltaStream: true}})
            if err != nil { t.Fatal(err) }
            var streamErr error
            for item := range stream { if item.Err != nil { streamErr = item.Err } }
            if streamErr == nil || !strings.Contains(streamErr.Error(), "tool call index") {
                t.Fatalf("stream error=%v", streamErr)
            }
        })
    }
}
```

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```powershell
go test -count=1 ./pkg/llm -run 'TestOpenAICompatible(StreamingPreservesSparseToolCallIndexes|RejectsMultipleChoices|RejectsInvalidToolCallIndexes)$'
```

Expected: sparse call 1 merges into call 0, and the non-zero choice is accepted instead of returning the required error.

- [ ] **Step 4: Implement index-aware decoding**

Change `openAIMessageToSchema` to accept whether the frame is a delta and return an error:

```go
func openAIMessageToSchema(value openAIMessage, indexed bool) ([]schema.Message, error)
```

In `decodeOpenAIResponse`, reject `choice.Index != 0`, call the new signature, and propagate its error. For delta tool calls, validate non-negative unique indexes, allocate through the highest index, keep base text/reasoning in slot zero, and place each call in its indexed slot. For non-streaming messages, use slice order because ordinary response tool calls do not carry an `index` field.

```go
for _, choice := range response.Choices {
    if choice.Index != 0 {
        return nil, nil, "", fmt.Errorf("unsupported OpenAI-compatible choice index %d", choice.Index)
    }
    value := choice.Message
    if delta { value = choice.Delta }
    converted, err := openAIMessageToSchema(value, delta)
    if err != nil { return nil, nil, "", err }
    messages = append(messages, converted...)
}

func openAIMessageToSchema(value openAIMessage, indexed bool) ([]schema.Message, error) {
    base := schema.Message{Role: parseRole(value.Role), Content: schema.TextContent(flexibleText(value.Content))}
    // Preserve the existing reasoning fallbacks before processing calls.
    if len(value.ToolCalls) == 0 { return []schema.Message{base}, nil }
    highest := len(value.ToolCalls) - 1
    if indexed {
        highest = 0
        seen := map[int]struct{}{}
        for _, call := range value.ToolCalls {
            if call.Index < 0 { return nil, fmt.Errorf("invalid tool call index %d", call.Index) }
            if _, exists := seen[call.Index]; exists { return nil, fmt.Errorf("duplicate tool call index %d", call.Index) }
            seen[call.Index] = struct{}{}
            highest = max(highest, call.Index)
        }
    }
    messages := make([]schema.Message, highest+1)
    messages[0] = base
    for order, call := range value.ToolCalls {
        index := order
        if indexed { index = call.Index }
        target := messages[index]
        if target.Role == "" { target.Role = schema.RoleAssistant }
        target.FunctionCall = &schema.FunctionCall{Name: call.Function.Name, Arguments: call.Function.Arguments}
        if call.ID != "" {
            if target.Extra == nil { target.Extra = map[string]any{} }
            target.Extra["function_id"] = call.ID
        }
        messages[index] = target
    }
    return messages, nil
}
```

Retain the existing content/reasoning/name/legacy `function_call` handling around this indexed branch rather than replacing those fields with the abbreviated skeleton above.

- [ ] **Step 5: Run focused and package tests and verify GREEN**

```powershell
go test -count=1 ./pkg/llm -run 'TestOpenAICompatible'
go test -count=1 ./pkg/llm
```

Expected: PASS.

- [ ] **Step 6: Commit only Task 1 files**

```powershell
git add -- pkg/llm/openai_compatible.go pkg/llm/openai_compatible_test.go
git commit -m "fix: preserve indexed OpenAI tool deltas"
```

### Task 2: Keep Supervisor Tool Calls Out of Child Histories

**Files:**
- Modify: `internal/agentrecipe/routing.go`
- Modify: `internal/agentrecipe/recipe.go:279-295`
- Test: `pkg/agent/routing_test.go`

**Interfaces:**
- Consumes: routing recipe state before and after the supervisor decision.
- Produces: child `Agent.Stream` input containing the pre-decision conversation only; supervisor state still contains decision plus observation.

- [ ] **Step 1: Write the failing provider-history regression test**

```go
func TestRoutingAgentDoesNotSendSupervisorToolCallToChild(t *testing.T) {
    var childInput []schema.Message
    worker, err := agent.NewBasicAgent(agent.BaseConfig{
        Name: "worker",
        Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
            childInput = schema.CloneMessages(messages)
            return []schema.Message{schema.AssistantMessage("child result")}, nil
        }),
    })
    if err != nil { t.Fatal(err) }
    router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
        BaseConfig: agent.BaseConfig{Name: "router", Model: llm.NewScriptedModel([]schema.Message{routingCall("worker")})},
        SubAgents: []agent.Agent{worker}, Mode: agent.RoutingModeRouter,
    })
    if err != nil { t.Fatal(err) }
    if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("delegate")}); err != nil { t.Fatal(err) }
    if len(childInput) != 1 || childInput[0].Role != schema.RoleUser || childInput[0].Text() != "delegate" {
        t.Fatalf("child input=%#v", childInput)
    }
    for _, message := range childInput {
        if message.FunctionCall != nil { t.Fatalf("unresolved supervisor call leaked: %#v", message) }
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
go test -count=1 ./pkg/agent -run TestRoutingAgentDoesNotSendSupervisorToolCallToChild
```

Expected: child input includes the supervisor assistant function call.

- [ ] **Step 3: Capture pre-decision dispatch input**

Add `dispatchInput []schema.Message` to `routingRunState`. In the decision node, clone `state.Messages` into `run.dispatchInput` before appending `output`. In the dispatch node, pass `run.dispatchInput` to `hooks.Dispatch`, then clear it after results are incorporated. Extend `withSession` in `internal/agentrecipe/recipe.go` to clone this slice when cloning routing run state.

```go
type routingRunState struct {
    maxSteps      int
    returnControl bool
    steps         int
    response      []schema.Message
    pending       []Call
    dispatchInput []schema.Message
    continueRun   bool
}

// In the decision node, immediately before appending output:
run.dispatchInput = schema.CloneMessages(state.Messages)
state.Messages = append(state.Messages, schema.CloneMessages(output)...)

// In the dispatch node:
dispatchInput := schema.CloneMessages(run.dispatchInput)
results, err := hooks.Dispatch(ctx, calls, dispatchInput)
if err != nil {
    return RecipeState{}, err
}
run.dispatchInput = nil

// In withSession:
cloned.dispatchInput = schema.CloneMessages(current.dispatchInput)
```

- [ ] **Step 4: Run focused and routing tests and verify GREEN**

```powershell
go test -count=1 ./pkg/agent -run TestRoutingAgent
```

Expected: PASS, including resume and supervisor return-control tests.

- [ ] **Step 5: Commit only Task 2 files**

```powershell
git add -- internal/agentrecipe/routing.go internal/agentrecipe/recipe.go pkg/agent/routing_test.go
git commit -m "fix: isolate routed child histories"
```

### Task 3: Enforce ToolsNode Concurrency Policies

**Files:**
- Create: `pkg/tool/node_scheduler.go`
- Modify: `pkg/tool/node.go`
- Test: `pkg/tool/node_test.go`
- Modify: `docs/mcp-tools.md`

**Interfaces:**
- Consumes: normalized `tool.ConcurrencyPolicy` from each registered tool.
- Produces: node-wide and group schedulers shared across simultaneous `ToolsNode.Invoke` calls.

- [ ] **Step 1: Write failing serialization and registration tests**

Add a test that launches two invocations against the same zero-policy tool. The first handler closes `started`, waits on `release`, and the second must not enter before release:

```go
func TestToolsNodeSerializesZeroPolicyAcrossInvocations(t *testing.T) {
    started := make(chan struct{}, 2)
    release := make(chan struct{})
    value := tool.NewFunctionTool(tool.Spec{Name: "mutate"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
        started <- struct{}{}
        select {
        case <-ctx.Done(): return tool.Result{}, ctx.Err()
        case <-release: return tool.TextResult("ok"), nil
        }
    })
    node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{value}, MaxConcurrency: 2})
    if err != nil { t.Fatal(err) }
    done := make(chan error, 2)
    invoke := func(id string) { _, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("mutate", `{}`, id)}); done <- err }
    go invoke("first")
    select { case <-started: case <-time.After(time.Second): t.Fatal("first call did not start") }
    go invoke("second")
    select { case <-started: t.Fatal("zero-policy calls ran concurrently"); case <-time.After(50 * time.Millisecond): }
    release <- struct{}{}
    select { case <-started: case <-time.After(time.Second): t.Fatal("second call did not start") }
    release <- struct{}{}
    for range 2 { if err := <-done; err != nil { t.Fatal(err) } }
}
```

Also add a constructor test with two tools in group `shared` using `MaxParallel` 1 and 2; expect an error containing `already has max parallel`.

```go
func TestToolsNodeRejectsConflictingGroupLimits(t *testing.T) {
    first := tool.NewFunctionTool(tool.Spec{
        Name: "first", Concurrency: tool.ConcurrencyPolicy{Group: "shared", MaxParallel: 1},
    }, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("first"), nil })
    second := tool.NewFunctionTool(tool.Spec{
        Name: "second", Concurrency: tool.ConcurrencyPolicy{Group: "shared", MaxParallel: 2},
    }, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("second"), nil })
    _, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{first, second}})
    if err == nil || !strings.Contains(err.Error(), "already has max parallel") {
        t.Fatalf("group limit error=%v", err)
    }
}
```

Add `strings` to the test imports.

Add a second concurrency test using two tools in different groups and `ToolNodeConfig.MaxConcurrency: 1`. Start the first invocation and wait until its handler enters, then start the second invocation and assert its handler cannot enter until the first is released:

```go
func TestToolsNodeSharesMaxConcurrencyAcrossInvocations(t *testing.T) {
    started := make(chan string, 2)
    release := make(chan struct{})
    makeTool := func(name string) tool.BaseTool {
        return tool.NewFunctionTool(tool.Spec{
            Name: name, Concurrency: tool.ConcurrencyPolicy{Group: name, MaxParallel: 1},
        }, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
            started <- name
            select { case <-ctx.Done(): return tool.Result{}, ctx.Err(); case <-release: return tool.TextResult(name), nil }
        })
    }
    node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.BaseTool{makeTool("first"), makeTool("second")}, MaxConcurrency: 1})
    if err != nil { t.Fatal(err) }
    done := make(chan error, 2)
    go func() { _, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("first", `{}`, "first")}); done <- err }()
    select { case name := <-started: if name != "first" { t.Fatalf("started=%q", name) }; case <-time.After(time.Second): t.Fatal("first did not start") }
    go func() { _, err := node.Invoke(context.Background(), []schema.Message{toolCallMessage("second", `{}`, "second")}); done <- err }()
    select { case name := <-started: t.Fatalf("node limit allowed %q", name); case <-time.After(50 * time.Millisecond): }
    release <- struct{}{}
    select { case name := <-started: if name != "second" { t.Fatalf("started=%q", name) }; case <-time.After(time.Second): t.Fatal("second did not start") }
    release <- struct{}{}
    for range 2 { if err := <-done; err != nil { t.Fatal(err) } }
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

```powershell
go test -count=1 ./pkg/tool -run 'TestToolsNode(SerializesZeroPolicyAcrossInvocations|RejectsConflictingGroupLimits|SharesMaxConcurrencyAcrossInvocations)$'
```

Expected: both calls start concurrently and conflicting group limits are accepted.

- [ ] **Step 3: Implement the node-owned group scheduler**

Create `toolNodeScheduler` with a mutex and `map[string]*toolNodeGroupLimiter`. Its `register` method validates and normalizes policy, rejects inconsistent limits, and creates a persistent buffered semaphore. Its `execute` method acquires the group semaphore with the invocation context and releases it after the tool call.

Add `scheduler *toolNodeScheduler` and `semaphore chan struct{}` to `ToolsNode`, create the node semaphore once during `NewToolsNode`, and register every tool. Replace the local semaphore currently created by each `Invoke` with the persistent node semaphore. Call `scheduler.execute` inside `invokeCall` after argument/message validation and before `invokeBaseTool`.

Also add `policies map[string]ConcurrencyPolicy` to `ToolsNode` and store the validated registration snapshot by tool name. Use that snapshot during execution; do not call a potentially mutable tool's `Spec` again to choose a semaphore.

```go
type toolNodeScheduler struct {
    mu     sync.Mutex
    groups map[string]*toolNodeGroupLimiter
}

type toolNodeGroupLimiter struct {
    max       int
    semaphore chan struct{}
}

func newToolNodeScheduler() *toolNodeScheduler {
    return &toolNodeScheduler{groups: map[string]*toolNodeGroupLimiter{}}
}

func (s *toolNodeScheduler) register(policy ConcurrencyPolicy) error {
    if err := policy.Validate(); err != nil { return err }
    policy = policy.Normalize()
    if policy.MaxParallel > MaxToolsNodeConcurrency {
        return fmt.Errorf("concurrency group %q max parallel %d exceeds node limit %d", policy.Group, policy.MaxParallel, MaxToolsNodeConcurrency)
    }
    s.mu.Lock()
    defer s.mu.Unlock()
    if current := s.groups[policy.Group]; current != nil {
        if current.max != policy.MaxParallel {
            return fmt.Errorf("concurrency group %q already has max parallel %d, cannot register %d", policy.Group, current.max, policy.MaxParallel)
        }
        return nil
    }
    s.groups[policy.Group] = &toolNodeGroupLimiter{max: policy.MaxParallel, semaphore: make(chan struct{}, policy.MaxParallel)}
    return nil
}

func (s *toolNodeScheduler) execute(ctx context.Context, policy ConcurrencyPolicy, call func() (Result, error)) (Result, error) {
    policy = policy.Normalize()
    s.mu.Lock()
    limiter := s.groups[policy.Group]
    s.mu.Unlock()
    if limiter == nil { return Result{}, fmt.Errorf("concurrency group %q is not registered", policy.Group) }
    select {
    case <-ctx.Done(): return Result{}, ctx.Err()
    case limiter.semaphore <- struct{}{}:
    }
    defer func() { <-limiter.semaphore }()
    return call()
}
```

- [ ] **Step 4: Document the standalone-node boundary**

Update `docs/mcp-tools.md` to state that `ToolsNode` enforces node-wide and `Spec.Concurrency` limits, while Agent middleware, retry, timeout, recursion, and authorization require assembling tools through an Agent.

- [ ] **Step 5: Run package tests and verify GREEN**

```powershell
go test -count=1 ./pkg/tool
Push-Location extensions; go test -count=1 ./mcp; Pop-Location
```

Expected: PASS.

- [ ] **Step 6: Commit only Task 3 files**

```powershell
git add -- pkg/tool/node_scheduler.go pkg/tool/node.go pkg/tool/node_test.go docs/mcp-tools.md
git commit -m "fix: enforce ToolsNode concurrency policies"
```

### Task 4: Make OptionPlan Deltas Exact

**Files:**
- Modify: `pkg/runnable/runnable.go:105-145,226-269`
- Test: `pkg/runnable/runnable_test.go`

**Interfaces:**
- Consumes: cumulative `OptionSet` mutations performed once by each `Option`.
- Produces: forwarded planned options that replay upserts and deletions without reassigning unchanged values to a later scope.

- [ ] **Step 1: Write failing deletion and opaque-scope tests**

```go
func TestOptionPlanForwardingReplaysDeletions(t *testing.T) {
    setValue := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { set.Metadata["removed"] = "value" }))
    deleteValue := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { delete(set.Metadata, "removed") }))
    plan, err := runnable.NewOptionPlan(setValue, deleteValue)
    if err != nil { t.Fatal(err) }
    forwarded, err := runnable.ResolveOptions(plan.ForwardOptionsFor(runnable.OptionScopeGlobal)...)
    if err != nil { t.Fatal(err) }
    if _, exists := forwarded.Metadata["removed"]; exists { t.Fatalf("deleted metadata was restored: %#v", forwarded.Metadata) }
}

func TestOptionPlanDoesNotLeakOpaqueGlobalValueToNodeScope(t *testing.T) {
    marker := func() {}
    global := runnable.GlobalOption(runnable.OptionFunc(func(set *runnable.OptionSet) { set.Values["opaque"] = marker }))
    node := runnable.NodeOption(runnable.WithMetadata("provider_mode", "fast"))
    plan, err := runnable.NewOptionPlan(global, node)
    if err != nil { t.Fatal(err) }
    forwarded, err := runnable.ResolveOptions(plan.ForwardOptionsFor(runnable.OptionScopeNode)...)
    if err != nil { t.Fatal(err) }
    if _, exists := forwarded.Values["opaque"]; exists { t.Fatalf("opaque global value leaked: %#v", forwarded.Values) }
    if forwarded.Metadata["provider_mode"] != "fast" { t.Fatalf("node metadata=%#v", forwarded.Metadata) }
}
```

- [ ] **Step 2: Run focused tests and verify RED**

```powershell
go test -count=1 ./pkg/runnable -run 'TestOptionPlan(ForwardingReplaysDeletions|DoesNotLeakOpaqueGlobalValueToNodeScope)$'
```

Expected: deleted metadata is restored, and `opaque` appears in the node-only set.

- [ ] **Step 3: Add tombstones and stable non-JSON snapshots**

Extend `plannedOption` with `deletedMetadata` and `deletedValues`. `Apply` deletes those keys before applying `resolved` upserts. Change option snapshots to record every key with a stable value representation: JSON bytes when `json.Marshal` succeeds, otherwise `fmt.Sprintf("%T:%#v", value, value)`. `optionDelta` must compare all after keys for upserts and all before keys for deletions.

```go
type plannedOption struct {
    raw             Option
    scope           OptionScope
    resolved        OptionSet
    deletedMetadata []string
    deletedValues   []string
}

func (o plannedOption) Apply(target *OptionSet) error {
    if target == nil { return errors.New("runnable option set is nil") }
    if target.Metadata == nil { target.Metadata = map[string]any{} }
    if target.Values == nil { target.Values = map[string]any{} }
    for _, key := range o.deletedMetadata { delete(target.Metadata, key) }
    for _, key := range o.deletedValues { delete(target.Values, key) }
    for key, value := range o.resolved.Metadata { target.Metadata[key] = value }
    for key, value := range o.resolved.Values { target.Values[key] = value }
    return nil
}

type optionSnapshot map[string]string

func snapshotOptionMap(values map[string]any) optionSnapshot {
    snapshot := make(optionSnapshot, len(values))
    for key, value := range values { snapshot[key] = snapshotOptionValue(value) }
    return snapshot
}

func snapshotOptionValue(value any) string {
    if encoded, err := json.Marshal(value); err == nil { return "json:" + string(encoded) }
    return fmt.Sprintf("go:%T:%#v", value, value)
}
```

Have `optionDelta` return the upsert `OptionSet` plus sorted deleted-key slices by comparing both the before snapshot and the after map. Store all three results on each `plannedOption` created by `NewOptionPlan` and `Cascade`.

- [ ] **Step 4: Run focused and package tests and verify GREEN**

```powershell
go test -count=1 ./pkg/runnable
go test -count=1 ./pkg/agent -run TestAgentToolBoundaryReceivesOnlyNodeOptions
```

Expected: PASS and existing apply-once behavior remains intact.

- [ ] **Step 5: Commit only Task 4 files**

```powershell
git add -- pkg/runnable/runnable.go pkg/runnable/runnable_test.go
git commit -m "fix: preserve exact option plan deltas"
```

### Task 5: Preserve Caller System Messages in Checkpoints

**Files:**
- Modify: `pkg/agent/state.go`
- Modify: `pkg/agent/interrupt.go:150-220`
- Test: `pkg/agent/context_state_test.go`
- Test: `pkg/agent/interrupt_test.go`

**Interfaces:**
- Consumes: caller input captured before `Base.prepareMessages` and working messages containing the injected Agent prompt.
- Produces: checkpoints and continuations containing caller-owned system messages only; resume injects the current Agent prompt once.

- [ ] **Step 1: Write the failing two-turn persistence test**

Add a test using Agent system text `agent policy` and first-turn caller input `[SystemMessage("caller policy"), UserMessage("first")]`. Capture the second model request and assert its first message is exactly `agent policy\n\ncaller policy`; assert the stored checkpoint begins with `caller policy`.

```go
func TestConversationCheckpointPreservesCallerSystemMessage(t *testing.T) {
    store := state.NewMemoryStore()
    var calls [][]schema.Message
    model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
        calls = append(calls, schema.CloneMessages(messages))
        return []schema.Message{schema.AssistantMessage("answer")}, nil
    })
    value, err := agent.NewBasicAgent(agent.BaseConfig{
        SystemMessage: "agent policy", Model: model, CheckpointStore: store,
    })
    if err != nil { t.Fatal(err) }
    first := []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("first")}
    if _, err := value.Invoke(context.Background(), first, agent.WithConversationID("caller-system")); err != nil { t.Fatal(err) }
    if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("second")}, agent.WithConversationID("caller-system")); err != nil { t.Fatal(err) }
    if len(calls) != 2 || calls[1][0].Role != schema.RoleSystem || calls[1][0].Text() != "agent policy\n\ncaller policy" {
        t.Fatalf("second model context=%#v", calls)
    }
    checkpoint, found, err := store.Load(context.Background(), "caller-system")
    if err != nil || !found { t.Fatalf("found=%t err=%v", found, err) }
    if len(checkpoint.Messages) == 0 || checkpoint.Messages[0].Role != schema.RoleSystem || checkpoint.Messages[0].Text() != "caller policy" {
        t.Fatalf("checkpoint messages=%#v", checkpoint.Messages)
    }
}
```

- [ ] **Step 2: Write the failing interrupt/resume persistence test**

Interrupt before the first model call with caller system text `caller policy`. Resume with a replacement Agent system text `replacement policy`; the replacement model must receive exactly one system message with `replacement policy\n\ncaller policy` followed by the original user message.

```go
func TestInterruptedCheckpointPreservesCallerSystemMessage(t *testing.T) {
    store := state.NewMemoryStore()
    first, err := agent.NewBasicAgent(agent.BaseConfig{
        SystemMessage: "old policy", Model: llm.NewScriptedModel(), CheckpointStore: store,
        Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
            return agent.InterruptDecision{Interrupt: true, Reason: "move"}, nil
        })},
    })
    if err != nil { t.Fatal(err) }
    input := []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("continue")}
    if _, err := first.Invoke(context.Background(), input, agent.WithConversationID("caller-system-resume")); !errors.Is(err, agent.ErrInterrupted) {
        t.Fatalf("interrupt error=%v", err)
    }
    resumed, err := agent.NewBasicAgent(agent.BaseConfig{
        SystemMessage: "replacement policy", CheckpointStore: store,
        Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
            if len(messages) != 2 || messages[0].Role != schema.RoleSystem || messages[0].Text() != "replacement policy\n\ncaller policy" || messages[1].Text() != "continue" {
                t.Fatalf("resume context=%#v", messages)
            }
            return []schema.Message{schema.AssistantMessage("resumed")}, nil
        }),
    })
    if err != nil { t.Fatal(err) }
    if _, err := agent.Resume(context.Background(), resumed, "caller-system-resume"); err != nil { t.Fatal(err) }
}
```

- [ ] **Step 3: Run focused tests and verify RED**

```powershell
go test -count=1 ./pkg/agent -run 'Test(ConversationCheckpointPreservesCallerSystemMessage|InterruptedCheckpointPreservesCallerSystemMessage)$'
```

Expected: the caller system message is absent on the second turn and after resume.

- [ ] **Step 4: Preserve caller input and reconstruct portable working history**

In `loadConversation`, set `session.input` from a full clone of `incoming`, not `withoutSystemMessages(incoming)`. Add a helper that collects system messages from `session.checkpoint.Messages` and `session.input`, then appends the non-system portion of working messages. Use this helper for `Continuation.Messages` in `checkBreakpoint`. Keep filtering system messages from model output during commit.

```go
func (s *conversationSession) portableWorkingMessages(working []schema.Message) []schema.Message {
    if s == nil { return withoutSystemMessages(working) }
    callerSystems := systemMessages(s.checkpoint.Messages)
    callerSystems = append(callerSystems, systemMessages(s.input)...)
    return append(schema.CloneMessages(callerSystems), withoutSystemMessages(working)...)
}

func systemMessages(messages []schema.Message) []schema.Message {
    systems := make([]schema.Message, 0)
    for _, message := range messages {
        if message.Role == schema.RoleSystem { systems = append(systems, schema.CloneMessages([]schema.Message{message})[0]) }
    }
    return systems
}
```

In `checkBreakpoint`, set `Continuation.Messages` to `session.portableWorkingMessages(messages)`.

- [ ] **Step 5: Run focused, state, and interrupt tests and verify GREEN**

```powershell
go test -count=1 ./pkg/agent -run 'Test(AgentLoads|Conversation|Checkpoint|Interrupt|ReactResume|RoutingAgentResume)'
```

Expected: PASS with no duplicate Agent prompt and no repeated tool/sub-agent execution.

- [ ] **Step 6: Commit only Task 5 files**

```powershell
git add -- pkg/agent/state.go pkg/agent/interrupt.go pkg/agent/context_state_test.go pkg/agent/interrupt_test.go
git commit -m "fix: preserve caller system messages"
```

### Task 6: Decouple Cache Flights From the Leader Context

**Files:**
- Modify: `extensions/cache/llm/cache.go:120-460`
- Test: `extensions/cache/llm/cache_test.go`

**Interfaces:**
- Consumes: per-caller contexts and one shared upstream model stream per cache key.
- Produces: independent incremental subscriber streams and an upstream flight canceled only when every active subscriber exits.

- [ ] **Step 1: Write the failing leader-cancellation test**

Use `streamModel` with a `release` channel. Start a leader stream with a cancelable context, start a follower for the same request, wait for `Stats().Shared == 1`, cancel and drain the leader, release the upstream, and assert the follower receives `shared`, upstream calls equal 1, and cache writes equal 1.

```go
func TestCachedModelFollowerSurvivesLeaderCancellation(t *testing.T) {
    release := make(chan struct{})
    upstream := &streamModel{items: []runnable.Item[llm.Event]{delta("shared")}, release: release}
    cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "leader-cancel"})
    leaderCtx, cancelLeader := context.WithCancel(context.Background())
    leader := mustStream(t, cached, leaderCtx, "same", llm.ChatOptions{})
    follower := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
    deadline := time.Now().Add(time.Second)
    for cached.Stats().Shared != 1 && time.Now().Before(deadline) { time.Sleep(time.Millisecond) }
    if cached.Stats().Shared != 1 { t.Fatalf("stats=%#v", cached.Stats()) }
    cancelLeader()
    for range leader {}
    close(release)
    events, err := collectStream(follower)
    if err != nil { t.Fatal(err) }
    if len(events) != 1 || events[0].Messages[0].Text() != "shared" { t.Fatalf("events=%#v", events) }
    if upstream.Calls() != 1 || cached.Stats().Writes != 1 { t.Fatalf("calls=%d stats=%#v", upstream.Calls(), cached.Stats()) }
}
```

- [ ] **Step 2: Write the failing incremental-follower test**

Create a controlled model that emits `delta("first")`, waits on `release`, then emits `delta("second")` and done. Start and consume the leader until `first`, then subscribe a follower. Assert the follower receives `first` before `release` is closed. This proves followers stream history incrementally instead of waiting for flight completion.

```go
type stagedStreamModel struct {
    release chan struct{}
    calls   atomic.Int64
}

func (m *stagedStreamModel) Invoke(ctx context.Context, request llm.Request, options ...runnable.Option) (llm.Response, error) {
    stream, err := m.Stream(ctx, request, options...)
    if err != nil { return llm.Response{}, err }
    return llm.ReduceEvents(ctx, stream, request.Options.DeltaStream)
}

func (m *stagedStreamModel) Stream(ctx context.Context, _ llm.Request, _ ...runnable.Option) (runnable.Stream[llm.Event], error) {
    m.calls.Add(1)
    out := make(chan runnable.Item[llm.Event])
    go func() {
        defer close(out)
        select { case <-ctx.Done(): return; case out <- delta("first"): }
        select { case <-ctx.Done(): return; case <-m.release: }
        select { case <-ctx.Done(): return; case out <- delta("second"): }
        select { case <-ctx.Done(): return; case out <- runnable.Item[llm.Event]{Value: llm.Event{Kind: llm.EventDone}}: }
    }()
    return out, nil
}

func (*stagedStreamModel) Capabilities() llm.Capabilities { return llm.Capabilities{Streaming: true} }
func (*stagedStreamModel) ModelInfo() llm.ModelInfo { return llm.ModelInfo{Name: "staged"} }

func TestCachedModelFollowerReceivesIncrementalHistory(t *testing.T) {
    release := make(chan struct{})
    released := false
    defer func() { if !released { close(release) } }()
    upstream := &stagedStreamModel{release: release}
    cached := newMemoryCachedModel(t, upstream, cachellm.CacheConfig{ModelID: "incremental"})
    leader := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
    first := <-leader
    if first.Err != nil || first.Value.Messages[0].Text() != "first" { t.Fatalf("leader first=%#v", first) }
    follower := mustStream(t, cached, context.Background(), "same", llm.ChatOptions{})
    select {
    case item := <-follower:
        if item.Err != nil || item.Value.Messages[0].Text() != "first" { t.Fatalf("follower first=%#v", item) }
    case <-time.After(100 * time.Millisecond):
        t.Fatal("follower did not receive incremental history")
    }
    close(release)
    released = true
    for range leader {}
    for range follower {}
}
```

- [ ] **Step 3: Run focused tests and verify RED**

```powershell
go test -count=1 ./extensions/cache/llm -run 'TestCachedModel(FollowerSurvivesLeaderCancellation|FollowerReceivesIncrementalHistory)$'
```

Expected: leader cancellation fails the follower, and the incremental follower blocks until completion.

- [ ] **Step 4: Implement flight-owned context and subscriptions**

Give `cacheFlight` an internal `context.Context`, cancel function, `changed chan struct{}`, and subscriber count. Construct it with `context.WithCancel(context.WithoutCancel(leaderCtx))`. Replace `wait` with `subscribe`, which replays unseen history, waits on the current `changed` channel, and releases its subscriber count on caller cancellation or completion. `append` closes and replaces `changed`; `finish` marks terminal, closes `changed`, and cancels the internal context after final history is visible.

Create the leader subscription before opening the upstream stream. Open and consume upstream with the flight context. Remove direct leader output from `consumeFlight`; all callers, including the leader, receive events through `subscribe`. Cancel upstream only when subscriber count reaches zero before completion.

```go
type cacheFlight struct {
    ctx         context.Context
    cancel      context.CancelFunc
    mu          sync.Mutex
    history     []runnable.Item[corellm.Event]
    changed     chan struct{}
    finished    bool
    subscribers int
}

func newCacheFlight(ctx context.Context) *cacheFlight {
    control, cancel := context.WithCancel(context.WithoutCancel(ctx))
    return &cacheFlight{ctx: control, cancel: cancel, changed: make(chan struct{})}
}

func (f *cacheFlight) subscribe(ctx context.Context) runnable.Stream[corellm.Event] {
    out := make(chan runnable.Item[corellm.Event])
    f.mu.Lock()
    f.subscribers++
    f.mu.Unlock()
    go func() {
        defer close(out)
        defer f.releaseSubscriber()
        index := 0
        for {
            f.mu.Lock()
            pending := cloneEventItems(f.history[index:])
            index = len(f.history)
            finished := f.finished
            changed := f.changed
            f.mu.Unlock()
            for _, item := range pending {
                if !emitEventItem(ctx, out, item) { return }
            }
            if finished { return }
            select { case <-ctx.Done(): return; case <-changed: }
        }
    }()
    return out
}
```

Implement `releaseSubscriber`, `append`, and `finish` under the same mutex. `releaseSubscriber` cancels `f.ctx` only when the count reaches zero and `finished` is false.

- [ ] **Step 5: Run focused and full cache tests and verify GREEN**

```powershell
go test -count=1 ./extensions/cache/llm
```

Expected: PASS, including concurrent miss coalescing, cancellation, error, and cache write tests.

- [ ] **Step 6: Commit only Task 6 files**

```powershell
git add -- extensions/cache/llm/cache.go extensions/cache/llm/cache_test.go
git commit -m "fix: isolate cache flight cancellation"
```

### Task 7: Accept Empty Arguments for No-Argument Tools

**Files:**
- Modify: `pkg/agent/tool_batch.go:69-80,179-191`
- Test: `pkg/agent/fncall_test.go`

**Interfaces:**
- Consumes: finalized provider tool-call argument strings.
- Produces: the same validity semantics as `tool.ParseArgsStrict`: blank means `{}`, malformed/non-object JSON remains invalid.

- [ ] **Step 1: Write the failing no-argument tool test**

```go
func TestReactAgentAcceptsBlankArgumentsForNoArgumentTool(t *testing.T) {
    var calls int
    ping := tool.NewFunctionTool(tool.Spec{Name: "ping"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
        calls++
        if len(input.Args) != 0 { t.Fatalf("args=%#v", input.Args) }
        return tool.TextResult("pong"), nil
    })
    call := schema.Message{Role: schema.RoleAssistant, FunctionCall: &schema.FunctionCall{Name: "ping", Arguments: ""}}
    model := llm.NewScriptedModel([]schema.Message{call}, []schema.Message{schema.AssistantMessage("done")})
    value, err := agent.New(agent.BaseConfig{Model: model, Tools: []tool.Tool{ping}})
    if err != nil { t.Fatal(err) }
    output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("ping")})
    if err != nil { t.Fatal(err) }
    if calls != 1 || len(output) != 3 || output[1].Text() != "pong" { t.Fatalf("calls=%d output=%#v", calls, output) }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
go test -count=1 ./pkg/agent -run TestReactAgentAcceptsBlankArgumentsForNoArgumentTool
```

Expected: handler call count is zero and the observation reports incomplete JSON.

- [ ] **Step 3: Use the canonical strict parser for completeness**

Replace direct `json.Valid`/`json.Unmarshal` checks in `isToolCallComplete` with `tool.ParseArgsStrict`. Use the same parser error in `unresolvedToolCallResult`, preserving the unknown-tool model-visible result. Remove the unused `encoding/json` import.

```go
func (b *Base) isToolCallComplete(call toolCallRequest) bool {
    if call.Name == "" { return false }
    if _, exists := b.tools[call.Name]; !exists { return false }
    _, err := tool.ParseArgsStrict(call.Arguments)
    return err == nil
}

func (b *Base) unresolvedToolCallResult(call toolCallRequest) toolCallResult {
    if call.Name == "" { return toolCallResult{Request: call, Err: errors.New("tool call name is incomplete")} }
    if _, exists := b.tools[call.Name]; !exists {
        return toolCallResult{Request: call, Content: schema.TextContent(fmt.Sprintf("Tool %s does not exist.", call.Name))}
    }
    if _, err := tool.ParseArgsStrict(call.Arguments); err != nil {
        return toolCallResult{Request: call, Err: fmt.Errorf("tool call arguments are incomplete: %w", err)}
    }
    return toolCallResult{Request: call, Err: errors.New("tool call is incomplete")}
}
```

- [ ] **Step 4: Run focused and Agent tests and verify GREEN**

```powershell
go test -count=1 ./pkg/agent -run 'TestReactAgent(CallsToolAndContinues|AcceptsBlankArgumentsForNoArgumentTool)$'
go test -count=1 ./pkg/agent
```

Expected: PASS.

- [ ] **Step 5: Commit only Task 7 files**

```powershell
git add -- pkg/agent/tool_batch.go pkg/agent/fncall_test.go
git commit -m "fix: accept blank tool arguments"
```

### Task 8: Whole-Project Verification and Final Review

**Files:**
- Verify: all files changed by Tasks 1-7

**Interfaces:**
- Consumes: the seven independently passing task commits.
- Produces: clean root and extension module verification evidence plus a final code-review verdict.

- [ ] **Step 1: Check formatting and module consistency**

```powershell
gofmt -l .
git diff --check
go mod tidy -diff
Push-Location extensions; go mod tidy -diff; Pop-Location
```

Expected: no files from `gofmt -l`, no diff errors, and no module-file diff.

- [ ] **Step 2: Run fresh root-module verification**

```powershell
go test -count=1 ./...
go vet ./...
$env:Path='D:\Program Files\msys64\ucrt64\bin;' + $env:Path
go test -race -count=1 ./...
```

Expected: PASS with zero failures and zero race reports.

- [ ] **Step 3: Run fresh extension-module verification**

```powershell
Push-Location extensions
go test -count=1 ./...
go vet ./...
$env:Path='D:\Program Files\msys64\ucrt64\bin;' + $env:Path
go test -race -count=1 ./...
Pop-Location
```

Expected: PASS with zero failures and zero race reports.

- [ ] **Step 4: Request final code review**

Review the complete working-tree change against `docs/superpowers/specs/2026-08-11-runtime-correctness-fixes-design.md`. Critical or Important findings must be fixed and re-reviewed before proceeding.

- [ ] **Step 5: Confirm repository state**

```powershell
git status --short --branch
git log -8 --oneline --decorate
```

Expected: only explicitly deferred unrelated user files remain uncommitted; all runtime-fix files are committed and the final reviewer reports ready.
