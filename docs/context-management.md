# 状态持久化与上下文治理

上下文管理分为三个边界明确的层次：

```text
State / Checkpoint
  Save / Load / versioned Store contract
          |
          v
Context Engine
  stable prefix + role repair + token budget + prompt cache alignment
          |
          v
[]schema.Message
  provider-neutral LLM payload
```

`pkg/state` 只负责完整会话状态，不负责决定本次模型窗口保留哪些消息；`pkg/contextengine` 只负责生成一次 LLM 请求，不修改持久化的完整历史。

## Checkpoint Store

The core module depends only on `state.Store`. Redis persistence is provided
by the optional `goharness/extensions` module; import it as
`redisstore "goharness/extensions/state/redis"` when needed.

内存 Store 适合单进程应用和测试：

```go
store := state.NewMemoryStore(state.MemoryStoreConfig{
    TTL: 24 * time.Hour,
})
```

Redis Store 使用 Lua CAS 原子校验版本，避免两个请求同时覆盖同一段会话：

```go
store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{
    Client: redisClient,
    Prefix: "customer-support:checkpoint",
    TTL:    30 * 24 * time.Hour,
})
```

`Checkpoint.Version` 为零时创建新会话。每次成功 `Save` 版本加一；旧版本写入返回 `state.ErrConflict` 和包含 `Expected/Actual` 的 `state.ConflictError`。

```go
checkpoint, found, err := store.Load(ctx, conversationID)
checkpoint.Messages = append(checkpoint.Messages, newMessages...)
saved, err := store.Save(ctx, checkpoint)
```

所有 Store 都会克隆消息与 Values，调用方修改返回值不会污染已保存状态。

## Interrupt 与 Resume

Checkpoint 是可传输的数据，不保存 goroutine、`context.Context`、HTTP/Redis client、Callback、模型对象、API Key 或任何运行时闭包。公开结构包含：

- `schema_version`、`conversation_id`、`version`、`status` 和时间戳。
- 已完成回合的 `messages` 与 JSON 可序列化 `values`。
- 中断时的 `continuation`：Agent 类型、安全点、工作消息、已完成响应、计数、cursor 和用户元数据。

Store 保存和 `Checkpoint.JSON()` 都会验证这些字段可 JSON 序列化；函数、channel、client、context 等运行时对象会被拒绝。

用户可以实现同步的私有 `Breakpointer`，在一致性安全点决定是否保存中断：

```go
approval := agent.BreakpointFunc(func(ctx context.Context, point agent.Breakpoint) (agent.InterruptDecision, error) {
    if point.Point == agent.BreakpointBeforeLLM && point.LLMCalls >= 1 {
        return agent.InterruptDecision{
            Interrupt: true,
            Reason:    "awaiting human approval",
            Metadata:  map[string]any{"ticket": "SUP-42"},
        }, nil
    }
    return agent.InterruptDecision{}, nil
})

value, err := agent.New(agent.BaseConfig{
    Model:           model,
    CheckpointStore: store,
    Breakpointers:   []agent.Breakpointer{approval},
})
```

安全点为：

- `before_llm`：上一轮工具结果已完整写入工作消息后。
- `before_round`：GroupChat 的完整轮次之间。
- `before_commit`：最终响应已经获得但尚未提交 Checkpoint；适合客户端在流结束前断开连接。

也可以由其他 goroutine 发出信号，框架会在下一个安全点消费它：

```go
signal := agent.NewInterruptSignal()
go func() {
    <-clientDisconnected
    signal.Interrupt("client disconnected", map[string]any{"transport": "websocket"})
}()

stream, err := value.Stream(
    ctx,
    messages,
    agent.WithConversationID(sessionID),
    agent.WithInterruptSignal(signal),
)
```

中断表现为可用 `errors.Is(err, agent.ErrInterrupted)` 判断的 `*agent.InterruptedError`。该错误携带 `PortableCheckpoint()`，可直接 JSON 传给其他进程或设备。

恢复时创建新的 Agent 实例即可，因此可替换模型、Provider Endpoint 或 API Key；这些连接身份不在 Checkpoint 内：

```go
replacement, err := agent.New(agent.BaseConfig{
    Model:           replacementModel, // new provider / new key
    CheckpointStore: store,
})

messages, err := agent.Resume(ctx, replacement, sessionID)
```

同一 `conversation_id` 的未完成 continuation 必须通过 `Resume` 恢复，普通 `Invoke` 会返回 `agent.ErrResumeRequired`，避免误把新输入追加到中断的工具回合。`before_commit` 恢复只提交已保存的最终响应，不会再次调用模型；`before_llm` 恢复会从已保存的工具结果继续下一次模型调用。

## Context Engine

默认 Agent 会在每次 LLM 调用前执行 Context Engine。它会：

- 将稳定前缀与 System 消息放在最前面。
- 修复 Assistant/Function 孤儿消息和无效 Role 顺序；也可使用严格模式直接报错。
- 按完整 User Turn 从最旧历史开始裁切，避免拆散工具调用上下文。
- 单个最新 Turn 仍超限时，裁掉 Reasoning 和文本开头，保留最新输入尾部。
- 保证输出 Token 估算不超过 `MaxContextTokens - ReservedOutputTokens`。
- 在稳定前缀末尾写入 Provider-neutral Prompt Cache breakpoint。

```go
assembler, err := contextengine.New(contextengine.Config{
    MaxContextTokens:      128_000,
    ReservedOutputTokens: 8_000,
    RolePolicy:           contextengine.RolePolicyRepair,
    MergeAdjacentRoles:   true,
    PromptCache: contextengine.PromptCacheConfig{
        Enabled:         true,
        MinPrefixTokens: 1024,
    },
})
```

默认使用无外部依赖的近似 Token Counter。需要精确预算时，实现 `contextengine.TokenCounter` 并注入模型对应的 tokenizer。

## Agent 会话生命周期

将 Context Engine 和 Store 配置到 Agent：

```go
value, err := agent.New(agent.BaseConfig{
    Name:            "support-agent",
    Model:           model,
    ContextEngine:   assembler,
    CheckpointStore: store,
})
```

运行时提供会话 ID：

```go
messages, err := value.Invoke(
    ctx,
    []schema.Message{schema.UserMessage("继续上次的问题")},
    agent.WithConversationID("tenant-a:user-42:session-7"),
    agent.WithStateValue("locale", "zh-CN"),
    agent.WithContextWindowTokens(128_000),
)
```

执行顺序为：

1. 根据 `conversation_id` Load 完整 Checkpoint。
2. 将历史与本轮输入交给 Context Engine。
3. 每次 LLM 调用都重新治理增长后的工具消息和输出。
4. Agent 流成功结束后保存本轮输入、工具消息、最终输出和 State Values。
5. LLM、工具或 Checkpoint 保存失败时不提交半成品会话。

不提供 Checkpoint Store 时使用 `WithConversationID` 会明确报错。GroupChat 只由外层 Group 提交会话，嵌套 Agent 自动绕过重复 Checkpoint。

Retriever、Middleware 和 Tool 可以从调用 context 安全读取当前状态快照：

```go
checkpoint, ok := state.FromContext(ctx)
values := state.ValuesFromContext(ctx)
```

Values 不会自动复制到 Callback 元数据，避免把可能敏感的业务状态写入监控后端。

## Builders

`ConversationBuilder` 负责构造并验证原始标准消息：

```go
conversation, err := contextengine.NewConversationBuilder().
    System("You are a document assistant").
    UserContent(schema.MultiContent(
        schema.TextItem("总结这个文件"),
        schema.ContentItem{File: "guide.pdf"},
    )).
    AssistantCall("search", `{"query":"architecture"}`, "call-1").
    Reasoning("need retrieval").
    Function("search", schema.TextContent("result"), "call-1").
    Assistant("summary").
    Build()
```

`ContextBuilder` 将稳定前缀、Checkpoint 历史和本轮输入送入治理层：

```go
result, err := contextengine.NewContextBuilder(assembler).
    Pinned(systemMessages...).
    History(checkpoint.Messages...).
    Input(currentMessages...).
    TokenBudget(128_000, 8_000).
    Build(ctx)

payload := result.Messages
stats := result.Stats
```

## Runnable Context Node

Agents use `contextengine.NewDefaultContextBuilder()` as the default context
Runnable. Applications can replace it with a plain function without creating
an engine type:

```go
value, err := agent.NewReactAgent(agent.ReactAgentConfig{
    BaseConfig: agent.BaseConfig{Model: model},
}, agent.WithContextBuilder(func(ctx context.Context, request contextengine.Request) (contextengine.Result, error) {
    // Apply a sliding window, retrieval compression or prefix alignment here.
    return contextengine.NewDefaultContextBuilder().Invoke(ctx, request)
}))
```

For a custom node that needs invocation Options, use
`agent.WithContextBuilderNode(runnable.Runnable[contextengine.Request, contextengine.Result])`.

`Stats` 包含输入/输出 Token、稳定前缀大小、Role 修复数、丢弃 Turn 数、截断消息数和 Prompt Cache 对齐状态。
