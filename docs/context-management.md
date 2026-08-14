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

核心模块只依赖 `state.Store`。Redis 持久化由可选的 `goharness/extensions` 模块提供；需要时以
`redisstore "goharness/extensions/state/redis"` 导入。

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

## Graph Interrupt 与 Resume

会话历史与执行恢复是两个独立契约：

| 用途 | 接口 | key | 内容 |
| --- | --- | --- | --- |
| 跨轮会话 | `state.Store` | `conversation_id` | 已完成消息、Values、乐观版本 |
| 执行恢复 | `control.CheckpointStore` | 应用自定义 run key | Graph Continuation 字节 |

`state.Checkpoint` 不包含执行状态、status 或 continuation。Graph Continuation 保存 State
快照、下一节点、步骤/访问计数、cursor、Graph revision 和中断元数据；不保存 goroutine、
`context.Context`、HTTP/Redis client、Callback、模型、API Key 或函数。

执行 Store 只需实现两个字节方法：

```go
type CheckpointStore interface {
    Save(ctx context.Context, key string, data []byte) error
    Load(ctx context.Context, key string) ([]byte, bool, error)
}
```

外部暂停由应用持有取消权。只有 typed `control.InterruptRequest` 会请求持久化；普通
`context.Canceled` 和 deadline 只结束运行，不会生成 Continuation：

```go
runCtx, cancel := context.WithCancelCause(ctx)
go func() {
    <-pauseRequested
    cancel(control.RequestInterrupt("用户暂停", map[string]any{
        "operator": "user-42",
    }))
}()

output, err := assistant.Invoke(runCtx, input,
    agent.WithCheckpoint(executionStore, "tenant-a:task-42"),
)
if errors.Is(err, graph.ErrInterrupted) {
    output, err = agent.Resume(context.Background(), assistant,
        agent.WithCheckpoint(executionStore, "tenant-a:task-42"),
    )
}
```

高危工具审批使用 `agent.WithBeforeToolBatch`。Hook 在完整调用批次解析后、任何 Tool worker
启动前同步执行；返回 `control.RequestInterrupt` 时整批 Tool 执行数为零。批准后再次 Resume，
Hook 会重新执行并读取应用提供的批准凭证。

Graph 只在成功保存后返回可由 `errors.Is(err, graph.ErrInterrupted)` 识别的
`*graph.InterruptedError`。编码或 Store 保存失败会透传真实错误，不能当作已挂起。

恢复可换用同名、同拓扑和兼容 Codec 的新 Agent/Graph 实例，因此 Provider Endpoint、模型
或 API Key 可以变化；Graph revision 或 Codec 不兼容会在任何用户节点运行前被拒绝。

恢复语义是 at-least-once。中断时正在运行的节点、以及并行节点的全部分支可能重新执行；
有副作用的节点和 Tool 必须由应用保证幂等。框架不提供 Lease、Redis 锁、fencing、CAS
消费或 exactly-once。多副本应用应在 Gateway、任务队列或数据库事务中自行串行化同一 key
的 Resume，例如在进入框架前使用 Redis SETNX。

## Context Engine

默认 Agent 会在每次 LLM 调用前执行 Context Engine。它会：

- 将稳定前缀与 System 消息放在最前面。
- 修复 Assistant/Function 孤儿消息和无效 Role 顺序；也可使用严格模式直接报错。
- 按完整 User Turn 从最旧历史开始裁切，避免拆散工具调用上下文。
- 单个最新 Turn 仍超限时，裁掉 Reasoning 和文本开头，保留最新输入尾部。
- 保证输出 Token 估算不超过 `MaxContextTokens - ReservedOutputTokens`。
- 在稳定前缀末尾写入 Provider-neutral Prompt Cache breakpoint。

```go
assembler, err := contextengine.NewContextBuilder(contextengine.Config{
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
    ContextBuilder:  assembler,
    CheckpointStore: store,
})
```

运行时提供会话 ID：

```go
messages, err := value.Invoke(
    ctx,
    []schema.Message{schema.NewMessage(schema.RoleUser, schema.NewTextBlock("继续上次的问题"))},
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
    UserBlocks(
        schema.NewTextBlock("总结这个文件"),
        schema.NewFileBlock("file:///guide.pdf", "application/pdf"),
    ).
    AssistantCall("call-1", "search", `{"query":"architecture"}`).
    Reasoning("need retrieval", "").
    ToolResult("call-1", "search", false, schema.NewTextBlock("result")).
    Assistant("summary").
    Build()
```

`RequestBuilder` 将稳定前缀、Checkpoint 历史和本轮输入送入治理层：

```go
result, err := contextengine.NewRequestBuilder(assembler).
    Pinned(systemMessages...).
    History(checkpoint.Messages...).
    Input(currentMessages...).
    TokenBudget(128_000, 8_000).
    Build(ctx)

payload := result.Messages
stats := result.Stats
```

## Runnable Context Node

Agent 默认使用 `contextengine.NewContextBuilder(contextengine.Config{})` 创建的
Runnable。自定义逻辑使用统一的 `runnable.Func` 适配后，通过唯一的
`agent.WithContextBuilder` 入口注入：

```go
standard, err := contextengine.NewContextBuilder(contextengine.Config{})
custom := runnable.Func[contextengine.Request, contextengine.Result](func(
    ctx context.Context,
    request contextengine.Request,
    _ ...runnable.Option,
) (contextengine.Result, error) {
    // Apply a sliding window, retrieval compression or prefix alignment here.
    return standard.Invoke(ctx, request)
})

value, err := agent.NewReactAgent(agent.ReactAgentConfig{
    BaseConfig: agent.BaseConfig{Model: model},
}, agent.WithContextBuilder(custom))
```

`Stats` 包含输入/输出 Token、稳定前缀大小、Role 修复数、丢弃 Turn 数、截断消息数和 Prompt Cache 对齐状态。
