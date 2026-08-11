# 工具运行时、Middleware 与 Callback

goHarness 将可改变业务执行的扩展与只读观测能力分开：Middleware 负责授权、输入输出转换和降级；Callback 负责性能监控、日志、审计和计费，不影响业务结果。

## 执行边界

```text
Agent run
  -> agent.start
  -> retriever.start / end / error
  -> llm.start / chunk / end / error
  -> Tool runtime
       -> tool.start
       -> Middleware 1..N
       -> batch semaphore / concurrency group
       -> Tool.Invoke / retry / panic guard
       -> tool.end / error
  -> agent.chunk
  -> agent.end / error
```

固定工具运行时位于 `internal/toolruntime`。外部应用不能替换其中的深度保护、worker 限制、超时、重试、并发组、死锁检查和 panic 恢复。

## Middleware

Middleware 是同步业务控制层，可以拒绝调用、转换输入输出或调用 `next`：

```go
rbac := tool.MiddlewareFuncs{
    MiddlewareName: "rbac",
    HandleFunc: func(
        ctx context.Context,
        spec tool.Spec,
        input tool.CallInput,
        next tool.Next,
    ) (tool.Result, error) {
        subject, err := tool.RequireSubject(ctx)
        if err != nil {
            return tool.Result{}, tool.Deny(err)
        }
        if !spec.IsReadOnly && !subject.HasPermission("tools:write") {
            return tool.Result{}, tool.Deny(errors.New("missing tools:write permission"))
        }
        return next(ctx, input)
    },
}
```

Middleware 不能绕过框架 semaphore、并发组和取消链。`tool.Deny` 返回的业务拒绝支持 `errors.Is(err, tool.ErrPermissionDenied)`。

## Callback 生命周期

`pkg/callback` 声明以下稳定事件：

| 组件 | 事件 |
| --- | --- |
| Agent | `agent.start`、`agent.chunk`、`agent.end`、`agent.error` |
| LLM | `llm.start`、`llm.chunk`、`llm.end`、`llm.error` |
| Retriever | `retriever.start`、`retriever.end`、`retriever.error` |
| Tool | `tool.start`、`tool.end`、`tool.error` |

用户可以实现统一的同步 `callback.Handler`，也可以使用只声明所需钩子的 `callback.Funcs`：

```go
monitor := callback.Funcs{
    CallbackName: "metrics-and-logs",
    OnLLMEnd: func(ctx context.Context, event callback.Event) error {
        log.Printf(
            "trace=%s model=%s latency=%s tokens=%d",
            event.Trace.TraceID,
            event.Name,
            event.Duration,
            event.Metrics.TotalTokens,
        )
        return nil
    },
    OnToolError: func(ctx context.Context, event callback.Event) error {
        log.Printf("trace=%s tool=%s err=%s", event.Trace.TraceID, event.Name, event.ErrorText)
        return nil
    },
}

value, err := agent.New(agent.BaseConfig{
    Name:      "document-agent",
    Model:     model,
    Tools:     tools,
    Callbacks:      []callback.Handler{monitor},
    CallbackConfig: callback.Config{FilterChunks: true},
})
```

也可以使用 `agent.WithCallback` 配置 Agent，或用 `agent.WithRunCallback` 仅配置一次运行。
每个 Handler 在当前事件分发 Goroutine 中按注册顺序执行；`Invoke` 返回时，已产生的
生命周期回调也已经完成。需要计费、审计和关键日志无损递交时，应直接使用同步 Handler。

## 上下文与元数据

框架为一次 Agent 运行创建根 span，LLM、Retriever 和 Tool 创建子 span。每个事件都包含：

- `trace_id`：可与上游 HTTP/OpenTelemetry trace 对齐。
- `run_id`：一次 Agent 运行的稳定标识。
- `span_id` 与 `parent_span_id`：组件父子关系。
- 事件序号、开始时间、耗时、状态和错误文本。
- 消息数、字节数、chunk 数、工具数以及 token 使用量。
- 模型 Provider、模型类型、能力声明、工具并发策略等结构化元数据。
- 用户通过 context 或 `agent.WithMetadata` 注入的元数据。

```go
ctx := callback.WithTraceID(context.Background(), upstreamTraceID)
ctx = callback.WithMetadata(ctx, map[string]any{
    "tenant":     "tenant-a",
    "request_id": requestID,
})

messages, err := value.Invoke(
    ctx,
    input,
    agent.WithMetadata("scenario", "customer-support"),
)
```

原始 Context 的值和取消信号会传递到所有 Handler。若 Handler 需要在 HTTP 请求取消后
继续持久化，应从原始业务 Context 派生独立的持久化 Context，并重新设置自己的 deadline：

```go
func persist(ctx context.Context, event callback.Event) error {
    persistCtx, cancel := context.WithTimeout(
        context.WithoutCancel(ctx), 5*time.Second,
    )
    defer cancel()
    return writeAudit(persistCtx, event)
}
```

不要对框架传入的带 Callback deadline 的 Context 直接调用 `WithoutCancel`，否则会同时
剥离框架设置的 deadline。框架默认只提供计数和结构化元数据，不把 Prompt、API Key 或
完整工具结果放入事件。

## 同步执行与错误隔离

Callback 调度具备以下约束：

- `SyncDispatcher.Dispatch` 在当前 Goroutine 内执行，不创建队列、worker、背压或 Flush
  生命周期；已接受的生命周期事件不会丢弃。
- `Event.Sequence` 使用原子递增，在 Graph Fan-Out 的并发节点下保持唯一。Handler 实现
  必须自行保证线程安全，因为同一个 Handler 可能被多个分支并发调用。
- 每个 Handler 收到独立的 Event 深拷贝，Metadata、`Metrics.Custom` 以及嵌套 map/slice
  不会被前一个 Handler 的修改污染。
- `Config.Timeout` 是协作式超时。Handler 必须主动感知 `ctx.Done()`；忽略 Context 的
  阻塞 I/O 仍可能卡住 Agent，框架不能强行终止同步调用。
- Handler 返回错误或 panic 时，`Dispatch` 返回聚合的 `callback.CallbackError`；Agent、
  LLM、Tool 和 Graph 调用点会隔离并忽略该错误，不替换业务结果，也不阻断后续 Handler。
- 高频 `agent.chunk` 和 `llm.chunk` 可通过 `callback.Config{FilterChunks: true}` 过滤。
  流本身仍走独立 StreamReader，`agent.end`/`llm.end` 保留累计消息、字节和最终 token
  指标。需要异步 Metrics 时，由应用自行包装 Handler 并负责 Goroutine、缓冲、重试和关闭：

```go
type asyncHandler struct{ next callback.Handler }

func (h asyncHandler) Handle(ctx context.Context, event callback.Event) error {
    snapshot := event.Clone()
    go func() {
        defer func() { _ = recover() }()
        _ = h.next.Handle(context.WithoutCancel(ctx), snapshot)
    }()
    return nil
}
```

该包装不提供无损保证；计费和审计仍应使用同步 Handler。

## Agent 组装

```go
reactAgent, err := agent.NewReactAgent(
    agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{Model: model}},
    agent.WithName("document-agent"),
    agent.WithTools(tools...),
    agent.WithMiddlewares(rbac, masking, approval),
    agent.WithCallbacks(monitor),
    agent.WithCallbackConfig(callback.Config{FilterChunks: true}),
    agent.WithMaxDepth(5),
    agent.WithTimeout(10*time.Second),
    agent.WithMaxConcurrency(4),
    agent.WithMaxRetries(2),
    agent.WithMaxTokens(4096),
    agent.WithMaxLLMCalls(20),
)
```

身份和取消仍从运行 Context 传播到 Middleware、并发等待和工具 Handler。Callback 是同步只读
观测接口；需要拒绝请求或修改结果时应使用 Middleware。`agent.WithMaxConcurrency(4)`
只限制工具批次并发，与 Callback Handler 执行模型无关。
