# Tool Runtime、Middleware 与 Callback

goHarness 将会改变业务结果的控制能力与只读观测能力分开：

- `tool.Runtime` 负责 Middleware、超时、只读重试、递归深度、并发组、死锁检测、HTTP Client 和 panic 隔离。
- `tool.BeforeBatchHook` 在整批 Tool Call 产生副作用前完成准入或挂起。
- 各领域强类型 Callback 只观察生命周期，不改变主业务结果。
- LLM Token、Tool 进度和 UI 数据通过 Context 中的 EventStream 发布。

Agent 和 `tool.ToolsNode` 共用公开的 `tool.Runtime`，不存在 Agent 私有的第二套工具执行路径。

## ToolsNode

消息保留 `ContentBlocks` 的精确顺序。`ToolsNode` 读取
Assistant 的 `FunctionToolCall` Block，并生成一个包含
ordered `FunctionToolResult` blocks; result blocks may themselves contain
嵌套文本或媒体 Block 的 `RoleTool` 消息。需要过滤视图时使用类型化顶层访问器；顺序重要时直接遍历 `ContentBlocks`。不会发生隐式文本展平。构造和访问器详情请参阅 README 的规范 Message 与 ContentBlocks
章节，查看完整的调用/结果与访问器示例。

`ToolsNode` 是完整的 `runnable.Runnable[[]schema.Message, []schema.Message]`。它只执行消息末尾连续的 assistant function calls，按原调用顺序返回 function-result 消息。

```go
node, err := tool.NewToolsNode(tool.ToolNodeConfig{
    Name:           "tools",
    Tools:          tools,
    Middlewares:    []tool.Middleware{rbac, masking},
    BeforeBatch:    []tool.BeforeBatchHook{approval},
    MaxConcurrency: 4,
    MaxDepth:       16,
    Timeout:        30 * time.Second,
    MaxRetries:     2,
    RetryBackoff:   100 * time.Millisecond,
})
```

`MaxConcurrency` 是持久的节点级上限，对并发的多次 `Invoke` 同样生效。每个 `tool.Spec.Concurrency` 还可以声明跨调用共享的并发组；未声明时使用保守的默认串行组。

需要直接构建更底层的执行组件时，可使用公开 Runtime：

```go
runtime, err := tool.NewRuntime(middlewares, tool.RuntimeConfig{
    MaxDepth:       16,
    Timeout:        30 * time.Second,
    MaxConcurrency: 8,
    MaxRetries:     2,
    RetryBackoff:   100 * time.Millisecond,
})
binding, err := runtime.Register(searchTool)
scope := runtime.NewScope(4)
result, err := scope.Execute(ctx, binding, input)
```

一个 `Scope` 可以跨多个调用复用；它拥有该批次或节点的并发上限。`Runtime` 拥有工具注册快照、Middleware 和并发组调度。

## Middleware

Middleware 在当前调用 Goroutine 中按注册顺序执行，可以拒绝调用或转换输入输出：

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

Runtime 的取消、超时、深度、并发组和死锁保护不能被 Middleware 吞掉。只有 `Spec.IsReadOnly` 为 `true` 的工具会自动重试，写操作默认只执行一次。

身份只通过 Context 传递：

```go
ctx := tool.WithSubject(ctx, tool.Subject{
    ID:          "user-42",
    TenantID:    "tenant-a",
    Permissions: []tool.Permission{"tools:write"},
})
```

不要把身份、API Key 或授权信息放入模型可见参数。

## 整批挂起

`BeforeBatchHook` 在完整 Tool Call 批次解析完成后、任何 worker 或 semaphore 启动前同步执行：

```go
approval := tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
    for _, call := range batch.Calls {
        if !call.Spec.IsReadOnly && !approved(ctx, call.ID) {
            return control.RequestInterrupt("等待高危工具审批", map[string]any{
                "tool_call_id": call.ID,
                "tool":         call.Spec.Name,
            })
        }
    }
    return nil
})
```

返回 typed interrupt 时整批执行数为零，由外层 Graph 保存 Continuation。Resume 会重新运行 Hook，审批凭证由应用通过 Context 或自己的存储提供。

## 生命周期 Callback

生命周期 Handler 只通过 Context 注册。各领域定义自己的强类型接口：

| 包 | 生命周期方法 |
| --- | --- |
| `pkg/llm` | `OnChatModelStart/End/Error` |
| `pkg/tool` | `OnToolStart/End/Error` |
| `pkg/retrieval` | `OnRetrieverStart/End/Error` |
| `extensions/mcp` | `OnMCPStart/End/Error` |
| `pkg/graph` | `OnGraphStart/End/Error`、`OnNodeStart/Output/End` |

```go
handlers := []callback.Handler{
    graph.CallbackFuncs{
        CallbackName: "graph-trace",
        NodeEnd: func(ctx context.Context, result graph.NodeResult) error {
            log.Printf("node=%s step=%d duration=%s", result.NodeID, result.Step, result.Duration)
            return nil
        },
    },
    llm.CallbackFuncs{
        CallbackName: "llm-usage",
        End: func(ctx context.Context, response llm.Response) error {
            if response.Usage != nil {
                log.Printf("tokens=%d", response.Usage.TotalTokens)
            }
            return nil
        },
    },
    tool.CallbackFuncs{
        CallbackName: "tool-audit",
        Error: func(ctx context.Context, spec tool.Spec, input tool.CallInput, err error) error {
            log.Printf("tool=%s err=%v", spec.Name, err)
            return nil
        },
    },
}

manager, err := callback.NewManager(
    callback.Config{Timeout: 2 * time.Second},
    handlers,
    callback.ErrorSinkFunc(func(ctx context.Context, failure callback.CallbackError) {
        log.Printf("callback failure: %v", failure)
    }),
)
ctx = callback.WithManager(ctx, manager)
output, err := assistant.Invoke(ctx, input)
```

Handler 同步直调、按注册顺序运行。Handler 的 error 或 panic 会被隔离并报告到 `ErrorSink`，不会替换 Agent、Graph、Model 或 Tool 的业务结果。`Config.Timeout` 是协作式 deadline，Handler 必须响应 `ctx.Done()`。

Graph 为每个节点创建独立子 Span。`graph.NodeExecution` 和 `graph.NodeResult` 直接携带 NodeID、Step、状态和耗时，不依赖通用 metadata 字符串。

## 实时 EventStream

实时数据不进入生命周期 Callback：

```go
ctx = callback.WithStreamCallbacks(ctx,
    callback.StreamCallbackOf[llm.Response]("ui", func(
        ctx context.Context,
        partial llm.Response,
    ) error {
        return renderPartial(ctx, partial)
    }),
)
```

组件使用 `callback.EmitStream(ctx, value)` 同步发布强类型数据。Stream observer 的失败同样隔离并交给 Manager 的 `ErrorSink`；框架不提供异步队列、Flush 或无界 Goroutine 管理器。

## Agent 装配

```go
reactAgent, err := agent.NewReactAgent(
    agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{Model: model}},
    agent.WithName("document-agent"),
    agent.WithTools(tools...),
    agent.WithMiddlewares(rbac, masking, approval),
    agent.WithBeforeToolBatch(preToolApproval),
    agent.WithMaxDepth(5),
    agent.WithTimeout(10*time.Second),
    agent.WithMaxConcurrency(4),
    agent.WithMaxRetries(2),
    agent.WithMaxTokens(4096),
    agent.WithMaxLLMCalls(20),
)
```

Callback Manager 不属于 Agent 配置；它只从运行 Context 解析。`WithMaxConcurrency` 限制工具批次并发，与 Callback 执行模型无关。
