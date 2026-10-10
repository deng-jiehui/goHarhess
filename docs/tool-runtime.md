# 工具

工具由 `tool.Spec` 描述，并用 `tool.NewFunctionTool` 包装函数。参数必须是 JSON
Object；空参数会规范化为 `{}`，数组、`null` 和不完整 JSON 会被拒绝。

```go
echo := tool.NewFunctionTool(tool.Spec{
    Name:        "echo",
    Description: "返回 text 参数。",
    Parameters: map[string]any{
        "type": "object",
        "properties": map[string]any{
            "text": map[string]any{"type": "string"},
        },
        "required": []string{"text"},
    },
    IsReadOnly: true,
}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
    return tool.TextResult(input.Args["text"].(string)), nil
})

toolsNode, err := tool.NewToolsNode(tool.ToolNodeConfig{
    Name:           "tools",
    Tools:          []tool.Tool{echo},
    MaxConcurrency: 4,
})
```

`ToolsNode` 的输入是完整消息历史，但只执行末尾连续的 assistant function-call 消息，
不会重放历史工具调用；输出是与调用顺序一致的 function-result 消息。调用方把结果追加到
历史，再进入下一个 ChatModel 节点。多数业务不必手写循环，把相同的工具交给 `agent.New`
即可让框架自动生成 `llm -> tools -> llm` 图。

`tool.Spec.Concurrency` 控制工具批次和 `ToolsNode` 的并发策略。未声明策略的工具
默认串行，避免把有副作用的操作无意并行化。对外部写操作应保持
`IsReadOnly: false`，并只在已验证幂等性和权限边界后配置重试。

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

参数校验：`Spec.Parameters`（JSON Schema）在 `Runtime.Register` 时预编译（`google/jsonschema-go`）。工具声明先深拷贝为注册快照，`Binding.Spec()` 与 `ToolsNode.Definitions()` 导出的 `Parameters` 均为独立副本，注册后修改原参数表或导出的定义不会改变校验规则。schema 无法解析、引用无法解析或 `$schema` 声明为不受支持的版本（仅支持 draft-07 与 2020-12，未声明按默认处理）都会在注册期直接报错。`Scope.Execute` 在进入 Middleware、占用 worker 槽位之前校验 `CallInput.Args`；不满足 schema 的调用不会执行工具本体——`ToolsNode` 路径把它转为 `isError` 结果返回给模型（文本为 `tool "<name>" arguments violate the declared schema: …`），便于模型自纠重试；直调 Runtime 的调用方收到 `*tool.InvalidArgsError`，可用 `errors.Is(err, tool.ErrInvalidToolArgs)` 分类。未声明 `Parameters` 的工具回落到 `EmptyParameters`（任意 JSON 对象可用）。

错误可见性：进入 `isError` 结果的文本与内部错误分离。默认模型只看到 `tool "<name>" failed`；参数 JSON 语法错误与 schema 违规、未注册工具名（`does not exist`）、权限拒绝（`permission denied`）与超时（`timed out`）有确定的模型可读文本。工具或中间件要给模型返回反馈（如"限流，30 秒后重试"、"缺少 write:files 权限"）时用 `tool.ModelVisible(err)` 显式包装，包括放在 `Deny` 理由内。内部错误原文、panic 值、中间件名等诊断信息不进入模型文本，完整错误始终由 `OnToolError`/`EndCallback` 携带。

结果复用与幂等：节点挂起（`during_node`）时同批已完成的工具调用结果记入会话 pending 区随进度落盘；恢复重放整节点时命中登记的调用直接复用结果，不再执行工具本体。去重键 = `批次序号/调用基准键`：`ToolsNode.Invoke` 每次进入开一个新批次（`control.CallRecorder.BeginBatch`），序号在会话内单调递增且恢复重放顺序一致——同节点主动再调一批正常执行，只有同批次的恢复重放才命中；调用基准键取提供商调用 ID，缺失时用 `工具名:参数摘要`，同批重复追加序号。挂起型错误（`graph.Interrupt`、子调用挂起）不是完成结果，不登记——恢复后该调用重新执行以重放交互。

`CallInput.IdempotencyKey` 为每次调用注入运行级幂等键：`运行范围/会话路径/批次/调用键`，运行范围取检查点 `RunID` 或运行入口生成的随机 ID（嵌套运行继承父级并按子调用分层）——同一次调用恢复前后值不变，不同运行、不同任务、不同批次各不相同，有副作用的工具可把它传给下游幂等接口兜底进程崩溃等未落盘场景。

复用与人工交互协调：工具内的 `Interrupt` 记录按调用键归属（`InterruptRecord.Call`），命中 pending 的调用连带标记其此前消费过的日志为已消费，后续调用按各自归属继续消费，不会错配审批答案。登记器经 `control.WithCallRecorder` 由图运行时注入，载荷为不透明字节；直调 `Runtime.Execute` 无登记器，行为不变。

历史按需提供：`ToolsNode` 默认不向 `CallInput.Messages` 注入历史，该字段为 `nil`。需要读取会话的工具在 `Spec` 中声明 `NeedsMessages: true`，即可获得本次调用的完整历史，包括 system prompt 和此前的工具输出。批次入口对外部消息做一次深拷贝；同批声明历史需求的工具各自获得独立的消息切片，共享其中的嵌套内容，不再逐调用深拷贝整段历史。工具应把历史当作只读数据，修改内容块或元数据会影响同批其他读取历史的工具。回调获得独立副本。`FileAccess` 单独控制文件引用的提供，声明文件访问无需同时声明历史需求。模型仍正常接收工具调用与结果。

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
    MaxAbandoned:   8,
})
```

`RetryBackoff` 未配置时沿用默认 100ms；要显式关闭重试间隔（重试立即执行）需同时设置 `RetryBackoffSet: true`，否则结构体零值视为未配置。

超时与取消是协作式约束：`Execute` 超时先返回，但忽略 `ctx` 的工具仍在后台占用全局 worker、批次与并发组槽位。框架把这类执行计为该工具的未归还数，每次放弃通过 `OnToolError` 派发 `AbandonedError` 告警（携带当前未归还数与阈值）；未归还数达到阈值后该工具的新调用直接拒绝，模型侧得到 `tool "<name>" is temporarily unavailable` 的 isError 结果，其他工具不受影响；后台执行归还后计数回落、熔断自动解除。工具实现必须在 `ctx.Done()` 后及时返回，长时间操作应定期检查取消信号。

熔断阈值在两个配置入口的零值语义不同：`ToolNodeConfig.MaxAbandoned = 0` 表示使用默认值 8（无法在该入口显式关闭）；直建 `Runtime` 时 `RuntimeConfig.MaxAbandoned = 0` 表示关闭熔断。两处上限均为全局 worker 数 128。

`MaxConcurrency` 是持久的节点级上限，对并发的多次 `Invoke` 同样生效。每个 `tool.Spec.Concurrency` 还可以声明跨调用共享的并发组；未声明时使用保守的默认串行组。死锁保护按调度器分域：同一 `Runtime` 的调用链持有组必须严格递增，持有低级组时不得再请求同级或低级组；嵌套 Graph / 其他 `Runtime` 内的 `ToolsNode` 使用独立 worker 池，不受外层持有组约束，工具因此可以安全地在工具体内调用子图或子 Agent。`MaxDepth` 递归深度仍按全局计数，跨 runtime 同样受限。

工具调用抛出的挂起与框架控制错误（交互等待、子调用挂起、恢复身份或输入校验失败、检查点读写失败）不会转成模型可见的错误结果：它们作为节点级错误上交调度器，由运行挂起或终止。普通业务错误继续按 `isError` 结果交给模型处理。

需要直接构建更底层的执行组件时，可使用公开 Runtime：

```go
runtime, err := tool.NewRuntime(middlewares, tool.RuntimeConfig{
    MaxDepth:       16,
    Timeout:        30 * time.Second,
    MaxConcurrency: 8,
    MaxRetries:     2,
    RetryBackoff:   100 * time.Millisecond,
    MaxAbandoned:   8,
})
binding, err := runtime.Register(searchTool)
scope := runtime.NewScope(4)
result, err := scope.Execute(ctx, binding, input)
```

一个 `Scope` 可以跨多个调用复用；它拥有该批次或节点的并发上限。`Runtime` 拥有工具注册快照、Middleware 和并发组调度。

## Middleware

Middleware 在当前调用 Goroutine 中按注册顺序执行，可以拒绝调用或转换输入输出。

传入的 `Spec` 是只读的工具注册信息，其 `Parameters` 描述参数规则，中间件不得修改这份规则。需要调整本次调用的参数值时，修改 `input.Args` 并传给 `next`。参数校验发生在 Middleware 之前；正式修改工具的参数规则需要用新声明重新注册工具。

例如，检查调用者权限：

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
            // 理由要展示给模型时用 ModelVisible 包装；否则模型只看到 permission denied
            return tool.Result{}, tool.Deny(tool.ModelVisible(errors.New("missing tools:write permission")))
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

返回 typed interrupt 时整批执行数为零，由外层 Graph 保存检查点。Resume 会重新运行 Hook，审批凭证由应用通过 Context 或自己的存储提供。

## 生命周期观测与实时事件流

运行期观测统一走 `pkg/observe`：`observe.With` 注册的 Observer 会收到本次运行的全部
run / node / llm / tool / interrupt / checkpoint 事件。工具事件 `Name` 为工具名，
`Attrs` 携带 `tool.abandoned` 等键。实时数据（消息增量、业务进度、生命周期通知）走
`pkg/stream` 公共流。完整的接入方式、同步语义与异步包装见[观测](observability.md)。

## Agent 装配

```go
reactAgent, err := agent.New(model,
    agent.WithName("document-agent"),
    agent.WithTools(tools...),
    agent.WithMiddlewares(rbac, masking, approval),
    agent.WithBeforeToolBatch(preToolApproval),
    agent.WithMaxDepth(5),
    agent.WithTimeout(10*time.Second),
    agent.WithMaxConcurrency(4),
    agent.WithToolRetries(2),
    agent.WithToolRetryBackoff(100*time.Millisecond),
    agent.WithMaxTokens(4096),
    agent.WithMaxLLMCalls(20),
)
```

`WithToolRetries`/`WithToolRetryBackoff` 只作用于只读工具的自动重试；`WithToolRetries(0)` 显式关闭重试。模型重试与限流不是 Agent 配置：需要时在构造前手工包装模型，`llm.NewRetryingModel(model, llm.RetryConfig{...})` 与 `llm.NewRateLimitedModel(model, llm.RateLimitConfig{...})` 可自由嵌套组合——例如 `NewRetryingModel(NewRateLimitedModel(inner))` 得到「重试外层、限流内层」，每次真实请求都重新占用配额。

Observer 注册不属于 Agent 配置；它只从运行 Context 解析。`WithMaxConcurrency` 限制工具批次并发，与观测执行模型无关。
