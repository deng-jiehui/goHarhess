# goHarness

以强类型 Graph 为核心的轻量 Go Agent 编排框架：一行接入观测、检查点恢复、
审批中断与并行超步，代码量对齐 LangGraph，类型错误尽量在编译期暴露。

```go
model, _ := llm.New(llm.Config{
    ModelType: "openai-compatible", Model: "your-model",
    ModelServer: "https://provider.example.com/v1", APIKey: os.Getenv("LLM_API_KEY"),
})
echo := tool.NewFunctionTool(tool.Spec{
    Name: "echo", Description: "返回 text 参数。",
    Parameters: map[string]any{"type": "object",
        "properties": map[string]any{"text": map[string]any{"type": "string"}},
        "required":   []string{"text"}},
    IsReadOnly: true,
}, func(ctx context.Context, in tool.CallInput) (tool.Result, error) {
    return tool.TextResult(in.Args["text"].(string)), nil
})
assistant, _ := agent.New(model, agent.WithName("assistant"), agent.WithTools(echo))
ctx = observe.With(ctx, observe.Slog(slog.Default())) // 一行接入调用树观测
out, err := assistant.Invoke(ctx, []schema.Message{
    schema.NewMessage(schema.RoleUser, schema.NewTextBlock("调用 echo 返回 hello")),
})
```

30 行内完成模型、工具、ReAct 装配与观测接入。更多组合见
[快速上手](docs/quickstart.md)与 [Cookbook](docs/cookbook.md)。

## 架构

```text
Runnable[I, O]  ── 统一调用契约（Invoke / Stream / Collect / Transform）
   ├─ ChatModel（llm）          ├─ Tool / ToolsNode（tool）
   ├─ Graph[I, O]（graph）      ├─ Agent（agent：Basic / ReAct 配方）
   └─ 扩展 Runnable：MCP、retrieval、子 Agent
Graph：节点 + 边 + 单次求值路由 + Fork/Join 并行 + Send 动态分派 + State
agent：agent.New(model, opts...) 在同一引擎上装配 ReAct 图
```

- 核心 `pkg/*`：schema / runnable / graph / agent / tool / llm / state /
  stream / observe / contextengine / control / httpclient
- 可选 `extensions/`（独立 module）：subagent、retrieval、cache、mcp、otel、
  state/redis
- 非核心能力（RAG、子 Agent 管理、Redis、MCP、OTel）全部在扩展模块，
  核心不携带对应依赖。

## 与 LangGraph / Eino 的差异

| 维度 | goHarness | LangGraph | Eino |
| --- | --- | --- | --- |
| 编排模型 | 单引擎：State 驱动图 + Fork/Join/Send | Graph + Send + Command | Chain/Graph/Agent 多层组件 |
| 类型安全 | 泛型图边界；Handler/分支/Codec 在 `Compile` 校验 | Python 动态 | 泛型组件边界；compose 编译校验 |
| 恢复语义 | 统一检查点（会话+执行），整图指纹判兼容 | checkpointer + interrupt | `CheckPointStore`/`CheckPointID`；ADK `Resume`/`ResumeWithParams`；无整图指纹约束 |
| 人工交互 | `Interrupt` + 按问题 ID 恢复，嵌套图/子 Agent 传播到根 | interrupt/resume | `WithInterruptBeforeNodes`/`StatefulInterrupt`；ADK Resume 可携带数据 |
| 观测 | `observe.With` 一行接入调用树/耗时/token | LangSmith 生态 | callbacks 接口；官方 Langfuse 扩展与 OTel 接入 |
| 体量 | 核心约 2.1 万行（pkg+internal） | — | — |

## 性能

各阶段以 `pre-slim` 固定基线、Full 99 场景 ×10 样本验收：阶段四唯一首轮
超标项经双侧各十次复测通过，最终满足门槛；并行与恢复路径显著改善
（`ResumeGraph` ns/op −38%~−64%，`PersistedGraph` allocs −35%）。
完整数据与口径见 [基准报告](docs/benchmarks/phase4.md)。

## 文档地图

- [快速上手](docs/quickstart.md) · [核心概念](docs/concepts.md) · [Agent](docs/agent.md)
- [工具](docs/tool-runtime.md) · [模型](docs/llm-providers.md) · [上下文](docs/context-management.md)
- [观测](docs/observability.md) · [流式输出](docs/public-stream.md)
- [检查点与中断](docs/checkpoints-interrupts.md) · [并行与分支](docs/graph-branches.md)
- [扩展](docs/extensions.md) · [Cookbook](docs/cookbook.md)
- [设计取舍](docs/设计取舍.md)：砍掉/外移的能力与理由；`pre-slim` tag 保留完整版
- 过程文档：[docs/design/archive/](docs/design/archive)；面试与学习材料：[docs/showcase/](docs/showcase)

## 消息协议

消息通过有序的 `ContentBlocks` 切片保存内容，Block 顺序即构造顺序：

```go
input := []schema.Message{
    schema.NewMessage(schema.RoleUser,
        schema.NewTextBlock("Describe this image"),
        schema.NewImageBlock("file:///tmp/image.png", "image/png"),
    ),
}
assistant := schema.NewMessage(schema.RoleAssistant,
    schema.NewTextBlock("I will look that up."),
    schema.NewFunctionToolCallBlock("call-1", "search", `{"query":"status"}`),
)
```

顶层访问器（`TextBlocks`、`JoinedText`、`FunctionToolCalls` 等）只返回克隆后的
同类载荷，不隐式展平或重排；`Opaque` 块保留 Provider 专属 JSON。`isError=true`
的工具结果是模型可见内容，不是传输错误。

## 恢复

```go
messages, err = assistant.Invoke(ctx, input, agent.WithCheckpoint(store, "tenant-a:task-42"))
if errors.Is(err, graph.ErrInterrupted) {
    messages, err = agent.Resume(context.Background(), assistant,
        agent.WithCheckpoint(store, "tenant-a:task-42"))
}
```

恢复是 at-least-once，同一会话由应用串行化。整图指纹不匹配拒绝恢复；
无 `WithMigration` 迁移通道。详见[检查点与中断](docs/checkpoints-interrupts.md)。

## 验证

```powershell
go test -count=1 ./... && go vet ./...          # 根模块
Push-Location extensions; go test -count=1 ./...; go vet ./...; Pop-Location
```

Windows 下运行 race 检测需要可用的 MinGW-w64 GCC。
