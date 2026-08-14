# goHarness

goHarness 是一个以强类型 Graph 为核心的 Go Agent 编排框架。开发者可以把 ChatModel、
ToolNode、MCP、Retriever 等 Runnable 节点快速组装成 Agent；标准场景也可直接使用
Basic、ReAct、Routing 和 GroupChat 预置图配方。

## 项目特点

- 运行时 State Graph：图边界保持强类型，节点通过 Pre/Post Handler 复用纯 Runnable
- 统一的 `runnable.Runnable[I, O]`，连接 ChatModel、ToolNode、Agent 与 Graph
- `llm.ChatModelNode`、`tool.ToolsNode`、Retriever、MCP 和 `agent.AgentNode` 提供可识别的编译期节点类别
- Basic、ReAct、Routing 和 GroupChat 预置图，减少标准 Agent 样板代码
- OpenAI-compatible 模型适配、SSE 流式输出和推理内容标准化
- 本地 Tool、独立 ToolsNode、MCP 动态工具发现与统一函数调用消息协议
- 工具参数校验、并发调度、Middleware、身份信息和托管 HTTP 客户端
- Token 裁切、Prompt Cache 对齐和上下文构建
- Graph Continuation、整批工具审批和独立的会话历史持久化
- 分块、批量索引、混合检索和重排
- Callback 生命周期和运行时错误隔离

## 环境要求

- Go 1.24 或更高版本
- Windows 下运行 race 检测需要可用的 MinGW-w64 GCC

## 快速开始

运行内置示例：

```powershell
go run ./examples/basic
```

示例会创建 ChatModel 和 `echo` Tool；`agent.New` 自动生成有界的
`llm -> tools -> llm` ReAct 图，并输出执行结果。

```go
assistant, err := agent.New(agent.BaseConfig{
    Name:  "assistant",
    Model: model,
    Tools: []tool.Tool{echoTool},
})

messages, err := assistant.Invoke(ctx, []schema.Message{
    schema.NewMessage(schema.RoleUser, schema.NewTextBlock("please use echo")),
})
```

消息通过有序的 `ContentBlocks` 切片保存内容。Block 参数保持传给 `NewMessage` 的顺序，因此文本加图片的请求可以写成：

```go
input := []schema.Message{
    schema.NewMessage(schema.RoleUser,
        schema.NewTextBlock("Describe this image"),
        schema.NewImageBlock("file:///tmp/image.png", "image/png"),
    ),
}
```

这些访问器有意只提供顶层过滤视图：

- `TextBlocks()` 仅返回克隆后的顶层文本载荷。
- `JoinedText(separator)` 使用调用方提供的分隔符连接同一批顶层文本载荷。
- `ReasoningBlocks()` 仅返回克隆后的顶层推理载荷。
- `FunctionToolCalls()` 仅返回克隆后的顶层函数调用载荷。
- `FunctionToolResults()` 仅返回克隆后的顶层函数结果载荷。

任何文本访问器都不会包含嵌套结果块、媒体、推理、Opaque 块及其中的文本。需要完整混合顺序时直接遍历 `ContentBlocks`，需要嵌套顺序时遍历 `FunctionToolResult.ContentBlocks`。
当每个文本片段必须保持独立（例如保留作者边界）时使用 `TextBlocks`；需要一个明确的单一文本表示时使用 `JoinedText("\n")`。
不存在通用的消息转字符串操作：使用 `TextBlocks` 保留片段，只有在有意构造一个文本协议值时才使用 `JoinedText(separator)`。

Tool 调用和结果在消息中始终保持为类型化 Block：

```go
assistant := schema.NewMessage(schema.RoleAssistant,
    schema.NewTextBlock("I will look that up."),
    schema.NewFunctionToolCallBlock("call-1", "search", `{"query":"status"}`),
)
toolResult := schema.NewMessage(schema.RoleTool,
    schema.NewFunctionToolResultBlock("call-1", "search", true,
        schema.NewTextBlock("The search failed."),
        schema.NewOpaqueBlock("provider.error", []byte(`{"code":"timeout"}`)),
    ),
)
```

结果中的 `CallID` 和 `Name` 用于与调用关联。`isError=true` 表示模型可见的工具失败；它仍然是内容，不是隐式的传输错误。

`ResponseMeta` 是可选的消息元数据，包含 `ID`、`Model`、`FinishReason`、`Usage` 和 `Extra`。`Usage` 类型为 `*schema.TokenUsage`，其计数器包括
`InputTokens`, `OutputTokens`, `TotalTokens`, `ReasoningTokens`,
`CacheReadTokens` 和 `CacheWriteTokens`（另含 `Extra`）。
`Opaque`/`NewOpaqueBlock` 用于保留 Provider 专属 JSON。任何访问器都不会隐式展平或重排内容。

需要暂停/审批后释放执行栈时，为本次运行提供独立的执行 Checkpoint key。只有 typed
`control.InterruptRequest` 会落盘，恢复使用相同 key：

```go
messages, err = assistant.Invoke(ctx, input,
    agent.WithCheckpoint(executionStore, "tenant-a:task-42"),
)
if errors.Is(err, graph.ErrInterrupted) {
    messages, err = agent.Resume(context.Background(), assistant,
        agent.WithCheckpoint(executionStore, "tenant-a:task-42"),
    )
}
```

恢复是 at-least-once。框架不提供 Lease 或分布式锁；多副本应用在进入 Resume 前自行
串行化同一 key。

需要审批、检索分支、并行任务或领域状态时，使用 `graph.NewGraph[I, O]` 直接组合
ChatModelNode、ToolsNode、MCP 节点或其他 Runnable。Provider 模型通过
`llm.NewChatModelNode(model)` 接入图；图只声明公开输入输出，每次运行的
`*State` 由 State Factory 创建，通过 Pre/Post Handler 与纯节点衔接。完整说明见
[中文使用手册](docs/使用手册.md)。

## 模块结构

| 目录 | 说明 |
| --- | --- |
| `pkg/schema` | 消息、内容、多模态内容和函数调用数据结构 |
| `pkg/runnable` | Runnable、流式、批处理和选项计划 |
| `pkg/graph` | Graph、运行时 State、节点适配、条件边、并行、编译与执行 |
| `pkg/llm` | LLM 模型接口、Provider 注册和 OpenAI-compatible 适配器 |
| `pkg/agent` | Basic、ReAct、Routing 和 GroupChat Agent |
| `pkg/tool` | 工具接口、公开 Runtime、Middleware、并发策略和 ToolsNode |
| `pkg/callback` | Context 注册、同步生命周期分发和实时 EventStream |
| `pkg/contextengine` | Token 预算、上下文裁切和 Prompt Cache 对齐 |
| `pkg/control` | typed 挂起请求与最小字节 CheckpointStore 契约 |
| `pkg/state` | 会话历史 Checkpoint 和乐观版本控制 |
| `pkg/retrieval` | 分块、索引、检索和重排 |
| `extensions` | Cache、Redis、MCP 等可选实现，单独维护 Go module |

## 可选扩展

核心模块只包含 provider-neutral 的接口和内存实现。Cache、Redis、MCP 等扩展位于嵌套的 `extensions` 模块中：

```powershell
Push-Location extensions
go test ./...
Pop-Location
```

详细说明见 [extensions 文档](docs/extensions.md) 和 [MCP 工具文档](docs/mcp-tools.md)。

## 核心执行契约

所有可执行组件都基于 `runnable.Runnable[I, O]`：

- Agent 和消息边界 Graph：`Runnable[[]schema.Message, []schema.Message]`
- ChatModel：`llm.ChatModel`，处理 `llm.Request` 和 `llm.Response`
- Tool：`tool.Tool`，处理 `tool.CallInput` 和 `tool.Result`
- ToolsNode：执行消息尾部的新函数调用并返回有序工具结果
- 检索、Embedding、Rerank 和向量数据库：各自的强类型 Runnable 契约

Graph 是唯一的编排引擎。流式和批处理是叠加在同一执行契约上的可选能力，不会引入另一套调用接口。

详见 [执行模型](docs/execution-model.md)、[工具运行时](docs/tool-runtime.md) 和 [上下文管理](docs/context-management.md)。

## 验证

根模块：

```powershell
go test -count=1 ./...
go vet ./...
```

扩展模块：

```powershell
Push-Location extensions
go test -count=1 ./...
go vet ./...
Pop-Location
```

Windows 下运行 race 检测时，将 MinGW-w64 的 bin 目录加入 `PATH`：

```powershell
$env:Path = 'D:\Program Files\msys64\ucrt64\bin;' + $env:Path
go test -race -count=1 ./...
```
