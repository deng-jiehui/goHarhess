# Cookbook

按任务组织的可运行配方。每个条目给出场景、关键代码与对应的 `examples/` 目录；
多数配方不需要外部模型服务，使用本地脚本模型即可运行。

## 配方索引

| 场景 | 入口 |
| --- | --- |
| 最小 Agent | `examples/basic` |
| 手写 3 节点线性图 | `examples/graph` |
| 手写 ReAct 图（不依赖 `agent.New`） | `examples/react` |
| 主管路由分派成员 Agent | `examples/routing` |
| 多 Agent 群聊 | `examples/groupchat` |
| 并行分支 | `examples/branches` |
| 动态任务分派（`Send`） | `examples/dispatch` |
| 交互中断与恢复输入 | `examples/interaction` |
| 公共流消费 | `examples/stream` |
| 观测调用树 | `examples/observe` |
| 结构化输出（单次 + Agent 后整理节点） | `examples/structured-output` |
| 通用数据模型（JSON 校验 + 严格解码） | `examples/datamodel` |
| 子 Agent（extensions 模块） | `extensions/examples/subagent` |

## 用底层节点显式构建 Agent 图

当预置 ReAct 配方不能表达业务流程时，可以直接把 ChatModel 和 ToolsNode 作为纯
Runnable 放进 Graph。Graph 只声明公开边界 `[I, O]`；每次运行的 `*AgentState` 由
State Factory 创建，Pre/Post Handler 负责节点与 State 之间的映射：

```go
type AgentState struct {
    Messages     []schema.Message
    Output       []schema.Message
    HasToolCalls bool
}

g := graph.NewGraph[[]schema.Message, []schema.Message]("custom-agent")
modelNode, err := llm.NewChatModelNode(model)
if err != nil {
    return err
}
if err := g.SetStateFactory(func(
    _ context.Context, input []schema.Message,
) (*AgentState, error) {
    return &AgentState{Messages: schema.CloneMessages(input)}, nil
}); err != nil {
    return err
}

if err := g.AddNode("chat", modelNode,
    graph.WithPreHandler(func(
        _ context.Context, state *AgentState,
    ) (llm.Request, error) {
        return llm.Request{
            Messages:  schema.CloneMessages(state.Messages),
            Functions: toolsNode.Definitions(),
        }, nil
    }),
    graph.WithPostHandler(func(
        _ context.Context, state *AgentState, response llm.Response,
    ) error {
        state.Messages = append(state.Messages, schema.CloneMessages(response.Messages)...)
        state.Output = schema.CloneMessages(response.Messages)
        state.HasToolCalls = len(tool.TailToolCalls(state.Messages)) > 0
        return nil
    }),
); err != nil {
    return err
}

if err := g.AddNode("tools", toolsNode,
    graph.WithPreHandler(func(
        _ context.Context, state *AgentState,
    ) ([]schema.Message, error) {
        return schema.CloneMessages(state.Messages), nil
    }),
    graph.WithPostHandler(func(
        _ context.Context, state *AgentState, results []schema.Message,
    ) error {
        state.Messages = append(state.Messages, schema.CloneMessages(results)...)
        state.HasToolCalls = false
        return nil
    }),
); err != nil {
    return err
}

finishNode := runnable.Func[[]schema.Message, []schema.Message](func(
    _ context.Context, output []schema.Message, _ ...runnable.Option,
) ([]schema.Message, error) {
    return schema.CloneMessages(output), nil
})
if err := g.AddNode("finish", finishNode,
    graph.WithPreHandler(func(
        _ context.Context, state *AgentState,
    ) ([]schema.Message, error) {
        return schema.CloneMessages(state.Output), nil
    }),
); err != nil {
    return err
}
if err := g.AddEdge(graph.Start, "chat"); err != nil {
    return err
}
if err := g.AddBranch(
    "chat", "tools", "has_tool_calls",
    func(_ context.Context, state *AgentState) (bool, error) {
        return state.HasToolCalls, nil
    },
); err != nil {
    return err
}
if err := g.AddEdge("chat", "finish"); err != nil { // 条件不匹配时的 fallback
    return err
}
if err := g.AddEdge("tools", "chat"); err != nil {
    return err
}
if err := g.AddEdge("finish", graph.End); err != nil {
    return err
}
if err := g.SetMaxSteps(20); err != nil {
    return err
}
compiled, err := g.Compile()
if err != nil {
    return err
}

reply, err := compiled.Invoke(ctx, []schema.Message{
    schema.NewMessage(schema.RoleUser, schema.NewTextBlock("调用 echo 返回 hello")),
})
```

这段代码展示了实际的数据流：PreHandler 从 State 组装 `llm.Request`，ChatModel 产生
function call，PostHandler 写回消息；ToolsNode 只执行消息尾部的新调用，工具 PostHandler
追加 function result，条件边再把控制流送回 ChatModel。ChatModel 和 ToolsNode 都保持纯粹，
不依赖 `AgentState`。`Compile` 会在运行前验证节点、Handler、分支和边类型，`MaxSteps`
防止模型持续调用工具形成无限循环。

标准 ReAct 场景直接使用 `agent.New` 可以省去上述状态、适配节点和边定义；只有需要
审批节点、检索分支、并行任务或领域状态时才下沉到显式 Graph。`examples/react`
是同一形态的完整可运行版本。

## MCP 工具编入自定义图

`extensions/mcp` 将 MCP `tools/list`/`tools/call` 适配成 `tool.Tool` 与动态
`ToolsNode`；装配进 `agent.New` 或作为动态节点编入自定义图均可，详见
[可选扩展](extensions.md#mcp-工具)。

## 并行查询与动态分派

- 形状固定的并行查询用 `AddFork`/`AddJoin`：见[并行与分支](graph-branches.md)
  与 `examples/branches`。
- 任务清单由运行期 State 生成时用 `AddDispatch` + `graph.Send`：见
  [并行与分支](graph-branches.md#动态任务分派)与 `examples/dispatch`。

## 审批与恢复

- 工具批次审批：`agent.WithBeforeToolBatch` + `control.RequestInterrupt`，
  见[检查点与中断](checkpoints-interrupts.md#交互中断与输入恢复)与 `examples/basic`。
- 图内动态交互：`graph.Interrupt` + `WithResume`，见 `examples/interaction`。

## 结构化输出

让模型输出经严格 JSON Schema 校验解码为 Go 结构体，可直接挂成图节点：

```go
output, err := llm.NewStructuredOutput[Decision](llm.WithOutputName("decision"))
if err != nil {
    return err
}
messages, err := assistant.Invoke(ctx, input, llm.WithResponseFormat(output.Format()))
if err != nil {
    return err
}
decision, err := output.Decode(messages) // 校验失败返回 ErrInvalidStructuredOutput
```

`omitempty` 只允许指针字段（仍 required 但允许 null）；`ResponseFormat` 与
`GenerateConfig["response_format"]` 冲突在发请求前报错；默认能力声明中
`JSONSchema=false`，需要时显式 `llm.Config.Capabilities`。带工具 Agent 的完整
图接线（业务 Agent → 整理模型 → decode，多一次模型调用但不重跑业务工具）见
`examples/structured-output` 与[结构化输出](structured-output.md)。
