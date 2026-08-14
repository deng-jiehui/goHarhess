# Execution Model

## One execution contract

所有可执行组件都实现完整的 `runnable.Runnable[I, O]` 契约：`Invoke`、`Stream`、`Collect` 和 `Transform`。

- Models: `Runnable[llm.Request, llm.Response]`.
- ToolsNode: `Runnable[[]schema.Message, []schema.Message]`.
- Agent 和消息 Graph：`Runnable[[]schema.Message, []schema.Message]`。
- General graphs compile to `Runnable[I, O]`.

`Stream` 返回 `*runnable.StreamReader[O]`；编译后的 Graph 仅暴露其
完整的 END 结果。实时模型分片、工具进度和 Agent 快照通过类型化 Context EventStream 回调传递。Graph 生命周期事件使用类型化 Graph Callback 接口，绝不会进入业务数据 Reader。

## Graph 边界与纯节点

只有公开的 Graph 输入和输出使用泛型：

```go
g := graph.NewGraph[Request, Response]("research")
```

`g.AddNode` 接受具有以下方法形式的任何组件：

```go
Invoke(context.Context, NI, ...runnable.Option) (NO, error)
```

ChatModelNode、ToolsNode、MCP、Retriever 和子 Agent 节点仍是普通 Runnable。它们不会导入或检查 Graph State。挂载前使用
挂载前使用 `llm.NewChatModelNode` 包装，使模型生命周期回调和 `chat_model` 节点类型附着在组件边界。`AddNode` 记录
组件类型及输入/输出 `reflect.Type` 只记录一次；`Compile` 会在执行前校验每条边和 END 输出。

```go
_ = g.AddNode("collect", collector)
_ = g.AddNode("render", renderer)
_ = g.AddEdge(graph.Start, "collect")
_ = g.AddEdge("collect", "render")
_ = g.AddEdge("render", graph.End)
_ = g.Compile()
```

分支同样是实例方法。谓词可以消费源节点输出或运行时的精确 State 指针：

```go
_ = g.AddBranch("chat", "tools", "has_tool_calls",
    func(_ context.Context, state *AgentState) (bool, error) {
        return state.HasToolCalls, nil
    },
)
_ = g.AddEdge("chat", "finish") // unconditional fallback
```

## Runtime State

State 是可选的。有状态 Graph 注册一个工厂，其结果必须是非 nil 的结构体指针：

```go
type ResearchState struct {
    Topic string
    Notes []string
}

_ = g.SetStateFactory(func(_ context.Context, input Request) (*ResearchState, error) {
    return &ResearchState{Topic: input.Topic}, nil
})
```

每次调用拥有一个具体的 `*ResearchState`。顺序节点复用
该精确指针。用户无需实现 `Clone`、调用 `GetState`，也不要从纯组件修改 State。由 Graph 所有的 Handler 负责映射边界：

```go
_ = g.AddNode("search", retriever,
    graph.WithPreHandler(func(
        _ context.Context, state *ResearchState,
    ) (retrieval.Query, error) {
        return retrieval.Query{Text: state.Topic}, nil
    }),
    graph.WithPostHandler(func(
        _ context.Context, state *ResearchState, output retrieval.Result,
    ) error {
        state.Notes = append(state.Notes, output.Text)
        return nil
    }),
)
```

没有 PreHandler 时，入边值必须可赋给节点输入。PostHandler 可以修改 State，但不会替换出边值；节点输出仍可供下游节点和基于输出的分支使用。

Graph 持有拓扑锁时不会运行用户函数。节点和 Handler 接收节点执行 Context，其中包含取消、截止时间、Trace 和 Callback span 值。

## State snapshots

顺序执行不会序列化 State。快照工作只发生在所有权或持久性边界：

| Boundary | Codec work |
| --- | --- |
| Sequential node or branch | none |
| Fan-Out | encode base once, decode one State per branch |
| 持久化中断 | 请求 Checkpoint 时延迟编码 |
| Resume | decode once at Graph entry |
| Ordinary completion | none |

默认 State Codec 是严格 JSON。State 字段必须导出，不能使用 `json:"-"`；通道、函数、非安全指针及其他运行时资源会被拒绝。存储在 `any` 中的动态值必须在快照前后保持精确的 Go 类型。对于 protobuf、msgpack、私有字段或领域资源，请安装类型化自定义 Codec：

```go
codec, err := graph.NewStateCodec[*ResearchState](
    "research-state", 1, encodeResearchState, decodeResearchState,
)
if err == nil {
    err = g.SetStateCodec(codec)
}
```

Fan-Out 分支接收独立的 State 指针，并通过显式 `WithStateMerger` 按声明顺序合并。分支失败会取消兄弟分支并跳过合并。

## Durable cursors

每个执行中的节点都会压入一个可安全编码为 JSON 的 `graph.CursorFrame`。中断的 Checkpoint 持久化完整 `CursorStack`；Resume 消费匹配的
顶层帧，无需重建 Go 调用栈。Context、goroutine、客户端和工具批次等运行时对象从不存储。

可恢复 Graph 要求每个可能的恢复目标都有 State Codec 和 PreHandler。任意异构边值不会被序列化。Graph 会在调用用户代码前校验 Checkpoint Schema、Graph 名称、确定性版本、Codec 和恢复节点。

只有组件返回或设为 `context.WithCancelCause` cause 的类型化 `control.InterruptRequest` 才会创建 Continuation。普通取消
普通取消和截止时间不会保存 Continuation。按调用配置持久化，并使用同一个由应用拥有的 key 恢复：

```go
output, err := g.Invoke(ctx, input, graph.WithCheckpoint(store, key))
if errors.Is(err, graph.ErrInterrupted) {
    output, err = g.Resume(context.Background(), graph.WithCheckpoint(store, key))
}
```

恢复语义为 at-least-once。执行中断的节点以及中断并行节点的每个分支都可能再次运行。框架有意不提供 Lease、分布式锁、fencing 或一次性 Checkpoint 删除；多副本应用必须在进入 Graph 前串行化 Resume。

内置 Agent 配方使用同一 Graph Continuation。普通顺序运行不会编码 State。Agent Prompt 会在每次模型调用前立即注入，因此持久化 State 保存调用方拥有的会话历史，而不是旧 Agent 实例的 Prompt 或凭据。

## Option 与 Context

metadata 等框架 Option 通过 `runnable.OptionPlan` 级联。
Provider- or node-specific options use `runnable.NodeOption`; graph-local node
Option 使用 `graph.WithNodeOptions`。State Handler 属于 Graph Option，绝不会转发给纯节点。

取消、截止时间、Trace、Callback 和身份信息仍保留在
`context.Context`。Graph Checkpoint 配置属于调用 Option，
由 Graph 消费，不会转发给纯节点。

## Agent recipes

`pkg/agent` 是同一 Graph 引擎上的快速组装层：

- Basic: one `chat_model` node.
- ReAct：`chat_model -> tools -> chat_model`，受模型调用和 Graph
  step limits.
- Routing: `decide(chat_model) -> agent:<name> -> decide` in Supervisor mode;
  Router 模式执行一个选定的 Agent 节点后结束。
- GroupChat: `select -> agent:<name> -> select`, bounded by round count.

`agent.AgentNode` 是公开的子节点适配器。它返回类型化 `runnable.Outcome`，使 Supervisor 模式可以将子节点失败转为
观测，而 Router 和 GroupChat 可以传播同一失败。子执行仍是真实的 Graph 节点，因此快照、回调和 Continuation 恢复点会暴露所选 Agent 名称，而不是不透明的分发步骤。

嵌套 Agent 和 Graph 创建独立的生命周期作用域。Graph 节点拥有自己的子 span，因此 APM 系统仍能保留节点级瀑布时序。
除节点 ID 和 step 外，`graph.NodeExecution` 还携带编译后的节点类型及输入/输出类型。

## Compilation

`Compile` 冻结 Graph，并校验拓扑、可达性、条件
回退、节点和 Handler 类型、END 输出、State Factory、State Codec、并行分支以及嵌套 Graph 循环。Reflection 用于绑定和校验类型并调用擦除类型的节点适配器；应用节点仍接收其声明的 Go 类型。
