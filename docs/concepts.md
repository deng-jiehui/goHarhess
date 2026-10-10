# 核心概念

## 一个执行契约

所有可执行组件都实现 `runnable.Runnable[I, O]`：

```go
type Runnable[I, O any] interface {
    Invoke(context.Context, I, ...runnable.Option) (O, error)
    Stream(context.Context, I, ...runnable.Option) (*runnable.StreamReader[O], error)
    Collect(context.Context, *runnable.StreamReader[I], ...runnable.Option) (O, error)
    Transform(context.Context, *runnable.StreamReader[I], ...runnable.Option) (*runnable.StreamReader[O], error)
}
```

- Models：`Runnable[llm.Request, llm.Response]`。
- ToolsNode：`Runnable[[]schema.Message, []schema.Message]`。
- Agent 和消息 Graph：`Runnable[[]schema.Message, []schema.Message]`。
- General graphs compile to `Runnable[I, O]`。

组件通常只需要实现最自然的一种执行形式，再通过 `runnable.Adapt` 或 Lambda 构造器补齐
其余形式。`Stream` 返回 `*runnable.StreamReader[O]`；编译后的 Graph 仅暴露其
完整的 END 结果。消息增量等实时数据经 `pkg/stream` 公共事件流发布，运行期调用树观测
使用 `pkg/observe`，二者都不与最终业务输出混在一起。

框架从底到上的分层是：

| 层级 | 职责 |
| --- | --- |
| Runnable | 统一 ChatModel、ToolNode、Retriever、Agent 和 Graph 的调用契约 |
| ChatModel / Tool / MCP | 提供可复用的模型与工具节点能力 |
| Graph | 定义节点、边、条件、循环、并行和强类型状态流动 |
| Agent | `agent.New(model, opts...)` 单一入口：无工具为 Basic，有工具为 ReAct；Routing、GroupChat 编排见 `examples/routing`、`examples/groupchat` 图示例 |
| App / Gateway | 由应用自行注册多个 Runnable、选择路由并管理 HTTP 或任务入口 |

一次典型的工具 Agent 图如下：

```text
[]schema.Message
        |
        v
Context Engine -> ChatModel (llm.ChatModel)
                       |
             assistant tool-call messages
                       v
                 ToolNode (tool.ToolsNode)
                       |
             ordered function-result messages
                       +------> ChatModel (next turn)
```

- `llm.ChatModel` 是 provider-neutral 的模型契约，输入 `llm.Request`，输出
  完整 `llm.Response`。实时模型增量通过 Context EventStream 发布。
- `tool.ToolsNode` 只执行消息末尾连续的 assistant function calls，返回有序的
  tool-result 消息；它本身也是一个 `Runnable[[]schema.Message, []schema.Message]`。
- `extensions/mcp` 将 MCP `tools/list` 和 `tools/call` 适配成 `tool.Tool` 与
  动态 `ToolsNode`，因此 MCP 工具和本地工具可以进入同一张图。
- `graph.Graph[I, O]` 负责节点、边、条件、并行和运行步数。业务 `*State` 是每次运行
  独占的 Context 状态，通过 Pre/Post Handler 与纯节点衔接。

Agent API 是这套图编排能力的快捷装配层。`agent.New` 在有工具时自动组装有界的
`ChatModel -> tools -> ChatModel` ReAct 图。Agent 与公开的 `tool.ToolsNode` 共用
`tool.Runtime`，统一获得 Middleware、鉴权、超时、只读重试、递归保护和并发组调度；
自定义 Graph 与 MCP 动态节点不会进入另一套工具执行路径。

## 调度循环与执行模式

Graph 只有一个调度循环。`Invoke`/`Resume` 与 `Stream`/`ResumeStream` 共用同一套控制语义：
路由、步数上限、嵌套深度、CursorFrame、中断检测与落盘、Pre/Post Handler 和生命周期事件。
两者唯一的区别是执行模式，它只决定每个节点采用哪一种执行形态、边上传递值还是流：

| 模式 | 节点执行形态 | 边上的数据 |
| --- | --- | --- |
| Invoke | 一律 `Invoke` | 完整值 |
| Stream | 输入为流时优先 `Transform`，其次 `Collect`；输入为值时优先 `Stream`，否则 `Invoke` | 值或流 |

Stream 模式在以下位置把流物化为完整值：节点有 PreHandler 或 PostHandler、节点是并行节点、
节点既无 `Transform` 也无 `Collect`、源节点存在条件出边、到达 END。配置 Checkpoint 或包含并行节点时，
Stream 仍按上述规则以流形态执行节点，不会退化为 Invoke。

每一步的固定顺序为：步数检查 → 前置中断检查 → 游标/NodeStart → PreHandler → 节点执行 →
执行后中断检查 → PostHandler 提交 → NodeOutput/NodeEnd → 提交后路由 → AfterCommit 中断检查。
流形态节点的 NodeEnd 延后到其输出流被下游消费完或提交之后。

## 原生并行分支

`AddFork` 和 `AddJoin` 可以直接组织普通节点的扇出与汇合。调度器每轮执行一批任务，短分支到达后等待长分支；整轮成功后通过 `SetStepMerger` 合并 State，再保存下一轮任务和汇合记录。`SetMaxConcurrency` 限制并发数，`MaxSteps` 按累计节点任务数计算。`AddBranch` 继续保持首个命中语义。

原生分支图要求普通节点有 PreHandler，并显式配置合并器。分支中的流在轮末提交前收集完毕，流执行失败不会提交 State。完整规则、限制与示例见 [并行与分支](graph-branches.md)。

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
`llm.NewChatModelNode` 包装，使模型生命周期回调和 `chat_model` 节点类型附着在组件边界。`AddNode` 记录
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

处理函数另有一组编译期检查签名的入口，类型参数全部从函数签名推导，生成的闭包直接调用、不再经过反射：
`graph.WithPreHandler`、`graph.WithPostHandler`、`graph.WithStateMerger` 本身就是泛型函数；`SetStateFactory`、`AddBranch`、`SetStepMerger` 因 Go 方法不能声明类型参数，提供同名包级函数 `graph.SetStateFactory(g, fn)`、`graph.AddBranch(g, from, to, label, fn)`、`graph.SetStepMerger(g, fn)`。两组入口混用合法，`Compile` 的类型与拓扑校验对两者完全一致。

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

### State 节点

当业务步骤本身就围绕 State 组织（读取字段、调用组件、就地写回）时，用 `graph.StateNode` 代替"组件 + Pre + Post"三段式；输入输出固定为 `*S`，函数内直接修改 State，不凭空产生业务输出：

```go
_ = g.AddNode("search", graph.StateNode(func(
    ctx context.Context, state *ResearchState,
) error {
    result, err := retriever.Invoke(ctx, retrieval.Query{Text: state.Topic})
    if err != nil {
        return err
    }
    state.Notes = append(state.Notes, result.Text)
    return nil
}))
```

State 节点自动携带恒等 PreHandler（`AddNode` 与并行分支识别该形态，显式 `WithPreHandler` 仍可覆盖），因此天然满足可恢复性与动态中断对 Pre 的要求，函数内的 `graph.Interrupt`、选项与回调照常生效。State 节点链可零胶水直连；接普通节点时由下游自己的 PreHandler 提取输入，作为图结束节点时由 finish 类节点声明输出。并行分支中的 State 节点修改分支自己的 State 副本，轮末仍由 merger 合并；未指定 Post 时默认为空操作。

Graph 持有拓扑锁时不会运行用户函数。节点和 Handler 接收节点执行 Context，其中包含取消、截止时间、Trace 和 Callback span 值。

## State 快照

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

## 持久化游标

每个执行中的节点都会压入一个可安全编码为 JSON 的 `graph.CursorFrame`。中断的 Checkpoint 持久化完整 `CursorStack`；Resume 消费匹配的
顶层帧，无需重建 Go 调用栈。Context、goroutine、客户端和工具批次等运行时对象从不存储。

可恢复 Graph 要求 State 可序列化，且每个可能的恢复目标节点具备输入重建能力：有 PreHandler 的节点由 PreHandler 从 State 重建输入；没有 PreHandler 的顺序节点在调度时把边输入冻结为 JSON 存入任务（编码发生在执行线程，节点之后的修改不影响已保存输入），其输入类型必须能 JSON 往返——接口、`chan`、函数、Reader 等类型在编译期就使图失去可恢复性，嵌套动态值（如 `map[string]any` 中的数字）在保存时做解码比对，漂移会被拒绝。Graph 会在调用用户代码前校验 Checkpoint Schema、Graph 名称、确定性版本、Codec 和恢复节点。

恢复语义为 at-least-once。执行中断的节点以及中断并行节点的每个分支都可能再次运行。框架有意不提供 Lease、分布式锁、fencing 或一次性 Checkpoint 删除；多副本应用必须在进入 Graph 前串行化 Resume。

## Option 与 Context

调用选项是不透明的 `runnable.Option`，只承载组件运行配置：每条配置按类型
归属组件，由 `runnable.WrapOption` 构造、`runnable.ResolveComponentOptions`
在所属组件入口解析一次。Graph 在运行入口完成静态分发：无目标选项按
`OptionTypes()` 声明的类型广播给同级节点；`runnable.ForNode`/
`runnable.ForPath` 定向选项按节点路径逐层消费，转发型节点（`OptionRelay`，
如 `ToolsNode`、`AgentNode`、嵌套图）把剩余路径交给内部组件继续解析。
无接收者的类型在入口明确报错。

追踪元数据统一经 `context.Context` 传递，与 Option 完全分离：

```go
ctx := observe.WithMetadata(ctx, map[string]any{
    "request_id": "req-123",
    "source":     "web",
})
```

`observe.WithMetadata`/`observe.WithoutMetadata` 基于当前快照返回派生
ctx，`observe.MetadataFromContext` 返回独立副本；三者对 map、slice 与
嵌套容器做递归复制，父调用与并行分支互不影响。框架在对应调用边界写入
自身信息——Agent 加载会话后注入 `conversation_id`/`checkpoint_version`，
Graph 节点与工具调用沿用既有回调入口的补充标签。组件需要分支级标签时
创建自己的派生 ctx，只影响该分支及其下游。元数据不写入检查点；需要随
会话保存的数据使用 `agent.WithStateValue`/`WithStateValues`。

取消、截止时间、Trace、Callback 和身份信息仍保留在
`context.Context`。Graph Checkpoint、Durability、恢复输入等配置归属
Graph 自身类型，在当前图入口消费，不会转发给纯节点。

## Agent 配方

`pkg/agent` 是同一 Graph 引擎上的快速组装层：

- Basic: one `chat_model` node.
- ReAct：`chat_model -> tools -> chat_model`，受模型调用和 Graph step limits。
- Routing 与 GroupChat 不再是公开配方类型，基于图门面与 `AgentNode` 的可运行
  实现见 `examples/routing`（`decide(chat_model) -> members -> supervisor`
  循环）与 `examples/groupchat`（`pick -> speak -> pick`，按轮数收口）。

每种配方有独立的 State（`ToolState`，公共部分为 Input/Messages/Response），
这是运行状态的唯一真相源：Handler 直接读写 State，纯节点只通过 PreHandler 取输入。
Context 只携带按配方实例隔离的只读运行配置（原始输入、模型调用预算、完成回调），
因此嵌套的子 Agent 无法读到父级配方的状态。ReAct 的模型调用预算在首次运行时写入 State，恢复时沿用。

`agent.AgentNode` 是公开的子节点适配器。子 Agent 的业务失败以类型化 `runnable.Outcome` 返回，使 Supervisor 模式可以将其转为
观测，而 Router 和 GroupChat 可以传播同一失败；控制类失败（中断、不支持的中断、缺少检查点、父级取消）直接作为错误返回，由父级 Graph 失败或挂起，绝不会变成模型可见的观测。子执行仍是真实的 Graph 节点，因此快照、回调和检查点中的恢复位置会暴露所选 Agent 名称，而不是不透明的分发步骤。

嵌套 Agent 和 Graph 创建独立的生命周期作用域。Graph 节点拥有自己的子 span，因此 APM 系统仍能保留节点级瀑布时序。
除节点 ID 和 step 外，`graph.NodeExecution` 还携带编译后的节点类型及输入/输出类型。

## 编译

`Compile` 冻结 Graph，并校验拓扑、可达性、条件
回退、节点和 Handler 类型、END 输出、State Factory、State Codec、并行分支以及嵌套 Graph 循环。Reflection 用于绑定和校验类型并调用擦除类型的节点适配器；应用节点仍接收其声明的 Go 类型。
