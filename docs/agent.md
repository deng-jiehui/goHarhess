# Agent 装配与编排

`agent.New` 是 Agent 的单一构造入口：未配置工具时创建 Basic Agent；配置
`WithTools` 后创建有界的 `chat_model -> tools -> chat_model` ReAct 图，
受 `MaxLLMCalls` 与 Graph `MaxSteps` 限制。

```go
assistant, err := agent.New(model,
    agent.WithName("assistant"),
    agent.WithSystemMessage("You are a helpful assistant."),
    agent.WithTools(echo),
    agent.WithMaxLLMCalls(4),
)
```

## 选择 Agent 工作流

| 场景 | 推荐 API | 运行方式 |
| --- | --- | --- |
| 单次模型回答 | `agent.New`（不带工具） | 一次模型调用 |
| 模型调用本地或 MCP 工具 | `agent.New` + `WithTools` | `LLM -> Tool -> LLM`，受 `MaxLLMCalls` 限制 |
| 主管模型分派专长 Agent | `graph.NewState` 门面 + `AgentNode` | 路由边选择成员，见 `examples/routing` |
| 多 Agent 轮流讨论 | `graph.NewState` 门面 + `AgentNode` | 选择器节点 + 轮数路由，见 `examples/groupchat` |

主管路由用单次求值的 `Branch` 路由边表达：决策节点产生路由目标，成员以
`agent.NewAgentNode` 挂为真实的 `agent:<name>` 图节点：

```go
app, err := graph.NewState[routeState]("dispatcher").
    Node("decide", callSupervisor).
    Node("member", dispatchMember).
    Start("decide").
    Branch("decide", route, "member", graph.End).
    Edge("member", "decide").
    Compile()
```

子 Agent 收到的是主管作出路由决定之前的对话快照（`DispatchInput` 语义），
不包含没有配对结果的主管工具调用消息。成员挂为真实的 `agent:<name>` Graph
节点后，Graph Snapshot、节点观测事件（`observe.KindNode`）和检查点中的执行
进度都能直接显示具体成员。

## 编排多个 Agent

Agent 本身也是 Runnable，因此可以继续作为更大图中的节点。State 驱动图用
`graph.NewState[S]` 门面创建 Builder：节点是 `func(context.Context, *S) error`，
`Edge`/`Branch`/`Parallel`/`Send` 直接以 State 为输入输出，`Compile()`
产出不可变、可并发复用的 `CompiledGraph`。完整示例见 `examples/graph/main.go`。

```go
app, err := graph.NewState[researchState]("research-team").
    Node("research", search).
    Node("review", review).
    Node("finish", summarize).
    Start("research").
    Edge("research", "review").
    Edge("review", "finish").
    Edge("finish", graph.End).
    Compile()
if err != nil {
    return err
}

result, err := app.Invoke(ctx, researchState{Query: "分析这个主题。"})
```

非 State 节点（`ChatModelNode`、`AgentNode` 等纯 Runnable）经
`builder.Graph().AddNode(name, node, WithPreHandler, WithPostHandler)` 接入同一图，
Pre/Post Handler 在业务 State 与节点输入输出之间映射。State 必须是指向
结构体的指针；顺序节点复用同一指针，用户不需要实现 `Clone`。默认 JSON State Codec
要求字段可导出且不能使用 `json:"-"`；复杂类型可通过 `graph.NewStateCodec` 提供自定义
Codec。序列化只发生在 Fan-Out、持久化中断和恢复边界。节点输出为空时，流也会发送空
`Snapshot`，因此前端必须用它替换旧结果，而不是忽略空快照。

## 让模型创建和管理子 Agent

`extensions/subagent`（独立模块 `github.com/goharness/goharness/extensions`）让父 Agent 在工具调用里把任务交给同步运行的子 Agent。使用者规定可选范围，模型提出任务与选择，框架解析并同步执行——子 Agent 的结果就是该次工具调用的结果：

```go
mgr, err := subagent.New(subagent.Config{
    Models:       map[string]llm.ChatModel{"main": mainModel, "review": reviewModel},
    DefaultModel: "main",
    Tools:        []tool.Tool{readFile, searchCode},
    DefaultTools: []string{"read_file"},
    Roles: map[string]subagent.Role{
        "auditor": {Instructions: "只做安全审查。", Model: "review", Tools: []string{"read_file", "search_code"}},
    },
    Instructions: "回答简洁，给出可复现的证据。",
    Store:        store, // 会话子 Agent 记录；MemoryStore 即可
})
parent, err := agent.New(mainModel,
    agent.WithName("lead"),
    agent.WithTools(mgr.Tools()...),
)
out, err := parent.Invoke(ctx, messages, agent.WithCheckpoint(store, "conversation-1"))
```

- **一次性任务**：`run_subagent`（`task` 必填，可选 `instructions`、`model`、`tools`、`role`）同步运行一个临时子图并直接返回结果，不创建会话记录，无 `Store` 也可用。
- **会话子 Agent**：`spawn_agent` 参数同上并多一个可选 `name`，创建持久记录并同步执行首个任务，返回 `agent_id`、`task_id` 与 `result`。`followup_task`（`agent_id` + `task`）给空闲子 Agent 追加任务并沿用其上下文；`list_agents`/`get_agent` 查看概览与历史；`close_agent` 关闭后记录仍可读取但拒绝新任务。会话记录以 `<conversationID>/agents/...` 独立键保存在 `Config.Store` 中，与父检查点分离。
- **创建者作用域**：每个调用方只能访问自己直接创建的子 Agent——子 Agent 也只能访问自己创建的孙 Agent；访问其他调用方创建的记录得到 `not found`。
- **注册表约束**：模型与工具名称只能取自注册表；超出范围、未知角色或未知名称得到列出可用名称的模型可见错误结果，父 Agent 可据此修正。
- **审批与恢复**：子 Agent 内的 `graph.Interrupt` 经命名空间执行树传播到根运行，统一返回 `InterruptedError`，从根入口用问题 ID `agent.Resume`；同批已完成的兄弟调用结果直接复用，不重复创建、不重复执行。父运行必须配置检查点，否则子 Agent 内的审批以 `ErrCheckpointRequired` 失败。
- **并发**：同一工具批次内的多个子 Agent 调用并发执行；`Config.MaxConcurrent` 提供扩展内并发上限。
- **放弃运行的清理**：`state.AbandonRun` 不回滚子 Agent 记录（子 Agent 数据属于会话层）；需要整洁时在 abandon 后调用 `mgr.Cleanup(ctx, conversationID, abandonedRunID)` 显式移除该轮创建的记录。
- **自建图**：在 `Config.Builder` 返回自己的图（输入输出为 `[]schema.Message`，需要有 State 才能恢复），同时给出新的 `BuilderID` 或 `BuilderVersion`；预置 Agent 用 `subagent.FromAgent` 包装。构建函数只收到组件配置与指令，任务文本从运行输入取得。图的上下文若不是消息历史，提供自己的 `Config.Codec`。
- **恢复能力必须明确声明**：返回的执行器要实现 `Recoverable() error`。`graph.CompiledGraph` 已实现；给图加日志、监控等外层包装时，让包装类型嵌入具体的图类型使方法被提升，或显式转发，或用 `subagent.WithRecovery(runner, check)` 声明。没有声明或声明不可恢复，创建任务时会在登记前以 `subagent.ErrNotRecoverable` 报错，不会留下子 Agent。构建函数还可以返回 `*subagent.ConfigError` 表示模型可修正的配置问题，模型会看到它的文本；恢复不兼容、检查点错误等框架错误则原样上交。会话记录损坏或与其登记实现不符时返回 `ErrAgentCorrupt`。
- **后台任务**：不提供——子 Agent 随发起它的工具调用同步运行，不存在后台存活、运行中发消息或取消的接口。

在 `extensions/` 目录运行 `go run ./examples/subagent` 可看到完整生命周期（本地脚本模型，无需外部模型服务）：创建批次出现一次审批恢复提示，汇总输出首轮结果与沿用上下文的追加结果。

## 延伸阅读

- [核心概念](concepts.md)
- [工具运行时](tool-runtime.md)
- [结构化输出](structured-output.md)：模型 Option 经 Agent 现有分发到达模型；
  推荐显式组成「业务 Agent → 整理模型 → decode」，多一次模型调用但格式化
  失败不会重新调用业务工具
- [检查点与中断](checkpoints-interrupts.md)
- [Cookbook](cookbook.md)
