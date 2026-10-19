# 多 Agent 协作指南

按优先级选模式：**静态工作流 → Agent 当工具 → handoff（近似写法）→
跨进程**。越靠前越确定、越好测、越便宜；只在确有动态委派需求时往后走。

## 模式选择

| 模式 | 适用 | 怎么做 |
| --- | --- | --- |
| 静态工作流 | 步骤与路由在编译期已知 | `graph.NewState` + 普通节点/`AgentNode` + 条件边；最推荐 |
| Agent 当工具 | 主管模型动态挑专长 | `subagent.AsTool` 把专家挂成工具；主管仍是普通 ReAct |
| handoff | 会话控制权整体移交 | 框架不提供终止型工具（W4-8 已评估关闭）；用结构化输出+条件边近似，见下 |
| 跨进程 | 对端是独立服务 | 远端 Agent 包成 FunctionTool，或自己的 Agent 经薄 HTTP 层暴露；见[对接说明](integration.md#a2a-对接) |

## Agent 当工具（`subagent.AsTool`）

```go
expert, _ := agent.New(expertModel, agent.WithName("db-expert"), agent.WithTools(sqlTool))
lead, _ := agent.New(mainModel,
    agent.WithTools(subagent.AsTool(expert, "db_expert", "数据库问题专家")))
```

- 专家默认**只收到任务描述**（`{"task": "..."}`），不转父历史；
  `WithHistory(filter)` 按过滤器裁剪后附带。
- 专家事件默认不透传父运行的公共事件流；`WithEmitEvents()` 打开
  （观测与检查点不受影响）。
- `WithInputSchema` 可换成自定义参数结构，`build` 负责把参数翻译成
  专家输入消息。

## handoff 的近似写法（W4-8 关闭后的替代）

没有「工具结果即最终答案」的原语；用**结构化输出 + 预声明条件边**
达到同等效果：

```go
// 主管输出 {"handoff": "tech"} → 条件边路由到对应 Agent 节点；
// 目标全部预声明，编译期校验，模型给未知值时报错（可提示重试）。
Node("supervise", structuredOutputNode).
Branch("supervise", routeByHandoff, "billing-agent", "tech-agent", "done")
```

与真 handoff 的差别：移交目标集合固定（恰好是预声明的优势——可审计），
主管每轮决策都有一次模型调用成本。多数场景够用。

## 与 LangGraph / Eino 对照

| 概念 | goHarness | LangGraph | Eino |
| --- | --- | --- | --- |
| 静态多节点流 | `graph.NewState` + Edge/Branch | StateGraph + addEdge/addConditionalEdges | Chain/Graph + AddEdge |
| 主管分派成员 | `graph.Send` 动态分派 或 `AgentNode`+Branch | Command/Send | `AgentNode` 分支 |
| Agent 当工具 | `subagent.AsTool` | 手工包 tool calling agent | adk 的 agent-as-tool |
| 会话子 Agent | `subagent.New`（spawn/followup/list/close） | 无内建等价 | 无内建等价 |
| 中断审批 | `graph.Interrupt` + `Resume` | `interrupt()` | `Interrupt` |
| 可视化 | `Snapshot.Mermaid()`/`DOT()` | `get_graph().draw_mermaid()` | — |

## 测试多 Agent 流程（脚本化模型）

```go
mgr, _ := subagent.New(subagent.Config{
    Models: map[string]llm.ChatModel{"main": llm.NewScriptedModel(
        []schema.Message{schema.NewMessage(schema.RoleAssistant,
            schema.NewFunctionToolCallBlock("c1", "run_subagent",
                `{"task":"审接口","role":"auditor"}`))},
        []schema.Message{schema.AssistantMessage("已委派并汇总")}),
    },
    Roles: map[string]subagent.Role{
        "auditor": {Instructions: "只做安全审查。", Model: "main"},
    },
})
```

脚本化模型按调用顺序给固定答复，多 Agent 编排因此可单测：断言
主管发出的子任务、子 Agent 的结果回流、审批中断/恢复路径。

## 护栏

| 护栏 | 配置 | 说明 |
| --- | --- | --- |
| 单任务模型调用上限 | `subagent.Config.MaxLLMCalls` / `agent.WithMaxLLMCalls` | 超限返回模型可修正的错误结果，非静默截断 |
| 单任务时限 | `subagent.Config.TaskTimeout` | 按次计，不含排队 |
| 子 Agent 并发 | `subagent.Config.MaxConcurrent` | 超限等待空位 |
| 任务/指令长度 | `subagent.Config.MaxTaskRunes` | 防失控长任务 |
| 图步数 | `graph` `MaxSteps` | 防死循环 |
| 整次请求预算 | `limits.Bulkhead` + ctx deadline | 端到端兜底 |
| 委派深度 | 应用自定：Agent 嵌套层数建议在 prompt/角色设计里封顶 | 子 Agent 只能访问自己创建的孙 Agent（创建者作用域） |

## 同步子 Agent 的限制与应对

子 Agent 随发起它的工具调用**同步**运行：没有后台存活、运行中发消息、
取消接口。对应做法：

- 长任务 → [异步任务模型](integration.md#异步任务模型)：整次运行入队，
  工具调用立即返回任务 ID。
- 需要等待外部事件 → `graph.Interrupt` 挂起，由外部回调触发 Resume。
- 需要流式观察子 Agent 进度 → `EmitEvents` 透传事件流；父运行本身
  用 SSE 输出。
- 同批多个子 Agent 调用并发执行，受 `MaxConcurrent` 约束；放弃运行
  后用 `mgr.Cleanup` 清掉该轮创建的会话子 Agent 记录。
