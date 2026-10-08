# 底层组件源码与面试导读重写实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `docs/底层组件设计与核心代码.md` 重写为一篇从底层依赖到 Agent 主链路的源码导读，使读者能解释设计动机、实现边界和面试追问。

**Architecture:** 正文按运行时依赖而非包名字母顺序展开：先解释消息和执行协议，再解释编排与持久化，随后进入副作用、模型和上下文治理，最后由 Agent 和扩展把组件串成完整请求。每个章节只保留能回答“为什么有它、谁调用它、它怎样失败、它不负责什么”的代码与说明。

**Tech Stack:** Go 1.24、Markdown、仓库现有 Go 源码和测试、PowerShell、`rg`。

## Global Constraints

- 只修改 `docs/底层组件设计与核心代码.md`；不修改 Go 源码、题库或已有使用手册。
- 保留真实包路径、导出符号和行为边界；教学缩略代码必须明确标记，不能伪装成原样源码。
- 代码必须嵌入解释：先给问题和结论，再给关键片段，随后逐行解释输入、状态变化和输出。
- 深入解释 `schema`、`runnable`、`graph`、`state`、`tool`、`llm`、`contextengine`、`agent` 主链路；`retrieval`、`httpclient` 和 `extensions` 只解释它们对主链路新增的能力与边界。
- 每章至少包含职责边界、真实源码入口、一次调用中的位置、错误/取消/并发边界和 2-4 个高价值面试追问。
- 不复制 `docs/项目面试题与参考答案.md` 的长答案；必要时引用题号或将答案浓缩为源码理解检查。
- 不提交或暂存文件：当前工作区含用户的未跟踪文档，提交由用户另行决定。

### Task 1: 建立学习主线与基础数据模型

**Files:**
- Modify: `docs/底层组件设计与核心代码.md`
- Read: `README.md`, `examples/basic/main.go`, `examples/graph/main.go`, `pkg/schema/message.go`, `pkg/schema/block.go`, `pkg/schema/clone.go`, `pkg/schema/stream.go`, `pkg/runnable/runnable.go`, `pkg/runnable/stream_reader.go`, `pkg/runnable/adapter.go`, `pkg/runnable/concat.go`
- Test references: `pkg/schema/message_test.go`, `pkg/schema/block_test.go`, `pkg/runnable/runnable_test.go`, `pkg/runnable/stream_reader_test.go`

**Interfaces:**
- Consumes: 项目模块图、`schema.Message` 的有序 `ContentBlocks`、`runnable.Runnable[I, O]`。
- Produces: 读者理解后续 Graph 和 Agent 前必须掌握的输入输出模型、流式关闭语义和阅读顺序。

- [ ] **Step 1: 重写开篇为“问题、边界、阅读方法”**

  说明 goHarness 是强类型 Graph 编排框架，不是模型 SDK；给出从用户消息到 Agent 输出的简图，并明确本文以真实调用链组织而不是罗列 API。

- [ ] **Step 2: 写入 `schema` 章节**

  用 `Message`、`ContentBlock`、工具调用/结果块和 `ResponseMeta` 说明为什么消息不能被扁平化为字符串；解释顺序、克隆、验证和流式 Delta 合并的实际风险。

- [ ] **Step 3: 写入 `Runnable` 与 `StreamReader` 章节**

  用四种输入输出形态解释 `Invoke`、`Stream`、`Collect`、`Transform`；用 `Send`、`Close`、`CloseWithError` 的关键分支解释背压、EOF、消费者提前退出和 context 取消。

- [ ] **Step 4: 以测试核对基础语义**

  运行：

  ```powershell
  go test -count=1 ./pkg/schema ./pkg/runnable
  ```

  记录成功或失败结果；不为文档任务修改测试或代码。

### Task 2: 解释观察、编排与可恢复状态

**Files:**
- Modify: `docs/底层组件设计与核心代码.md`
- Read: `pkg/callback/dispatcher.go`, `pkg/callback/context.go`, `pkg/callback/event_stream.go`, `pkg/control/control.go`, `pkg/graph/compile_plan.go`, `pkg/graph/compiled.go`, `pkg/graph/runtime_execute.go`, `pkg/graph/runtime_state.go`, `pkg/graph/parallel.go`, `pkg/graph/continuation.go`, `pkg/state/checkpoint.go`, `pkg/state/memory.go`
- Test references: `pkg/callback/context_test.go`, `pkg/graph/compiled_test.go`, `pkg/graph/runtime_parallel_test.go`, `pkg/graph/continuation_test.go`, `pkg/state/checkpoint_test.go`

**Interfaces:**
- Consumes: `Runnable` 执行契约和 `schema` 消息。
- Produces: Graph 的编译期边界、每次运行 State 的所有权、并行隔离，以及会话 Checkpoint 与执行 Continuation 的区别。

- [ ] **Step 1: 写入 Callback 与 Control 章节**

  区分低频生命周期 Callback、高频 EventStream 和有控制权的 `control.InterruptRequest`；说明为什么观察者不能替代鉴权或业务控制。

- [ ] **Step 2: 写入 Graph 编译与执行章节**

  解释 Builder 与 `CompiledGraph` 分离、节点/边/条件分支的编译检查、State Handler、最大步数，以及为何用户代码不在框架锁内执行。

- [ ] **Step 3: 写入并行、Interrupt 与 Checkpoint 章节**

  对比顺序 State、分支隔离 State、会话 `state.Checkpoint` 与 `graph.Continuation`；解释 JSON 快照、版本冲突、恢复前校验和 at-least-once 的业务含义。

- [ ] **Step 4: 以代表性测试核对控制流**

  运行：

  ```powershell
  go test -count=1 ./pkg/callback ./pkg/control ./pkg/graph ./pkg/state
  ```

### Task 3: 解释副作用、模型与上下文治理

**Files:**
- Modify: `docs/底层组件设计与核心代码.md`
- Read: `pkg/tool/tool.go`, `pkg/tool/node.go`, `pkg/tool/runtime.go`, `pkg/tool/runtime_context.go`, `pkg/tool/runtime_scheduler.go`, `pkg/tool/middleware.go`, `pkg/tool/batch_hook.go`, `pkg/llm/model.go`, `pkg/llm/node.go`, `pkg/llm/openai_compatible.go`, `pkg/llm/sse.go`, `pkg/llm/retry.go`, `pkg/contextengine/engine.go`, `pkg/contextengine/builder.go`, `pkg/contextengine/token.go`
- Test references: `pkg/tool/runtime_test.go`, `pkg/tool/node_test.go`, `pkg/llm/openai_compatible_test.go`, `pkg/llm/response_stream_test.go`, `pkg/contextengine/engine_test.go`

**Interfaces:**
- Consumes: 图中的消息流、`Tool` 的 `Spec` 和 `CallInput`、`llm.ChatModel` 请求响应。
- Produces: Tool Runtime 的安全边界、SSE/流式协议、模型适配边界和 Token 预算计算的解释。

- [ ] **Step 1: 写入 Tool Runtime 章节**

  以 Assistant 工具调用到 `ToolsNode`、Scope、Runtime、Middleware、Scheduler、`Tool.Invoke` 的顺序解释批次审批、超时、只读重试、并发组、panic 隔离与模型可见错误。

- [ ] **Step 2: 写入 LLM 与 SSE 章节**

  解释 provider-neutral 的 `ChatModel`、OpenAI-compatible 适配、SSE 帧解析、Delta 标准化、重试边界和 Usage 的来源；明确网络断开、半个工具调用和正常 EOF 的差别。

- [ ] **Step 3: 写入 Context Engine 章节**

  用 Request/Result、TokenCounter、角色修复、按 User Turn 裁剪和稳定前缀解释它为何每次 LLM 调用前运行，以及 Prompt Cache 对齐不等于框架缓存。

- [ ] **Step 4: 以代表性测试核对能力组件**

  运行：

  ```powershell
  go test -count=1 ./pkg/tool ./pkg/llm ./pkg/contextengine
  ```

### Task 4: 把能力组件装配成项目，并收束面试表达

**Files:**
- Modify: `docs/底层组件设计与核心代码.md`
- Read: `pkg/retrieval/types.go`, `pkg/retrieval/chunk.go`, `pkg/retrieval/indexer.go`, `pkg/retrieval/hybrid.go`, `pkg/httpclient/service.go`, `pkg/httpclient/pool.go`, `pkg/agent/new.go`, `pkg/agent/base.go`, `pkg/agent/react_agent.go`, `pkg/agent/routing.go`, `internal/agentrecipe/recipe.go`, `extensions/mcp/mcp.go`, `extensions/cache/llm/cache.go`, `extensions/cache/retrieval/cache.go`, `extensions/state/redis/redis.go`
- Test references: `pkg/retrieval/pipeline_test.go`, `pkg/agent/react_agent_test.go`, `pkg/agent/routing_test.go`, `extensions/mcp/mcp_test.go`, `extensions/cache/llm/cache_test.go`, `extensions/state/redis/redis_test.go`

**Interfaces:**
- Consumes: 核心 Graph、Tool、LLM、Context Engine 和 State 边界。
- Produces: Agent 配方怎样复用 Graph，以及检索、HTTP、MCP、缓存和 Redis 怎样通过接口向外扩展的完整叙事。

- [ ] **Step 1: 写入 Retrieval、HTTP 与扩展边界**

  说明检索流水线的 Chunk/Index/Retrieve/Rerank 职责，HTTP Client 如何被 Runtime 管理，以及 MCP、Cache、Redis 为什么在独立 module 中实现。

- [ ] **Step 2: 写入 Agent 与 Recipe 章节**

  从 `agent.New` 自动选择 Basic/ReAct 开始，说明 ReAct、Routing、GroupChat 不是第二套执行引擎，而是 Graph、LLM、Tool、State 的装配配方。

- [ ] **Step 3: 写入端到端运行剖面与面试复盘**

  用一次含工具调用、审批中断、恢复和流式关闭的时序串起所有组件；每个关键转折点给出面试官追问、简洁回答和可回看的源码路径。

- [ ] **Step 4: 写入“职责边界速记”而非重复题库**

  以一张边界表收束：每个组件负责什么、不负责什么、典型误用和对应题库章节，避免把 140 多道题的答案重复粘贴进正文。

### Task 5: 做文档正确性与学习价值验证

**Files:**
- Modify: `docs/底层组件设计与核心代码.md`
- Read: `docs/项目面试题与参考答案.md`, `README.md`

**Interfaces:**
- Consumes: 已完成的分层正文和仓库源码。
- Produces: 无占位、能定位源码、可独立回答核心问题的最终文档。

- [ ] **Step 1: 检查 Markdown 结构和占位符**

  运行：

  ```powershell
  rg -n "^#{1,4} " "docs/底层组件设计与核心代码.md"
  rg -n "TODO|TBD|待补|稍后|待写" "docs/底层组件设计与核心代码.md"
  ```

  预期：标题按依赖关系递进，第二条命令没有匹配结果。

- [ ] **Step 2: 检查源码路径和关键符号仍可找到**

  运行：

  ```powershell
  rg -n "type Runnable|type Message|func \(.*Compile|type Tool interface|type ChatModel|func New\(" pkg internal extensions
  ```

  将文中引用与结果逐项比对，修正过期路径、签名或误导性结论。

- [ ] **Step 3: 进行陌生读者问题自测**

  用最终文档逐项回答：消息为何有序块、消费者提前关闭流会怎样、Graph 为什么要 Compile、Checkpoint 和 Continuation 有何不同、为什么 Tool 只读才重试、Context Engine 何时运行、ReAct 如何复用 Graph、MCP 在哪里接入。找不到答案则补在对应章节，而不是新增无关章节。

- [ ] **Step 4: 检查差异格式**

  运行：

  ```powershell
  git diff --check -- "docs/底层组件设计与核心代码.md"
  git status --short
  ```

  确认没有空白错误，并报告仅本任务涉及的文档变更。
