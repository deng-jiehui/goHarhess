# goHarness 源码学习与面试指南实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新建 `docs/goHarness源码学习与面试指南.md`，按依赖自底向上解释 goHarness 的公开 API、核心私有算法、运行时数据流和面试题映射。

**Architecture:** 文档采用“分层教材 + API 索引 + 面试题映射”。主章节按 `schema → runnable → callback/control → graph/state → tool/llm/contextengine/retrieval → agent/extensions` 展开；每个组件使用统一模板，并在末尾提供端到端时序、符号索引和术语词典。

**Tech Stack:** Go 1.24、Markdown、仓库现有 Go 源码/测试、PowerShell、`rg`、`go test`。

## Global Constraints

- 读者熟悉 Go 语法，但不默认理解程序员黑话；术语首次出现必须用中文解释、原名和小例子。
- 示例优先使用当前仓库真实公开 API；简化算法示例必须标记为“教学缩略版”。
- 所有源码路径、符号名和方法签名以当前仓库为准。
- 覆盖所有 `pkg/*` 和 `extensions/*` 的公开 API；深入 Graph、Runnable、Message、Tool Runtime、Checkpoint、SSE、Agent 主链路的关键私有算法。
- 不修改 `docs/项目面试题与参考答案.md`、Go 源码和无关文档。
- 每个组件章节必须包含职责、源码地图、关键类型/方法、示例、调用链、算法解释、边界、误区、测试和面试题映射。
- 文档完成后必须通过 Markdown 链接检查、关键符号搜索检查和 `git diff --check`。

## 文件结构

- Create: `docs/goHarness源码学习与面试指南.md` - 最终学习手册。
- Read-only references: `README.md`, `docs/使用手册.md`, `docs/execution-model.md`, `docs/tool-runtime.md`, `docs/context-management.md`, `docs/rag-pipeline.md`, `docs/extensions.md`, `docs/项目面试题与参考答案.md`。
- Read-only implementation references: `pkg/**`, `extensions/**`, `internal/agentrecipe/**`, `examples/**`。
- Modify none: 现有代码和现有文档保持不变。

### Task 1: 建立学习手册骨架与项目地图

**Files:**
- Create: `docs/goHarness源码学习与面试指南.md`
- Read: `README.md`, `go.mod`, `extensions/go.mod`, `examples/basic/main.go`, `examples/graph/main.go`

**Interfaces:**
- Consumes: 仓库模块路径、入口示例和已确认的 15 章结构。
- Produces: 可继续填充的 Markdown 标题树、项目包依赖图、阅读顺序、命令清单和术语解释规则。

- [ ] **Step 1: 写入文档标题、读者说明和学习目标**

  在文件开头加入标题、适用读者、如何使用本手册，以及“源码版本以当前工作区为准”的说明。

- [ ] **Step 2: 写入模块和目录地图**

  建立 `pkg/schema`、`pkg/runnable`、`pkg/callback`、`pkg/control`、`pkg/graph`、`pkg/state`、能力组件、Agent 和扩展的职责表，并说明 `extensions` 是独立 Go module。

- [ ] **Step 3: 写入验证命令**

  给出 `go test ./...`、`go test -race ./...`、`go vet ./...`、`rg` 符号搜索和 Markdown 链接检查命令，说明每条命令解决什么问题。

- [ ] **Step 4: 检查骨架标题和源码目录一致**

  运行 `rg -n '^#{1,4} ' docs/goHarness源码学习与面试指南.md`，确认 15 个主章节及附录标题已出现，且没有 `TODO` 或 `TBD`。

### Task 2: 补充 Go 基础、`schema` 和消息模型

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/schema/message.go`, `pkg/schema/block.go`, `pkg/schema/clone.go`, `pkg/schema/response_meta.go`, `pkg/schema/stream.go`, `pkg/schema/message_test.go`, `pkg/schema/block_test.go`

**Interfaces:**
- Consumes: Task 1 的标题骨架；真实 `schema` 类型和测试行为。
- Produces: Go 语法补课、消息/内容块章节、JSON/校验/克隆/Delta 的真实 API 说明，以及面试题 137-160 的映射。

- [ ] **Step 1: 解释本项目使用的 Go 基础**

  用短代码解释接口满足、泛型 `Runnable[I,O]` 的含义、`context.Context` 的取消和值传递、channel 的发送/关闭、`defer` 清理和错误包装；每个术语配中文释义。

- [ ] **Step 2: 建立 `schema` 源码地图和类型表**

  覆盖 `Message`、`ContentBlock`、文本/工具调用/工具结果/媒体/推理块、`ResponseMeta`、`TokenUsage`、`MessageEvent`，标出文件路径和核心字段。

- [ ] **Step 3: 解释 Message 构造、访问、Validate 和 JSON**

  以真实构造函数/访问器/`Validate`/JSON 方法为中心，说明顶层有序块、`RoleTool`、工具调用 ID、`IsError` 和 `Extra` 的边界，并链接对应测试。

- [ ] **Step 4: 解释 Clone 和流式 Delta 合并**

  说明普通克隆与严格克隆的差异、未知指针为何被拒绝，以及 Delta 合并为何不能简单字符串拼接；给出一个多工具调用顺序保持的例子。

- [ ] **Step 5: 添加本章学习检查和面试映射**

  至少列出 6 个检查题，链接现有面试题 137-160，并指向 `pkg/schema/message_test.go`、`pkg/schema/block_test.go`。

### Task 3: 补充 `runnable`、流式 Reader、Adapter 和 Callback/Control

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/runnable/runnable.go`, `pkg/runnable/stream_reader.go`, `pkg/runnable/adapter.go`, `pkg/runnable/invoke_adapter.go`, `pkg/runnable/lambda.go`, `pkg/runnable/concat.go`, `pkg/runnable/runnable_test.go`, `pkg/runnable/stream_reader_test.go`, `pkg/callback/*.go`, `pkg/control/control.go`

**Interfaces:**
- Consumes: Task 2 的消息模型和 Task 1 的组件模板。
- Produces: 四态 Runnable 协议、流生命周期、适配器、回调隔离和中断控制的教程与题号映射。

- [ ] **Step 1: 解释 `Runnable[I,O]` 四个方法**

  写出真实接口签名，比较 `Invoke`、`Stream`、`Collect`、`Transform` 的输入输出形态，并说明为何用户常实现一个方法、其余由 Adapter 补齐。

- [ ] **Step 2: 解释 `StreamReader` 生命周期**

  说明读取、发送、错误、EOF、`Close`、消费者提前退出和 context 取消之间的关系，用时序图展示生产者与消费者的退出路径。

- [ ] **Step 3: 解释 Option、Func、Lambda、Concat 和 Adapter**

  按真实类型和构造函数说明 Option 作用域、函数适配、流合并策略，以及四种执行形态之间的转换成本。

- [ ] **Step 4: 解释 Callback、Invocation、EventStream 和 Control**

  区分同步生命周期回调与实时事件流，说明错误隔离、克隆、`context.WithCancelCause` 和 typed `InterruptRequest` 的使用方式。

- [ ] **Step 5: 添加检查题、测试索引和面试映射**

  链接面试题 8-18、65-82、106-112，并指向 runnable/callback/control 测试文件。

### Task 4: 补充 Graph 编译、节点、State Handler 和执行器

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/graph/types.go`, `pkg/graph/compile_plan.go`, `pkg/graph/compiled.go`, `pkg/graph/execute.go`, `pkg/graph/runtime_graph.go`, `pkg/graph/runtime_execute.go`, `pkg/graph/runtime_state.go`, `pkg/graph/parallel.go`, `pkg/graph/stream_execute.go`, `pkg/graph/events.go`, `pkg/graph/continuation.go`, `pkg/graph/*_test.go`, `docs/execution-model.md`

**Interfaces:**
- Consumes: Task 3 的 Runnable 协议、Callback/Control 语义。
- Produces: Graph 从构建到编译再到运行的完整源码导读，涵盖类型擦除、条件分支、并行、流式和保护机制。

- [ ] **Step 1: 解释 Graph Builder 与 CompiledGraph**

  使用真实 `NewGraph`、`AddNode`、`AddEdge`、`AddBranch`、`Compile` 示例，说明为什么把拓扑和类型校验前移到 Compile。

- [ ] **Step 2: 解释节点适配和反射校验**

  说明 Graph 如何记录节点输入/输出 `reflect.Type`、何时拒绝错误边、为什么泛型不能独立完成运行时边界校验。

- [ ] **Step 3: 解释 State Factory、PreHandler、PostHandler**

  给出一个状态型图示例，说明纯节点不直接读写 State，Handler 如何把边界值和 State 连接起来。

- [ ] **Step 4: 解释顺序、条件分支、并行 Fan-Out/Fan-In**

  用 ASCII 图展示调度顺序、分支谓词读取 State、分支结果合并、失败取消兄弟分支和状态快照边界。

- [ ] **Step 5: 解释流式执行、事件和安全保护**

  区分 Graph 最终业务输出与控制面事件，说明最大步数、锁外执行用户代码、节点 span 和嵌套 Graph 的实现边界。

- [ ] **Step 6: 添加检查题、测试索引和面试映射**

  链接面试题 19-35、79-82、121-124，并指向 graph architecture、runtime、parallel、stream 和 continuation 测试。

### Task 5: 补充 State、Checkpoint、Interrupt/Resume 和可靠性语义

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/state/checkpoint.go`, `pkg/state/memory.go`, `pkg/state/context.go`, `pkg/state/*_test.go`, `pkg/graph/checkpoint_state.go`, `pkg/graph/revision.go`, `pkg/graph/continuation.go`, `pkg/control/control.go`, `extensions/state/redis/redis.go`, `extensions/state/redis/redis_test.go`, `docs/context-management.md`

**Interfaces:**
- Consumes: Task 4 的 Graph 状态和 Continuation 语义。
- Produces: 会话状态/执行状态边界、Store 合约、版本冲突、JSON 快照和恢复语义的可操作说明。

- [ ] **Step 1: 区分会话历史与 Graph 执行状态**

  对比 `state.Checkpoint` 与 Graph Continuation 的字段、生命周期、存储 key 和所有权，给出一张边界表。

- [ ] **Step 2: 解释 Checkpoint Validate、Clone、JSON 和 Store**

  说明公开字段的 JSON 限制、`any` 的动态类型风险、拷贝隔离、版本递增和 `ConflictError` 的产生条件。

- [ ] **Step 3: 解释 Memory Store 与 Redis Store**

  展示内存实现和 Redis Lua CAS 的使用方式，解释 TTL、前缀和并发保存冲突，不把 Redis 扩展混入核心包依赖。

- [ ] **Step 4: 解释 Interrupt/Resume 的状态机**

  说明只有 typed `InterruptRequest` 才创建 Continuation，普通取消/超时为何不保存；用时序图写出保存成功、恢复、版本不兼容和 Store 失败路径。

- [ ] **Step 5: 解释 at-least-once 和幂等边界**

  用扣款/工具副作用例子说明恢复可能重复执行，列出框架不提供 exactly-once、Lease 或分布式锁的事实边界。

- [ ] **Step 6: 添加检查题、测试索引和面试映射**

  链接面试题 36-46、108-112，并指向 state、graph continuation、Redis 测试。

### Task 6: 补充 Tool Runtime、Middleware、并发策略和 Tool Callback

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/tool/tool.go`, `pkg/tool/node.go`, `pkg/tool/runtime.go`, `pkg/tool/runtime_context.go`, `pkg/tool/runtime_scheduler.go`, `pkg/tool/middleware.go`, `pkg/tool/batch_hook.go`, `pkg/tool/callback.go`, `pkg/tool/concurrency_policy.go`, `pkg/tool/errors.go`, `pkg/tool/*_test.go`, `docs/tool-runtime.md`, `docs/tool-http.md`

**Interfaces:**
- Consumes: Task 3 的 Runnable/Callback，Task 5 的中断语义。
- Produces: 从 Tool 定义到 ToolsNode 执行的完整说明，覆盖安全、重试、超时、并发和审批。

- [ ] **Step 1: 解释 `Spec`、`CallInput`、`Result`、`Tool` 和 `FunctionTool`**

  给出真实构造和调用示例，说明 JSON Schema 是边界校验而不是业务校验的替代品。

- [ ] **Step 2: 解释 ToolsNode 如何提取和追加工具消息**

  说明只执行尾部 Tool Call、保留块顺序、错误转为模型可见 Tool Result，以及并发调用为何要深拷贝输入。

- [ ] **Step 3: 解释 Runtime/Middleware 顺序和身份传递**

  用链式示意说明 middleware before/after、Context 身份、拒绝、错误和 panic 恢复的返回规则。

- [ ] **Step 4: 解释超时、重试、深度、死锁和并发组**

  说明只读工具才允许重试、系统超时上限、递归深度、并发组 semaphore、冲突声明和死锁检测。

- [ ] **Step 5: 解释批次审批和生命周期 Callback**

  展示 `BeforeBatchHook` 的完整批次、零工具执行和 `InterruptRequest` 挂起点，并说明 Callback 错误为什么与业务结果隔离。

- [ ] **Step 6: 添加检查题、测试索引和面试映射**

  链接面试题 47-60、75-82、103-105、130、132，并指向 tool runtime、node、batch hook、middleware 测试。

### Task 7: 补充 LLM/SSE、Context Engine、Retrieval 和 HTTP Client

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/llm/*.go`, `pkg/contextengine/*.go`, `pkg/retrieval/*.go`, `pkg/httpclient/*.go`, `docs/llm-providers.md`, `docs/context-management.md`, `docs/rag-pipeline.md`, `docs/retrieval-services.md`, `docs/tool-http.md`

**Interfaces:**
- Consumes: Task 2 的 Message/ContentBlock、Task 3 的 Runnable、Task 6 的 Tool 生命周期。
- Produces: 模型流、上下文治理、RAG 管道和外部 HTTP 服务的组件导读与面试映射。

- [ ] **Step 1: 解释 LLM 模型节点和 provider-neutral 消息**

  说明请求/响应类型、模型节点如何实现 Runnable、Provider 适配层为何不污染核心 Message。

- [ ] **Step 2: 解释 OpenAI-compatible SSE 生命周期**

  展示请求构造、SSE 行解析、Delta 标准化、正常结束、错误结束、Reader Close、消费者取消和 Retry 的处理。

- [ ] **Step 3: 解释 Rate Limit、Usage 和 Token 统计**

  说明限流器、重试边界、流结束时 Usage 何时触发以及如何通过 Callback 监听而不消费每个 Token。

- [ ] **Step 4: 解释 Context Engine**

  覆盖 TokenCounter、ContextBuilder、RequestBuilder、角色修复、按 User Turn 裁剪、稳定前缀、Prompt Cache 和 Stats。

- [ ] **Step 5: 解释 Retrieval 和 HTTP Client**

  覆盖 Chunker/Indexer、Embedding/Vector Store、Hybrid Search/RRF/MMR/Rerank、Retriever，以及 Service/Pool/超时隔离。

- [ ] **Step 6: 添加检查题、测试索引和面试映射**

  链接面试题 61-71、83-88、95-97、114，并指向 llm、contextengine、retrieval、httpclient 测试。

### Task 8: 补充 Agent 装配层和所有扩展模块

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: `pkg/agent/*.go`, `internal/agentrecipe/*.go`, `extensions/mcp/*.go`, `extensions/cache/llm/*.go`, `extensions/cache/retrieval/*.go`, `extensions/state/redis/*.go`, `examples/*.go`, `docs/mcp-tools.md`, `docs/extensions.md`

**Interfaces:**
- Consumes: Task 4 Graph、Task 6 Tool、Task 7 LLM/Retrieval/Context、Task 5 Checkpoint。
- Produces: Basic/ReAct/Routing/GroupChat 和 MCP/缓存/Redis 扩展的源码级装配说明。

- [ ] **Step 1: 解释 Agent 基础类型和节点适配**

  说明 `BaseConfig`、Agent Node、消息输入输出和嵌套 Graph 的生命周期，明确 Agent 不拥有另一套执行引擎。

- [ ] **Step 2: 解释 Basic 和 ReAct**

  用图展示模型节点、工具节点和循环边，说明工具调用追加消息、上下文重建和步数限制。

- [ ] **Step 3: 解释 Routing 和 GroupChat**

  说明 supervisor/router、成员节点、轮次限制、路由状态和结果汇总，链接 `internal/agentrecipe` 实现。

- [ ] **Step 4: 解释 MCP Client 和 DynamicToolsNode**

  展示 Initialize、RefreshTools、Tools、CallTool、动态工具规格转换、内容块结果和 MCP Callback。

- [ ] **Step 5: 解释 LLM/Vector Cache 与 Redis Checkpoint 扩展**

  说明装饰器如何保持 Runnable/Store 合约、缓存 key/version/TTL、失效和扩展 module 的依赖方向。

- [ ] **Step 6: 添加检查题、测试索引和面试映射**

  链接面试题 89-98、119-127、133-136，并指向 agent、MCP、cache、Redis 测试和 examples。

### Task 9: 编写端到端运行剖面、索引和术语词典

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md`
- Read: 已完成章节、`docs/使用手册.md`、`docs/项目面试题与参考答案.md`

**Interfaces:**
- Consumes: Tasks 1-8 产出的组件章节和真实符号。
- Produces: 普通/流式/工具审批/恢复调用的整合时序，面试题总索引，包-类型-方法索引和黑话词典。

- [ ] **Step 1: 写普通调用时序**

  从 Agent/Graph `Invoke` 开始，串起 State Load、Context Engine、LLM、Tool、State Save 和最终输出，标注每个控制面事件。

- [ ] **Step 2: 写流式、审批和恢复时序**

  分别展示 SSE/StreamReader、BeforeBatch 中断、Checkpoint 保存和 Resume 的成功与失败分支。

- [ ] **Step 3: 建立包-类型-方法索引**

  为每个 `pkg/*` 和 `extensions/*` 列出公开入口、关键类型和对应章节锚点，使用真实符号名。

- [ ] **Step 4: 建立面试题总索引**

  将现有 1-160 题按本手册章节链接，重复题只保留一个主链接并注明相关章节。

- [ ] **Step 5: 编写黑话词典和学习检查清单**

  至少解释反射、适配器、装饰器、背压、扇出/扇入、CAS、幂等、at-least-once、provider-neutral、控制面/数据面、RRF、MMR、SSE、span，并给出项目内对应位置。

### Task 10: 完整文档验证和陌生读者自测

**Files:**
- Modify: `docs/goHarness源码学习与面试指南.md` (仅修复验证发现的问题)
- Read: `docs/goHarness源码学习与面试指南.md`, 所有被引用源码/测试文件

**Interfaces:**
- Consumes: Task 9 的完整文档。
- Produces: 无断链、无过期符号、无占位符且能回答典型读者问题的最终文档。

- [ ] **Step 1: 检查 Markdown 结构和占位符**

  运行 `rg -n '^#{1,6} |TODO|TBD|待定|占位' docs/goHarness源码学习与面试指南.md`，修复缺失标题、残留占位符和重复章节。

- [ ] **Step 2: 检查源码符号和路径**

  对文档中的包路径、类型和方法名使用 `rg` 在仓库中逐项核对；删除已不存在或未导出的 API 引用。

- [ ] **Step 3: 检查 Markdown 链接**

  用 PowerShell 提取文档中的相对链接和锚点，确认目标文件存在；对题号链接确认目标标题存在。

- [ ] **Step 4: 运行项目验证命令**

  运行 `go test ./...`、`go test -race ./...`、`go vet ./...`，并在 `extensions` 目录运行对应的 `go test ./...` 和 `go vet ./...`；记录结果，不因文档任务修改代码。

- [ ] **Step 5: 执行陌生读者问题自测**

  用文档本身回答至少 10 个问题：如何写最小 Graph、Runnable 四方法差异、工具错误语义、SSE 关闭、State 与 Checkpoint 区别、Resume 是否 exactly-once、ReAct 图结构、MCP 接入、Context 裁剪和缓存失效。找不到答案时补充对应章节。

- [ ] **Step 6: 最终 diff 检查**

  运行 `git diff --check` 和 `git status --short`，确认只新增目标学习手册（以及计划执行期间明确产生的文档文件），不包含 Go 源码或现有面试题文档改动。
