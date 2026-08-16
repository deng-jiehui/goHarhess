# goHarness 源码学习与面试指南设计

## 目标

新建一篇面向“熟悉 Go 语法、但不熟悉程序员术语”的中文 Markdown 学习手册，帮助读者从底层开始理解 goHarness 的组件边界、关键类型、方法实现、运行时数据流和设计取舍，并能把这些理解映射到现有的项目面试题。

现有的 `docs/项目面试题与参考答案.md` 保持不变。新文档承担源码导读和学习路线职责，并通过题号链接到现有题库。

## 读者与写作约束

- 读者已经能阅读 Go，但不应被默认要求理解“反射、幂等、背压、扇出、CAS、at-least-once”等术语。
- 每个术语第一次出现时提供中文解释、英文或 Go 原名和一个小例子；文末提供黑话词典。
- 代码示例优先采用仓库中的真实公开 API；为了突出算法而简化的代码必须标记为“教学缩略版”。
- 源码路径、符号名和方法签名以当前仓库为准，不凭概念编造 API。
- 不修改现有面试题文档、业务代码或无关文档。

## 组织方式

采用“分层教材 + API 索引 + 面试题映射”的组合结构：主体按照依赖关系自底向上组织，章节内部使用统一模板，文末提供按包、符号和题号反查的索引。

## 章节结构

1. **如何阅读本项目**：模块地图、示例入口、验证命令，以及本项目实际用到的接口、泛型、`context`、goroutine/channel、错误包装、反射等 Go 基础。
2. **`schema`：消息与内容块**：`Message`、`ContentBlock`、工具调用/结果、媒体、推理、响应元数据、JSON 校验、深拷贝和 Delta 合并。
3. **`runnable`：统一执行协议**：`Runnable[I,O]` 四种形态、`StreamReader`、`Option`、`Func`、适配器和 `Concat`，包括关闭与取消规则。
4. **`callback` 与 `control`**：生命周期回调、Invocation、EventStream、错误隔离和 `InterruptRequest` 的传递。
5. **`graph`：编译、执行和类型边界**：Builder/CompiledGraph、节点和边、条件分支、State Handler、反射校验、顺序/并行/流式执行、步数与死循环保护。
6. **`state` 与 Checkpoint**：会话状态与执行状态、Memory Store、版本冲突、JSON 快照、Continuation、Interrupt/Resume 和 at-least-once 语义。
7. **`tool`：工具定义和 Runtime**：工具规格、输入/结果、`Tool`/`FunctionTool`/`ToolsNode`、参数解析、Middleware、超时、只读重试、批次审批、并发组和 panic 恢复。
8. **`llm`：模型适配和流式协议**：模型节点、OpenAI-compatible Provider、SSE、Retry、Rate Limit、Usage，以及消息到 HTTP 的转换。
9. **`contextengine`：上下文窗口治理**：Token 估算、角色修复、按 User Turn 裁剪、稳定前缀、Prompt Cache 和 Builder。
10. **`retrieval` 与 `httpclient`**：分块、索引、Embedding、Vector Store、Hybrid Search、Rerank、Retriever、外部 HTTP 服务和连接池。
11. **`agent` 与 `internal/agentrecipe`**：Basic、ReAct、Routing、GroupChat，以及它们作为 Graph 装配层的实现方式。
12. **扩展模块**：MCP、Redis Checkpoint、LLM/Vector 缓存，以及扩展与核心接口的依赖方向。
13. **端到端运行剖面**：普通调用、流式调用、工具调用、人工审批、恢复调用的时序，及观测/错误/取消/重试/持久化边界。
14. **面试与实战复习**：按基础、进阶、场景、设计取舍和现场编码重新索引现有面试题。
15. **附录**：包-类型-方法索引、黑话词典、常用命令、测试文件索引和建议阅读顺序。

## 组件章节模板

每个组件按以下顺序说明：

1. 组件目标。
2. 源码地图。
3. 关键接口和类型表。
4. 关键方法逐个拆解：真实签名、参数/返回值、生命周期、输入到输出的内部过程。
5. 最小可运行示例。
6. 一次调用的时序或数据流。
7. 实现中的关键算法，用白话解释程序员术语。
8. 并发、错误、取消和持久化边界。
9. 常见误区和教学反例。
10. 对应面试题、相关源码和测试文件。

## 源码覆盖边界

- 覆盖所有 `pkg/*` 和 `extensions/*` 的公开 API。
- 深入解释 Graph、Runnable、Message、Tool Runtime、Checkpoint、SSE 和 Agent 主链路中的关键私有算法。
- 测试辅助结构、纯内部胶水和重复适配器不逐一展开，只在行为说明处链接代表性测试。
- 关键行为至少链接一个实际测试文件，例如 `pkg/graph/runtime_parallel_test.go`、`pkg/tool/runtime_test.go` 和 `pkg/schema/message_test.go`。

## 示例与术语规则

- 方法说明先给一句话结论，再给签名和不超过 30 行的最小示例，最后解释实现。
- 并发、流式、恢复逻辑使用 ASCII 时序图或状态图。
- 对“反射”等黑话，先说明它解决的实际问题，再说明代码怎样使用它，最后说明代价和边界。
- “教学缩略版”示例不得冒充仓库中的真实函数。

## 验收标准

- 每个公开包都有职责、入口、关键类型表和至少一个使用示例。
- Graph、Runnable、Message、Tool Runtime、Checkpoint、SSE、Agent 有完整调用链说明。
- 每个章节都有学习检查题、现有面试题编号和建议测试文件。
- 所有路径、符号和示例与当前源码一致。
- 文档完成后执行 Markdown 链接检查和关键符号搜索检查。
- 用陌生读者问题清单进行一次自测，确认常见问题能够从文档定位答案。

## 非目标

- 不重构或修改 Go 代码。
- 不把现有面试题答案复制成第二份长文；新文档只做必要的题意摘要和链接。
- 不解释与项目无关的 Go 语言特性。
- 不承诺框架自动提供分布式锁、exactly-once 或业务级安全策略。
