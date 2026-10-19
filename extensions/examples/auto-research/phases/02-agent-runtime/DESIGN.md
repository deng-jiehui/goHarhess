# 阶段 02 设计：让研究助手在会话中工作

**状态：** 阶段二 MVP 已实施；完整论文、实验和远程执行能力按后续阶段交付。  
**日期：** 2026-10-19。  
**前置：** [阶段 01 已验收](../01-workspace/ACCEPTANCE.md)。  
**任务：** T04–T06；业务图详见 [GRAPH_DESIGN.md](GRAPH_DESIGN.md)，执行步骤见 [IMPLEMENTATION.md](IMPLEMENTATION.md)。

**编排决策：** AutoResearch 在应用层直接使用 `pkg/graph` 定义研究目标澄清、笔记整理、项目问答的业务流程，复用 `ChatModelNode`、`ToolsNode`、contextengine、callback 和检查点。路径由图上的业务门槛约束，主流程不使用 `agent.New` 或框架内部 agentrecipe。

> 当前实现以 [业务图](GRAPH_DESIGN.md) 和 [路由与交互验收](ROUTING_AND_INTERACTION.md) 为准。本文件保留总体目标；事务恢复、取消、事件和领域契约中尚未落地的设计不代表当前已具备。尤其没有实现完整项目工具目录、共享计费预算或结构化论文证据校验。

## 1. 用户能获得什么

把已经保存的会话变成可持续协作的研究助手。用户提出目标，Agent 读取项目、使用工具保存笔记、追问缺失信息，并在页面中展示回答与工具过程。关闭页面不停止执行，重新进入后能看到真实进度；服务重启后，仍能处理原来的问题。

具体演示：项目研究问题是“Agent 的工具选择如何影响代码修复成功率？”，用户输入：

> 读取我的项目目标，帮我明确下一步关注点；如果评测对象不清楚就先问我。确认后，把要关注的内容保存为项目笔记。

Agent 先由 load_context 注入项目和笔记快照，模型输出结构化业务路线；当前业务模型按需调用 ask_user，生成符合当前主题的问题和选项。回答后继续原业务节点，persist_note 调用现有保存用例。用户在另一个会话提问时重新加载同一项目的笔记快照。没有把这些初始化和保存节点冒充独立模型工具。

这是流程与数据一致性的演示。真实模型可以讨论研究思路，但当前没有论文来源，回答不得声称已检索或阅读特定论文。

| 本阶段交付 | 对用户的表现 |
| --- | --- |
| 可发送消息 | 消息被接受后立即显示，回答逐步更新 |
| 业务流程 | 看见研究范围、本轮步骤和已检查产物；不同请求经过不同路径 |
| 项目工具 | 看见工具名称、参数、执行结果，笔记真实保存；普通问答保持只读 |
| 补充信息 | 看到问题与回答表单，回答后继续同一轮执行 |
| 后台执行与恢复 | 切换会话、刷新、关闭页面后可继续查看；服务重启后核对进度 |
| 失败处理 | 显示失败原因，可继续原轮或结束本轮；不自动重复提交目标 |
| 两种模型模式 | 离线演示模式验证固定流程，真实模型模式处理自由输入 |

论文库、MCP、论文证据、自动综述、SSH、实验任务和桌面安装包仍由阶段 03–08 交付。本阶段不增加模型网关、独立 Agent 服务、ObservedModel 或新的框架事件协议。

## 2. 页面怎样变化

沿用 `/p/:projectId/s/:sessionId`，保留现有三栏布局。

- 左侧继续使用项目与会话导航；活动轮在当前会话旁显示状态。
- 中间启用输入框，展示用户消息、助手回答、可展开的工具卡和补充信息卡。工具卡来自真实 function-call / function-result 内容；无须展示模型内部推理。
- 会话区在最终答复前显示经过筛选的研究进度卡：读取项目、理解目标、范围确认、阶段产物和笔记保存；答复增量只来自 `compose_reply`，内部 JSON 不进入用户消息。
- 右侧显示本轮采用的研究范围、步骤清单、已检查产物和最近 20 条项目笔记；笔记支持本阶段的新增与读取，修改、删除在后续需求中独立设计。清单是业务进度，不是模型推理记录。
- 断线时保留已有内容与未提交输入，提示“连接中断，正在重连”；不能将断线显示为执行失败。
- 活动轮存在时禁用普通发送。等待回答使用专门表单；失败或恢复受阻时显示“继续本轮”“结束本轮”。
- `fixture` 模式页面显示“离线演示”，只支持该模式声明的项目与笔记场景；未识别的自由输入明确说明演示范围。真实模型错误不得切换为演示回答。

“结束本轮”停止后续执行，但已经保存的笔记仍保留。归档项目或会话前须先结束活动轮；单纯编辑项目设置、重命名不取消执行。

## 3. 四层中的职责

```text
React 会话页面
    ↓ HTTP 提交、查询、回答；SSE 读取已保存事件
应用用例：接收目标 → 保存执行轮 → 后台运行 AutoResearch 主图
    ↓ 调用项目工具                         ↓ 读取执行事件
领域规则与仓储接口                      消息投影与执行状态落库
    ↑                                   ↑
SQLite、模型适配器、进程锁、现有 callback / observe
```

| 部分 | 职责 | 不承担的职责 |
| --- | --- | --- |
| 交互：web、httpapi | 输入、状态、回答表单、SSE、错误提示 | 不持有 Agent 或模型密钥 |
| 应用：SessionService、TurnService | 接收目标与动作、上下文快照、状态转换 | 不直接依赖 HTTP 请求寿命 |
| 应用：TurnWorker、BuildResearchGraph、tools | 定义主图节点与分支、工具装配、事件消费、结果核对 | 不重新实现 Graph 调度器或检查点引擎 |
| 领域：Turn、MessageRecord、Note、仓储接口 | 归属、活动轮、幂等、正式历史与笔记规则 | 不导入 goHarness、SQL、HTTP |
| 基础设施：SQLite、models、instance | 持久化、模型连接、单实例数据目录保护 | 不决定用户研究方向 |

入口创建适配器，注入应用。`app/research_graph.go` 接收已经构造好的工具，不导入 `app/tools`；工具依赖领域接口或注入的笔记用例，避免包循环。AutoResearch 是 Agent 产品，主图是其应用层实现，无须增加一个 Agent 架构层。

第二阶段只要求核心框架模块。保留两个现有 replace，在实际导入时增加 require；extensions/MCP 不作为会话运行的初始化依赖。

### 3.1 主图与节点职责

```text
读取项目/笔记 → 模型结构化路由 → 业务模型（方向 / 笔记 / 问答 / 工作区）
                                      ├─ ask_user → 工具中断 → 原业务模型
                                      ├─ 工作区工具 → 真实结果 → 工作区模型
                                      └─ 草稿检查 → 按保存意图写笔记 → 最终答复
```

understand_goal 的请求不携带工具，返回 GoalDecision JSON。各业务模型自行判断缺失信息是否值得追问，不再有固定 scope_gate；clarify 是只注册 ask_user 的 ToolsNode，恢复后按 State 保存的 ReturnNode 回到原业务阶段。

普通起草模型仅获得 ask_user。workspace_model 获得既有七个工作区工具及 ask_user，完整批次先校验再执行；模型不能选择任意图节点或跳过工具执行策略。压缩和最终答复节点没有工具定义。

详细节点、条件边及当前校验强度在 [GRAPH_DESIGN.md](GRAPH_DESIGN.md) 维护。图是 Agent 产品的应用实现，无须新增 Agent 架构层。

### 3.2 状态、公开接口与扩展边界

| 类型/接口 | 所在位置 | 所有者与职责 |
| --- | --- | --- |
| ResearchState | app/research_state.go | 应用拥有：规范消息、真实上下文、目标/范围、问题答案、步骤、草稿与检查结果、保存凭据、各级修复预算；完整定义见图设计 |
| `*graph.CompiledGraph[[]schema.Message, []schema.Message]` | 由 BuildResearchGraph 返回 | 框架执行节点、分支、持久化和恢复；输入是冻结历史加唯一新用户消息，输出仅是本轮助手/工具消息 |
| `graph.StateCodec` | app/research_state.go 构造 | 用 `graph.NewStateCodec[*ResearchState]` 保存完整状态；ID 为 autoresearch:research-state，版本 2 |
| BuildResearchGraph | app/research_graph.go | 按轮装配模型、工具闭包与固定定义版本，返回已编译主图 |

ResearchState 不保存模型客户端、工具对象、数据库连接、Context、锁或密钥；领域层不持有该状态。State Codec 保存完整消息，JSON 解码不接受未知顶层字段；暂不实现 ConversationExternalizer/Restorer，不对框架 Execution 做应用侧解析。

扩展能力需要同时设计业务路径和工具权限：后续论文检索、证据定位、候选审核、实验确认与任务核对的接入点见图设计第 9 节。新增路径须同步定义版本、State Codec（字段语义变化时）和恢复测试。本阶段不实现动态插件系统、可视化图编辑器或通用图 DSL。图结构在每次执行前编译并固定，升级后不把新节点静默插入旧检查点。中断与副作用先限定在根图顺序路径，嵌套/并行流程需在引入时单独验证。

## 4. 执行身份和核心对象

以下均为应用候选类型，不是新增的框架 API。

| 对象 | 关键字段 | 所有者与用途 |
| --- | --- | --- |
| Session | 沿用 ID、ProjectID、Title、Archived | 用户的长期会话容器 |
| Turn | ID、SessionID、State、Revision、InputText、IdempotencyKey、ContextJSON、DefinitionVersion、Attempt、CancelRequested、CanonicalMessageIDs、ProgressJSON、ProgressCheckpointID/Version | 一次用户目标，从提交到完成可经历多次暂停与恢复；完成时保存本轮正式消息顺序 |
| TurnAction | ID、TurnID、Kind、IdempotencyKey、RequestHash、PayloadJSON、State | 持久记录回答、重试和结束请求，避免收到请求后仅放进内存 |
| MessageRecord | ID、SessionID、TurnID、Attempt、Seq、ContentJSON、Committed、Superseded | 规范消息的持久投影；显示部分内容与正式历史分开 |
| Note | ID、ProjectID、Text、SourceSessionID、SourceTurnID、Revision、ToolKey、RequestHash | 跨会话可见的项目笔记；ToolKey 用于恢复防重 |
| SessionEvent | ID、SessionID、TurnID、Attempt、Kind、StreamRunID、Sequence、PayloadJSON | 提供 SSE 与页面续读；ID 使用数据库全局递增整数 |
| TurnContext | Project、ProjectRevision、Notes、History、UserMessage | 接受目标时冻结的上下文；只在应用内使用规范消息 |

`ContentJSON`、`ContextJSON`、`PayloadJSON` 只出现在数据库/传输边界。内部消息用 `schema.Message`，执行事件用 `stream.Event[[]schema.Message]`。

必须区分三种身份：

1. `Turn.ID`：业务轮 ID，回答与重试均保留。
2. `Checkpoint.RunID`：框架持久执行身份，恢复时沿用，不能由应用伪造。
3. `Event.Meta.RunID`：一次公共事件流的身份。每次调用 StreamEvents / ResumeStreamEvents 都可能变化；与 Sequence 合起来用于诊断，不作为 SSE 游标。

检查点键采用 `ar:v1:` 加 `[userID, projectID, sessionID, turnID]` 的 JSON 数组编码。每轮独立保存完整框架检查点；创建新轮时从业务消息表读取此前 **已完成轮** 的正式历史，加上本轮唯一用户消息。失败轮的半段回答仍可展示，但不自动混入下一轮模型上下文。

选择每轮独立检查点，是为了让失败轮、重试轮与后续目标的边界明确。代价是检查点会重复保存部分历史；本阶段保留完整历史，不提前设计压缩或裁剪。

## 5. 工具和模型

当前真实可调用工具为 ask_user，以及 list_directory、read_file、search_files、git_diff、run_shell、write_file、apply_patch。read_project/list_notes/read_note/save_note 的统一目录属于后续扩展；当前由初始化快照与现有笔记用例提供数据和保存能力。

ask_user 的 question 和 choices 来自当前业务模型，完整调用先提交 State 后进入 clarify。问题固定后不在恢复时重新生成。前端提供单选、选项编辑和独立回答框，答案作为原工具调用结果返回；每轮最多完成三次追问。追问不等同于写入审批。

业务路线使用普通 JSON 提示词与 StructuredOutput 本地校验，不依赖服务商原生 JSON Schema；路由格式最多修复一次，失败直接报错，不静默采用关键词。真实模型处理交互和工作区操作时需要支持标准工具调用。

ToolsNode 使用 MaxConcurrency=1、Timeout=15s、MaxRetries=0；模型节点输出上限为 2048 token，笔记压缩节点为 1024。工作区最多八轮、二十四次工具调用；Graph 最多四十八步；worker 每个执行段五分钟。等待输入不占用执行段。尚未实现全图共享 LLMCalls 预算。

当前上下文使用项目快照、最近六条正式消息、最多十条笔记摘要和确认答案，不声称已装配完整 contextengine 窗口管理。正式历史保存在数据库，工具上下文的配对结果保存在业务 State。

fixture 使用 HandlerModel，仅用于离线验证流程；它的确定性路由和示例问题不参与真实模型逻辑。remote 使用现有 OpenAI-compatible 适配器，错误不会降级成离线回答。模型设置、密钥存放及作用域规则沿用现有实现。

图定义版本固定为 autoresearch-phase2-graph-v2，StateCodec 版本为 2；版本不一致的旧未完成轮次进入明确失败，并提示结束后重发。没有实现按模型、端点、提示词自动计算版本哈希。完整协议与验收步骤见 [ROUTING_AND_INTERACTION.md](ROUTING_AND_INTERACTION.md)。

## 6. 状态、并发和请求幂等

```text
queued → running → completed
             ├──→ awaiting_input → queued（同一轮回答后恢复）
             ├──→ failed → queued（继续原轮）
             └──→ recovery_required → queued（问题已解决）
活动或未完成轮 ──结束请求、worker 退出确认──→ cancelled
```

活动状态为 queued、running、awaiting_input、recovery_required；SQLite 部分唯一索引保证同一会话最多一条活动轮。failed 可显式重试，但仅在会话没有其他活动轮且没有更新的轮时重新取得活动名额；有后续轮时返回 409，提示在新消息里重新提出目标。

Send 在同一事务内完成归属与归档检查、幂等检查、读取项目/笔记/此前正式历史、保存上下文、创建用户消息与 queued Turn、追加 turn/message 事件，然后返回 202。不能在事务外先读历史后排队，否则上一轮在两步之间完成时，新轮会漏掉刚提交的回答。先检查已有幂等键，再检查忙碌：同键同内容始终返回原轮，不同内容复用键返回 409。每条新目标最多 8000 个 Unicode 字符，幂等键为客户端生成的 UUID，长度最多 128。

恢复、重试、结束请求均要求 `revision` 与 `idempotency_key`，以 TurnAction 保存请求内容摘要。同一请求重发返回原动作及当前轮状态；不同答案不能复用键。回答先完整落库，再将原轮置 queued，不能直接在 HTTP handler 中执行 Agent。

这是单服务进程的本地版本。持有 data/service.lock 的 OS 排他锁后才能执行迁移和启动 worker；同一数据目录启动第二个服务明确失败。Windows 使用 LockFileEx，Linux 使用 flock，进程异常退出由 OS 释放；保留锁文件，不靠删除文件解锁。数据库约束仍保护多个 HTTP 请求，进程锁不代替事务。

worker 以条件更新领取 queued 轮，每次执行段递增 Attempt；最多同时执行 2 个不同会话，每 500ms 扫描持久队列，内存通知只用于加速。模型实例可共享无状态连接，工具与上下文按轮构造，不能把第一位用户的 Scope 捕获进全局 Agent。

结束正在执行的轮时先持久设置 CancelRequested，再取消执行 Context，持续消费公共 reader 到自然终止，最后写 cancelled 并释放活动名额。worker 固定使用 stream.Config.SendTimeout=0，不主动调用 Reader.Close 代替等待；Close 只取消并关闭管道，不保证执行已退出。持久化失败也先取消并继续 drain，丢弃无法落库的后续事件；不能在旧执行仍可能写入时领取下一轮。

取消与完成竞争以正式完成事务为边界：已完成返回 completed，否则 CancelRequested 阻止成功提交。若所用模型或工具不响应取消，保持该轮受阻与会话占用，调度器停止接收新目标并报告原因，直到执行退出或进程结束；不能将超时或提前关闭 reader 当成安全退出。页面断开不设置取消字段；异常退出后启动核对仍尊重它。模型适配器和四个工具都须验证取消后会退出。

## 7. 检查点与重启核对

SQLiteCheckpointStore 完整实现当前 state.Store 的 Get/List/Load/Save/Delete；保存 `Checkpoint.JSON()` 编码，读取使用 `state.DecodeCheckpoint()`。保存失败必须返回给框架，不修改、猜测或迁移 Execution 内部结构。

Save 事务同时核对 head 的 ID 与 Version，生成新 ID、ParentID、递增 Version，插入历史并交换 head；删除重建后旧 ID 仍须失败。Get/List/Load 返回隔离副本，List 按新到旧分页。使用 `graph.DurabilitySync`，本阶段不裁剪历史，也不做检查点消息前缀去重。

每个 store 适配器绑定可信 user/project/session/turn，允许该轮根键与框架派生子键；数据库记录包含归属，不提供让浏览器传任意检查点键的 API。

框架检查点与业务消息不是同一事务，所以必须核对：

启动核对按以下优先级执行：先跳过业务终态 completed/failed/cancelled；再处理非终态的 CancelRequested；再核对完成检查点；最后才恢复进行中或中断的检查点。failed 只有用户明确重试并成功置 queued 后才再次执行。完成检查点不能覆盖取消意图，也不能把 failed/cancelled 补成成功。

对通过上述筛选、定义兼容的可恢复轮，使用编译图的 **PendingInterrupts** 只读查询，传入原检查点键；不为发现问题而启动执行，也不自行解析 Execution。当前 pending ID 集合与未应用回答动作完全匹配时，调用 ResumeStreamEvents 并附 graph.WithResume；两者无交集（或 pending 已空）说明旧答案已被检查点接纳，将该动作记 applied，再无答案继续恢复或展示下一组问题。部分交集、未知问题或载荷不一致进入 recovery_required，不猜测答案。回答动作只有在当时 pending 全集校验成功后才能创建，且同轮执行与查询串行；这让“旧问题已不再 pending”成为可靠依据。

若答案已写入检查点而动作表尚未确认便崩溃，PendingInterrupts 不再返回旧问题；新问题不能接收旧答案。若原问题仍 pending，则恢复才带同一持久动作的答案。正常暂停也从已保存检查点重建问题快照。Get/List/Load 和 PendingInterrupts 的错误不得当作空问题。

| 重启发现的情况 | 处理 |
| --- | --- |
| 业务已 completed/failed/cancelled | 保留业务终态，不自动恢复；failed 的后续重试须经过显式动作 |
| 非终态且 CancelRequested 已保存 | 原进程已退出，直接补 cancelled，不恢复模型或工具；即使检查点 completed 也不补成功 |
| queued，没有检查点 | 用已保存上下文和用户消息首次执行 |
| running，有进行中检查点且定义一致 | ResumeStreamEvents 恢复，不重发用户消息 |
| running，无检查点 | DurabilitySync 在首节点前保存初始检查点，故尚无工具副作用；可从已保存输入重新开始并记录原因，损坏或读取失败不属于“无检查点” |
| 检查点为 interrupted，有待回答项 | 通过编译图 PendingInterrupts 重建 awaiting_input；匹配持久答案动作后才恢复 |
| interrupted，无待回答项 | 视为可恢复执行暂停，重新排队，不展示空回答表单 |
| 检查点已 completed，业务轮非终态且未请求取消 | 校验 Messages/Result 后幂等补齐业务消息与唯一 result；无效完成补 failed，不再请求模型 |
| 定义不兼容、编码损坏或恢复校验失败 | recovery_required，显示原因，等待修复或结束本轮 |

正常执行成功必须同时满足：主图 completion_gate 通过、finish 返回、框架完成检查点保存、公共流 result 与正常 EOF，随后业务事务再次核对规范消息并提交 Turn.completed 与唯一 result。收到部分文本或普通 lifecycle.end 都不能宣布完成。完成检查点补交是重启时的替代路径，不要求再次创建流；仍须通过同一业务核对与事务。框架已完成但业务事务失败时，不能重新调用模型。

等待回答必须来自保存成功的框架中断，保存 pending 列表及问题载荷后展示；框架回答记录由 WithResume 交给执行器消费，不能由应用提前标记为已消费。部分执行时的消息可展示，正式历史以完成检查点中的规范消息核对。

消息核对使用以下固定规则：

1. Admit 冻结历史消息 ID 集合 H 和唯一用户消息 U。完成检查点 Messages 含 H、U 和本轮输出；Result 只作本次执行输出核对，不能当成完整会话。只把 Messages 中排除 H 后的有序集合 C 作为本轮正式消息，并检查 U 恰好出现一次。历史消息内容与归属不得被本轮改写；ID 冲突或不一致进入 recovery_required。
2. 每次领取新 Attempt（含恢复）在同一事务保留 U、将本轮此前非正式输出标记 superseded，并发布带 replace_turn_messages=true、messages=[U] 的 turn 事件。页面暂时收起旧片段；此前已完成轮不受影响。运行中检查点的内部执行状态不用于猜测尚未正式提交的消息。
3. 成功事务按 C 更新或插入本轮消息，设置 Committed=true、Superseded=false；本轮其他输出保持 superseded。已有消息保留原 ID、TurnID、Seq，不能重挂历史消息。Turn.CanonicalMessageIDs 保存 C 的顺序，后续模型历史按已完成轮及该顺序读取；Seq 仅作为稳定的显示分页游标。
4. 同一事务发布唯一 result，带 replace_turn_messages=true、canonical_message_ids 及 C 的完整消息快照。前端仅替换该轮消息，移除不在 C 的旧 partial，并按 canonical_message_ids 排列；Snapshot 返回相同投影并默认排除 superseded。失败/取消不晋升正式历史，仍显示本次片段和失败状态。

已经提交的笔记不会因消息替代而删除或重复新增。

## 8. 新增数据库迁移

保留 001、002，新增 `003_agent_runtime.sql`。只增加下列七张表，共计十张业务/管理表：

| 表 | 内容与约束 |
| --- | --- |
| turns | user/project/session 外键、本轮输入与上下文、状态、revision、attempt、取消标记、问题快照、正式消息 ID 顺序、公开研究进度及来源检查点 ID/Version、错误码；唯一 session+幂等键；活动轮部分唯一索引 |
| turn_actions | 轮内动作幂等键、类型、请求摘要、答案 JSON、消费状态；唯一 turn+幂等键 |
| messages | 规范消息 JSON、会话内 Seq、Turn、Attempt、Committed/Superseded；唯一消息 ID、唯一 session+Seq |
| notes | 项目笔记与工具去重键；唯一 project+ToolKey，记录原请求摘要 |
| events | INTEGER PRIMARY KEY AUTOINCREMENT、归属、Turn/Attempt、规范事件 JSON；框架事件按 turn+attempt+stream_run_id+sequence 防重；progress 使用 source_checkpoint_id，按 turn+source_checkpoint_id 防重 |
| checkpoints | 完整框架检查点历史与归属、conversation_key、version、parent；不解释 Execution |
| checkpoint_heads | conversation_key 对应最新 checkpoint ID 与 version |

每轮只允许一个 `result` 事件，使用部分唯一索引；应用生成的 turn/problem 事件不占用框架 Sequence。消息 Seq 在 SQLite 事务内按会话分配，与 SSE 事件 ID 分开。

项目删除在原事务增加活动轮检查，存在活动轮返回 409；允许删除后级联删除消息、笔记、事件、动作和检查点，绝不遍历或删除外部文件。项目/会话归档同样在事务检查活动轮；允许重命名，但当前轮上下文仍沿用接受时的版本。

## 9. HTTP 接口

沿用第一阶段项目路径，令 `S = /api/projects/{projectID}/sessions/{sessionID}`。所有接口同时校验可信用户、项目与会话；归属不符统一 404。不额外维护不带项目的短路径别名。

| 方法与路径 | 输入 | 成功结果 |
| --- | --- | --- |
| POST S/messages | text、idempotency_key | 202，`{turn, replayed}`；首次与重复均明确返回同一轮 |
| GET S/snapshot | 无 | 200，最近 100 条消息、相关轮及活动轮、research_progress、20 条笔记、event_cursor、next_message_seq |
| GET S/messages | before_seq、limit（1–100，默认 50） | 200，消息分页；首次与翻页均按 Seq 正序显示 |
| GET S/turns/{turnID} | 无 | 200，状态、revision、pending_questions、can_retry |
| POST S/turns/{turnID}/resume | revision、idempotency_key、answers | 202，`{turn, action_id, replayed}` |
| POST S/turns/{turnID}/retry | revision、idempotency_key | 202，`{turn, action_id, replayed}`，或 409 无法继续 |
| POST S/turns/{turnID}/cancel | revision、idempotency_key | 202，`{turn, action_id, replayed}`；完成轮返回 200 当前状态及空 action_id |
| GET S/events | Last-Event-ID，或 after 查询参数 | SSE，按持久 EventID 续读 |
| GET /api/projects/{projectID}/notes | limit（1–20，默认 20） | 200，最近项目笔记 |

回答示例：

```json
{
  "revision": 4,
  "idempotency_key": "c288575e-c3a0-4f15-86d6-8d72f052613d",
  "answers": [{"interrupt_id": "服务返回的问题ID", "text": "仓库级缺陷"}]
}
```

答案集必须恰好覆盖当前 pending 列表，每项 1–4000 字符，选择题必须命中现有选项；错误的轮、旧 revision、失效问题或重复不同答案不启动执行。错误沿用 code/message/request_id，增加可区分的 busy、stale_revision、idempotency_conflict、recovery_required、invalid_cursor 码，HTTP 仍为 409；字段错误 422，存储/模型初始化不可用 503。运行中的模型失败记录为轮失败及 problem 事件。

## 10. 事件和页面一致性

worker 独立消费框架事件；模型 callback/observe 仅用于诊断。按 NodeID 可见性过滤内部结构化消息，仅公开最终答复和规范工具消息；范围/产物卡由保存成功的 Checkpoint.Messages 投影。完整过滤与进度规则见图设计第 8 节。

服务端将框架 delta 经 `schema.MessageAccumulator` 还原为规范消息；写入 messages 投影和 **完整消息快照事件**。前端收到快照后按 message_id 替换，不再实现第二套工具参数增量拼接。连续更新最多按 50ms 或 32 条聚合写入；等待回答、错误、完成等控制事件立即刷新。内部仍保留类型化事件，不修改框架消息结构。

应用事件种类为 message、activity、lifecycle、result、turn、problem、progress。progress 与 snapshot.research_progress 的字段和已保存检查点投影规则见图设计第 8 节。前四项来自框架并在传输边界映射；turn/problem 描述应用队列、状态和已脱敏错误。result 使用第 7 节的本轮正式集合，包含 canonical_message_ids、messages 及 replace_turn_messages=true，不重复附带其他轮的历史。新 Attempt 的 turn 事件使用相同替换语义，清除旧 partial；普通 turn 状态更新不携带该标志。

```text
id: 123
event: message
data: {"id":"123","turn_id":"t1","attempt":1,"kind":"message","message":{"message_id":"m1","snapshot":{"id":"m1","role":"assistant","content_blocks":[{"type":"text","text":{"text":"已保存关注点"}}]}}}
```

ID、event_cursor 通过十进制字符串传输，避免 JavaScript 大整数精度问题。前端以已应用 EventID 去重；相同文字出现在两个不同事件/消息里不能按文本去重。

GET snapshot 在同一只读事务取得投影和当前会话的最大 EventID 作为 event_cursor（无事件为 "0"）；前端先加载快照，再从该游标订阅。SSE 每 200ms 从数据库按会话读取 `id > cursor LIMIT 100`，空闲时退到 2 秒；没有“查历史再挂内存订阅”的丢事件窗口。本阶段不裁剪 events；游标非法或超过当前会话最大 EventID 时，在响应头发送前返回 409 并要求重新读取 snapshot。

SSE 每 15 秒发送无 ID 心跳，使用 fetch 流读取以支持状态码、Last-Event-ID 和将来的认证头。重连按 1/2/5/10 秒退避，成功后重置；404/401 停止重连并显示原因，409 重新拉快照。卸载页面只 abort 订阅。

每次从数据库取出的 rows 必须在网络写入前关闭，不能占住第一阶段唯一数据库连接；连接缓冲最多一批 100 事件。SSE 用 ResponseController 清除该响应默认写入期限，并给每次 flush 设置 5 秒期限，写完清除；慢浏览器断开后自行续读，不阻塞 Agent worker，也不建立无限内存队列。存储写入失败按第 6 节取消并 drain；暂停接收新目标，数据库恢复后记录 problem/恢复状态，不能继续产生无法保存的业务结果。

前端本阶段以纯文本渲染消息与参数摘要，原始 HTML 不执行；代码块/Markdown 富展示后续另行选择解析器，不作为会话闭环的前置条件。

## 11. 配置、观测与生命周期

保留现有配置字段，模型限制作为阶段 02 的应用常量记录在定义版本；暂不引入复杂可编辑的 Agent 配置页面。默认仍为本机单用户，服务端认证与桌面模式不在本阶段扩展。

worker 使用服务级 Context，注入可信 Scope、turn_id、attempt 和已有 observe.Slog；沿用框架 LLM/tool/checkpoint callback 记录耗时、错误、Usage。公共流 RunID、检查点 RunID、TurnID 分别保留字段，不能混写。默认不启用 CapturePayload，不把提示、笔记和密钥写进诊断日志。

启动顺序：配置校验 → 数据目录进程锁 → SQLite 迁移 → 模型/应用装配 → 未结束轮核对 → 启动 worker → ready。readyz 检查数据库与调度器初始化，不在每次请求调用远程模型；模型端点故障按具体轮报告。

退出顺序：停止接收新目标 → 停止领取 queued → 取消执行段并尽力同步保存检查点 → drain 等待 worker 退出 → 关闭数据库与进程锁。优雅等待期限为 10 秒；超过期限则结束整个服务进程，由 OS 释放锁，不能在存活 worker 仍可能写入时先释放锁或重新启动调度。服务退出不标记用户取消；queued、待回答与可恢复轮保持持久状态供下次启动处理。

## 12. 验收与需求边界

| 检查 | 必须观察到的结果 |
| --- | --- |
| 三条业务路径 | 关注点经过范围与产物检查；笔记整理保留来源；项目问答不写入 |
| 公开与内部状态 | 内部 JSON 不进 SSE/正式历史；范围、步骤、产物从已保存检查点恢复 |
| 真实工具闭环 | 当前项目信息到达模型，笔记落库，另一会话可读 |
| 多轮上下文 | 第二轮理解前一轮正式结果，不重复拼接历史或用户输入 |
| 同会话并发提交 | 同键返回原轮，不同新目标仅一个获准；两个项目不串数据 |
| 暂停后跨进程恢复 | 同一 Turn 和问题继续；答案只消费一次，已完成工具不重复保存笔记 |
| 模型完成后业务提交失败 | 重启从完成检查点补齐，不增加模型调用次数 |
| 页面断开与慢消费 | 后台继续；恢复后顺序正确，文字和工具卡不重复 |
| 重试与结束 | 半段消息标记清楚，结束后可以发新目标；已保存笔记保留 |
| 升级冲突 | 定义不兼容明确受阻，可结束本轮，不静默丢状态 |
| 第一阶段回归 | 项目、设置、会话、归档删除、窄屏仍可用；活动轮阻止归档删除 |

FR-S01 消息持久化、FR-S02 流式过程、FR-S06 恢复、FR-U03/U07 状态反馈在本阶段落地。FR-S03 先覆盖项目与笔记；选中论文/方向由阶段 03–04 补齐。FR-S04 的研究任务工具、FR-S05 的论文/报告引用、FR-X12 的真实复现与用户实验均不作为本阶段完成项。

实施后再创建 ACCEPTANCE.md，分别记录 fixture、真实模型、数据库重启、浏览器及故障检查结果；没有真实模型凭据时明确标记该项未验证，不能用 fixture 冒充真实模型联通。
