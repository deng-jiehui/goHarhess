# 阶段 02 实施：Agent 会话、持久执行与事件展示

> 执行者：使用 executing-plans 按复选框逐项实施；不要求委派子代理。

**状态：** 阶段二 MVP 已实施；SQLite 检查点、Turn、HTTP、业务进度流和前端会话路径已落地，完整取消语义仍按后续任务补齐。  
**Goal:** 用户能在现有工作台与研究 Agent 协作，并在刷新、断线、暂停和服务重启后继续原轮。  
**Architecture:** 延续四层；应用直接用 goHarness Graph 编排研究目标澄清、关注点、笔记整理、项目问答、产物检查与受控写入。worker 消费图事件并保存投影，HTTP/SSE 只提交动作和读取持久数据；每轮独立完整检查点。
**Tech Stack:** Go 1.24、现有 goHarness 核心、SQLite 1.36.3、React 19、TypeScript 5、Vite 7、Vitest、Playwright。

设计依据为 [DESIGN.md](DESIGN.md) 与 [业务图详细设计](GRAPH_DESIGN.md)，跨阶段约束见 [CONTRACTS.md](../../tasks/CONTRACTS.md)。当前实现优先覆盖离线 fixture 的主路径、范围中断恢复、Turn 幂等、SQLite 检查点和 HTTP/前端展示；节点、状态和预算以 GRAPH_DESIGN 为唯一详细定义。

> 2026-10-19 改造已完成：路由返回严格 JSON，动态补充信息通过真实 ask_user，回答后继续原业务节点。现行设计与测试结果见 [ROUTING_AND_INTERACTION.md](ROUTING_AND_INTERACTION.md)；本文件下方旧任务清单保留为历史目标，其中 scope_gate、路由工具和固定缺陷粒度要求已废止。本轮实施清单见 [结构化路由计划](../../docs/superpowers/plans/2026-10-19-structured-routing-and-ask-user.md)。

## 业务进度流

Graph 通过 `pkg/stream` 发布带 `autoresearch.progress` 标记的阶段摘要。worker 使用 `StreamEvents`/`ResumeStreamEvents` 持续 drain 公共流，只将以下内容写入会话事件：

- `research.progress`：读取项目、理解目标、范围确认、步骤、阶段草稿、产物检查和笔记保存的摘要；同一进度卡使用稳定 ID 更新。
- `assistant.delta`：仅保留 `compose_reply` 的答复增量，理解目标和起草节点的原始 JSON 不进入前端或正式历史。
- `turn.completed`：Graph 成功完成后提交正式助手消息，临时答复由正式消息接管。

`GET /events` 在活动轮期间保持 SSE 连接，接受 `after` 和 `Last-Event-ID` 续读；终态后关闭连接。页面保留快照轮询作为断线兜底，刷新后从持久事件重建进度卡。

## 全局约束与实施顺序

- [ ] 核对阶段 01 验收和现有工作区，只在本项目目录实施；不要覆盖其他未提交改动。
- [ ] 先补失败行为用例再实现；集中跑阶段检查，不运行竞态测试。
- [ ] Go 源码使用 gofmt、中文注释；领域不依赖框架、HTTP、SQL。
- [ ] 仅在实际导入时增加核心框架 require，保留两个本地 replace；不修改父模块或提前接入 MCP。
- [ ] 主循环直接使用 pkg/graph、pkg/llm、pkg/tool、pkg/contextengine，不使用 agent.New，不导入内部 agentrecipe，不为主图再包装 agent.Agent。
- [ ] 所有 worker 与恢复均用可信 Scope；模型参数、URL 或客户端 JSON 不能指定拥有者。
- [ ] 不更改 001/002，新增 003；旧项目和会话迁移后继续可读。
- [ ] 不调用远程模型作为默认测试；外部联通结果单独记录。

按 T04.0 → T04.1 → T04.2 → T05.1 → T05.2 → T05.3 → T06.1 → T06.2 → T06.3 顺序实施。子编号用于阶段内审查，不新增全局任务 ID。先建运行数据基础，工具测试才有真实项目、会话、轮可引用。T04 的执行器先通过集成测试验证，不启用页面发送；全部完成后才替换阶段 01 的空会话页。

## 文件职责图

| 路径 | 新增/修改 | 职责 |
| --- | --- | --- |
| internal/domain/turn.go、message.go、note.go、runtime_repositories.go | 新增 | 轮/动作/消息/笔记对象与原子仓储契约 |
| internal/app/turns.go、notes.go、context.go、snapshot.go | 新增 | 目标接收、动作、笔记用例与上下文快照 |
| internal/app/research_graph.go、research_state.go、research_goal.go、research_artifact.go、research_nodes.go | 新增 | 业务拓扑与状态、理解/范围、产物检查与模板、节点适配处理器 |
| internal/app/research_progress.go、turn_worker.go、recovery.go | 新增 | 公开产物卡、已保存检查点进度投影、执行段与启动核对 |
| internal/app/tools/project.go、notes.go、ask.go | 新增 | 受作用域限制的四个工具 |
| internal/app/events.go | 新增 | 类型化框架事件到业务投影的转换 |
| internal/infra/models/factory.go、fixture.go | 新增 | 现有 ChatModel 的真实/演示工厂 |
| internal/infra/sqlite/turns.go、messages.go、notes.go、actions.go、events.go、checkpoints.go、snapshot.go | 新增 | 事务、查询、幂等、完整检查点 |
| internal/infra/sqlite/migrations/003_agent_runtime.sql | 新增 | 阶段 02 七张表、索引和删除关联 |
| internal/infra/instance/lock.go、lock_windows.go、lock_linux.go、lock_other.go | 新增 | 单一数据目录的服务进程锁；other 文件用 !windows && !linux 构建约束明确拒绝 |
| internal/transport/httpapi/turns.go、messages.go、events.go、notes.go | 新增 | 发送、查询、回答、重试、结束、SSE、笔记 |
| main.go、internal/config/config.go、httpapi/router.go | 修改 | 注入 runtime、初始化/关闭、配置校验、路由与错误码 |
| internal/app/sessions.go、sqlite/projects.go、sqlite/sessions.go | 修改 | 接入会话执行用例，事务保护归档与删除 |
| web/src/agent/types.ts、reducer.ts、sse.ts、useSessionEvents.ts | 新增 | 快照投影、SSE 解析与重连 |
| web/src/components/MessageList.tsx、ToolCallCard.tsx、QuestionCard.tsx、RunStatus.tsx、ResearchProgress.tsx | 新增 | 对话、真实工具过程、范围/步骤/合格产物卡 |
| web/src/pages/SessionPage.tsx、WorkspacePage.tsx、components/ContextPanel.tsx、api/client.ts、types.ts、styles.css | 修改 | 启用发送、笔记栏、活动状态和窄屏 |
| web/e2e/session.spec.ts、testdata/agent/project-note.json | 新增 | 固定场景与浏览器验收 |

每个有行为的新增单元配同目录 *_test.go / *.test.ts；以下任务列出关键测试，不要求为简单类型再写镜像测试。

## T04.0：运行数据基础

**输入：** 阶段 01 数据库与迁移机制。  
**输出：** 阶段 02 领域对象、003 全部七张表及约束；尚不创建空仓储实现。  
**文件：** domain/turn.go、message.go、note.go、sqlite/migrations/003_agent_runtime.sql、sqlite/store_test.go。

- [ ] 在保留阶段 01 项目和会话的临时库应用 003，验证七张新增表存在、原数据可读、重复运行不改旧迁移。
- [ ] 定义设计第 4 节的 Turn、TurnAction、MessageRecord、Note、SessionEvent；Turn 加 CanonicalMessageIDs，规范消息仅以 JSON 穿过领域边界。
- [ ] 创建活动轮、请求键、笔记工具键、源事件、唯一 result 和会话消息 Seq 约束；直接插入受控冲突数据，验证数据库拒绝重复活动轮、重复笔记与第二个 result。turns 增加 ProgressJSON、ProgressCheckpointID/Version；events 增加 source_checkpoint_id 及 kind='progress' 的 turn+source_checkpoint_id 部分唯一索引。
- [ ] 测试 fixture 使用独立 database/sql 连接向已迁移的临时库插入合法项目、会话和轮；之后各包笔记测试采用相同建数方式，无须访问仓储私有字段或等待 T04.2 的接收目标用例。

**验收：** 旧数据迁移可重复执行，后续工具测试不依赖尚未实现的 worker。

## T04.1：三条研究业务路径与受控工具

**输入：** 阶段 01 的 Scope、项目仓储及 T04.0 的轮/笔记数据基础。  
**输出：** 业务图全部节点、ResearchState/Codec、模型与工具工厂、NoteService；不启用浏览器发送。  
**文件：** 文件职责图中的 research_*、context.go、tools/*、models/*、app/notes.go、sqlite/notes.go。  
**测试：** research_graph_test.go、research_goal_test.go、research_artifact_test.go、research_state_test.go、tools/tools_test.go、models/fixture_test.go。

~~~go
// app：不导入 app/tools，由入口构造闭包后注入。
func BuildResearchGraph(model llm.ChatModel, tools []tool.Tool, definitionVersion string, builder runnable.Runnable[contextengine.Request, contextengine.Result]) (*graph.CompiledGraph[[]schema.Message, []schema.Message], error)
func NewTurnContextBuilder() (runnable.Runnable[contextengine.Request, contextengine.Result], error)
func NewResearchStateCodec() (graph.StateCodec, error)
func (s *ResearchState) CheckpointMessages() []schema.Message
func PrepareStageRequest(ctx context.Context, s *ResearchState, nodeID string, builder runnable.Runnable[contextengine.Request, contextengine.Result]) (llm.Request, error)
func CommitStageResponse(ctx context.Context, s *ResearchState, nodeID string, response llm.Response) error
func PrepareBusinessToolInput(ctx context.Context, s *ResearchState, nodeID string) ([]schema.Message, error)
func CommitBusinessToolResults(ctx context.Context, s *ResearchState, nodeID string, results []schema.Message) error
func CheckScope(ctx context.Context, s *ResearchState) error
func BuildTurnPlan(ctx context.Context, s *ResearchState) error
func CheckArtifact(ctx context.Context, s *ResearchState) error
func ValidateCompletion(ctx context.Context, s *ResearchState) error

type NoteInput struct { Text, ToolKey string }
func (s *NoteService) Save(ctx context.Context, scope domain.Scope, sessionID, turnID string, in NoteInput) (domain.Note, error)
func (s *NoteService) List(ctx context.Context, scope domain.Scope, limit int) ([]domain.Note, error)

// tools：接口由 NoteService 实现，身份不接受模型覆盖。
type Notes interface {
    Save(context.Context, domain.Scope, string, string, app.NoteInput) (domain.Note, error)
    List(context.Context, domain.Scope, int) ([]domain.Note, error)
}
func ProjectSummary(project domain.Project) tool.Tool
func ProjectNotes(notes Notes, scope domain.Scope, sessionID, turnID string) []tool.Tool
func AskUser() tool.Tool
~~~

ResearchState、GoalDecision、ScopeQuestion/Answer、DraftArtifact、SourceRef、PlanStep、CheckIssue、NoteReceipt 和公开 ProgressSnapshot 的字段以 GRAPH_DESIGN 第 6、8 节为准；不再维护第二份简化 State。NewTurnContextBuilder 只配置 contextengine 窗口策略，逐节点 Pinned 由 PrepareStageRequest 从 State 构造，避免冻结旧笔记副本长期覆盖真实工具结果。

- [ ] **先固定路径测试。** TestFocusClarifiesBeforeSaving 使用真实临时 SQLite 与 HandlerModel：项目缺评测对象时，第一次执行只读取并暂停；save_note 次数必须为 0。PendingInterrupts 查到问题，带用户“仓库级缺陷”答案恢复，才生成合格关注点并保存一条笔记。去掉 scope_gate 或 write_gate 后此测试必须失败。
- [ ] **定义状态和严格编码。** StateFactory 校验 H+U，History 克隆 H、UserMessage 克隆 U，其余字段清零，MaxLLMCalls=8；恢复不再次初始化。Codec ID=autoresearch:research-state、version=1，UseNumber/DisallowUnknownFields、拒绝尾随值，校验所有预算、路径和消息身份。CheckpointMessages 只返回 H+U+Output；ReplyCandidate 和内部 Decision/Draft 不混入。声明 ConversationProvider，不实现外部化接口。
- [ ] **装配真实节点。** 按 GRAPH_DESIGN 第 3–4、7 节注册全部 NodeID：3 个 ToolsNode、5 个 ChatModelNode、6 个返回 BusinessStepResult 的业务 Runnable、2 个 StateNode、finish。分支 Runnable 用恒等类型化 PreHandler；不能将 StateNode 的 *State 输出与同类型 State 谓词直接组合，当前会触发 edge.branch.ambiguous。Snapshot 检查全部节点/分支/默认边，SetMaxSteps(48)、真实定义版本后 Compile；分支无副作用或中断。
- [ ] **实现理解与范围检查。** 三条路线及 unsupported 都由结构化 GoalDecision 识别；槽位来源必须存在，明确用户约束优先。代码修复的缺陷粒度缺失会追问，qa 查笔记不追问范围。理解格式反馈最多 1 次，追问最多 2 次；回答次数只在 clarify 成功提交后递增，问题取自已提交 State，不在恢复时改题。
- [ ] **实现真实来源快照。** load_context 固定 read_project/list_notes(limit=10)，项目来自 Admit 快照，笔记取第一次成功读取结果。Facts 的 ID/Revision/内容必须来自工具结果；两个项目交错执行、重建模型或重启均不能串用。模型应收到真实工具返回值，测试故意替换项目时必须失败。
- [ ] **实现业务产物与有限反馈。** focus 必须包含一致范围、2–3 项有目的/动作/预期产物的后续行动；notes 必须引用快照中的笔记且不修改原笔记；qa 区分事实与建议。JSON 错误/范围被改/来源不存在时回原 draft 一次，再失败为 invalid_artifact。空 notes 集合绕到 explain_boundary，不调用起草/保存；明确选择快照外 ID 时先澄清，不偷偷换集合。
- [ ] **实现写入前后门槛。** write_gate 只有 focus/notes 且本轮明确要求保存时才生成 SaveText；正文用合格产物固定模板，1–4000 字符。无意图只读、含糊不保存。verify_note 检查真实 ID、正文/摘要和 IsError；失败不能声称保存成功。保存意图不能由项目资料中的指令取得。
- [ ] **保留框架工具身份。** 每个工具 PreHandler 纯构造规范调用，ID 基于 U.ID/NodeID/逻辑访问序号，不含 Attempt/时间；可 PublishSnapshots 显示调用。PostHandler 保留原 FunctionID/IsError，将同一调用与 EnsureMessageIDs 后的真实结果一起加入 Output 并发布快照；中断不进入 PostHandler。save_note 原样传递 CallInput.IdempotencyKey，相同键同文返回原记录，异文冲突。
- [ ] **实现分阶段模型输出。** understand_goal、各 draft 都是 ChatModelNode，Functions 为空，普通 JSON 提示配 StructuredOutput.Decode 本地检查；格式错仍增加 LLMCalls 并保存安全反馈，不直接丢掉修复机会。compose_reply 的文本候选通过 completion_gate 后才加入 Output；空白/意外调用最多修复一次且不重存笔记，原候选不进正式历史。
- [ ] **限制全部模型调用。** 每个模型节点设置 WithMaxTokens(2048)，PreHandler 检查共享 LLMCalls<8；每个完整响应（含格式错）提交一次计数。构造首次理解+2 次追问+1 次理解修复+2 次草稿+2 次答复的最坏路径，8 次可结束；请求第 9 次拒绝，恢复/重试不能重置预算。非法工具调用不执行，不能再套用“第 8 次还有工具调用”的旧判据。
- [ ] **构造上下文。** contextengine 窗口 16384、输出预留 2048；当前节点系统指令只注入一次，History/本轮工具配对不破坏，Facts 与反馈有明确来源。模型没有 Functions，也不需要原生 ToolCalls/JSONSchema 能力；未知来源提示和历史笔记不能获得指令优先级。
- [ ] **演示模型可重建。** fixture 用 NewHandlerModel，依请求中的阶段标记、目标、工具事实、答案与反馈返回相应 JSON/文本，不依赖全局调用次数。覆盖 focus/notes/qa、空笔记、不支持请求、一次修复和两个会话交错。
- [ ] **真实模型错误透明。** remote 用现有 openai-compatible 工厂；认证缺失拒绝启动，401/超时保存真实错误，不降级 fixture；测试仅用 httptest 端点，真实服务联通另记。
- [ ] **版本覆盖业务语义。** 节点/分支、提示、来源/保存规则、工具 schema、模型配置、Codec、预算都参与 DefinitionVersion；密钥不参与。变更规则后旧检查点拒绝恢复，不能给新图套旧版本号。

工具防重关键断言：

~~~go
func TestSavedNoteUsesStableToolKey(t *testing.T) {
    // 测试辅助函数用 T04.0 临时库建立真实项目、会话和轮。
    ctx, scope, sessionID, turnID, notes := newNoteFixture(t)
    in := app.NoteInput{Text: "关注仓库级缺陷与工具选择", ToolKey: "stable-call-key"}
    one, err := notes.Save(ctx, scope, sessionID, turnID, in)
    if err != nil { t.Fatal(err) }
    two, err := notes.Save(ctx, scope, sessionID, turnID, in)
    if err != nil { t.Fatal(err) }
    if one.ID != two.ID { t.Fatal("恢复重放产生了两条笔记") }
}
~~~

newNoteFixture 仅为测试辅助，用 T04.0 数据库 fixture 建轮，不依赖尚未实现的 Send。生产类型不增加专供测试的创建方法。

**检查：** 在本模块运行 `go test ./internal/app/... ./internal/infra/models/... ./internal/infra/sqlite/...`。  
**验收：** 业务路径不能绕过范围/产物/写入门槛；真实来源到达模型，暂停恢复产生一条真实笔记；普通问答只读。

## T04.2：持久接收目标与后台队列

**输入：** T04.1 的主图工厂、阶段 01 会话。  
**输出：** Send/Turn 服务、原子活动轮、单进程 worker；Snapshot 由 T06.1 完成。  
**文件：** domain/turn.go、message.go、runtime_repositories.go，app/turns.go、turn_worker.go，sqlite/turns.go、messages.go，instance/*；测试 turns_test.go、turn_worker_test.go、instance/lock_test.go。

~~~go
// app：SessionService 用现有 SessionOption 注入 TurnService。
func WithTurnService(service *TurnService) SessionOption
type SendInput struct { Text, IdempotencyKey string }
type SendResult struct { Turn domain.Turn; Replayed bool }
func (s *SessionService) Send(ctx context.Context, scope domain.Scope, sessionID string, in SendInput) (SendResult, error)
type ResearchGraphFactory func(context.Context, TurnContext) (*graph.CompiledGraph[[]schema.Message, []schema.Message], error)

// domain：事务聚合接口，不把 SQL 事务暴露给应用。
type AdmitResult struct { Turn Turn; Replayed bool }
type TurnRepository interface {
    Admit(context.Context, Scope, string, TurnDraft) (AdmitResult, error)
    GetTurn(context.Context, Scope, string, string) (Turn, error)
    ClaimQueued(context.Context) (Turn, bool, error)
}
~~~

`TurnDraft` 位于 domain/turn.go，包含 ID/InputText/IdempotencyKey/DefinitionVersion/UserMessageJSON。Admit 在同一事务读取项目、笔记和正式历史，构造不依赖框架的 AdmissionSnapshot（Project、Notes、HistoryJSON、UserMessageJSON）并编码为轮的 ContextJSON，再分配消息 Seq；不能接受事务外预读的旧历史。TurnContext 位于 app/context.go，按设计第 4 节解码规范消息；不把 schema.Message 放进领域对象。

SendResult.Replayed 直接映射 AdmitResult.Replayed；该标志在原子幂等检查时确定，HTTP 层不另查时间或状态推断。

- [ ] 先写并发 Send 回归：同键同内容返回同一轮，不同内容同键冲突；两个不同键同会话只能一个成功。先去掉部分唯一约束证明回归能捕获重复活动轮。
- [ ] 为 turns 增加以下索引；Admit 的幂等查询先于忙碌查询，用户消息、轮、上下文和初始事件在同一事务。

~~~sql
CREATE UNIQUE INDEX turns_request_key ON turns(session_id, idempotency_key);
CREATE UNIQUE INDEX turns_one_active ON turns(session_id)
WHERE state IN ('queued', 'running', 'awaiting_input', 'recovery_required');
~~~

- [ ] 每轮在 Admit 事务冻结项目版本、最近 10 条笔记、既有正式历史和本轮唯一用户消息。用屏障令上一轮在入队前完成，确认新上下文包含其正式回答；恢复不读取新版本项目替换旧上下文。
- [ ] 实现 OS 文件锁并在 SQLite 初始化之前获取；同目录第二个服务启动失败，释放/进程退出后可再次取得；锁文件保留，不在未持锁时删除。
- [ ] worker 以 SQLite 条件更新领取 queued，500ms 扫描、最多 2 个不同会话并发；请求取消不能取消 worker。用受控阻塞模型验证 HTTP 返回后 worker 仍能继续。
- [ ] 首次执行传入冻结的 History+UserMessage，调用编译图 StreamEvents；恢复调用 ResumeStreamEvents，不重新传输入。工厂按轮创建项目工具闭包与上下文构建器，不持有全局可变 ResearchState。
- [ ] 队列已保存但内存通知丢失时，下次扫描仍执行；服务未装配 TurnService 时 Send 明确不可用，不能返回虚构成功。
- [ ] SessionService 现有创建/列表/修改测试继续通过；T04 最小服务集成直接查询轮结果，前端发送仍保持阶段 01 状态直至 T06 完成。

**验收：** 202 只在目标持久保存后返回；后台任务独立于 HTTP，进程重启可以扫描 queued。

## T05.1：完整 SQLite 检查点

**输入：** state.Store 与每轮可信检查点键。  
**输出：** SQLiteCheckpointStore 的五方法实现，完整历史、CAS 与作用域隔离。  
**文件：** sqlite/checkpoints.go、checkpoints_test.go；使用 T04.0 已创建的 checkpoints/checkpoint_heads。

~~~go
func NewCheckpointStore(store *Store, scope domain.Scope, sessionID, turnID string) *CheckpointStore
// *CheckpointStore 完整实现 state.Store，方法签名以 tasks/CONTRACTS.md 第 4 节为准。
~~~

- [ ] 写入→关闭数据库→重开→Load/Get/List；比较消息、Values、Execution、Result 和父链。实际数据用框架编码，不能只保存两条文本消息充当执行恢复。
- [ ] 两个保存者读取同一 ID/Version，一个成功后另一个返回 state.ErrConflict；删除重建后保存旧 ID 也失败。
- [ ] Save 内用 PersistClone/JSON 校验，事务校验 head 后 state.NewID、递增 Version、插入完整历史、交换 head；第一次保存要求空 ID 与 Version 0。
- [ ] List 默认 100、最多 100，按新到旧分页；Before 不含该记录且必须属于同一键。修改返回值不能污染存储；取消 Context 或非法检查点不写入新版本。
- [ ] 每个适配器按已绑定的 scope/session/turn 限制根键及其框架派生子键，子记录保存同一归属；不通过简单 sessionID 猜测租户。
- [ ] Worker 注入 graph.WithCheckpoint 与 graph.WithDurability(graph.DurabilitySync)；关闭检查点写入时图必须失败，不能让页面出现成功 result。Checkpoint.Messages 必须等于 State.CheckpointMessages，Result 只含 Output，Execution 由框架编解码。

关键 CAS 断言：

~~~go
head, err := checkpoints.Save(ctx, state.Checkpoint{ConversationID: key})
if err != nil { t.Fatal(err) }
stale := head.Clone()
if _, err = checkpoints.Save(ctx, head); err != nil { t.Fatal(err) }
if _, err = checkpoints.Save(ctx, stale); !errors.Is(err, state.ErrConflict) {
    t.Fatalf("旧检查点覆盖了新记录：%v", err)
}
~~~

**验收：** 对完整框架检查点验证跨数据库连接/进程重开、旧保存者、删除重建和深拷贝边界。

## T05.2：回答、重试、结束和启动核对

**输入：** 持久轮与检查点。  
**输出：** TurnAction、Resume/Retry/Cancel 和 Recovery，沿用原 Turn ID。  
**文件：** app/turns.go、recovery.go、turn_worker.go，sqlite/actions.go、turns.go；测试 recovery_test.go、actions_test.go。

~~~go
type AnswerInput struct { InterruptID, Text string }
type ResumeInput struct {
    Revision int64
    IdempotencyKey string
    Answers []AnswerInput
}
type TurnActionInput struct { Revision int64; IdempotencyKey string }
type ActionResult struct { Turn domain.Turn; ActionID string; Replayed bool }
func (s *TurnService) Resume(ctx context.Context, scope domain.Scope, sessionID, turnID string, in ResumeInput) (ActionResult, error)
func (s *TurnService) Retry(ctx context.Context, scope domain.Scope, sessionID, turnID string, in TurnActionInput) (ActionResult, error)
func (s *TurnService) Cancel(ctx context.Context, scope domain.Scope, sessionID, turnID string, in TurnActionInput) (ActionResult, error)
func (s *TurnService) Recover(ctx context.Context) error
~~~

动作仓储在 domain/runtime_repositories.go 定义 ApplyAction(context.Context, Scope, string, string, ActionDraft) (ActionAdmission, error)，两个 string 依次为 sessionID、turnID。ActionDraft 包含 ID/Kind/IdempotencyKey/RequestHash/PayloadJSON/ExpectedRevision；ActionAdmission 包含 Turn、ActionID、Replayed。动作事务返回真实元数据，应用映射为 ActionResult。Cancel 遇到已 completed 且无对应旧动作时返回空 ActionID；HTTP 映射 200。queued/awaiting_input/recovery_required 且无运行领取者时可在动作事务直接 cancelled；running 必须先持久请求、取消执行并等待退出，不能只根据内存 map 暂时没有 reader 就认定安全。

- [ ] 用真实业务路径在保存前的 clarify 暂停，保存 pending，销毁编译图和 Store，重建后查询 PendingInterrupts 并提交原答案。保留同一 Turn 和用户消息；恢复后才生成一条笔记。另在 save_note 已写库、节点未提交处注入崩溃，验证同工具键恢复不重复写入。
- [ ] Resume 事务核对 revision、归属和完整答案集，保存动作及 request hash，再置 queued；相同动作键重发返回原轮，不同答案复用键 409。消费者中止后重新启动仍可读取未消费动作。
- [ ] 把 Answers 逐一转为 graph.WithResume，复用原检查点键；先判断 graph.InterruptedError，再区分普通错误。启动先跳过业务终态，再处理 CancelRequested，再补完成检查点；可恢复轮调用编译图 PendingInterrupts 只读查询，不为发现问题启动执行，不解析 Execution。
- [ ] 当前 pending 与动作答案 ID 全集匹配才带答案 ResumeStreamEvents；完全无交集或 pending 已空则将旧动作记 applied，恢复时不带旧答案，或展示下一组问题；部分交集/载荷冲突进入 recovery_required。查询错误不得当空问题。轮内查询、动作消费与执行串行。
- [ ] 注入“答案已持久进入框架、动作表仍未确认”后重启的故障；旧答案不能被传给下一组问题，已经回答的工具结果不得重复新增。生命周期 phase=interrupted 必须来自保存成功的中断。
- [ ] 实现设计第 7 节所有重启分支；覆盖 CancelRequested=true 且检查点 completed、业务尚未提交的组合，必须补 cancelled 而非成功。无检查点首次重跑的依据为 DurabilitySync 在首节点前落盘；存储读取失败不得当作不存在。
- [ ] Retry 只对 can_retry 的失败/恢复受阻轮重新取得活动名额，且不能越过更新的轮；model_budget_exceeded、incomplete_tool_calls、invalid_goal、invalid_artifact、invalid_completion、note_write_failed、context_budget_exceeded 不可重试，不清除检查点。定义未恢复一致时保留 recovery_required。
- [ ] 新 Attempt 事务保留本轮用户消息，旧非正式输出标记 superseded，并发布替换本轮投影的 turn 事件；最终正式集合可重新晋升恢复中仍有效的旧消息。
- [ ] Cancel 原子保存动作和 CancelRequested；取消执行 Context 后持续 drain 自然终止，再释放活动约束。固定 SendTimeout=0，Reader.Close 不作为退出确认。用受控工具在收到取消后延迟退出，验证旧工具未退出前新目标仍冲突；完成/取消竞争只提交一个终态。
- [ ] 对项目删除、项目/会话归档增加同事务活动轮检查；不能先应用查询后数据库无条件删除。活动轮 409、完成轮正常归档删除，级联只作用本产品数据库。
- [ ] 退出与用户取消区分处理；服务关闭尽力保存检查点，启动后 queued/awaiting_input/recovery_required 场景可核对，不误记为 cancelled。10 秒优雅期限后只能退出整个进程；不能在存活 worker 仍会写入时释放数据目录锁。模型/工具不响应取消时停止接收新目标并保留占用。

**验收：** 真正重建对象与数据库后恢复，不依赖内存协程；恢复与业务提交之间的故障不会重复写笔记或增加成功结果。

## T05.3：框架事件到持久消息与结果

**输入：** stream.Event[[]schema.Message]，SQLiteCheckpointStore。  
**输出：** 已持久消息快照、公开研究进度、轮状态与唯一 result。  
**文件：** app/events.go、research_progress.go 及测试，sqlite/events.go、messages.go、turns.go 及测试，修改 turn_worker.go。

- [ ] 先按 NodeID 过滤 understand_goal/三个 draft 的原始消息，不写 messages/events；未知 NodeID 默认拒绝投影。compose_reply 的流在内存累积后仅投影文本，去掉 reasoning/意外调用/保留应用 Extra；ReplyCandidate 同样处理并记录意外调用错误。工具节点仍保留完整规范调用/结果。用内部私有标记验证 SSE/快照/正式历史均无该标记。
- [ ] 用 schema.MessageAccumulator 还原允许公开节点的 delta，再按上述规则净化快照、替换同 ID 投影；两个真实重复文本 delta 必须都保留。
- [ ] 实现公开 ProgressSnapshot allowlist 和稳定 ID 产物卡，卡数据放 schema.Message.Extra["autoresearch.progress"]；原始卡 message 不先行投影。每 200ms 只读加载已保存 root checkpoint，从 Messages 提取卡，比较 ProgressCheckpointVersion，仅新版本且内容变化时在事务更新 turns 投影与 progress 事件。不解析 Execution、不依赖 Values 或 callback。
- [ ] 注入节点提交成功而进度事务失败，重启能从 Checkpoint.Messages 补投；重复 checkpoint 只有一条 progress，旧版本不能覆盖新进度。未提交卡不显示为已完成。新 Attempt 的 recovering 标记在核对后替换；终态补交 result 包含相同 research_progress，完整正式 C 仍含公开卡消息且不重复展示。
- [ ] 用框架式事件内存模拟验证 message/工具结果到 JSON 快照的映射；连续快照只更新同一消息，重放相同源事件不重复插入。
- [ ] 每 50ms/32 条上限批量写消息投影与事件，控制事件先刷新 pending 消息；只在数据库提交后允许 SSE 读到对应 EventID。
- [ ] 成功 result 暂存到正常 EOF 后，加载完成检查点，以冻结历史 ID 集合 H 排除旧轮消息，保留唯一用户消息 U，得到本轮有序集合 C。确认完成检查点 Result 等于本轮 Output，C 等于 U+Output；旧历史内容/归属不变。已有 ID 保留原 Turn/Seq，新消息才分配 Seq。
- [ ] 在完成事务将 C 置 committed 且清除 superseded，本轮其余输出置 superseded；保存 Turn.CanonicalMessageIDs、Turn.completed 与唯一 result。result 包含 replace_turn_messages=true、canonical_message_ids、C 的完整快照；正式历史按已完成轮及 canonical ID 顺序读取。
- [ ] 故障场景包含：第一个 Attempt 输出 partial 后重试生成不同 ID；中断恢复复用已完成工具消息 ID。最终在线页面和 Snapshot 都只保留 C，旧轮 TurnID/Seq 不变；最后一次无流的完成检查点补交采用同一核对函数。
- [ ] 构造跨 understand_goal、draft 和 compose_reply 的预算用尽：第 8 个完整响应后可执行无模型节点完成，请求第 9 次才报 model_budget_exceeded。另测 invalid_goal/invalid_artifact/invalid_completion 的修复次数和 incomplete_tool_calls；均 failed、can_retry=false，不发成功 result，不进正式历史。
- [ ] 对 framework completed / business commit failed 注入一次故障，重建 runtime 核对：模型调用数不增加，result 只有一条。
- [ ] event ID 对外字符串、源事件元数据单独字段；唯一 result、源事件防重索引与消息 Seq 分配集中在事务，不用前端文本去重弥补后端重复提交。
- [ ] 持久化失败取消执行 Context 并 drain 到自然终止，不主动 Close 提前丢失退出确认；后续事件丢弃，数据库恢复后写 problem 或启动核对。未确认退出前不释放轮与 worker 名额，失败不能变成空成功回答。

**验收：** 页面可依赖的全部状态都有持久来源，callback 丢失不影响正式消息与工具卡。

## T06.1：HTTP 和可续读 SSE

**输入：** TurnService、持久消息/事件投影。  
**输出：** 设计第 9 节全部 API；快照游标与无丢失 SSE。  
**文件：** httpapi/turns.go、messages.go、events.go、notes.go、router.go，app/snapshot.go、sqlite/snapshot.go；测试 httpapi/events_test.go、turns_test.go、sqlite/snapshot_test.go。

- [ ] 先写真实 httptest API：Send 返回 202，错误项目路由 404，同键重发返回同轮且 replayed=true；回答/重试/结束返回持久 action_id 与 replayed，完成轮 Cancel 为 200。未知字段 422，忙碌与旧版本采用明确 code。
- [ ] Snapshot 在同一只读事务取得最近 100 消息、相关轮和活动轮、research_progress、20 笔记与 event_cursor，排除 superseded；已完成轮返回 CanonicalMessageIDs。消息分页上限 100，分页按 Seq，轮内正式消息按 canonical ID 展示。
- [ ] 用可控屏障在 Snapshot 返回之后、新 SSE 请求之前插入事件；SSE 必须从 cursor 读到它。不能用固定等待时间掩盖订阅空窗。
- [ ] SSE 每次按归属和 id 查询最多 100，先关闭 rows，再执行网络写入；断开后 Last-Event-ID 可续读，相同事件不会生成新 ID。
- [ ] 验证心跳无 ID，非法或超前 cursor 在 SSE headers 前返回 409；Last-Event-ID 与 after 同时提供且不同返回 422。
- [ ] 使用 ResponseController 给 SSE 清除默认响应期限，并每次 flush 设 5 秒写期限、写后清除；慢消费者断开不占住 worker 或唯一数据库连接。
- [ ] 关闭 SSE 连接后模型仍完成，之后 Snapshot 可读取结果；错误与归属检查发生在打开流之前。

**验收：** 页面任意时刻重新订阅都能恢复持久顺序，HTTP 客户端断开不改变后台执行。

## T06.2：前端会话与状态

**输入：** Snapshot、Turn API 和持久 SSE。  
**输出：** 可发送、可追问、可查看工具、可恢复的会话 UI。  
**文件：** web/src/agent/*、MessageList/ToolCallCard/QuestionCard/RunStatus/ResearchProgress、SessionPage、ContextPanel、client、styles；测试 reducer.test.ts、sse.test.ts、client.test.ts。

~~~typescript
type MessageEvent = {
  id: string;
  kind: "message";
  turn_id: string;
  attempt: number;
  message: { message_id: string; snapshot: MessageDTO };
};
type TurnProjection = {
  replace_turn_messages: true;
  canonical_message_ids: string[];
  messages: MessageDTO[];
};
// MessageDTO 对应服务端规范消息投影，在 agent/types.ts 定义。
// result 使用完整 TurnProjection；attempt_started 的 turn 事件保留用户消息、清空 canonical IDs。
// reducer 以 EventID 去重，snapshot 按 message_id 替换。
it("断线补发相同快照不会重复文字", () => {
  const event = messageSnapshotEvent("7", "m1", "已保存笔记");
  const one = reduceAgentEvent(emptyAgentState(), event);
  const two = reduceAgentEvent(one, event);
  expect(two.messages).toEqual(one.messages);
  expect(two.lastEventID).toBe("7");
});
~~~

`messageSnapshotEvent` 为 reducer.test.ts 本地构造函数，按上面 DTO 生成完整事件；实现 reducer 时不得把它放进生产导出。

- [ ] 测试事件重复、同消息更新、不同消息同文本；新 Attempt 的 turn 事件仅保留本轮用户消息；result 按完整 C 替换、删除旧 partial 并按 canonical IDs 排序，其他轮不变；409 重载快照。先运行 `npm --prefix web test -- reducer.test.ts` 确认缺失行为失败。
- [ ] SSE parser 测试 UTF-8 中文跨字节分片、多行 data、心跳、CRLF、断开后残留半条事件；只在完整事件成功应用后推进游标。
- [ ] 发送前生成并保留幂等键；202 后才从输入草稿移除，网络结果不明时用原键重试，不为同一目标换新键。
- [ ] 初始化先 Snapshot 后续读；每次切换会话 abort 旧订阅与请求，避免旧会话事件进入新页面。
- [ ] 工具卡显示调用中/完成/错误；问题卡绑定 pending ID 与 revision，完整回答后提交同一 Turn；按钮请求失败保留答案。
- [ ] progress/snapshot/result 使用同一个公开进度 DTO，显示 Kind、范围、步骤、合格产物和已核对笔记 ID；schema.Message.Extra 的公开卡按 ID 合并，不显示原始 JSON。旧版本进度不覆盖新版本，断线/切换会话/新 Attempt 不串用；内部规划和失败草稿始终不可见。
- [ ] 已有活动轮禁用普通发送；停止/重试使用专门 API，处理中可见；离线提示不能覆盖正式轮状态。
- [ ] 右侧加载与更新项目笔记，390px 窄屏保留现有导航和表单操作；纯文本渲染不执行 HTML。

**验收：** 页面由真实 API 驱动，刷新后能重建消息、工具结果、问题卡与笔记，不靠组件内临时历史。

## T06.3：集中验证与交付记录

**文件：** web/e2e/session.spec.ts、main_test.go，阶段 ACCEPTANCE.md（实施后创建），README、ROADMAP、IMPLEMENTATION_TASKS、TECH_STACK、DEPENDENCIES。

- [ ] 浏览器：读取项目→澄清范围→检查关注点→保存→另一会话只读查询；整理笔记保留来源，空笔记无写入；第二轮引用正式结果；等待回答→刷新/重启→继续；产物修复不跳过检查，答复修复不重存；公开范围/步骤/产物恢复；断线不重复、失败重试/结束、移动端输入和回答。
- [ ] 保留阶段 01 浏览器流程，但把“发送按钮禁用”断言改为本阶段“空输入不可发送、正常输入可发送”；其他项目/设置/会话回归不得删除。
- [ ] 真实子进程：暂停后停止后端，使用同一临时数据目录重启；回答后同一 Turn 完成且笔记只一条。另测 queued 后进程退出、完成检查点后业务提交失败。
- [ ] 作用域：两用户 fixture、两项目、两会话交错执行与跨路由查询/恢复/事件/笔记均隔离；仍不宣称支持多用户登录。
- [ ] 实际使用 Go 1.24.4 验证独立模块；重查依赖清单与锁文件，不顺带升级其他模块。

~~~powershell
# 在 auto-research 根目录执行；Windows 需要时使用可写缓存。
$env:GOCACHE = Join-Path $env:TEMP 'autoresearch-go-cache'
$env:GOTOOLCHAIN = 'go1.24.4'
go mod tidy
go mod verify
go test -count=1 ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web test
npm --prefix web run build
npm --prefix web run test:e2e -- workspace.spec.ts session.spec.ts
~~~

- [ ] 配置可用模型时单独记录 remote 的项目读取、工具调用、追问、错误与恢复；缺少凭据记录“未验证”，不跳过 fixture 核心验收。
- [ ] 更新 README 的模型配置、默认模式、限制和演示路径；文档与实际 API 保持一致。
- [ ] 验收记录包含实际版本、命令与退出结果、真实进程重启、桌面/窄屏检查、外部能力验证状态；通过后才勾选任务和更新路线图。

可按上述子任务形成审查单元；仅在需要提交时暂存本项目的明确文件，禁止全工作区提交。本计划只设计阶段 02，不自动开始源码实施、部署、发布或阶段 03。
