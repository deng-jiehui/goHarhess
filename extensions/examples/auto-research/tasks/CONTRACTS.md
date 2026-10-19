# AutoResearch 契约与实现决策

**状态：** 阶段 01 的 Scope、ResearchSettings、Project、Session、仓储接口与工作台 API 已实现，其余为跨阶段接口草案。以下类型、端点和文件均属于本项目；现有 goHarness API 另行列出。

本文件是跨阶段约定，按[阶段路线图](../ROADMAP.md)逐步实现。阶段 01 已交付，阶段 02 已编写独立[设计](../phases/02-agent-runtime/DESIGN.md)与[实施](../phases/02-agent-runtime/IMPLEMENTATION.md)，尚未实现；其余类型不需要提前创建空实现。

## 1. 工作目录与模块

项目根目录为 extensions/examples/auto-research，独立模块已创建。阶段 01 使用以下模块及本地替换；实际 Agent 装配在阶段 02 引入对应框架 require：

~~~go
module github.com/goharness/autoresearch

go 1.24.0

require modernc.org/sqlite v1.36.3

// 间接依赖以项目 go.mod 为准。

replace github.com/goharness/goharness => ../../..
replace github.com/goharness/goharness/extensions => ../..
~~~

纯 Go SQLite 驱动已锁定 modernc.org/sqlite v1.36.3，支持 Go 1.24，并写入 go.mod/go.sum。已通过 go mod tidy、go mod verify 和 go test，许可见[依赖清单](../DEPENDENCIES.md)。前端使用 Node 22.12+、React 19、TypeScript 5、Vite 7，实际版本以 package-lock.json 为准。

Node 下限依据 [Vite 官方入门说明](https://vite.dev/guide/)，于 2026-10-16 核对。真实 arXiv MCP 所需 Python/uv 环境由 T07 接入检查单独验证，离线模式不需要安装第三方学术服务。

~~~text
main.go
go.mod / go.sum
config.example.json
internal/config/
internal/domain/
internal/app/
internal/transport/httpapi/
internal/infra/sqlite/
internal/infra/artifacts/
internal/infra/models/
internal/infra/mcp/
internal/infra/runner/
internal/infra/connector/
internal/connector/
web/src/ / web/e2e/
testdata/
data/
tasks/
~~~

data 和运行产物不进版本库。Markdown 需求、测试 fixture 与演示配置进版本库。项目根 go.mod 会使父模块测试跳过本项目，必须显式进入本目录验证。

## 2. 作用域和核心对象

领域包不导入 goHarness、HTTP、SQL 或 SSH。ID 使用 string，Scope 是可信应用身份：

~~~go
type Scope struct {
    UserID    string
    ProjectID string
}
type ResearchSettings struct {
    YearFrom, YearTo, CandidateLimit, AnalyzeLimit int
    Providers []string
}
type Project struct {
    ID, OwnerID, Title, Question string
    Field                       string
    Settings                    ResearchSettings
    Archived                    bool
    Revision                    int64
}
type Session struct {
    ID, ProjectID, Title string
    Archived             bool
}
type Turn struct {
    ID, SessionID, State string
    InputText           string
    IdempotencyKey      string
    Revision            int64
    Attempt             int
    ContextJSON         []byte
    DefinitionVersion   string
    CancelRequested     bool
    CanonicalMessageIDs []string
}
type Paper struct {
    ID, ProjectID, SourceKey, Title string
    Authors                        []string
    Year                           int
    DOI, ArxivID, URL, Abstract     string
    Tags                           []string
    Note, ReadingState             string
    Revision                       int64
}
type Candidate struct {
    SourceKey, Title, URL, DOI, ArxivID string
    Authors                           []string
    Year                              int
    Abstract, Provider                string
}
type PaperQuery struct {
    Text, Provider string
    YearFrom, YearTo, Limit int
}
type SourceDocument struct {
    ID, PaperID, ArtifactID, Text, SHA256, Locator string
}
type Evidence struct {
    ID, ProjectID, PaperID, DocumentID string
    Field, Value, Quote, Locator       string
    StartRune, EndRune                 int
}
type Claim struct {
    ID, ProjectID, Text string
    EvidenceIDs         []string
    Kind                string
}
type Direction struct {
    ID, ProjectID, Name, Description, State string
    PaperIDs, Questions                     []string
    Revision                                int64
}
type ResearchSpec struct {
    Question string
    YearFrom, YearTo, CandidateLimit, AnalyzeLimit int
    Providers, SelectedPaperIDs []string
}
type ResearchRun struct {
    ID, ProjectID, SessionID, TurnID, State string
    Spec ResearchSpec
    SelectedPaperIDs, EvidenceIDs, ArtifactIDs []string
    ReviewRevision int64
}
type Artifact struct {
    ID, ProjectID, RunID, Kind, RelPath, SHA256, MIME string
    Bytes int64
}
~~~

Task 类型是 literature_research / paper_reproduction / user_experiment。Session 是长期会话，Turn 是一次用户目标，可跨多个执行段与 Attempt；ResearchRun / ExperimentRun 是业务任务。Checkpoint.RunID 是持久执行身份，公共 Event.Meta.RunID 是一次事件流身份，不能与 TurnID 或 SSE 游标混用。

项目范围必须与拥有者匹配。模型工具参数不接受 Scope；API 从本机登录态 / 本地固定用户生成 Scope。第一版只支持本地单用户；若绑定非回环地址，启动必须要求显式认证配置。阶段 01 固定入口身份为 local-user，非回环使用 AuthTokenEnv 引用的非空 Bearer Token 保护敏感 API；完整多用户登录在阶段 07 设计。

ResearchSettings 保存项目默认研究范围；阶段 04 启动研究时复制为 ResearchSpec，后续修改项目设置不改变已有任务。阶段 01 的默认值为 arxiv、候选数 30、分析数 12、年份不限。具体字段校验见阶段 01 设计。

## 3. 应用接口

下列服务构造函数在相应任务中定义，实现通过依赖注入创建；接口形状为共同基线：

~~~go
// app 包
func (s *ProjectService) Create(ctx context.Context, userID, title string) (domain.Project, error)
func (s *ProjectService) Get(ctx context.Context, scope domain.Scope) (domain.Project, error)
func (s *SessionService) Create(ctx context.Context, scope domain.Scope, title string) (domain.Session, error)
func (s *SessionService) Send(ctx context.Context, scope domain.Scope, sessionID string, in SendInput) (SendResult, error)
type SendInput struct { Text, IdempotencyKey string }
type SendResult struct { Turn domain.Turn; Replayed bool }
type ActionResult struct { Turn domain.Turn; ActionID string; Replayed bool }
func (s *PaperService) Import(ctx context.Context, scope domain.Scope, reference string) (domain.Paper, error)
func (s *PaperService) SaveNote(ctx context.Context, scope domain.Scope, paperID, text string, expectedRevision int64) error
func (s *EvidenceService) Save(ctx context.Context, scope domain.Scope, ev domain.Evidence) error
func (s *DirectionService) Accept(ctx context.Context, scope domain.Scope, directionID string, revision int64) error
func (s *ResearchService) Start(ctx context.Context, scope domain.Scope, sessionID string, spec domain.ResearchSpec) (domain.ResearchRun, error)
func (s *ResearchService) Review(ctx context.Context, scope domain.Scope, runID string, revision int64, selectedIDs []string) error
func (s *ReportService) Generate(ctx context.Context, scope domain.Scope, runID string) (domain.Artifact, error)
~~~

阶段 02 的 Turn/TurnAction/Note/MessageRecord 和 ResumeInput/TurnActionInput 的完整字段、方法见其独立设计与实施文档；以上片段只列跨阶段需要理解的身份与输入，不要求后续任务重复定义类型。

AutoResearch 应用直接使用 pkg/graph 定义“读取上下文→理解/澄清→步骤→关注点/笔记整理/项目问答→产物检查→按意图保存→核对→答复”的业务图，复用 ChatModelNode、ToolsNode、contextengine 和 callback，不使用 agent.New。精确节点/路由和 ResearchState 字段以阶段 02 的 [GRAPH_DESIGN.md](../phases/02-agent-runtime/GRAPH_DESIGN.md) 为准。BuildResearchGraph 返回 `*graph.CompiledGraph[[]schema.Message, []schema.Message]`；首次输入 H+U，输出仅公开 Output。内部 Decision/Draft、答案、有限修复计数和凭据随完整 State 保存，不进入正式对话；Codec ID=autoresearch:research-state、version=1，不实现会话外部化。工具由图确定性构造，各节点仅装配允许工具；模型请求 Functions 为空，不要求原生 ToolCalls/JSONSchema。

单会话活动 Turn 状态为 queued / running / awaiting_input / recovery_required，终态为 completed / failed / cancelled。新输入在 SQLite 原子登记时冲突返回 ErrBusy，HTTP 409。恢复通过 TurnService.Resume，回答使用完整 pending 答案集，保留 Turn ID；不能重发用户输入。重试只允许在没有更新的轮且没有其他活动轮时继续；结束操作不回滚已完成笔记或将来已提交的实验。

输入幂等键在同一用户和会话内唯一；重复相同内容返回原 Turn，不同内容复用键返回 409。

Send 的原子 Admit 事务同时读取项目、笔记和已完成轮正式历史，保存 AdmissionSnapshot、用户消息、轮与初始事件；不能先在事务外读历史再入队。SendResult.Replayed 和动作 ActionResult 的 ActionID/Replayed 由同一幂等事务返回，HTTP 层不另查推断。

回答、重试、结束均带 revision 与独立幂等键，持久保存 TurnAction 后才交 worker。阶段 02 的所有会话运行 API 使用 `/api/projects/{projectID}/sessions/{sessionID}/...`，复用阶段 01 的作用域校验，不维护不带项目的消息/事件别名。

## 4. 存储和框架检查点

完整本地版本的 SQLite 表：schema_migrations、projects、sessions、turns、turn_actions、messages、notes、papers、directions、direction_papers、research_runs、candidates、source_documents、evidence、claims、artifacts、events、checkpoints、checkpoint_heads、server_profiles、experiment_plans、experiment_runs、approvals、submissions、connectors、connector_jobs。

阶段 01 只创建 schema_migrations、projects、sessions；阶段 02 新增 turns、turn_actions、messages、notes、events、checkpoints、checkpoint_heads 七张表。其余表在相应阶段通过新迁移加入，不修改已发布的历史迁移。大文件与索引按实际能力阶段创建。

框架记录单独放 checkpoints、checkpoint_heads。MVP 保存完整检查点，不实现消息前缀去重、TTL 裁剪或自定义迁移。使用现有 Checkpoint.JSON() 和 state.DecodeCheckpoint()，不在应用中猜测 Execution 内部字段。

SQLiteCheckpointStore 必须满足当前实际 state.Store：

~~~go
Get(context.Context, string, string) (state.Checkpoint, bool, error)
List(context.Context, string, state.ListOptions) ([]state.Checkpoint, error)
Load(context.Context, string) (state.Checkpoint, bool, error)
Save(context.Context, state.Checkpoint) (state.Checkpoint, error)
Delete(context.Context, string) error
~~~

Save 在一个事务中校验旧 ID 和 Version，插入历史、更新 head；失配返回 state.ErrConflict。新 ID 用 state.NewID()；删除重建后不能让旧 ID 仅凭相同 Version 写入。调用方和返回值都隔离可变对象。历史完整保留，框架可按父链找回 AbandonRun 回退依据。

阶段 02 的 ConversationID 为 `ar:v1:` 加 `[userID, projectID, sessionID, turnID]` JSON 编码，每轮独立完整检查点；不能把 session UUID 当全局键。后续轮从业务表读取已完成轮的正式历史，不重用失败轮的半段输出作为正式历史。框架派生子键仍由框架管理，存储适配器绑定本轮归属。

检查点与业务记录不是一个事务。启动依次排除业务终态、处理 CancelRequested、补交完成检查点，再恢复进行中检查点；completed 检查点不得覆盖取消意图。编译图 PendingInterrupts 使用原键只读查询：pending 全集与未应用回答动作完全匹配才带 graph.WithResume；无交集或 pending 已空则确认旧动作 applied，恢复不再附旧答案；部分交集或载荷冲突进入 recovery_required。查询/消费/执行在同轮串行，查询失败不能当作空问题。不为查问题启动图，也不解析 Execution 内部结构。无检查点可首次重跑的前提是 DurabilitySync 在首节点前已要求初始落盘，不能只依赖工具去重键。

完成核对排除冻结历史 ID，仅提交本轮用户消息与规范 Output；旧轮内容、TurnID、Seq 不变。Checkpoint.Messages=H+U+Output，Result=Output；内部工作产物和未通过的 ReplyCandidate 不混入。Turn.CanonicalMessageIDs 保存本轮正式顺序。全图最多 8 个已提交完整模型响应、48 节点步数；理解格式修复 1 次、完成追问 2 次、产物修复 1 次、答复修复 1 次，预算恢复不重置。第 8 次后仍可执行无模型节点，请求第 9 次才报 model_budget_exceeded。预算/配对/修复耗尽错误不发成功 result、不可原轮重试；失败/取消消息只展示，不作为后续正式历史。模型请求与节点提交间故障仍可能重发，不承诺计费恰好一次。

数据库建议启用 foreign_keys、busy_timeout、WAL；通过显式事务保护唯一活动 Turn 和审批/提交意图，不依赖进程内锁作为持久一致性保证。

阶段 02 为单服务进程本地版，同一数据目录以 OS 文件锁保护调度器所有权；它不代替 SQLite 请求并发约束。项目删除、项目/会话归档在事务中拒绝活动 Turn。阶段 05 再扩展未结束实验检查。

## 5. 事件、浏览器和长任务

- Agent worker 在后台消费编译图 StreamEvents / ResumeStreamEvents，使用 graph.WithCheckpoint 和同步持久化，不把 worker 生命周期绑定某个 SSE HTTP 请求。
- 消费类型保持 stream.Event[[]schema.Message]；只在 HTTP/存储边界 JSON 编码。
- 数据库存储 EventID（持续递增）、项目/会话/Turn/Attempt、公共流 RunID、Sequence、类型和 JSON。公共流 RunID 与检查点 RunID 分开，持久化成功后浏览器才可读取。
- SSE id 使用数据库 EventID；携带 Last-Event-ID 时从存储续读。框架 Sequence 只在一次框架运行内有意义。
- result 只有一次；错误是独立 problem 事件或失败终止，不能伪造成功 result。
- 框架 delta 在应用边界经 schema.MessageAccumulator 还原，阶段 02 向浏览器发送完整消息快照；前端 reducer 按 EventID 去重、按 message_id 替换，不能重复追加快照文本。游标对外为十进制字符串。
- 每次新 Attempt 的 turn 事件以 replace_turn_messages=true 保留本轮用户消息、清除旧 partial；result 带相同替换标志、canonical_message_ids 与本轮正式消息快照。前端仅替换对应轮，Snapshot 默认排除 superseded，正式顺序按 canonical IDs。
- Snapshot 在同一只读事务返回消息投影与 EventID 高水位，SSE 直接查询该游标之后的数据库记录；本阶段不裁剪事件，不依赖内存发布队列保证不漏事件。
- 工具调用从 schema.Message 的 function-call / function-result 内容及持久化调用记录派生。observe 是诊断通道，不拿可能丢失的异步观测事件充当唯一业务记录。
- 模型节点复用 ChatModelNode 的流与 callback；按 NodeID 过滤 understand_goal/各 draft 的内部 JSON，仅 compose_reply 文本和规范工具消息进入即时消息投影。工具 PostHandler 保留原始 FunctionID/IsError、完整调用/结果对，不包装 ObservedModel。
- 公开进度卡存在 Output 的 schema.Message.Extra["autoresearch.progress"] 中。worker 从已保存 Checkpoint.Messages 提取 allowlist 的 ProgressSnapshot，以 progress SSE 与 snapshot.research_progress 提供范围/步骤/合格产物；不解析 Execution、不从 Values 猜 State。turns 保存进度投影及来源检查点 ID/Version，events 按 turn+source_checkpoint_id 防重；result 也携带最终进度。
- 实验日志是独立带游标的日志对象；activity 通知“有新日志”，按日志 API 拉取。不要伪装成模型 token。
- 用户关闭浏览器只关闭 SSE 订阅，不取消服务器实验；显式 Cancel 另行处理。
- 取消和存储失败先取消执行 Context，再持续 drain 到自然终止后释放轮；固定 SendTimeout=0，不以 Reader.Close 作为执行退出确认。未退出时保留占用并停止接收新目标；优雅退出 10 秒后只能结束整个进程，不在存活 worker 仍可能写入时提前释放数据目录锁。
- 前端 Markdown 禁用原始 HTML执行、限制链接协议；引用使用应用 artifact/paper ID 路由，不信任模型生成的任意路径。

## 6. 学术来源与引用

~~~go
type PaperSource interface {
    Search(context.Context, PaperQuery) ([]Candidate, error)
    Resolve(context.Context, string) (Candidate, error)
    Read(context.Context, string, string) (SourceDocument, error)
}
~~~

arXiv MCP 是第一真实来源，离线模式提供同接口的 fixture。Zotero 后续适配，不是 MVP 先决条件。MCP tools/list/call 由 extensions/mcp 完成；项目桥接工具归一化第三方结果并保存项目对象，不把外部工具结果当成项目存储。

同一个来源导入两次不得创建重复项目论文；跨项目各有独立笔记。SourceKey 规范化 DOI 大小写、arXiv 基础 ID 与版本；文档版本和证据定位保留具体版本。

SourceDocument 的 Text 在归档时归一化换行并冻结，SHA256 按该 UTF-8 文本计算。Evidence 的 StartRune/EndRune 使用 Unicode rune 的半开区间；Quote 必须等于该区间原文。模型不能自报一个不存在的页码；只有摘要时标记 abstract，不声称读取了全文。

Claim 必须引用当前项目真实 Evidence；是否“有引用”和“引用是否支持语义”分别评测。没有证据的建议保存为 inference / unverified，不作为论文事实。

MCP 配置包含 server ID、传输、命令/端点、前缀、允许工具、允许环境变量、超时和版本。应用明确设置 Policy.Strict；零值策略并不限制所有命令和端点。运行中的工具 schema 快照与定义版本固定，变化后重新装配新执行，不静默改变待恢复任务。

## 7. 实验契约

~~~go
type CommandSpec struct { Executable string; Args []string }
type ResourceSpec struct { CPU, MemoryMiB, GPU, WallSeconds int }
type MetricTarget struct {
    Name, Split, Unit, Direction string
    Reference *float64
    AbsTolerance float64
}
type ExperimentPlan struct {
    ID, ProjectID, TaskType, Goal, PaperID string
    ServerID, RepositoryURL, Commit, DatasetVersion string
    WorkspaceRel string
    Commands []CommandSpec
    Resources ResourceSpec
    Expected []MetricTarget
    Revision int64
    Hash string
}
type Approval struct {
    PlanID string
    Revision int64
    Hash, UserID string
    ApprovedAt time.Time
}
type JobRef struct {
    ExperimentID, ServerID, RunnerKind, RemoteID, SubmissionKey string
    WorkspaceRel, StartedFingerprint string
}
type JobStatus struct { State, ExitReason string; ExitCode *int }
type EnvironmentSnapshot struct {
    OS, Python, Scheduler string
    CPU, MemoryMiB, GPUs, FreeDiskMiB int
}
type LogChunk struct { Text, NextCursor string; Truncated bool }
type ExperimentRun struct {
    ID, ProjectID, SessionID, PlanID, State, Comparison string
    PlanRevision int64
    Job JobRef
    ArtifactIDs []string
}
type ServerProfile struct {
    ID, ProjectID, ConnectorID, Alias, WorkspaceRoot, RunnerKind string
}
type SubmitRequest struct {
    RunID, SubmissionKey string
    Plan ExperimentPlan
    Approval Approval
}
type Runner interface {
    Preflight(context.Context, Scope, ServerProfile) (EnvironmentSnapshot, error)
    Submit(context.Context, Scope, SubmitRequest) (JobRef, error)
    Find(context.Context, Scope, string, string) (JobRef, error)
    Status(context.Context, Scope, JobRef) (JobStatus, error)
    Logs(context.Context, Scope, JobRef, string) (LogChunk, error)
    Collect(context.Context, Scope, JobRef) ([]Artifact, error)
    Cancel(context.Context, Scope, JobRef) error
}
~~~

Find 的两个 string 为 serverID、submissionKey。工作区准备作为经过确认的 Submit 内部步骤，不单独暴露任意远程文件操作。应用 ExperimentService 提供：

~~~go
CreatePlan(context.Context, Scope, ExperimentPlan) (ExperimentPlan, error)
Approve(context.Context, Scope, string, int64, string) (Approval, error)
Submit(context.Context, Scope, string, int64, string) (ExperimentRun, error)
~~~

Approve 参数为 planID、revision、hash；Submit 参数为 planID、revision、idempotencyKey。修改计划产生新版本，旧审批不能用于新版本。用户实验 PaperID 可空；论文复现必须有论文和明确比较基准。

执行状态为 planned / awaiting_approval / submitting / queued / running / collecting / completed / failed / cancelled / unknown。Comparison 独立为 matched / partial / not_comparable / not_requested；进程退出 0 仅表示执行成功，不等于论文复现。

提交前先落库意图；SSH Submit 按 SubmissionKey 在远端原子登记。响应丢失先 Find，不能因 HTTP 超时盲目重新训练。RemoteID 对普通进程必须含身份指纹，取消不能只信任可复用 PID。

MVP 服务器为用户可访问的 Linux + OpenSSH + Python；长任务采用经过 preflight 确认的 tmux 或既有调度方式。无可管理的后台执行器时明确拒绝长任务。允许上传短小的受控包装脚本，不部署 AutoResearch 常驻 Runner。

结构化命令和路径检查不构成 OS 沙箱。运行用户代码继承其远程账户权限；不能宣称仅凭命令关键字过滤就能阻止代码读取账户可见文件。需要更强隔离时单独配置低权限账号或实验室已有隔离环境。

## 8. 测试与错误约定

领域错误：ErrNotFound、ErrForbidden、ErrConflict、ErrBusy、ErrInvalid、ErrUnavailable。Scope 不匹配对外统一 404，字段/引用错误 422，活动会话冲突 409，来源不可用 503。

所有 Go 示例是对应任务测试文件中的函数片段，package/import 由文件所在包和上述类型补齐。测试使用真实临时 SQLite、httptest、本地 fixture 和 SDK 的 in-process MCP；不把测试辅助方法放入生产对象。

用户实验 JSON、MCP schema 和包依赖都需版本锁定。离线 E2E 使用固定脚本模型，根据输入消息和工具结果决定响应，不共享全局计数状态；真实模型、MCP 和 SSH 验证分别记录，不相互代替。
