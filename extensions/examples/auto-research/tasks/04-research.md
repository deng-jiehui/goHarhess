# 阶段 04：方向整理与自主调研（任务参考草案）

**使用状态：** 保留 T10–T12 拆解，尚不是本阶段执行基线。阶段 03 验收后，在 phases/04-auto-research/ 单独编写设计和实施文档，包含项目设置快照和 CLI fixture 调研，并根据实际接口校准本草案。阶段安排见[路线图](../ROADMAP.md)。

> 执行者：使用 executing-plans；不要求子代理。
**Goal:** 同一个 Agent 能在会话中整理方向、开展调研并产出可核验报告。
**Architecture:** 研究任务是可持久的业务对象；Agent 自主选工具，固定批处理可封装在工具内部。
**Tech Stack:** Agent / Tool / Checkpoint、项目证据、SQLite、React。全局约束见[总览](../IMPLEMENTATION_TASKS.md)与[契约](CONTRACTS.md)。

## T10：方向提案和用户确认

**前置：** T09。  
**需求：** FR-D01–D04。  
**文件：**
- 新增 internal/domain/direction.go、direction_test.go。
- 新增 internal/app/directions.go、directions_test.go、tools/directions.go。
- 新增 internal/infra/sqlite/directions.go。
- 新增 internal/transport/httpapi/directions.go。
- 新增 web/src/pages/DirectionsPage.tsx、DirectionDetailPage.tsx。

**接口：** DirectionService.Accept(ctx, scope, id, revision) error。领域方法 AcceptDirection(direction Direction, expectedRevision int64) (Direction, error) 只修改当前版本的 proposed 记录。

**步骤：**
- [ ] 先验证用户确认不能意外接受新版本提案。

~~~go
func TestDirectionApprovalBindsRevision(t *testing.T) {
    d := domain.Direction{ID: "d1", ProjectID: "p1", Name: "工具调用", State: "proposed", Revision: 2}
    _, err := domain.AcceptDirection(d, 1)
    if !errors.Is(err, domain.ErrConflict) { t.Fatalf("旧版本确认 = %v", err) }
}
~~~

- [ ] 实现手动创建/编辑/归档、Agent 提案、接受/拒绝；确认前不进入正式方向树。
- [ ] 提案附当前项目真实代表论文、关键问题、待验证点和来源会话；论文不足时显示证据不足。
- [ ] 工具保存 proposed 状态；用户确认使用独立可信 API，不让模型把“用户已同意”作为参数。
- [ ] 方向详情显示关联论文、问题、会话和研究任务；点击“继续研究”向同一 Agent 发送带方向引用的目标。
- [ ] 修改标题/描述保留引用与版本；错误项目 ID 和跨项目 PaperID 返回 404/422。
- [ ] 运行应用版本冲突和前端方向确认用例，确认用户修改不被重新提案覆盖。

**验收：** 会话提出方向后，用户可选择并持续整理，而非生成一段无法管理的文本。

## T11：ResearchRun、候选审核与任务恢复

**前置：** T05、T09、T10。  
**需求：** FR-R01–R04、FR-R07–R09、FR-U05。  
**文件：**
- 新增 internal/domain/research.go、research_test.go。
- 新增 internal/app/research.go、research_test.go、tools/research.go。
- 新增 internal/infra/sqlite/research.go。
- 新增 internal/transport/httpapi/research.go。
- 新增 web/src/pages/ResearchRunsPage.tsx、ResearchRunPage.tsx、CandidateReview.tsx。
- 新增 testdata/research/review.json。

**接口：** ResearchService.Start / Review 见契约；领域方法 ValidateSelection(candidateIDs, selectedIDs []string) error 必须拒绝不在本任务候选集的论文。

**步骤：**
- [ ] 先验证审核不能把另一个任务的论文 ID 注入当前任务。

~~~go
func TestResearchReviewCannotAddForeignCandidate(t *testing.T) {
    err := domain.ValidateSelection([]string{"p1", "p2"}, []string{"p1", "foreign"})
    if !errors.Is(err, domain.ErrInvalid) { t.Fatalf("非法选择 = %v", err) }
}
~~~

- [ ] Start 冻结问题、年份、来源、候选/分析上限、当前选中方向；项目设置后续变化不覆盖该快照。
- [ ] Start 注册 ResearchRun 并向当前会话安排一个 Agent 执行目标；不创建第二种研究 Agent 服务。活动会话已有执行时按 T05 规则排队或明确 409，不能并发覆盖。
- [ ] 工具允许登记检索计划、查询候选、批量筛选、归档、抽取和读取证据；每次写入带 research_run_id 并验证归属。
- [ ] 12 篇分析 / 30 篇候选作为默认上限；Agent 和工具内模型调用各有配额，任务累计预算持久化，不能靠不停启动新 turn 绕过预算。
- [ ] 批量来源与抽取可以在工具内部 Graph 并行，限定并发数；用户研究路径仍由 Agent 选择。
- [ ] 进入候选审核时工具内 graph.Interrupt，保存问题 ID、候选版本、计划哈希。Review 验证选中子集后按阶段 02 的编译图 PendingInterrupts 核对规则，使用 graph.WithResume 和同步检查点恢复。
- [ ] 拒绝论文不进入分析；全部拒绝结束任务并保留原因；修改检索计划产生新审核版本。
- [ ] 恢复复用已提交证据和稳定子任务幂等键，外部读取可能重试；不得宣称一次 HTTP 都不会重发。
- [ ] 任务状态包含 running / awaiting_input / failed / completed / cancelled；界面显示阶段和模型工具过程，不暴露 Go 错误堆栈。
- [ ] 取消停止新分析、保存已有结果；模型/网络超时显示明确可重试状态，而不是把缺失论文丢掉。

**验收：** 用户可从项目或会话创建同一类研究任务，审核、刷新、重启恢复并查看已完成部分。

## T12：证据矩阵、报告和后续追问

**前置：** T11。  
**需求：** FR-R06、FR-U06、FR-S05。  
**文件：**
- 新增 internal/domain/claim.go、claim_test.go。
- 新增 internal/app/reports.go、reports_test.go、tools/reports.go。
- 新增 internal/infra/artifacts/report.go、templates/report.md.tmpl。
- 新增 internal/transport/httpapi/artifacts.go。
- 新增 web/src/pages/ReportPage.tsx、EvidenceMatrix.tsx、web/e2e/research.spec.ts。

**接口：** ReportService.Generate 见契约；ValidateClaim(scope Scope, claim Claim, evidence []Evidence) error 校验当前项目全部引用。报告生成只读取已登记对象。

**步骤：**
- [ ] 先验证“形式上有引用，但引用了别人的证据”不能通过。

~~~go
func TestClaimCannotReferenceOtherProject(t *testing.T) {
    scope := domain.Scope{UserID: "alice", ProjectID: "p1"}
    claim := domain.Claim{ID: "c1", ProjectID: "p1", Text: "测试执行是一项工具", EvidenceIDs: []string{"e1"}, Kind: "finding"}
    evidence := []domain.Evidence{{ID: "e1", ProjectID: "p2", PaperID: "x"}}
    if err := domain.ValidateClaim(scope, claim, evidence); !errors.Is(err, domain.ErrInvalid) {
        t.Fatalf("跨项目引用 = %v", err)
    }
}
~~~

- [ ] 证据矩阵按论文显示 problem/method/tools/dataset/metrics/result/limitation；缺失字段显示未知，不补“常见值”。
- [ ] 综合工具只能引用当前 ResearchRun 的 EvidenceID；保存 finding / inference / unverified，未证实推断单独标记。
- [ ] 模板固定包含问题、方法、搜索时间/来源/检索式、论文范围、证据矩阵、综合、冲突、局限、未验证项和参考文献。
- [ ] 报告持久化为 Artifact，版本不可变；用户重新生成新增版本，引用保留原文档版本。
- [ ] UI 可按引用打开论文与原文位置；Markdown 禁用原始 HTML 和危险链接，不直接渲染模型返回的 script/iframe。
- [ ] 将矩阵/报告作为项目上下文引用回会话，支持“哪些论文使用了测试执行工具？”等追问。
- [ ] 写 E2E：5 篇 fixture → 人工排除 1 篇 → 报告只含纳入论文 → 引用跳到正确片段。
- [ ] 人工抽查引用是否真正支持结论；结构性 100% 引用不等同于语义 100% 正确。

**阶段 D 验证：**

~~~powershell
go test -count=1 ./internal/domain/... ./internal/app/... ./internal/infra/artifacts/... ./internal/transport/httpapi/...
npm --prefix web run test:e2e -- directions.spec.ts research.spec.ts
~~~

**建议提交：** T10 feat: organize research directions；T11 feat: run resumable agent research tasks；T12 feat: generate evidence-grounded reports。
