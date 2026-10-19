# 阶段 05：服务器实验（任务参考草案）

**使用状态：** 保留 T13–T16 拆解，尚不是本阶段执行基线。阶段 04 验收后，在 phases/05-remote-experiments/ 单独编写设计和实施文档，并根据实际接口校准本草案。阶段安排见[路线图](../ROADMAP.md)。

> 执行者：使用 executing-plans；不要求子代理。
**Goal:** 同一个研究 Agent 能把论文复现或用户想法转成可确认、可监控、可比较的实验。
**Architecture:** 应用保存计划/审批/任务台账，Runner 只执行；本机 Connector 通过 SSH 访问服务器。
**Tech Stack:** Go、SQLite、OpenSSH、Linux 已有任务管理器、React。全局约束与类型见[总览](../IMPLEMENTATION_TASKS.md)、[契约](CONTRACTS.md)。

## T13：ServerProfile、实验规格和不可变审批

**前置：** T05。  
**需求：** FR-X01、FR-X03–X05、FR-X11–X12。  
**文件：**
- 新增 internal/domain/experiment.go、approval.go、plan_hash.go、approval_test.go。
- 新增 internal/app/experiments.go、experiments_test.go、tools/plans.go。
- 新增 internal/infra/sqlite/experiments.go、approvals.go、servers.go。
- 新增 internal/transport/httpapi/experiments.go、servers.go。
- 新增 web/src/pages/ExperimentPlanPage.tsx、ServersPage.tsx。

**接口：** 按 CONTRACTS.md 实现 ExperimentService.CreatePlan/Approve/Submit；领域函数 ValidateApproval(plan ExperimentPlan, approval Approval) error、SealPlan(plan ExperimentPlan) (ExperimentPlan, error)。

**步骤：**
- [ ] 确认旧版本审批不能用于新计划。

~~~go
func TestApprovalCannotAuthorizeChangedPlan(t *testing.T) {
    plan := domain.ExperimentPlan{ID: "plan1", ProjectID: "p1", Revision: 2, Hash: "new"}
    approval := domain.Approval{PlanID: "plan1", Revision: 1, Hash: "old", UserID: "alice"}
    if err := domain.ValidateApproval(plan, approval); !errors.Is(err, domain.ErrConflict) {
        t.Fatalf("旧审批结果 = %v", err)
    }
}
~~~

- [ ] ServerProfile 保存 SSH alias、Connector ID 引用、允许工作根目录、资源上限；不保存 SSH 私钥和账号密码。
- [ ] 两种任务模式分别校验：paper_reproduction 必须有本项目论文和参考指标，user_experiment 必须有假设、输入数据和目标指标，PaperID 可空。
- [ ] LLM 只提出候选计划；Go 校验代码/数据位置、模型变体、参数矩阵、运行次数和时限，缺失内容向用户提问。
- [ ] SealPlan 对归一化可执行字段编码并计算哈希，包含 commit、数据版本、环境、命令参数、资源、产物路径、指标和 revision；不把客户端传来的 Hash 当真。
- [ ] 用户确认必须经认证 API，绑定 user/plan/revision/hash；模型参数中的 approved=true 无效。
- [ ] 计划编辑生成新 revision，保留旧版本及审批，UI 显示变更范围和失效原因。
- [ ] 提交入口先验证审批再写 submission 意图；同一业务幂等键只能产生一次远程提交意图。
- [ ] Agent 工具创建计划与等待确认使用既有 graph.Interrupt；普通用户笔记或论文查询不触发实验审批。
- [ ] 计划页显示变量矩阵、命令、数据只读路径、资源和输出，不能只有一个“是否运行”的模糊按钮。

**验收：** 用户可以在同一会话安排两类实验；未确认或改版计划不能执行。此任务不连接真实服务器。

## T14：SSH Runner 与可核对提交

**前置：** T13。  
**需求：** FR-X02、FR-X05–X06、FR-X09–X10。  
**文件：**
- 新增 internal/infra/runner/ssh.go、transport.go、quote.go、ssh_test.go。
- 新增 internal/infra/runner/scripts/launch.sh、status.sh、cancel.sh。
- 新增 internal/app/submissions.go、reconcile.go、submissions_test.go。
- 新增 testdata/experiments/metrics_job.py、fixture_metrics.json。

**接口：** 实现 CONTRACTS.md 的 Runner 全部方法；RemoteTransport 接口为：

~~~go
type ExecResult struct { Stdout, Stderr []byte; ExitCode int }
type RemoteTransport interface {
    Exec(ctx context.Context, server domain.ServerProfile, argv []string, stdin []byte) (ExecResult, error)
}
~~~

SSHTransport 使用 OS OpenSSH 客户端，支持既有 SSH config/ProxyJump/SSH Agent。先实现受控 Linux 执行模式；Slurm 是 P02，不能用“返回一个 job ID”假装已经提交。

**步骤：**
- [ ] 先证明 SSH 远程参数编码不把数据解释成 shell 操作符。

~~~go
func TestRemoteArgumentIsSingleWord(t *testing.T) {
    got, err := runner.QuotePOSIX("data; touch /tmp/unexpected")
    if err != nil { t.Fatal(err) }
    if got != "'data; touch /tmp/unexpected'" { t.Fatalf("参数转义 = %q", got) }
    if _, err := runner.QuotePOSIX("bad\x00value"); err == nil { t.Fatal("接受了 NUL") }
}
~~~

QuotePOSIX 签名为 func(string) (string, error)，单引号使用 POSIX 标准分段编码；拒绝 NUL。单元测试只证明 argv 编码，不能据此宣称用户代码被沙箱隔离。

- [ ] 预检查 OS、Python、CPU/GPU、可用磁盘、目标根目录和已有后台执行器。第一版默认 tmux；不存在时拒绝长任务并提示配置，不自动 sudo 安装。
- [ ] 远程工作区为已授权 root 下的项目/实验子目录；核对真实路径与链接，拒绝越界写、危险产物路径和其他项目 job。
- [ ] 仅执行用户确认的脚本/argv。包装脚本是本应用固定模板，模型输出不能作为模板或原始 shell 插入；用户自己的代码按其账户权限运行。
- [ ] SSH 服务器 host key 使用现有 known_hosts 或显式用户确认；不通过关闭 host key 校验解决连接问题。
- [ ] 提交前落库 intent，远端按 SubmissionKey 原子创建运行目录与 manifest。若记录已存在，返回已有身份而不是第二次运行。
- [ ] 包装脚本记录 started/exit_code/finished/日志路径和身份指纹，任务与 SSH 会话分离。取消只处理匹配该 manifest 的任务。
- [ ] lost-ack 用例：远端创建任务后模拟响应超时；恢复流程调用 Find，必须得到同一个 JobRef，不能再启动训练。
- [ ] 网络错误返回 unknown / unreachable，不能仅凭连接失败标记实验 failed；前端提示“状态待核对”。
- [ ] logs API 带游标、单次大小上限和截断标记；collect 只读取允许路径并保存哈希，不复制整个远程项目和数据集。
- [ ] 单元测试使用记录远端状态的传输 fixture，验证真实业务判定。Linux 包装脚本与真实 SSH 在专用验收服务器单独验证，不能以 fixture 通过代替。

**验收：** 单个已审批的 CPU 演示实验可后台运行、读取状态、取消、找回及收集指标；无本产品服务器常驻进程。

## T15：用户本机 Connector

**前置：** T14。  
**需求：** FR-X13。  
**文件：**
- 新增 internal/connector/client.go、worker.go、client_test.go。
- 新增 internal/domain/connector.go、connector_test.go。
- 新增 internal/app/connectors.go、connector_jobs.go、connector_jobs_test.go。
- 新增 internal/infra/connector/runner.go、runner_test.go。
- 新增 internal/transport/httpapi/connectors.go、connector_jobs.go。
- 新增 SQLite connector/submission 迁移；新增 web/src/pages/ConnectorPage.tsx。
- 修改 main.go 增加 connector 子命令。

**接口：** CLI 为 go run . connector --server https://control.example --pair-code CODE。控制服务提供 register、heartbeat、GET jobs/poll、POST jobs/{id}/ack、POST jobs/{id}/result；作业包含 job/lease/experiment/server/plan-hash 身份。ConnectorRunner 实现同一个 Runner。

**步骤：**
- [ ] 配对与授权先测试错误身份的拒绝行为。

~~~go
func TestConnectorCannotClaimForeignProjectJob(t *testing.T) {
    job := domain.ConnectorJob{ID: "j1", ProjectID: "p1", ConnectorID: "device-a"}
    err := domain.ValidateConnectorAccess(job, "device-b", "p1")
    if !errors.Is(err, domain.ErrForbidden) { t.Fatalf("错误设备领取 = %v", err) }
}
~~~

ConnectorJob 在本任务新增，字段为 ID/ProjectID/ConnectorID/ExperimentID/PlanHash/State/LeaseID；ValidateConnectorAccess(job, connectorID, projectID) error 同时检查设备绑定和项目归属。

- [ ] 一次性配对码只能用一次；服务保存设备凭据哈希，用户电脑在 OS 安全存储或受限本地配置保存凭据；撤销后不能领取新任务。
- [ ] Connector 主动通过 HTTPS（本机开发可回环 HTTP）长轮询，单次最长 30 秒，支持心跳和退避；浏览器不直连 SSH。
- [ ] 作业领取是带租约的原子状态变化；设备通过 authenticated channel 获取已确认的计划引用和 hash，领取时再次核验审批。
- [ ] 回传只接收对应租约和设备的数据；重复 ack/result 幂等，结果超限拒绝，日志不能夹带服务器密钥或环境全量内容。
- [ ] 租约过期不意味着远程实验不存在；重新领取同一实验必须先按 SubmissionKey 找回，不能创建第二次实验。
- [ ] Connector 调用 T14 SSHRunner；它的 SSH Agent 仅做身份认证。服务持有设备授权，不持有用户 SSH 私钥。
- [ ] 控制服务有在线/离线/授权失效状态；UI 展示设备归属、服务器可用性和撤销按钮。
- [ ] 用 httptest 模拟配对、重复领单、掉线、过期租约、补交结果和撤销；再在用户明确授权的实验服务器实测一条 CPU fixture。
- [ ] 可使用同一程序分别启动 serve 和 connector；后端本机部署时也通过接口装配，不把“必须云端部署”作为前置条件。

**验收：** 学生在本机启动 Connector 后可从 Web 发起已审批实验，服务器只需已有 SSH/Python/后台任务管理能力。

## T16：监控、指标与实验结果工作台

**前置：** T09、T12、T15。  
**需求：** FR-X07–X08、FR-X11–X12。  
**文件：**
- 新增 internal/domain/metrics.go、metrics_test.go。
- 新增 internal/app/experiment_monitor.go、experiment_results.go、experiment_results_test.go。
- 新增 internal/infra/artifacts/experiment_report.go。
- 新增 internal/transport/httpapi/experiment_logs.go、experiment_results.go。
- 新增 web/src/pages/ExperimentsPage.tsx、ExperimentRunPage.tsx、MetricComparison.tsx、LogViewer.tsx。
- 新增 web/e2e/experiments.spec.ts。

**接口：** MetricObservation 字段为 Name/Split/Unit（string）、Value（float64）、Seed（int）。CompareMetrics(targets []MetricTarget, observations []MetricObservation) (string, error) 返回契约中的 comparison；任务执行状态另存。

**步骤：**
- [ ] 先验证指标同名但评测划分不同不能误判复现成功。

~~~go
func TestDifferentSplitIsNotComparable(t *testing.T) {
    ref := 0.80
    targets := []domain.MetricTarget{{Name: "recall@10", Split: "test-v1", Unit: "ratio", Reference: &ref, AbsTolerance: 0.02}}
    got, err := domain.CompareMetrics(targets, []domain.MetricObservation{
        {Name: "recall@10", Split: "dev-v1", Unit: "ratio", Value: 0.80, Seed: 1},
    })
    if err != nil || got != "not_comparable" { t.Fatalf("比较 = %q, %v", got, err) }
}
~~~

- [ ] 后台 monitor 定时读取 Runner 状态，去重更新事件和游标；进程/SSH 已结束不代表计划目标完成。
- [ ] 收集 JSON 指标、日志、图表和环境快照；解析 NaN/Inf、缺失指标、错误单位、超限文件和损坏 JSON 时给出原因。
- [ ] 论文比较要求数据集/版本、评测 split、指标方向和单位匹配；缺关键信息为 not_comparable，容差来自计划，不能由 LLM事后放宽。
- [ ] 用户实验无论文参考值时 Comparison=not_requested，仍可完成执行、比较实验组和保存建议。
- [ ] 多组实验保存每组参数/seed/commit/data/env；单 seed 不声称统计显著；有多 seed 时明确汇总规则。
- [ ] 运行状态 completed 和比较 matched/partial 分开。报告映射为复现匹配、部分匹配、无法比较或执行失败。
- [ ] 结果作为项目 Artifact 和会话引用回流，Agent 可解释错误和提出下一步；额外消耗资源的下一组实验仍创建新计划版本。
- [ ] 页面提供状态、计划、日志游标、参数矩阵、指标比较和产物；浏览器刷新或切会话不取消任务。
- [ ] E2E 走 user_experiment 和 paper_reproduction 两种模式，验证同一 Agent 定义与项目资料的衔接。

**阶段 E 验证：**

~~~powershell
go test -count=1 ./internal/domain/... ./internal/app/... ./internal/infra/runner/... ./internal/infra/connector/... ./internal/connector/... ./internal/transport/httpapi/...
npm --prefix web run test:e2e -- experiments.spec.ts
~~~

记录真实 SSH 验收的服务器 OS、Python、执行器、任务 ID、输入和产物哈希。未提供实验服务器时，只能报告离线 / 协议测试通过，保留真实执行待验收状态。

**建议提交：** T13 feat: approve versioned experiment plans；T14 feat: run idempotent SSH experiments；T15 feat: connect local research devices；T16 feat: analyze experiment outcomes。

## P02：集群及服务器侧执行器

前置 T14–T17。新增 internal/infra/runner/slurm.go 和服务器侧执行进程适配器，复用 Runner。Slurm 需要验证真实 sbatch/squeue/sacct/scancel 语义、任务取消归属、提交回包丢失和共享盘产物。服务器执行进程使用主动连接及设备身份，不能假设控制服务能直接进入实验室内网。此项不是第一版 SSH 演示的强制条件。
