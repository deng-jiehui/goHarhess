# 阶段 01 实施：本地项目工作台

> 执行者：使用 executing-plans 按复选框实施；不要求子代理。
**状态：** T01–T03 已完成，2026-10-17 验收通过，见[实际验收记录](ACCEPTANCE.md)。

**Goal:** 用户可创建研究项目、保存研究设置和管理会话，刷新或重启后仍能读取。
**Architecture:** 一个项目根目录、独立模块；交互/应用/领域/基础设施四层。
**Tech Stack:** Go 1.24、SQLite、React、TypeScript、Vite。全局约束见[任务总览](../../IMPLEMENTATION_TASKS.md)与[契约](../../tasks/CONTRACTS.md)。

## 实施边界

设计依据为[本阶段设计](DESIGN.md)，阶段位置见[路线图](../../ROADMAP.md)。本文件只覆盖本地工作台，不实现 Agent、SSE、论文检索和远程实验。所有文件路径均相对项目根目录。

- [x] 开始前核对设计、契约、Go 1.24 和 Node 22.12+ 环境；保留工作区其他改动。
- [x] Go 模块固定为 github.com/goharness/autoresearch，两个框架依赖只使用本地 replace。
- [x] 只创建本阶段表；后续表由相应阶段新增迁移。业务依赖和源码只写在本项目内。
- [x] 先运行关键失败用例，再实现对应行为；阶段末集中验证，不运行竞态测试。
- [x] 实现后记录实际结果和限制；设计文档、测试片段不代表已经实现或通过。

## T01：可独立启动的应用

**前置：** 无。  
**文件：**
- 新增 go.mod、go.sum、main.go、config.example.json、.gitignore。
- 新增 internal/config/config.go、config_test.go。
- 新增 internal/infra/sqlite/db.go、internal/infra/sqlite/migrations/001_bootstrap.sql、internal/infra/sqlite/db_test.go。
- 新增 internal/transport/httpapi/health.go、health_test.go、auth.go、auth_test.go。

**接口：**

~~~go
// config 包；ModelMode 为 fixture 或 remote，配置只保存密钥的环境变量名；本阶段不装配模型。
type Config struct {
    Listen, DataDir, ModelMode, ModelEndpoint, ModelName, APIKeyEnv, AuthTokenEnv string
}
func (c Config) Validate() error

// sqlite 包
func Open(ctx context.Context, dataDir string) (*Store, error)
func (s *Store) Close() error
func (s *Store) Ping(ctx context.Context) error
~~~

**步骤：**
- [x] 配置先写拒绝用例：remote 模式缺少端点/模型名、DataDir 为空、非回环监听而未配置认证都报错。

~~~go
func TestRemoteModelNeedsEndpoint(t *testing.T) {
    cfg := config.Config{Listen: "127.0.0.1:8080", DataDir: t.TempDir(), ModelMode: "remote", ModelName: "chat"}
    if err := cfg.Validate(); err == nil {
        t.Fatal("缺少模型端点却允许启动")
    }
}
~~~

- [x] 在实现前运行配置拒绝用例并确认失败；实际回归集中在 config_test.go 的 TestValidateRejectsIncompleteOrExposedConfiguration，以上为单场景示意。
- [x] 写入 CONTRACTS.md 中的两个本地 replace，增加支持 Go 1.24 的纯 Go SQLite 驱动并固定 go.sum。
- [x] Config.Validate 先检查 DataDir，再检查监听地址和 remote 必需字段；remote 配置合法也只保存配置，不发出模型请求。默认本地身份为 local-user；非回环监听必须在敏感 API 上启用 Bearer Token 校验，AuthTokenEnv 引用的变量为空时拒绝启动。
- [x] 打开 data/autoresearch.db；迁移使用 schema_migrations 事务记录版本，重复启动幂等，失败不记录成功版本。T01 只创建 schema_migrations，T02 增加项目和会话表。
- [x] 创建 GET /healthz、GET /readyz。readyz 依赖数据库和应用初始化，模型端点不在每次探活时调用。
- [x] CLI 只实现 serve 入口，未知子命令返回非零；fixture 调研命令留到阶段 04，不启动任何模型、Connector 或外部 MCP。
- [x] 用临时数据库断开再重新打开，验证迁移只执行一次；用数据库路径已被普通文件占用的目录验证启动失败，避免依赖管理员账户的目录写权限判断。
- [x] 阶段内只生成配置样例，不填真实密钥；忽略 data、web/node_modules 和测试浏览器产物。

**验收：** go run . serve --config config.example.json 可启动，探活返回真实状态；无需任何外部服务，重复启动不会重复迁移。数据库不可用时 readyz 返回 503；项目与会话操作由 T02 交付。

## T02：项目与会话数据和 API

**前置：** T01。  
**需求：** FR-P01–P04、FR-S01。  
**文件：**
- 新增 internal/domain/project.go、session.go、scope.go、errors.go、repositories.go。
- 新增 internal/app/projects.go、sessions.go、projects_test.go（集中覆盖项目与会话持久化、归属检查）。
- 新增 internal/infra/sqlite/projects.go、sessions.go、internal/infra/sqlite/migrations/002_projects_sessions.sql；真实 SQLite 作用域回归位于 app 与 httpapi 测试。
- 新增 internal/transport/httpapi/projects.go、sessions.go、router.go、projects_test.go。

**接口：** 实现跨阶段契约中的 Scope / ResearchSettings / Project / Session 和 ProjectService.Create/Get、SessionService.Create；Turn 在阶段 02 实现。构造函数为 NewProjectService(domain.ProjectRepository) 与 NewSessionService(domain.SessionRepository, ...SessionOption)。SessionOption 为 func(*SessionService)，T04 添加执行器选项；本任务 Create 不要求模型。

以下仓储接口定义在 domain 包；ListProjects/ListSessions 返回作用域内所有记录，包括归档项，由应用按 archived 查询条件筛选。删除确认名称和 Revision 必须在同一事务中校验后执行，不能先读名称、事务外再按 ID 删除。

~~~go
type ProjectRepository interface {
    CreateProject(context.Context, string, string) (Project, error)
    GetProject(context.Context, Scope) (Project, error)
    ListProjects(context.Context, string) ([]Project, error)
    UpdateProject(context.Context, Scope, Project, int64) error
    DeleteProject(context.Context, Scope, string, int64) error
}
type SessionRepository interface {
    CreateSession(context.Context, Scope, string) (Session, error)
    GetSession(context.Context, Scope, string) (Session, error)
    ListSessions(context.Context, Scope) ([]Session, error)
    UpdateSession(context.Context, Scope, Session) error
}
~~~

DeleteProject 的 string 为 confirmTitle，int64 为 expectedRevision；仓储事务同时校验归属、当前标题和版本，再删除本阶段的项目与会话记录。

**步骤：**
- [x] 建立真实 SQLite 作用域回归。SQLite Store 实现上述接口；每次查询同时校验 owner 和 project。

~~~go
func TestProjectScopeCannotReadOtherOwner(t *testing.T) {
    ctx := context.Background()
    store, err := sqlite.Open(ctx, t.TempDir())
    if err != nil { t.Fatal(err) }
    defer store.Close()
    svc := app.NewProjectService(store)
    p, err := svc.Create(ctx, "alice", "检索模型")
    if err != nil { t.Fatal(err) }
    _, err = svc.Get(ctx, domain.Scope{UserID: "bob", ProjectID: p.ID})
    if !errors.Is(err, domain.ErrNotFound) { t.Fatalf("越权查询 = %v", err) }
}
~~~

- [x] 在作用域接口实现前运行拒绝用例并确认失败；实现后验证不同 owner 返回 ErrNotFound。
- [x] 按设计 API 表实现项目 create/list/get/patch/archive；PATCH 与 archive 校验 Revision 并返回更新后的 Project，delete 校验 Revision 并返回 204；永久删除必须是显式带确认的独立动作，不默认级联删远程文件。
- [x] 永久删除前确认项目名称和 revision，当前阶段事务内删除归属项目的会话与项目记录；阶段 02、05 新增执行对象时，必须补上活动 Turn 或未结束实验返回 409 的检查；仅删除本产品记录，远程实验文件不作为数据库级联操作处理。
- [x] 实现会话 create/list/get/rename/archive；SessionRepository.GetSession 同时检查 session.ProjectID。
- [x] 实现设计 API 表的全部端点，包括 DELETE 项目、会话 GET/PATCH 与归档筛选；列表返回 items，创建返回 201，删除返回 204。
- [x] HTTP DTO 使用 snake_case；错误为 code/message/request_id，不把 SQL 错误或绝对文件路径原样返回。
- [x] 用户身份固定由可信入口产生；JSON 中的 owner_id 和 user_id 作为未知字段拒绝，不能指定拥有者；未知 JSON 字段、空标题和超长标题返回 422。
- [x] 重开数据库验证项目和会话仍存在；并发携带旧 revision 更新只允许一个成功，另一个 409。
- [x] 按契约和设计补 question、field、settings；默认 arxiv、30 篇候选、12 篇分析、年份不限。校验标题 1–120 字符、研究问题最多 4000 字符、领域最多 120 字符、年份成对且有序、来源标识、候选上限 100、分析上限 30 且不超过候选数。未知嵌套字段同样拒绝，失败不能写入部分设置。归档项目默认从活动列表隐藏。

**验收：** 两个项目和两个用户 fixture 交叉查询无泄露；项目和会话可经 API 持久编辑。两个用户仅为服务与仓储测试身份，不代表实现了多用户登录。

## T03：React 项目工作区

**前置：** T02。  
**需求：** FR-U01–U02、FR-U07。  
**文件：**
- 新增 web/package.json、package-lock.json、tsconfig.json、vite.config.ts、index.html。
- 新增 web/src/main.tsx、App.tsx、api/client.ts、types.ts、styles.css。
- 新增 web/src/pages/ProjectsPage.tsx、WorkspacePage.tsx、SessionPage.tsx。
- 新增 web/src/components/ProjectNav.tsx、ContextPanel.tsx、EmptyState.tsx。
- 新增 web/playwright.config.ts、web/e2e/workspace.spec.ts。

**接口：** 前端 API 客户端映射 T02 JSON；项目详情 /p/:projectId，项目会话 /p/:projectId/s/:sessionId。当前未实现的 Agent 显示明确空状态，不生成模拟回答。

**步骤：**
- [x] 建立浏览器用例，先验证项目/会话持久关系。

~~~typescript
test('创建项目和会话后刷新仍可继续', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: '新建项目' }).click();
  await page.getByLabel('项目名称').fill('检索模型对比');
  await page.getByRole('button', { name: '创建项目', exact: true }).click();
  await page.getByRole('button', { name: '新建会话' }).click();
  await expect(page).toHaveURL(/\/s\/[^/]+$/);
  await page.reload();
  await expect(page.getByRole('heading', { name: '检索模型对比' })).toBeVisible();
  await expect(page.getByRole('navigation', { name: '会话列表' })).toContainText('新会话');
});
~~~

- [x] 先启动 T02 后端，再运行这条浏览器用例；在页面未实现时确认失败。
- [x] 配置 Vite /api 代理，开发只需 Go 和 Node；固定依赖锁文件，禁止浏览器直接访问 MCP 或模型。
- [x] 选择与 Node 22.12+ 兼容的 React 19、TypeScript 5、Vite 7 和测试依赖，用 package-lock.json 锁定。固定 package.json 脚本：dev=vite --host 127.0.0.1、typecheck=tsc --noEmit、test=vitest run、test:e2e=playwright test、build=tsc -b && vite build；浏览器测试由配置启动独立本地后端；使用独立临时数据目录和测试端口，不连接开发者数据，套件结束关闭自建服务。本阶段后端不需要脚本模型。
- [x] 实现左侧项目/会话导航、中间工作区、右侧已选资料上下文；窄屏允许折叠侧栏。
- [x] 实现创建、重命名、归档与恢复、项目设置；永久删除放独立确认入口，要求输入项目名称并提交 revision。所有动作调用真实 API；错误状态保留用户输入；409 提示刷新项目数据后重新确认。
- [x] 路由中的项目与会话不匹配时展示“内容不存在”，不能在前端自动跨项目查找。
- [x] 加载、空结果、请求失败、离线和刷新状态分别显示；Agent 发送入口、论文和任务入口显示未启用状态；不生成模拟回答。
- [x] 用 npm run typecheck、npm run build 和该浏览器用例验收。

## 阶段验证

源码及依赖已实现，以下命令已实际通过，工作目录必须是本项目根目录。根框架的测试不会自动验证这个独立模块，具体结果见[验收记录](ACCEPTANCE.md)。

首次初始化依赖时生成 package-lock.json；交付和复验使用 npm --prefix web ci。Playwright 浏览器首次安装使用 npm --prefix web exec -- playwright install chromium。安装失败记录环境问题，不能用跳过浏览器用例代替通过。

**集中验证：**

~~~powershell
go test -count=1 ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web test
npm --prefix web run build
npm --prefix web run test:e2e -- workspace.spec.ts
~~~

除浏览器刷新用例外，还需通过 API 或集成测试：关闭并重开 SQLite 后读取原数据、错误项目下读取会话返回 404、旧版本更新返回 409、归档恢复、删除名称与版本不匹配被拒绝。浏览器用例覆盖研究设置保存、错误反馈、归档恢复与删除确认，不只检查页面文字。

本目录 ACCEPTANCE.md 已记录 Go/Node 与依赖版本、命令及退出结果、重启演示、当前限制、下一阶段可消费的接口；上述勾选对应本次实际验收。

**建议任务提交：** T01 chore: create standalone autoresearch app；T02 feat: persist research projects and sessions；T03 feat: add project workspace。只提交本任务文件。
