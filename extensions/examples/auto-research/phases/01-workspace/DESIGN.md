# 阶段 01 设计：本地项目工作台

**状态：** 已实现，2026-10-17 验收通过，见[验收记录](ACCEPTANCE.md)。  
**前置：** 无。  
**关联任务：** T01–T03。  
**依据：** [阶段路线图](../../ROADMAP.md)、[整体架构](../../ARCHITECTURE.md)、[跨阶段契约](../../tasks/CONTRACTS.md)。

## 1. 本阶段要解决什么

先为研究 Agent 建立可持久使用的项目工作区。用户可以创建研究项目、保存研究问题与范围、管理项目内会话，刷新或重启后继续使用。

演示场景：创建“代码缺陷修复 Agent”项目，研究范围设为 2021–2026 年、arXiv、30 篇候选论文和 12 篇分析论文；创建“方向讨论”和“论文阅读”两个会话，重启后继续查看和编辑。

本阶段交付项目与会话元数据、真实 API 和页面。消息发送与 Agent 执行在阶段 02 接入；论文与上下文资料在阶段 03 接入；本阶段会话区显示明确空状态，发送入口保持未启用。

## 2. 用户流程和页面

1. 用户打开项目列表，创建项目并进入工作区。
2. 编辑项目名称、研究问题、领域、年份、来源和数量限制。
3. 创建会话，在同一项目内切换、重命名和归档。
4. 刷新或关闭服务后重启，按原地址读取持久数据。
5. 归档项目后，从默认活动列表隐藏；用户可以查看归档项目。

| 页面 | 内容 | 本阶段行为 |
| --- | --- | --- |
| `/` | 项目列表、创建入口、归档筛选 | 使用真实项目 API；没有运行任务时不展示虚构计数 |
| `/p/:projectId` | 左侧会话导航、中间项目概览、右侧研究设置 | 展示已保存研究问题与范围 |
| `/p/:projectId/s/:sessionId` | 会话标题、项目导航、上下文空状态 | 校验会话归属；Agent 能力尚未接入 |

加载、无数据、请求失败和服务离线使用不同反馈；保存失败保留表单内容。窄屏允许折叠侧栏。

## 3. 运行方式与职责

```text
浏览器 React 工作区
        ↓ HTTP /api
Go 交互入口 → 应用用例 → 领域对象与仓储接口
                          ↑
                   SQLite 仓储实现
```

| 部分 | 所有者与职责 | 依赖边界 |
| --- | --- | --- |
| `web/src/` | 项目列表、会话导航、表单和请求状态 | 只通过 API 读写项目，浏览器不访问 SQLite |
| `internal/transport/httpapi/` | 路由、DTO、可信身份、错误映射 | 调用应用用例，不直接承载 SQL 与业务规则 |
| `internal/app/` | 创建、修改、归档与删除等用例 | 依赖领域对象和仓储接口 |
| `internal/domain/` | Scope、Project、Session、设置校验、领域错误与仓储接口 | 不导入 HTTP、SQLite、SSH、模型 SDK 或 goHarness |
| `internal/infra/sqlite/` | 表结构、事务、归属查询、持久化 | 实现领域接口 |
| `main.go`、`internal/config/` | 配置校验、组件创建、注入、启动与关闭 | 在入口装配具体实现 |

AutoResearch 仍是一个 Agent 产品，当前先交付承载它的工作区。本阶段不装配执行循环。

## 4. 数据设计

类型沿用[契约第 2 节](../../tasks/CONTRACTS.md)。Project 增加 Field 与 ResearchSettings，设置在阶段 04 创建研究任务时复制为任务快照，修改项目设置不反向修改已有任务。

| 对象 | 字段 | 约束 |
| --- | --- | --- |
| Scope | UserID、ProjectID | 由可信入口产生，不接受 JSON 指定拥有者 |
| Project | ID、OwnerID、Title、Question、Field、Settings、Archived、Revision | ID 不变，Revision 从 1 开始，每次更新递增 |
| ResearchSettings | YearFrom、YearTo、Providers、CandidateLimit、AnalyzeLimit | 年份均为 0 表示不限，或均为正且起始不大于结束；来源去重；0 < 分析数 ≤ 候选数 |
| Session | ID、ProjectID、Title、Archived | 会话必须属于当前项目；本阶段不承载消息执行 |

项目默认来源为 `arxiv`，候选数 30、分析数 12，年份不限。来源设置只保存选择，实际搜索在阶段 03 接入；第一阶段仅接受已知来源标识 `arxiv`。

标题去除两端空白后必须为 1–120 个 Unicode 字符，研究问题最长 4000 个字符、领域最长 120 个字符；年份有效值为 1–9999。候选数最多 100，分析数最多 30，服务端对同一请求整体校验，不保存部分成功字段。

SQLite 位于 `data/autoresearch.db`。本阶段只创建 `schema_migrations`、`projects`、`sessions` 三张表；项目设置可作为 `settings_json` 编码，OwnerID、归档和版本使用独立列。sessions.project_id 为外键，项目更新与删除使用事务及版本条件。

后续消息、任务、检查点与 Connector 表在对应阶段迁移，不能在第一阶段一次创建全部空表。大文件目录预留 `data/artifacts/`，本阶段不产生论文与实验文件。

## 5. API 设计

HTTP JSON 使用 snake_case，列表返回 `{ "items": [] }`，单对象返回对象本身。示例 URL 中的项目与会话 ID 都是 API 创建后返回的真实 ID。

| 方法和路径 | 用途 | 成功结果 |
| --- | --- | --- |
| `GET /healthz` | 进程存活 | 200 |
| `GET /readyz` | 初始化完成且数据库可用 | 200；未就绪为 503 |
| `GET /api/projects?archived=false` | 当前用户的活动项目；true 查询归档项目 | 200 |
| `POST /api/projects` | 创建项目，输入 title，其他字段使用默认值 | 201，返回 Project |
| `GET /api/projects/{id}` | 读取项目 | 200 |
| `PATCH /api/projects/{id}` | 更新 title、question、field、settings；必须携带 revision | 200，返回新 Revision |
| `POST /api/projects/{id}/archive` | 设置 archived；携带 revision，可用于恢复归档 | 200 |
| `DELETE /api/projects/{id}` | 显式永久删除；JSON 携带 confirm_title 和 revision | 204 |
| `GET /api/projects/{id}/sessions?archived=false` | 按项目列出会话 | 200 |
| `POST /api/projects/{id}/sessions` | 创建会话，输入 title | 201，返回 Session |
| `GET /api/projects/{id}/sessions/{sessionId}` | 读取会话 | 200 |
| `PATCH /api/projects/{id}/sessions/{sessionId}` | 重命名或更新 archived | 200 |

本阶段常用操作是归档。永久删除在项目设置中单独展示，要求输入当前项目名称；删除本产品中的项目与会话记录，不操作服务器文件。阶段 02、05 引入活动执行后，删除前必须增加活动 Turn 和未结束实验检查。

更新项目时执行 `WHERE id=? AND owner_id=? AND revision=?`；存在归属但版本不匹配返回 409。所有仓储入口都需要作用域校验，会话不能仅凭 session ID 查询。

错误统一为 `code`、`message`、`request_id`：无权限与不存在统一返回 404；版本冲突 409；非法字段、未知 JSON 字段、无效设置 422；数据库不可用 503；其他内部错误 500。内部日志保留诊断，响应不暴露 SQL、密钥或本机绝对路径。

## 6. 配置、存储与生命周期

- Go 1.24，独立模块 `github.com/goharness/autoresearch`，本地 replace 引用两个框架模块。
- SQLite 使用支持 Go 1.24 的 `modernc.org/sqlite` 版本，初始化时锁定，不引入外部数据库服务。
- 配置包含 Listen、DataDir、ModelMode、ModelEndpoint、ModelName、APIKeyEnv、AuthTokenEnv；默认回环地址和 fixture 模式。模型配置仅校验，不连接或装配模型。
- `APIKeyEnv`、`AuthTokenEnv` 保存环境变量名。非回环监听必须显式配置并启用 Bearer Token 校验，变量未设置或为空则拒绝启动；这只是访问保护，完整多用户登录在阶段 07 设计。
- 身份由入口固定为本地用户 `local-user`；测试可用多个用户验证仓储边界，不宣称本阶段已支持多用户登录。
- 数据库启用 foreign_keys、busy_timeout、WAL。迁移版本与 SQL 在同一事务内提交，重复启动不会重复迁移；失败退出，不自动删除或重建数据。
- 服务退出先停止接受新请求，再关闭数据库。readyz 不请求外部模型或 MCP。

开发模式分别启动 Go 与 Vite，Vite 将 `/api` 请求代理到 Go。阶段 06 再交付前端静态文件托管与统一启动方式。

CLI 本阶段只实现 `serve --config ...`；未知命令明确失败。报告生成的 `fixture` 子命令在阶段 04 实现，本阶段不提供伪造研究结果。

## 7. 向后续阶段提供的接口

| 提供内容 | 消费阶段 | 复用方式 |
| --- | --- | --- |
| Scope 与项目/会话 ID | 02–08 | 后续消息、论文和任务沿用相同归属 |
| ProjectService、SessionService 与仓储接口 | 02–04 | Agent 工具调用与手动操作使用同一应用规则 |
| ResearchSettings | 04 | 启动研究时复制到 ResearchSpec |
| SQLite 迁移入口 | 02–06 | 每阶段增加自己的表，不修改历史迁移 |
| HTTP 客户端封装与工作区路由 | 02–08 | SSE、论文、实验逐步接入；桌面可复用 React 页面 |

未来服务端化主要改变身份来源、部署和存储适配，业务用例不依赖浏览器或桌面壳。本阶段不引入任务队列、微服务、桌面框架或远端执行组件。

## 8. 阶段验收

- [x] 不配置模型、学术服务和 SSH 也可以启动工作区。
- [x] 经真实 API 创建项目、保存设置、创建会话后，刷新和重开数据库仍可读取。
- [x] 项目 A 的路由不能读取项目 B 的会话；不同拥有者不能查询彼此项目。
- [x] 两个请求携带同一旧 Revision 修改项目，仅一个成功，另一个返回 409。
- [x] 归档与恢复正确；永久删除需要名称及版本确认。
- [x] 数据库不可用时 readyz 返回 503；保存失败保留表单，不展示成功。
- [x] 页面明确区分尚未接入的 Agent、论文与实验能力。

任务和命令见[本阶段实施文档](IMPLEMENTATION.md)，实际环境、检查结果和限制见[验收记录](ACCEPTANCE.md)。
