# 阶段 01 验收：本地项目工作台

**日期：** 2026-10-17。  
**结果：** T01–T03 完成；阶段 02 尚未开始。  
**范围：** 独立应用、项目与会话 API、SQLite 持久化、React 工作区和研究设置。

## 1. 实际交付

| 任务 | 已交付行为 | 对应位置 |
| --- | --- | --- |
| T01 | 配置校验、`serve`、SQLite 事务迁移、存活与就绪检查、退出时关闭资源 | main.go、internal/config、internal/infra/sqlite、internal/transport/httpapi |
| T02 | 项目创建与设置、归档恢复、名称和版本确认删除；会话创建、读取、重命名、归档恢复 | internal/domain、internal/app、SQLite 仓储与 HTTP API |
| T03 | 项目列表、三栏工作区、会话导航、研究设置表单、错误与空状态、窄屏导航 | web/src、web/e2e |

设置包含研究问题、领域、起止年份、来源、候选数与分析数。标题按 Unicode 字符计数；非法或未知字段返回 422，失败不会部分保存。项目旧版本更新返回 409；跨项目会话和其他拥有者的项目返回 404。永久删除在同一事务核对归属、名称和版本，并删除本产品内的项目与会话记录。

当前数据库只有 `schema_migrations`、`projects`、`sessions` 三张表；启用外键、WAL 和 busy_timeout。没有提前创建消息、论文、检查点或实验表。

## 2. 验证环境

| 项目 | 实际版本 |
| --- | --- |
| 操作系统 | Windows / amd64 |
| Go | 1.24.4；模块 go 1.24.0 |
| Node.js / npm | 24.19.0 / 11.17.0；项目最低 Node 22.12 |
| SQLite 驱动 | modernc.org/sqlite 1.36.3，纯 Go |
| React / React Router | 19.2.9 / 7.18.4 |
| TypeScript / Vite | 5.9.3 / 7.3.7 |
| Vitest / Playwright | 4.1.11 / 1.64.1 |
| 浏览器 | Playwright Chromium 156.0.8078.4，build 1248 |

Go 使用可写临时缓存。两个框架本地 replace 已保留，阶段 01 尚未导入框架；`go mod tidy` 去掉未使用的框架 require，阶段 02 装配时再加入。依赖版本和许可见[依赖清单](../../DEPENDENCIES.md)。

## 3. 实际检查结果

以下命令在本项目根目录执行，均返回退出码 0；后端整理依赖后以 Go 1.24.4 再次验证。

| 命令 | 结果 |
| --- | --- |
| `go mod tidy` | 完成，锁定实际使用的依赖 |
| `go mod verify` | all modules verified |
| `go test -count=1 ./...` | 6 个包通过 |
| `go vet ./...` | 通过，无诊断 |
| `npm --prefix web run typecheck` | 通过 |
| `npm --prefix web test` | API 客户端 3 条测试通过 |
| `npm --prefix web run build` | 通过，生成 web/dist |
| `npm --prefix web run test:e2e -- workspace.spec.ts` | 6 条浏览器用例通过 |
| `npm --prefix web audit` | 最终锁定依赖 0 vulnerabilities |

Go 回归覆盖真实 SQLite 的重开与迁移幂等、拥有者与项目作用域、并发版本冲突、设置原子校验、确认删除、Bearer 校验、数据库不可用的 readyz 503、非法命令与严格 JSON 解析。未运行竞态测试。

浏览器六条用例覆盖：

1. 创建项目和会话，刷新后仍存在，重命名会话后再次刷新。
2. 保存研究设置并刷新读取；非法设置不会部分写入。
3. 另一窗口更新造成 409 时，保留正在编辑的表单。
4. 项目归档、恢复；输入正确名称后才能确认永久删除。
5. 跨项目会话地址显示不存在；390px 窄屏无横向溢出，导航可展开。
6. 创建请求断线时保留输入，恢复请求后可重试成功。

测试服务使用独立临时数据与端口，不使用开发工作区数据库。Vite 构建中有 React Router 的两条 `use client` 指令被忽略提示，构建正常完成。

## 4. 服务重启与视觉验收

另用独立临时数据库启动真实 Go 可执行程序，通过 HTTP 创建项目、保存研究问题/领域/范围、创建会话；结束服务后以同一配置重新启动，项目设置和会话保持一致。实际数据库表与本阶段设计一致。本机证据记录在 `.cache/verification/restart-result.json`。

桌面检查项目列表、项目概览、会话页与设置窗口；390×844 检查会话页、展开/收起导航与设置窗口。等待侧栏动画结束后，其关闭位置为 x=-248、宽度 248，右缘完全移出视口；设置窗口及保存按钮均在可见区域，无横向滚动，没有浏览器运行错误。本机截图和检查结果位于 `.cache/verification/`，属于忽略的验收产物。

开发预览地址为 [本地工作区](http://127.0.0.1:5173/)，后端位于 `127.0.0.1:8080`。已创建“LLM Agent 工具调用研究（演示）”及“方向讨论”“基线论文阅读”两条会话，供本次体验。演示数据位于开发用 `data/`，与自动化测试数据分开；新用户按 README 启动后可自行创建项目。

## 5. 当前限制与下阶段接口

Agent 发送入口明确禁用，当前没有模型回复、工具调用、SSE、论文检索、MCP 或 SSH 执行。配置中的模型字段只校验，不发起请求；`fixture` 和 `connector` 子命令尚未实现。前端由 Vite 开发服务提供，Go 静态托管和安装包属于阶段 06。

默认身份为单用户 `local-user`。多身份作用域测试验证仓储边界，不代表完整登录或多用户服务已经交付；非回环监听的 Bearer 保护也不能代替阶段 07 的身份与权限设计。

阶段 02 可复用 Scope、Project、Session、ResearchSettings、ProjectService、SessionService、仓储接口、SQLite 迁移入口和工作区路由，接入真实 goHarness Agent、消息持久化与执行事件。继续实施前先编写阶段 02 的独立设计和实施文档。

本次未部署、发布、提交或推送，也没有修改其他项目的实现。
