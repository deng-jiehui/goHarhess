# 阶段 06：验收与本地交付（任务参考草案）

**使用状态：** 保留 T17–T18 拆解，尚不是本阶段执行基线。阶段 05 验收后，在 phases/06-local-release/ 单独编写设计和实施文档，并根据实际接口校准本草案。各阶段自行验证本阶段行为，本阶段聚焦跨模块恢复和可复现交付。阶段安排见[路线图](../ROADMAP.md)。

> 执行者：使用 executing-plans；不要求子代理。
**Goal:** 用真实可检查的证据交付学生可运行、可展示的 Agent 项目。
**Architecture:** 验收覆盖会话、项目数据、工具边界、持久执行和远程任务；不以演示截图代替接口行为。
**Tech Stack:** Go test/vet、Playwright、fixture 与单独启用的外部服务测试。全局约束见[总览](../IMPLEMENTATION_TASKS.md)与[契约](CONTRACTS.md)。

## T17：故障恢复与全链路行为验收

**前置：** T16。  
**需求：** FR-P04、FR-R07、FR-U07、FR-X09，以及各阶段完整路径。  
**文件：**
- 新增 integration/research_test.go、agent_restart_test.go、submission_recovery_test.go。
- 新增 web/e2e/full-flow.spec.ts、reconnect.spec.ts。
- 新增 testdata/evaluation/questions.json、relevance.json、expected_sources.json。
- 新增 docs/ACCEPTANCE.md，记录环境、命令和未验证依赖。

**步骤：**
- [ ] 用完整浏览器路径先确认会话、资料与任务关联未丢失。

~~~typescript
test('关闭页面后仍能恢复等待确认的研究任务', async ({ page, context, request }) => {
  const project = await (await request.post('/api/projects', {
    data: { title: '恢复验收项目' }
  })).json();
  const session = await (await request.post('/api/projects/' + project.id + '/sessions', {
    data: { title: '恢复验收会话' }
  })).json();
  await page.goto('/p/' + project.id + '/s/' + session.id);
  await page.getByLabel('消息').fill('调研项目论文中的工具调用方式');
  await page.getByRole('button', { name: '发送', exact: true }).click();
  await expect(page.getByText('等待候选论文确认', { exact: true })).toBeVisible();
  const url = page.url();
  await page.close();
  const next = await context.newPage();
  await next.goto(url);
  await next.getByRole('button', { name: '确认所选论文' }).click();
  await expect(next.getByRole('link', { name: '查看研究报告' })).toBeVisible();
});
~~~

测试通过应用 API 创建自己的项目和会话并使用返回 ID；fixture 只提供固定模型和论文来源响应，不能绕过归属约束。服务使用明确 fixture 配置并绑定随机回环端口，浏览器测试不得连接开发者正在使用的数据目录。

- [ ] Go 集成测试关闭并重开 SQLite 与 Agent，恢复真正的中断检查点；不能只重建页面而把后台对象留在内存。
- [ ] 注入来源超时、MCP 重连、模型截断、检查点写入失败、浏览器慢消费；预期是明确失败/暂停，不是成功 result。
- [ ] 在远端提交成功但本地落 job_id 前中断，重启后按 SubmissionKey 找回；断言运行目录与实际启动记录各一次。
- [ ] 验证旧计划审批、旧租约回传、跨项目/用户 ID、危险 artifact 路径和伪造 Quote 都被拒绝。
- [ ] 用户取消只影响该项目拥有的任务；日志截断、进程 ID 重用和未知状态不能误杀其他任务。
- [ ] 每次模型调用可关联到会话、Turn 和工具；日志/检查点/数据库不出现配置中的哨兵 API Key。
- [ ] 评测使用 10 个固定问题、人工相关性和证据标签；报告 Recall@10（目标 0.80）、结构引用覆盖、原文定位和人工证据支持率。
- [ ] 指标未达目标写实际值与失败案例；用量未知保留 unknown，fixture 成本不能代替真实模型成本。
- [ ] 真实模型、MCP、SSH 各有单独 opt-in 验收；记录版本和测试输入。缺少依赖标记未运行，不伪装通过。

**验收：** 最终演示覆盖项目 → 会话 → 学术工具 → 论文 → 证据 → 研究任务 → 服务器实验 → 结果回流；重启和断线后保持归属与执行一致性。

## T18：构建、启动与作品集交付

**前置：** T17。  
**文件：**
- 新增 scripts/dev.ps1、check.ps1、build.ps1。
- 新增 docs/QUICKSTART.md、DEMO_SCRIPT.md、DEPENDENCIES.md、KNOWN_LIMITATIONS.md。
- 新增 configs/fixture.json、remote.example.json、connector.example.json。
- 修改 main.go、README.md、TECH_STACK.md、IMPLEMENTATION_TASKS.md。
- 新增 release 输出忽略规则；不把真实资料和数据目录打包。

**接口与命令：**

~~~powershell
# 全部命令在 auto-research 项目根执行。
go run . serve --config configs/fixture.json
go run . fixture --question "比较项目论文中的工具调用方式"
go run . connector --server http://127.0.0.1:8080 --pair-code USER_CODE
npm --prefix web ci
npm --prefix web run dev
~~~

USER_CODE 由本机页面生成的一次性配对码替换；它不是文档中的共享密码。remote.example.json 仅保存环境变量名和服务端点样例。

**步骤：**
- [ ] check.ps1 对任一验证失败返回非零，不用后续成功命令掩盖前一项失败。脚本作用域固定为本项目。
- [ ] README 按“离线工作台 → 真实模型 → arXiv MCP → 本机 Connector → 真实 SSH”给出逐项启用步骤。
- [ ] Go 服务托管 web/dist 的构建产物，并对前端路由回退到 index.html；/api 错误不能回退为 HTML。
- [ ] 构建结果包含 serve/connector CLI、前端资源、许可与版本清单，不包含私钥、API Key、本地数据库或实验数据。
- [ ] 使用临时用户数据目录启动构建产物，验证项目 CRUD、同一 Agent 工具循环、报告引用和静态资源。
- [ ] 使用至少一台真实 Linux 实验服务器完成 CPU 演示。只在用户授权的测试目录运行固定代码，保存日志与哈希。
- [ ] 演示脚本解释每一步输入、预期页面、真实工具、可能错误与恢复方法；明确 PDF/OAuth/Slurm 等当前支持范围。
- [ ] 核对已有需求逐项由哪项测试或演示覆盖；后续项 P01/P02 单独列出。
- [ ] 更新任务状态时附实际证据，只有完成验收的任务才勾选。

**最终集中验证：**

~~~powershell
go test -count=1 ./...
go vet ./...
npm --prefix web ci
npm --prefix web run typecheck
npm --prefix web run test -- --run
npm --prefix web run build
npm --prefix web run test:e2e
~~~

不运行 race。本项目使用嵌套模块，父模块/核心框架检查另算；只有修改了框架或公共扩展时才补相应回归，本计划没有授权顺带改核心。

**交付清单：**
- 单个项目根目录中的源代码与锁文件。
- 一键本机启动说明及离线 fixture。
- 学术 MCP 配置、版本和真实接入结果。
- 本机 Connector 与 SSH 实验演示。
- 从结论跳回论文/原文的研究报告。
- 从实验结果跳回计划/commit/命令/数据/指标/产物的记录。
- 测试与评测报告，包括失败、未运行和限制。
- 可复现的前端截图、演示视频脚本与依赖许可。

**建议提交：** T17 test: cover research and experiment recovery；T18 docs: deliver reproducible autoresearch demo。提交、推送、发布分别处理，不能以打包完成代替上线。
