# AutoResearch：面向计算机专业学生的研究 Agent

这是一个基于 goHarness 的学生项目。它面向计算机专业学生，提供类似代码 Agent 的“项目 + 会话 + 上下文 + 任务”工作区，帮助用户发现、整理、理解、比较和复现论文，并自动形成可复核的研究报告。

AutoResearch 本身就是研究 Agent。论文整理、自动调研、论文复现和用户实验是同一个 Agent 的任务能力；它在会话中理解目标、澄清范围，按研究任务流程检查产物、执行工具并核对结果。项目内部采用交互、应用、领域和基础设施四层实现，应用直接用 goHarness Graph 定义主流程，复用模型/工具节点、观测与恢复。

项目示例问题：

> 2021 至 2026 年公开论文中，LLM Agent 在软件工程代码缺陷修复任务中使用了哪些工具调用方式？常见数据集、评测指标和主要局限是什么？

一次完整调研可以包括：拆解问题 → 生成检索式 → 查询公开论文元数据 → 去重与筛选 → 人工确认候选集 → 获取摘要或开放全文 → 抽取证据 → 跨论文比较 → 生成带引用报告 → 自我检查。用户也可以只要求整理已有论文、解释证据，或继续运行自己的实验。

## 项目形态

这是一个可以作为课程设计、毕业设计或作品集项目完成的前后端项目。第一版提供 Web 工作区和 CLI 离线模式，不做通用互联网爬虫，也不承诺自动阅读付费论文；它只接入 OpenAlex、arXiv 等公开来源，保存来源 ID、URL、抓取时间和原文片段。

阶段 01 本地项目工作台已实现：项目创建、设置、归档与恢复、确认删除，以及会话创建、重命名、归档与恢复均使用真实 API 和 SQLite。刷新或重启服务后可以继续读取。

阶段 02 已接入 Agent 会话：发送目标后由 `pkg/graph` 编排项目上下文、结构化路由、方向整理、笔记写入和答复。业务模型按需调用真实 `ask_user`，问题与可编辑选项由模型生成，回答后继续原业务节点。Turn、消息、事件、笔记和 Graph 检查点保存到 SQLite。工作区提供七个受控文件、Shell 和写入工具，由模型选择工具、由图控制执行策略。默认 `fixture` 模式无需模型密钥，`remote` 模式使用 goHarness 的 OpenAI-compatible 模型适配器。当前协议和测试步骤见 [路由与交互验收](phases/02-agent-runtime/ROUTING_AND_INTERACTION.md)。阶段 03 已完成论文库基础闭环和离线学术搜索 fixture，真实 MCP、证据和引用定位仍在实施。

项目笔记用于保留研究目标、关键结论和实验计划，在同一项目的会话间共享。新建会话或修改项目主题不会自动清空旧笔记；界面会注明笔记来自本轮、本会话历史或其他会话。模型应以当前研究目标为准，仅参考相关历史笔记。

会话底部可展开或打开笔记，每条笔记提供删除按钮，点击确认即可，无需输入名称。删除后不再进入后续新消息的项目笔记上下文，原聊天记录保留；已提交或正在执行的任务仍使用当时的上下文。后台保留删除标记与保存调用身份，避免重试旧轮次时复活笔记。中间会话区独立滚动，标题和输入框保持可见。

先看启动说明，再看阶段文档：

- [阶段路线图](ROADMAP.md)：六个本地交付阶段，以及后续服务端、桌面客户端两个扩展阶段
- [阶段 01 设计](phases/01-workspace/DESIGN.md)：项目、会话、设置、页面、数据和 API
- [阶段 01 实施](phases/01-workspace/IMPLEMENTATION.md)：T01–T03 的文件、步骤和验收命令
- [阶段 01 验收](phases/01-workspace/ACCEPTANCE.md)：实际验证结果、演示路径和当前限制
- [阶段 02 设计](phases/02-agent-runtime/DESIGN.md)：Agent 会话、基础工具、持久执行与断线恢复
- [阶段 02 业务图](phases/02-agent-runtime/GRAPH_DESIGN.md)：关注点、笔记整理、项目问答的节点/路由、产物检查和恢复
- [阶段 02 实施](phases/02-agent-runtime/IMPLEMENTATION.md)：T04–T06 的接口、文件、步骤和验收计划

整体需求与跨阶段参考：

- [研究协议](RESEARCH_PROTOCOL.md)：题目、输入输出、阶段和验收指标
- [产品需求](PRODUCT_REQUIREMENTS.md)：项目、会话、论文库、研究方向、前端和 AutoResearch 功能需求
- [技术栈](TECH_STACK.md)：组件选择与替换边界
- [架构设计](ARCHITECTURE.md)：基于 goHarness 的 Graph、Agent、RAG 和恢复流程
- [实施任务总览](IMPLEMENTATION_TASKS.md)：18 项本地版本任务、依赖与需求映射
- [实现契约](tasks/CONTRACTS.md)：模块、持久化、会话、MCP 与实验执行的接口约定

产品特色是论文复现：用户登记实验室服务器后，Agent 可以生成复现计划、执行环境检查、提交受控实验、收集日志和指标，并把结果回写论文和研究报告。远程命令必须经过计划校验和用户确认。

各阶段分别编写设计和实施文档，再推进开发与验收。阶段 01 已验收，阶段 02 MVP 已实施并通过后端、前端和浏览器验收；阶段 03 正在实施，已完成论文库基础闭环，真实学术 MCP 和证据定位尚未完成。阶段 04 完成调研闭环，阶段 06 完成完整本地交付。

所有实现文件均位于本项目文件夹，后端入口为 `main.go`，前端在 `web/`，分层模块在 `internal/`。本项目是独立 Go 模块，父模块的测试不会包含本项目。

## 本地启动

需要 Go 1.24+、Node.js 22.12+ 和 npm。以下命令均在本项目根目录执行，需要两个终端。

终端一启动后端：

```powershell
go run . serve --config config.example.json
```

终端二安装锁定依赖并启动前端：

```powershell
npm --prefix web ci
npm --prefix web run dev
```

打开 [本地工作区](http://127.0.0.1:5173/)。后端默认监听 `127.0.0.1:8080`，Vite 将 `/api` 代理到后端；`GET /healthz` 与 `GET /readyz` 位于后端地址。当前 Go 服务只提供 API，构建前端不会自动启用静态文件托管；统一启动与打包在阶段 06 实现。

若更改后端端口，在启动前端的终端设置 `$env:AUTORESEARCH_API_URL = 'http://127.0.0.1:新的端口'`。配置中的 `data_dir` 相对于后端工作目录，默认持久数据位于 `data/autoresearch.db`；`data/` 已忽略，不进入版本库。结束前后端进程后，使用原配置重启即可继续访问已有项目。

前端左侧栏底部的“模型设置”可以在 `fixture` 和 `remote` 之间切换。选择 `remote` 后填写 OpenAI-compatible 端点、模型名称和 API Key，保存后新提交的研究轮次立即使用该模型。Key 仅保存在当前浏览器本地存储和后端进程内存中，不写入 SQLite，也不会由状态接口返回；服务重启后需要重新保存一次。

当前固定本地身份为 `local-user`。非回环监听必须配置 `auth_token_env` 并设置对应非空环境变量，API 才接受 Bearer Token；浏览器工作台当前面向默认本机模式，完整登录和远程访问在阶段 07 设计。`fixture` 模式使用离线模型；`remote` 模式启动时创建 OpenAI-compatible 模型并在执行中报告模型错误，不会降级为 fixture。

CLI 目前仅支持 `serve`；`fixture` 调研命令计划于阶段 04 实现，`connector` 计划于阶段 05 实现，当前调用这些命令会明确失败。

## 开发验证

首次运行浏览器测试前安装 Chromium：

```powershell
npm --prefix web exec -- playwright install chromium
```

```powershell
go test -count=1 ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web test
npm --prefix web run build
npm --prefix web run test:e2e -- workspace.spec.ts
```

开发模式下，打开会话后可以点击右上角的“开发运行详情”。面板会实时显示 Graph 节点、模型调用、耗时、Token 用量、检查点和错误事件，数据来自当前会话的事件快照，每 1.5 秒刷新一次。它只保存和展示脱敏的生命周期摘要，不包含 Prompt、模型回复正文或 API Key；生产构建默认隐藏入口，调试时可在地址后加 `?dev=1` 显式打开。

浏览器测试会启动独立的后端和前端，使用临时数据库，不连接开发工作区数据。Windows 下如默认 Go 缓存无写权限，可先设置 `$env:GOCACHE = Join-Path $env:TEMP 'autoresearch-go-cache'`。锁定版本和依赖许可见 [依赖清单](DEPENDENCIES.md)。

## 完整本地版本验收（阶段 01–06）

- 固定问题可以生成稳定的检索计划；
- 公开来源返回的论文可以去重并保留来源信息；
- 每条综合结论至少关联一个 `paper_id` 和证据片段；
- 抓取失败可以从检查点恢复，不重新执行已完成阶段；
- 人工拒绝候选论文后，不进入全文分析；
- 离线 fixture 模式不依赖网络、API Key、Redis 或 Docker。
