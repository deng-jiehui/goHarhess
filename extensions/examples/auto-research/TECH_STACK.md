# 技术栈设计

本文列出完整本地版本的技术选择，按[阶段路线图](ROADMAP.md)逐步引入。阶段 01 只使用 Go、SQLite 和 React 工作区所需依赖；Agent、MCP、检索、SSH 与桌面框架不在初始化时一次装配。

## 项目取舍

第一版是一个可复现实验型 Web + CLI 项目，目标是证明研究流程和证据链可靠。它不做 GPU 推理、付费数据库接入和大规模分布式调度。

| 组成 | 技术 | 用途 | 第一版取值 |
| --- | --- | --- | --- |
| 运行时 | Go 1.24 | CLI、HTTP 采集、状态和报告生成 | 项目独立模块，两个本地 replace 引用 goHarness 与 extensions |
| 前端 | React + TypeScript + Vite | 项目工作区、会话、论文库、研究任务和报告展示 | 与 Go API 分离开发，生产时可由 Go 服务托管静态文件 |
| 前端事件 | SSE + `pkg/stream` 公共事件模型 | 展示消息增量、工具调用、阶段进度和中断 | 第一版不引入 WebSocket |
| Agent 产品核心 | 应用直接组合 `pkg/graph`、`pkg/llm`、`pkg/tool` | 理解研究目标、澄清范围、按业务分流、检查产物与核对工具结果 | AutoResearch 主图由应用定义，不依赖内置 agent.New 配方 |
| 执行与恢复机制 | `pkg/graph` + 应用 ResearchState/State Codec | 主图节点与分支执行、检查点恢复及后续工具内批处理 | 每轮独立编译图状态，按会话隔离；直接查询 PendingInterrupts |
| 模型 | `pkg/llm` | 统一 ChatModel、结构化输出、流式响应 | 离线脚本模型；可换 OpenAI-compatible |
| 消息与输出 | `pkg/schema` | 研究消息、工具调用、引用块 | 保留来源 ID 和结构化 citations |
| 公开来源 | 标准库 `net/http` + `pkg/httpclient` | OpenAlex、arXiv 元数据和摘要 | httptest fixture；真实 API 可配置 |
| 远程执行 | `Runner` 接口 + SSH 适配器 | 服务器检查、隔离工作区、任务提交、日志和产物收集 | MVP 使用用户电脑 Connector + SSH Agent；后续支持服务器侧 Runner Agent / Slurm |
| 文档解析 | `extensions/retrieval/loader` | text / Markdown / HTML 字节流解析 | 公开摘要和开放全文先转为文本 |
| 分块与索引 | `extensions/retrieval` | 分块、嵌入、混合检索 | 哈希嵌入器 + 内存存储 |
| 检索工具 | `retrieval.NewSearchTool` | 返回带编号的证据片段 | 只读，参数不包含租户字段 |
| 项目持久化 | SQLite + 纯 Go 驱动 | 保存项目、会话、任务、审批、事件及元数据 | 本地单用户，无需外部数据库服务 |
| 研究文件 | 本地 JSON / Markdown / 原文文件 | 保存来源正文、图表、报告和运行清单 | `data/artifacts/`，业务表保存 ID 和哈希 |
| 检查点 | `pkg/state` + 项目 SQLite Store | 阶段恢复和人工确认 | 完整实现 state.Store，MemoryStore 仅用于短期 fixture |
| 实时事件 | `pkg/stream` + 持久事件表 | 阶段进度、模型增量、挂起通知 | worker 先保存再 SSE 发布，支持游标重连 |
| 学术 MCP | `extensions/mcp` | 搜索、读取学术资料并归档到当前项目 | arXiv 优先；Zotero 只读接入为后续项 |
| 观测 | `pkg/observe` | run、node、llm、tool、checkpoint 事件 | `observe.Slog`；可选 OTel |
| 测试 | Go test + `httptest` | 固定来源、模型和失败恢复测试 | 不依赖外部服务 |

前端第一版只需要项目列表、项目工作区、会话面板、论文库、研究任务审核页和报告阅读页。状态管理优先使用 React 内置状态和少量请求封装，避免为 Demo 引入过多前端基础设施。

## 公开数据源

| 来源 | 用途 | 约束 |
| --- | --- | --- |
| OpenAlex | 论文搜索、作者、年份、DOI、开放获取信息 | 记录查询时间和返回原始 ID |
| arXiv API | 计算机领域预印本元数据和摘要 | 只取公开条目；不绕过访问限制 |
| Crossref | DOI 和出版信息补全 | 作为元数据补充，不作为全文来源 |

应用侧负责请求、限速、重试、缓存和来源合并；`retrieval.Source` 只接收已经取得的字节流，符合框架防 SSRF 的边界。

## 模型策略

AutoResearch 在同一 Agent 会话中按目标使用模型和工具，以下是能力划分：

1. **规划**：把研究问题转换为 3 至 5 个检索式和筛选条件；输出结构化 JSON。
2. **证据抽取**：从单篇论文文本中提取任务、方法、数据集、指标、结果、局限和原文证据。
3. **综合与批评**：只使用已登记的证据对象，生成带引用的结论，并检查引用覆盖和冲突。

规划、抽取、综合是统一 Agent 的能力。应用通过 Graph 规定范围检查、候选审核、证据检查、实验确认等业务顺序与门槛，模型负责对应节点的理解和生成。阶段二先实现关注点、笔记整理、项目问答三条路径，详见 [业务图设计](phases/02-agent-runtime/GRAPH_DESIGN.md)。工具按节点收窄，内部结构化消息不公开；后续局部自主工具选择和嵌套批处理在相应阶段单独验证恢复。

所有任务都经过 callback 发送模型生命周期事件；不增加 `ObservedModel`。离线脚本模型只保证流程确定，不代表研究结论质量。

## 可替换项

| Demo 组件 | 生产或实验替换 |
| --- | --- |
| 脚本模型 | `llm.New` 创建的 OpenAI-compatible、DeepSeek 或 Qwen 模型 |
| `eval.HashEmbedder` | `retrieval/embedding` 的 OpenAI-compatible 嵌入客户端 |
| 内存向量存储 | Redis Stack、pgvector 或其他 VectorStore |
| SQLite 检查点 | 后续可替换 RedisStore；SQLite 已支持本机跨进程恢复 |
| 本地来源缓存 | 对象存储或带版本的数据库 |
| OpenAlex / arXiv 适配器 | Semantic Scholar、CORE 等公开来源适配器 |

## 依赖边界

离线 fixture 模式不需要 Docker、Redis、GPU 或模型 API Key。真实 Agent 运行需要可用的模型端点和相应认证；远程实验使用用户授权的 SSH 连接。更换这些适配器不改变项目、会话、工具和任务对象。

远程实验不把私钥交给模型。控制层只保存 ServerProfile、远程 job ID、工作区和实验产物元数据；命令由结构化 ExperimentPlan 经过策略校验后执行。Runner 是逻辑接口，MVP 不要求在实验室服务器安装常驻服务；本机 Connector 负责调用用户已有的 SSH Agent，后续再增加服务器侧 Runner Agent。

实施决策、精确接口和分阶段验证以 [实施任务](IMPLEMENTATION_TASKS.md)及[契约](tasks/CONTRACTS.md)为准。阶段 01 已交付 Go / SQLite 和 React 工作区，其余组件按阶段引入；阶段 02 已决定直接使用 Graph 编排，主图尚未实施。

阶段 01 锁定 SQLite 驱动 1.36.3、React 19.2.9、React Router 7.18.4、TypeScript 5.9.3、Vite 7.3.7、Vitest 4.1.11 和 Playwright 1.64.1。Go 实际以 1.24.4 验证；精确传递版本由 go.mod / go.sum 和 web/package-lock.json 固定，许可见[依赖清单](DEPENDENCIES.md)。两个框架本地 replace 已保留，实际 require 在阶段 02 导入框架时加入，当前 go.mod 已通过 go mod tidy。
