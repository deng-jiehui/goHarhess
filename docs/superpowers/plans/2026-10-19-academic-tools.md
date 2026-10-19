# Academic Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 AutoResearch 项目中接入可追溯的论文搜索、项目归档、冻结文档和证据定位能力。

**Architecture:** 应用直接把论文工具接入现有 Graph 和会话；MCP 只负责来源通信，领域层负责身份、归属、版本和证据校验。SQLite 保存项目元数据，Artifact Store 保存冻结文本，前端只访问项目作用域 API。

**Tech Stack:** Go 1.24、现有 goHarness MCP 扩展、SQLite、React 19、TypeScript 5、Vite 7。

## Global Constraints

- 不修改 goHarness 核心框架，不增加通用 Agent 层。
- 领域代码不导入 HTTP、SQL、MCP 或模型 SDK。
- 所有工具调用由应用注入可信 Scope；模型不能指定项目身份或服务器路径。
- 真实来源失败不得静默转换为空结果；fixture 不代表真实学术服务兼容性。
- 证据 Quote 必须与冻结文档 rune 区间逐字匹配。

---

### Task 1: Academic source adapter

**Files:** `internal/domain/paper_source.go`, `internal/infra/mcp/catalog.go`, `internal/infra/mcp/source.go`, `internal/infra/mcp/normalize.go`, `internal/infra/mcp/source_test.go`, `testdata/mcp/*.json`

- [ ] 写 arXiv identity、版本和无效响应的失败测试。
- [ ] 运行测试，确认归一化和来源适配器尚不存在。
- [ ] 实现 `PaperSource`、`ParseCandidates`、Catalog 状态和工具白名单。
- [ ] 用 in-process MCP fixture 验证 tools/list、搜索和分节读取。
- [ ] 运行 `go test ./internal/domain/... ./internal/infra/mcp/...`。

### Task 2: Project paper library

**Files:** `internal/domain/paper.go`, `internal/domain/paper_identity.go`, `internal/app/papers.go`, `internal/infra/sqlite/migrations/005_papers.sql`, `internal/infra/sqlite/papers.go`, `internal/transport/httpapi/papers.go`, `web/src/pages/PapersPage.tsx`, `web/src/pages/PaperDetailPage.tsx`

- [ ] 写跨项目隔离、重复导入、用户笔记 revision 冲突测试。
- [ ] 添加数据库迁移、仓储和 `PaperService`，保持 Note/Tags 不被重复分析覆盖。
- [ ] 暴露搜索、导入、列表、详情、笔记和阅读状态 API。
- [ ] 添加论文库和详情页面及浏览器回归用例。
- [ ] 运行 Go、TypeScript、构建和论文库 e2e 验证。

### Task 3: Frozen documents and evidence

**Files:** `internal/domain/evidence.go`, `internal/app/evidence.go`, `internal/infra/artifacts/store.go`, `internal/infra/sqlite/evidence.go`, `web/src/components/EvidencePanel.tsx`, `web/src/components/CitationLink.tsx`

- [ ] 写伪造 Quote、越界区间、跨项目和旧版本文档失败测试。
- [ ] 实现文本冻结、SHA-256、Artifact 路径保护和证据校验。
- [ ] 接入项目范围检索和证据读取 API。
- [ ] 添加引用面板和冻结文本跳转用例。
- [ ] 运行阶段三完整验收命令并记录未完成的真实 MCP 联调。
