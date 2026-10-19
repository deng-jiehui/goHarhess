# 阶段 03 实施：论文库与学术工具

> 按阶段三设计逐项实施；每项先写失败用例，再实现最小行为并运行该项测试。

## 全局约束

- 所有修改留在 `extensions/examples/auto-research/`，不修改 goHarness 核心框架。
- 领域包不导入 HTTP、SQL、MCP 或模型 SDK。
- 论文与证据查询必须带可信 `domain.Scope`；不相信模型传入的项目身份。
- MCP 工具使用现有 `extensions/mcp`，并设置 Strict Policy、工具白名单和名称前缀。
- 外部来源结果先归一化；来源失败不能静默转为空结果。
- 运行测试使用独立临时数据库；真实 arXiv 联调单独记录，不把 fixture 作为真实兼容性证明。

## Task 1：来源契约和 arXiv MCP 归一化

**文件：**

- Create: `internal/domain/paper_source.go`
- Create: `internal/infra/mcp/catalog.go`, `source.go`, `normalize.go`
- Create: `internal/infra/mcp/source_test.go`
- Create: `testdata/mcp/arxiv-search.json`, `arxiv-metadata.json`, `arxiv-section.json`, `tools.json`
- Modify: `go.mod` only if the existing local MCP extension is not already imported

**行为：**

- `ParseCandidates("arxiv", data)` 读取固定的 `id/title/published/summary/authors/url` 字段，生成规范 Candidate。
- 统一 arXiv 版本键，保留具体版本供文档 Locator 使用。
- MCP Catalog 根据配置建立连接、初始化、发现工具并按白名单映射调用；保存状态和 schema 版本。
- 对工具缺失、无效 JSON、超时和连接失败返回可分类错误。

**验证：** `go test ./internal/domain/... ./internal/infra/mcp/...`；使用 in-process MCP server 验证 tools/list 和只读调用。

## Task 2：项目论文库和 HTTP/工具

**文件：**

- Create: `internal/domain/paper.go`, `paper_identity.go`, tests
- Create: `internal/app/papers.go`, tests
- Create: `internal/infra/sqlite/migrations/005_papers.sql`, `papers.go`
- Create: `internal/transport/httpapi/papers.go`
- Modify: `internal/domain/repositories.go`, `internal/transport/httpapi/router.go`, `main.go`
- Create: `web/src/pages/PapersPage.tsx`, `PaperDetailPage.tsx`, `web/e2e/papers.spec.ts`
- Modify: `web/src/types.ts`, `web/src/api/client.ts`, `web/src/App.tsx`

**行为：**

- Import 按项目 Scope 和 SourceKey 幂等；重复导入不覆盖用户 Note/Tags。
- List/Get/Patch 和 SaveNote 做归属检查及 revision 校验。
- 搜索结果只有明确保存后才进入项目库。
- 页面展示来源、年份、摘要级别、阅读状态、标签和用户笔记；详情页支持打开文档。

**验证：** 两项目隔离、重复导入、revision 冲突、刷新持久化；Go 单测、TypeScript 检查、浏览器论文库用例。

## Task 3：冻结文档、证据和引用面板

**文件：**

- Create: `internal/domain/evidence.go`, tests
- Create: `internal/app/evidence.go`, `internal/app/analysis.go`, tests
- Create: `internal/infra/artifacts/store.go`, tests
- Create: `internal/infra/sqlite/evidence.go`
- Create: `web/src/components/EvidencePanel.tsx`, `CitationLink.tsx`
- Modify: `internal/transport/httpapi/papers.go`, `web/src/types.ts`

**行为：**

- 文本归一化后计算 UTF-8 SHA-256 并冻结；拒绝绝对路径、路径逃逸和符号链接逃逸。
- Evidence.Save 校验项目、论文、文档、rune 半开区间和逐字 Quote。
- 分析输出缺失字段使用 unknown；不具备原文支持的结论标记 unverified。
- 检索仅使用当前项目文档，引用可从前端打开冻结片段。

**验证：** 伪造 Quote、越界、跨项目和旧版本文档用例；人工标注 fixture 的定位回归；前端引用跳转。

## 阶段验收命令

```powershell
go test -count=1 ./internal/domain/... ./internal/app/... ./internal/infra/mcp/... ./internal/infra/retrieval/... ./internal/infra/artifacts/... ./internal/infra/sqlite/... ./internal/transport/httpapi/...
npm --prefix web run typecheck
npm --prefix web run build
npm --prefix web run test:e2e -- papers.spec.ts citations.spec.ts
```

真实 arXiv MCP 联调需另行记录命令、服务版本、tools/list 结果和只读查询结果；没有外网或服务时，验收报告明确标记未联调。
