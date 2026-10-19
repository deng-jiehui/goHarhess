# 阶段 03 设计：论文库与学术工具

**状态：** 设计基线，进入实施。
**前置：** 阶段 02 Agent 会话 MVP 已完成。
**目标：** 让同一个 Agent 能搜索公开学术来源、将用户选择的论文归档到项目，并从已归档文本中提供可定位的证据。

## 1. 用户流程

用户在项目会话中提出“找几篇关于 X 的论文”时，Agent 通过学术来源工具搜索候选。候选只作为临时结果展示；用户或 Agent 明确调用保存动作后，论文才进入当前项目的论文库。论文库页面支持筛选、归档状态、标签、阅读状态和用户笔记。

用户打开论文详情时可以查看来源元数据、摘要和已归档文本。若来源只提供摘要，界面明确标记“仅摘要”，不把摘要描述成全文。后续阶段的调研 Agent 只从项目论文库和证据接口读取资料。

## 2. 边界与职责

阶段三新增论文与证据业务能力，仍然只有一个 AutoResearch Agent：

| 层 | 职责 |
| --- | --- |
| Graph/应用 | 将搜索、保存、读取和证据工具接入现有会话图；模型选择工具，应用校验项目归属和参数 |
| 领域 | 规范化论文身份、版本、来源级别、阅读状态、笔记和证据定位规则 |
| 基础设施 | 通过现有 MCP 扩展连接来源；SQLite 保存元数据；Artifact Store 保存冻结文本 |
| 前端 | 搜索结果、论文库、详情、笔记和引用跳转；不显示模型原始 JSON |

MCP 只是来源适配器。第三方工具不能直接写项目表，也不能决定项目 ID、归档策略或证据合法性。第一真实来源是 arXiv MCP；fixture 来源用于离线验收。Zotero、OAuth、MCP Resources/Prompts 不进入本阶段。

## 3. 领域对象

```go
type Paper struct {
    ID, ProjectID, SourceKey, Title string
    Authors []string
    Year int
    DOI, ArxivID, URL, Abstract string
    Tags []string
    Note, ReadingState string
    Revision int64
}

type Candidate struct {
    SourceKey, Title, URL, DOI, ArxivID string
    Authors []string
    Year int
    Abstract, Provider string
}

type SourceDocument struct {
    ID, PaperID, ArtifactID, Text, SHA256, Locator string
}

type Evidence struct {
    ID, ProjectID, PaperID, DocumentID string
    Field, Value, Quote, Locator string
    StartRune, EndRune int
}
```

`SourceKey` 是项目内去重键。DOI 统一为小写 `doi:<value>`；arXiv 统一为不带版本后缀的基础 ID，具体文档仍保留版本信息。原文以规范化换行后的 UTF-8 文本冻结并计算 SHA-256。证据使用 Unicode rune 半开区间，`Quote` 必须与文档区间逐字一致。

## 4. 来源接口与工具

应用依赖以下接口，fixture 和 MCP 来源都实现它：

```go
type PaperSource interface {
    Search(context.Context, PaperQuery) ([]Candidate, error)
    Resolve(context.Context, string) (Candidate, error)
    Read(context.Context, string, string) (SourceDocument, error)
}
```

应用工具固定提供：

- `search_papers`：只读，返回有界候选列表，不写入项目。
- `save_paper`：写入当前项目，按 `SourceKey` 幂等去重。
- `get_paper` / `list_papers`：读取当前项目论文及用户笔记。
- `read_paper`：读取已归档文档，结果包含来源级别和文档版本。
- `save_paper_note`：按 revision 乐观并发更新用户笔记。

模型参数不接受 `user_id`、`project_id` 或任意服务器路径；Scope 从可信应用上下文注入。工具输出限制长度，原文通过 Artifact ID 引用。

## 5. MCP 连接

来源配置包含 provider、transport、command/endpoint、NamePrefix、AllowedTools、AllowedEnvKeys、超时、版本和 Strict Policy。应用启动时不强制连接外部 MCP；第一次使用来源工具时初始化、执行 `tools/list`，按实际 schema 映射搜索、元数据和分节读取能力。

连接状态为 `configured`、`connecting`、`ready`、`unavailable`、`unsupported`。工具缺失、schema 变化、超时和来源格式错误都返回可解释错误，不能静默返回空结果。断线重试有上限，重连后重新获取 schema 快照。

第三方返回先经过 `ParseCandidates(provider, data)` 和文档归一化，再交给领域校验。原始来源 ID、抓取时间、版本和截断状态写入文档元数据。

## 6. 持久化与 HTTP

阶段三新增迁移，不修改已发布迁移：`papers`、`paper_notes`、`source_documents`、`evidence`、`artifacts` 及必要索引。所有查询带 Scope；`UNIQUE(project_id, source_key)` 保证项目内幂等，跨项目保留独立论文和笔记。

HTTP 端点统一挂在项目作用域下：

```text
GET  /api/projects/{projectID}/papers
POST /api/projects/{projectID}/papers
GET  /api/projects/{projectID}/papers/{paperID}
PATCH /api/projects/{projectID}/papers/{paperID}
POST /api/projects/{projectID}/papers/{paperID}/notes
GET  /api/projects/{projectID}/papers/{paperID}/documents
GET  /api/projects/{projectID}/papers/{paperID}/evidence
POST /api/projects/{projectID}/academic/search
```

写入接口使用 revision；导入已存在论文返回已有对象而不是重复插入。来源暂时不可用与论文不存在分别映射为 503 和 404/422。

## 7. 证据规则

证据保存前必须验证：论文、文档和项目归属一致；区间非负且不越界；`Quote` 等于文档 rune 区间；文档哈希和版本匹配。只有摘要时 `Locator` 标记 `abstract`。模型没有找到原文支持的内容保存为 `inference` 或 `unverified`，不能作为论文事实。

检索范围由 Scope 派生，只查询当前项目已归档文本。TopK、片段长度和引用数量有上限。引用返回 PaperID、DocumentID、EvidenceID 和 Locator，前端通过应用路由打开冻结片段，不暴露服务器绝对路径。

## 8. 验收场景

1. fixture 来源搜索返回一条带 `arxiv:2401.00001` 的候选，候选不自动进入论文库。
2. 同一项目重复保存同一 DOI/arXiv ID 返回同一个 Paper ID；两个项目各自有独立笔记。
3. 论文详情能区分摘要和全文，来源 ID、版本、抓取时间可追溯。
4. 合法证据可保存并在冻结文本中定位；伪造 Quote、跨项目文档、越界区间被拒绝。
5. MCP 缺工具、超时或格式变化显示明确错误；不会伪造空论文列表。
6. 浏览器完成搜索、归档、筛选、编辑笔记、打开证据片段；刷新后数据仍在。

