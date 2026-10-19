# 阶段 03：学术 MCP、论文和证据（任务参考草案）

**使用状态：** 保留 T07–T09 拆解，尚不是本阶段执行基线。阶段 02 验收后，在 phases/03-paper-library/ 单独编写设计和实施文档，并根据实际接口校准本草案。阶段安排见[路线图](../ROADMAP.md)。

> 执行者：使用 executing-plans；不要求子代理。
**Goal:** Agent 能通过学术工具找论文，并把资料与证据真正保存在项目中。
**Architecture:** 应用工具使用来源接口，MCP 是基础设施适配器；项目归属、归档和引用由本应用管理。
**Tech Stack:** extensions/mcp、retrieval、SQLite、文件 Artifact Store。全局约束见[总览](../IMPLEMENTATION_TASKS.md)与[契约](CONTRACTS.md)。

## T07：MCP 连接和第一学术来源

**前置：** T04、T06。  
**需求：** FR-M01–M04、FR-L01。  
**文件：**
- 新增 internal/domain/paper_source.go。
- 新增 internal/infra/mcp/catalog.go、source.go、normalize.go、source_test.go。
- 新增 internal/app/tools/search.go、search_test.go。
- 新增 internal/transport/httpapi/mcp.go。
- 新增 testdata/mcp/arxiv-search.json、arxiv-metadata.json、arxiv-section.json、tools.json。
- 新增 web/src/pages/MCPSettingsPage.tsx。

**接口：** 实现 CONTRACTS.md 的 PaperSource。MCPSourceConfig 指定 Provider、Transport、Command/Args 或 Endpoint、NamePrefix、AllowedTools、AllowedEnvKeys、超时和包版本；Source 归一化函数为 ParseCandidates(provider string, data []byte) ([]domain.Candidate, error)。

**步骤：**
- [ ] 用明确记录的响应字段开始归一化测试；固定 fixture 的 papers 元素使用 id/title/published/summary/authors/url。

~~~go
func TestArxivIdentityIsNormalized(t *testing.T) {
    raw, err := os.ReadFile("../../../testdata/mcp/arxiv-search.json")
    if err != nil { t.Fatal(err) }
    items, err := mcpadapter.ParseCandidates("arxiv", raw)
    if err != nil { t.Fatal(err) }
    if len(items) != 1 || items[0].SourceKey != "arxiv:2401.00001" || items[0].Year != 2024 {
        t.Fatalf("归一化结果 = %+v", items)
    }
}
~~~

fixture 的唯一记录是手写测试样本，id 为 2401.00001v2，published 为 2024-01-02，标题为 Fixture Research；它不代表真实论文。真实服务的返回格式必须另录制，不把测试样本当作已验证兼容性。

- [ ] 用现有 mcpext.NewClient → Tools/Invoke 接入 stdio / Streamable HTTP；固定使用的 MCP 包版本和工具 schema。
- [ ] 第一来源候选为 blazickjp/arxiv-mcp-server。需要的能力是搜索、元数据和有界分节阅读；以实际 tools/list 映射功能，不假定每版本工具名一致。
- [ ] 使用 AllowedTools、NamePrefix 和显式只读判断；stdio 命令、环境变量或 HTTP 端点使用 Strict 策略；密钥只做环境变量引用。
- [ ] 注册受项目范围约束的 search_papers / get_paper_metadata / read_paper_section 工具；第三方结果映射为 Candidate / SourceDocument，不接受模型指定项目 ID。
- [ ] 实现 ProviderCatalog 状态：configured / connecting / ready / unavailable / unsupported；断线重连有退避和上限。
- [ ] 来源错误、格式变化、缺少所需工具返回可解释错误，不静默转为空论文列表。
- [ ] 记录原始来源 ID、内容级别 abstract/full_text、抓取时间、来源包版本和被截断状态。第三方路径不能直接成为前端文件 URL。
- [ ] 用 SDK in-process MCP 验证 schema 发现、工具调用、结果归一化和 Agent 回流；网络异常用 httptest。
- [ ] 真实 arXiv 启动一次 tools/list 和只读查询联调，记录版本/命令/结果；没有网络时标记未联调，离线测试不代替它。
- [ ] 暂不实现 Resources/Prompts 或自动 OAuth 登录；发现服务依赖这些能力时标记 unsupported，另立适配任务。

**验收：** fixture 下 Agent 可以找到一条来源可追溯的候选；真实 arXiv 工具目录与只读检索分别有实测记录。

## T08：项目论文库与整理工具

**前置：** T07。  
**需求：** FR-L02–L05、FR-U04。  
**文件：**
- 新增 internal/domain/paper.go、paper_identity.go、paper_identity_test.go。
- 新增 internal/app/papers.go、papers_test.go。
- 新增 internal/infra/sqlite/papers.go、notes.go。
- 新增 internal/transport/httpapi/papers.go。
- 新增 internal/app/tools/papers.go。
- 新增 web/src/pages/PapersPage.tsx、PaperDetailPage.tsx、web/e2e/papers.spec.ts。

**接口：** PaperService.Import / SaveNote 采用 CONTRACTS.md 签名；domain.NormalizePaperKey(input string) (string, error) 用于保存时去重。保留 arXiv 文档版本，不把版本差异当作重复项目论文。

**步骤：**
- [ ] 先校验 DOI 的大小写与 URL 包装归一化。

~~~go
func TestDOIHasOneProjectIdentity(t *testing.T) {
    for _, raw := range []string{"10.1234/ABC", "https://doi.org/10.1234/abc"} {
        key, err := domain.NormalizePaperKey(raw)
        if err != nil || key != "doi:10.1234/abc" { t.Fatalf("%q -> %q, %v", raw, key, err) }
    }
}
~~~

- [ ] 保存使用 UNIQUE(project_id, source_key)，同一项目重复导入返回已有 ID；不同项目笔记和标签独立。
- [ ] 导入支持 DOI、arXiv ID 和允许来源 URL；解析后调用 PaperSource.Resolve，不抓取任意 URL。
- [ ] 候选搜索结果只有用户或 Agent 调用 save_paper 后才加入论文库；把“不存在”与“来源暂时不可用”区分。
- [ ] 实现论文 list/get/patch、标签/年份/来源/方向筛选、阅读状态和用户笔记；SaveNote 校验 expectedRevision。
- [ ] Agent 分析结果写分析字段，不覆盖用户手写 Note 和 Tags。
- [ ] 前端提供结果表、归档按钮、论文详情、“在会话中讨论”和选中上下文；引用路由用项目论文 ID。
- [ ] 保存原文和下载对象经 Artifact Store 限大小、计算哈希、使用相对路径；不依赖 MCP 进程临时目录长期存在。
- [ ] 通过两项目去重与笔记隔离回归，再运行论文导入/筛选浏览器用例。

**验收：** 两个来源指向同一 DOI 可关联到同一项目论文；笔记不会因再次分析丢失；浏览器可读真实归档资料。

## T09：证据、RAG 和来源定位

**前置：** T08。  
**需求：** FR-L06、FR-R05、FR-U06。  
**文件：**
- 新增 internal/domain/evidence.go、evidence_test.go。
- 新增 internal/app/evidence.go、analysis.go、tools/evidence.go、evidence_test.go。
- 新增 internal/infra/artifacts/store.go、store_test.go。
- 新增 internal/infra/retrieval/index.go、index_test.go。
- 新增 internal/infra/sqlite/evidence.go、sources.go。
- 新增 web/src/components/EvidencePanel.tsx、CitationLink.tsx。
- 新增 testdata/evidence/source.txt、analysis-valid.json、analysis-fabricated.json。

**接口：** SourceDocument / Evidence 见契约；新增 domain.ValidateEvidence(doc SourceDocument, ev Evidence) error，EvidenceService.Save 在持久化前调用它。证据检索接口复用 retrieval.Retriever，查询范围从 Scope 派生。

**步骤：**
- [ ] 构造有合法 paper_id 但伪造 Quote 的用例，证明仅检查 ID 不够。

~~~go
func TestEvidenceMustMatchSource(t *testing.T) {
    doc := domain.SourceDocument{ID: "d1", PaperID: "p1", Text: "测试使用了十个样本。"}
    ev := domain.Evidence{PaperID: "p1", DocumentID: "d1", StartRune: 0, EndRune: 10, Quote: "测试使用了一百万样本。"}
    if err := domain.ValidateEvidence(doc, ev); !errors.Is(err, domain.ErrInvalid) {
        t.Fatalf("伪造片段被接受：%v", err)
    }
}
~~~

- [ ] 先确认错误 Quote 未被拦截时失败；增加合法 Quote、空区间、越界、跨论文、跨项目和旧文档版本测试。
- [ ] 正文以规范化换行的不可变版本保存；计算 UTF-8 SHA256，证据定位按 rune 半开区间，不用字节偏移猜 Unicode 字符位置。
- [ ] 使用现有 loader/chunker 建立项目索引；fixture 可用 HashEmbedder，真实模式可配置 embedding 端点；不得把 fixture 分数当真实语义质量。
- [ ] analyze_paper 提取 problem/method/tools/dataset/metrics/result/limitation；字段缺失记 unknown，不补造结果。
- [ ] JSON-schema 能力未声明时不用强制 llm.WithResponseFormat；解析模型输出后仍按业务 schema 与原文定位校验，失败重试上限 1。
- [ ] search_project_evidence 查询仅包含当前项目，TopK 和引用预算有上限；引用含 PaperID、DocumentID、EvidenceID 和 Locator。
- [ ] UI 点击引用后显示已归档文档片段、来源级别和时间；摘要不能显示成“已读全文”。
- [ ] Artifact Store 拒绝越界相对路径、绝对路径和符号链接逃逸；返回 Artifact ID，不向客户端返回服务器绝对路径。
- [ ] 以人工标注固定语料验证检索排序与引用可定位性，再用真实资料抽查；分别报告结果。

**阶段 C 验证：**

~~~powershell
go test -count=1 ./internal/domain/... ./internal/app/... ./internal/infra/mcp/... ./internal/infra/retrieval/... ./internal/infra/artifacts/...
npm --prefix web run test:e2e -- papers.spec.ts citations.spec.ts
~~~

**建议提交：** T07 feat: connect academic MCP tools；T08 feat: organize project papers；T09 feat: ground research evidence in archived sources。

## P01：Zotero 后续适配

前置 T07–T09；新增 internal/infra/mcp/zotero.go、zotero_test.go 与来源配置页。先以 tools/list 确认库检索、元数据、笔记与附件读取能力，复用 PaperSource 和项目归档。第一步只读；远端写入另经授权和版本确认。记录 Zotero 版本、本地/云端连接方式、凭据引用和真实联调。控制服务部署在云端时，本机 Zotero 必须由用户侧连接器或可达的受保护端点桥接，不能假设云端可读取用户电脑的文件。未安装 Zotero 不阻塞第一版。
