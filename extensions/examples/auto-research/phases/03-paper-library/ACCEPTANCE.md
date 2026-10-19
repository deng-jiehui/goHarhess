# 阶段 03 验收记录：论文库基础闭环

**当前状态：** 阶段三基础闭环已完成。本轮完成论文身份归一化、项目论文归档、去重、笔记更新、HTTP API、论文库页面、离线学术搜索 fixture、来源文本冻结和证据校验。

## 已验证

在 `extensions/examples/auto-research` 执行：

```powershell
go test ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web run build
npm --prefix web test -- --run
```

结果：Go 全量测试、`go vet`、TypeScript 检查、前端构建和 22 项前端单元测试均通过。

## 已交付路径

1. 打开项目后进入“论文库”。
2. 可通过 DOI 或 arXiv ID 手动归档论文。
3. 同一项目重复归档复用原论文 ID，不覆盖原笔记；不同项目互相隔离。
4. 使用“搜索学术来源”可以查询离线 fixture 候选，再明确点击“归档到项目”。
5. 论文详情可以编辑项目笔记，使用 revision 防止旧页面覆盖新内容。
6. 来源文本保存规范化换行和 SHA-256；证据引用必须匹配冻结文本的 rune 区间。

## 尚未完成

- 当前搜索来源是离线 fixture，尚未接入真实 arXiv MCP 的连接配置、tools/list 和只读联调。
- 当前文档文本保存在 SQLite；Artifact Store 大文件落盘和项目内向量检索仍属于后续增强。
- 还没有把论文工具接入 Agent Graph 的动态工具目录；目前可以通过论文库页面验证搜索、归档和证据闭环。

