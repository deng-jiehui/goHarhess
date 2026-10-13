# RAG 端到端示例

演示完整链路：**入库 → 租户作用域混合检索 → Agent 检索工具 → 编号引用**。

```bash
# 全离线（哈希假嵌入器 + 内存存储 + 脚本化模型），直接可跑
go run .

# 接真实 Redis Stack（向量 + 关键词同一后端）
go run . -redis 127.0.0.1:6379

# 再接真实嵌入端点（OpenAI 兼容）
go run . -redis 127.0.0.1:6379 \
  -embed-url http://localhost:8080/v1/embeddings -embed-key $KEY \
  -embed-model bge-m3 -dim 1024
```

## 它演示了什么

| 环节 | 组件 |
|---|---|
| 文档加载 | `retrieval/loader`：text / Markdown（按标题切）/ HTML（剥脚本样式） |
| 入库流水线 | `retrieval.NewIngestor`：分块 → 嵌入 → upsert，稳定块 ID、按批次检查点恢复 |
| 向量/关键词 | `retrieval/redisstore`（Redis Stack）或 `eval.MemoryVectorStore`（离线） |
| 混合检索 | `retrieval.NewHybridSearcher`：向量 + 关键词 RRF 融合 |
| 租户隔离 | `retrieval.Scoped` 钉向量命名空间；关键词经 `_ns` 保留键注入同一租户 |
| 检索工具 | `retrieval.NewSearchTool`：编号引用文本 + 结构化引用块，模型参数里没有租户字段 |
| 模型 | `llm.NewHandlerModel` 脚本化演示：先调 search，再把引用拼进答案 |

末尾的 `other-co` 查询演示跨租户零命中。真实部署中模型换成 `llm` 适配器即可，其余链路不动。

## 注意

- `redisstore` 的客户端必须是 **RESP2**（`redis.Options{Protocol: 2}`）；go-redis 的 FT.* 在 RESP3 下解析有缺陷，FT.SEARCH 会静默返回空结果。
- 哈希假嵌入器（`eval.HashEmbedder`）只演示链路，不代表语义检索质量；接 `-embed-url` 换成真实嵌入。
- 框架的 `Source` 只收字节流，不抓 URL——抓取与富格式解析由应用侧完成（防 SSRF）。
