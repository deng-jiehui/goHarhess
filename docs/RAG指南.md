# RAG 指南

完整链路：**加载 → 分块 → 嵌入 → 向量存储 →（关键词）→ 融合 →
（重排）→ 检索工具 → 结构化引用**。所有组件在 `extensions` 模块，
接口是 `runnable.Runnable`，可单独替换。可运行示例：
`extensions/examples/rag`。

## 组件选择

| 环节 | 实现 | 备注 |
| --- | --- | --- |
| 加载器 | `retrieval/loader`：Text / Markdown / HTML | 只收 `io.Reader` 字节流，**不抓 URL**（防 SSRF）；PDF/Office 由应用侧旁路解析 |
| 分块 | `retrieval.NewTextChunker` | 带重叠的 Token 窗口；`TokenCounter` 可换成真实分词器 |
| 嵌入 | `retrieval/embedding` | OpenAI 兼容 HTTP 客户端：分批保序、抖动重试、`Retry-After`、限流器注入 |
| 向量+关键词 | `retrieval/redisstore` | Redis Stack / Redis 8，向量 KNN 与关键词同一后端；**客户端必须 RESP2**（`Options{Protocol: 2}`，RESP3 下 FT.SEARCH 静默返回空结果） |
| 融合 | `retrieval.NewHybridSearcher` | RRF 或加权，可选 MMR 去重 |
| 重排 | `retrieval/rerank` | 通用 HTTP 客户端，字段映射可配（Cohere/Jina/TEI 等） |
| 检索缓存 | `cache/retrieval` | 键含租户，跨租户零命中 |
| 检索工具 | `retrieval.NewSearchTool` | 只读；编号引用文本 + `retrieval.citations` 结构化引用块 |
| 其他向量库 | 自实现四个接口 | pgvector 等见[对接说明](integration.md#其他向量库) |

## 入库与更新

`retrieval.NewIngestor` 把加载到清理包成一次图运行：

```go
ing, _ := retrieval.NewIngestor(retrieval.IngestorConfig{
    Loader: loader.Text(), Chunker: runnable.Func[...](chunker.Invoke),
    Embedder: embedder, Store: retrieval.Scoped(store), // Scoped 钉租户
    EmbeddingBatchSize: 8, MaxConcurrency: 4,
    Checkpoints: state.NewMemoryStore(), // 生产用 Redis store
})
stats, err := ing.Invoke(ctx, retrieval.IngestRequest{ID: docID, Source: src})
```

- **幂等与更新**：块 ID 由文档 ID+内容哈希+序号生成；同版本重复
  入库全部跳过（`Unchanged=true`）。更新先写新块再删旧块，两步之间
  新旧并存，没有检索空窗。
- **恢复**：失败重跑同检查点键，已落盘批次直接复用不重嵌入；失败
  瞬间在途的兄弟批次恢复时重做（至少一次语义）。`MaxConcurrency=1`
  时按序执行、复用最确定。
- **Source 只收字节流**：抓取与富格式解析归应用侧。

## 租户隔离

存储和检索两侧都强制：

- `retrieval.Scoped(store)`：每次调用从 `tenant.FromContext` 取租户写进
  `Namespace`，显式传他人命名空间直接拒绝；向量、关键词（`_ns`
  保留键）、list、delete 全覆盖。
- `NewSearchTool` 的参数面**没有**租户字段——schema 直接拒绝模型
  传入 `namespace`/`tenant`；租户永远来自 ctx。
- 检索缓存键含同一租户，缓存命中路径也无法跨租户。

## 评测

`retrieval/eval`：固定数据集（哈希假嵌入器 + 内存存储，离线确定可跑）
上跑 `Run(searcher, cases, ks)`，产出 Recall@K / NDCG@K / MRR /
延迟 p50/p95；内嵌基线（当前全 1.0），指标低于基线回归测试即失败。
同一 `Run` 可指向真实数据集与真实模型做线下评估。

```go
s, _ := eval.NewFixedSearcher(ctx)
report, _ := eval.Run(ctx, s, eval.FixedCases(), []int{1, 3, 5})
```

## 端到端示例

`go run ./extensions/examples/rag` 默认全离线（哈希嵌入器+内存存储+
脚本化模型）；`-redis`/`-embed-url` 接真实 Redis Stack 与嵌入端点。
