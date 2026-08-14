# RAG 管道

RAG 实现包含三个独立阶段：文档索引、混合检索和为 Agent 组装引用。

## 分块与索引

```go
chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{
    MaxTokens:     512,
    OverlapTokens: 64,
    MinTokens:     64,
}, modelTokenizer)

indexer, err := retrieval.NewDocumentIndexer(retrieval.IndexerConfig{
    Chunker:             chunker,
    Embedder:            embedder,
    Store:               vectorStore,
    EmbeddingModel:      "text-embedding-model",
    Namespace:           "knowledge",
    EmbeddingBatchSize:  64,
    UpsertBatchSize:     100,
    MaxConcurrency:      4,
})

stats, err := indexer.Invoke(ctx, documents)
```

分块器使用带重叠的滑动 Token 窗口，并优先选择段落、
句子、行和单词边界。它记录稳定的 chunk ID、rune 偏移、Token 数量和来源元数据。若需严格限制，请注入 Embedding 模型的 tokenizer；内置计数器只是近似值。

## 混合检索

```go
searcher, err := retrieval.NewHybridSearcher(retrieval.HybridSearchConfig{
    Embedder:      embedder,
    VectorStore:   cachedVectorStore,
    KeywordSearch: elasticsearch,
    Reranker:      reranker,
    Fusion:        retrieval.FusionRRF,
    VectorTopK:    30,
    KeywordTopK:   30,
    RerankTopK:    20,
    FinalTopK:     8,
    MMRLambda:     0.6,
})
```

关键词检索与查询 Embedding 和向量检索并行执行。
结果按 ID（或文本哈希）去重，然后通过 RRF 或归一化加权融合。MMR 可选，仅在启用时请求向量。Rerank 可选；`StrictRerank` 控制回退行为，
`RequireBoth` 控制一个检索分支失败时是否允许降级为仅使用另一个分支。

`HybridQuery.VectorRaw` carries backend-specific settings such as Qdrant
`hnsw_ef`, Milvus `nprobe`, or equivalent adapter parameters. Branch candidate
当请求更大的最终 TopK 时，分支候选数量会自动增加。

使用同一规范契约执行检索：

```go
hits, err := searcher.Invoke(ctx, retrieval.HybridQuery{Text: query, TopK: 8})
```

## 模型引用上下文

`retrieval.HybridRetriever` 实现 `Runnable[retrieval.ReferenceRequest, string]`。它构建文件过滤器，生成带引用的参考 Block，并按 `ReferenceRequest.MaxTokens` 截断。

使用默认文件过滤器时，文档应包含 `file` 元数据。使用其他元数据结构的应用可以提供自定义
`retrieval.FileFilterBuilder`.
