# 检索组件（extensions/retrieval）

RAG 实现包含三个独立阶段：文档索引、混合检索和为 Agent 组装引用。
组件实现 `runnable.Runnable` 契约，可直接接入图节点。

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

## 检索与推理 HTTP 服务

`pkg/httpclient.DefaultPool` 是基于 HTTP 的集成默认连接池。
一个 `http.Transport` 会按目标主机自动维护独立连接池，不同超时的客户端共享该 Transport。

```text
DefaultPool / one Transport
  client timeout=0s   -> LLM SSE streams
  client timeout=30s  -> vector and search REST APIs
  client timeout=10s  -> embedding and rerank APIs
```

### 服务示例

Qdrant 等 REST 集成用池化客户端与标准库构造请求：

```go
client := httpclient.NewClient(httpclient.ClientConfig{Timeout: 30 * time.Second})

request, err := http.NewRequestWithContext(ctx, http.MethodPost,
    "https://qdrant.example.com/collections/docs/points/search", body)
if err != nil {
    return err
}
request.Header.Set("Content-Type", "application/json")
request.Header.Set("api-key", os.Getenv("QDRANT_API_KEY"))
response, err := client.Do(request)
```

Pinecone 改用 `Api-Key` 头；Elasticsearch 用 `Authorization: ApiKey …`；
Chroma 或内部 Milvus REST 网关可以省略 API key。托管的 Embedding 和
Rerank API 通常使用默认 Bearer 授权。

适配器直接实现与 Provider 无关的 `runnable.Runnable` 契约：

- 向量数据库：`Runnable[retrieval.VectorRequest, retrieval.VectorResponse]`。
- 搜索引擎：`Runnable[retrieval.TextSearchRequest, []retrieval.SearchHit]`。
- Embedding：`Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse]`。
- Rerank：`Runnable[retrieval.RerankRequest, []retrieval.RerankResult]`。

`VectorRequest.Operation` 选择 query、upsert 或 delete，无需多个执行接口。
HTTP 适配器应使用 `httpclient.NewClient` 获取池化客户端。

### 连接池隔离

当集成需要不同的 mTLS 证书、不同代理、自定义拨号行为或严格流量隔离时，
可以使用独立的 `httpclient.Pool`。仅改变请求超时不需要单独的 Pool。
受控关闭期间，专用 Pool 不再需要时调用 `Pool.CloseIdleConnections`；活动请求不会被中断。

### 向量查询缓存扩展

缓存有意放在框架模块之外。使用 `extensions/cache/retrieval` 中的缓存装饰器：

```go
import retrievalcache "github.com/goharness/goharness/extensions/cache/retrieval"

cache, err := retrievalcache.NewMemoryVectorCache(retrievalcache.MemoryVectorCacheConfig{
    Capacity: 10_000,
})
vectorStore, err := retrievalcache.NewCachedVectorStore(qdrantStore, retrievalcache.VectorCacheConfig{
    Store:   cache,
    StoreID: "qdrant:documents",
    TTL:     time.Minute,
})
```

扩展模块为多实例应用提供 `RedisVectorCache`。精确查询键包含 tenant、Store ID、vector、TopK、filters、namespace 和 include 选项；相同键的并发未命中会合并。

成功调用 `VectorUpsertOperation` 和 `VectorDeleteOperation` 会递增每个 namespace 的数据
版本。新查询会立即看不到旧缓存条目；旧条目通过 TTL 过期，无需扫描 Redis key。
使用 `retrievalcache.WithVectorCacheTenant` 实现 tenant 隔离，使用
`retrievalcache.WithVectorCacheBypass` 处理对新鲜度敏感的读取。
