# RAG Pipeline

The RAG implementation has three independent stages: document indexing,
hybrid retrieval, and reference assembly for Agents.

## Chunk and index

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

The chunker uses a sliding token window with overlap and prefers paragraph,
sentence, line, and word boundaries. It records stable chunk IDs, rune offsets,
token count, and source metadata. Inject the embedding model's tokenizer for
strict limits; the built-in counter is an approximation.

## Hybrid retrieval

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

Keyword retrieval runs in parallel with query embedding and vector retrieval.
Results are deduplicated by ID (or text hash), then combined with RRF or
normalized weighted fusion. MMR is optional and requests vectors only when it
is enabled. Rerank is optional; `StrictRerank` controls fallback behavior, and
`RequireBoth` controls whether one failed retrieval branch may degrade to the
other branch.

`HybridQuery.VectorRaw` carries backend-specific settings such as Qdrant
`hnsw_ef`, Milvus `nprobe`, or equivalent adapter parameters. Branch candidate
counts automatically grow when a request asks for a larger final TopK.

Run retrieval with the same canonical contract:

```go
hits, err := searcher.Invoke(ctx, retrieval.HybridQuery{Text: query, TopK: 8})
```

## Agent memory

`memory.HybridRetriever` implements
`Runnable[memory.RetrieveRequest, string]`. It builds a file filter, emits citation-bearing reference blocks, and
truncates them to `RAGConfig.MaxRefToken`.

Documents should include a `file` metadata value when the default file filter
is used. Applications with another metadata schema can supply a custom
`memory.FileFilterBuilder`.
