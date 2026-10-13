// RAG 端到端示例：入库 → 租户作用域混合检索 → Agent 检索工具 → 编号引用。
//
// 默认全离线可跑：哈希嵌入器 + 内存向量/关键词存储 + 脚本化模型，
// 直接 `go run .` 即可看到完整链路（入库、检索、工具、引用、租户隔离）。
//
// 接真实后端：
//
//	-redis 127.0.0.1:6379        Redis Stack 向量与关键词检索
//	-embed-url http://.../embeddings -embed-key k -embed-model m -dim 1024
//
// 嵌入端点缺省时仍用哈希嵌入器（仅演示，不代表语义质量）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/extensions/retrieval/embedding"
	"github.com/goharness/goharness/extensions/retrieval/eval"
	"github.com/goharness/goharness/extensions/retrieval/loader"
	"github.com/goharness/goharness/extensions/retrieval/redisstore"
	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

func main() {
	redisAddr := flag.String("redis", "", "Redis Stack 地址（默认内存存储）")
	embedURL := flag.String("embed-url", "", "OpenAI 兼容嵌入端点（默认哈希假嵌入器）")
	embedKey := flag.String("embed-key", "", "嵌入服务 API key")
	embedModel := flag.String("embed-model", "demo", "嵌入模型名")
	dim := flag.Int("dim", 64, "向量维度（须与索引/嵌入一致）")
	tenantID := flag.String("tenant", "acme", "演示租户")
	question := flag.String("query", "goHarness 的租户检索是怎么工作的？", "提问")
	flag.Parse()

	ctx := context.Background()

	// ---- 嵌入器 ----
	var embedder runnable.Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse]
	if *embedURL != "" {
		var err error
		embedder, err = embedding.New(embedding.Config{
			Endpoint: *embedURL, APIKey: *embedKey, Model: *embedModel,
			Dimensions: *dim, BatchSize: 32, Timeout: 30 * time.Second,
		})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		embedder = eval.HashEmbedder{Dim: *dim}
		log.Print("未提供 -embed-url，使用哈希假嵌入器（仅演示链路）")
	}

	// ---- 向量与关键词后端 ----
	var (
		vectorStore  runnable.Runnable[retrieval.VectorRequest, retrieval.VectorResponse]
		keywordStore runnable.Runnable[retrieval.TextSearchRequest, []retrieval.SearchHit]
	)
	if *redisAddr != "" {
		store, err := redisstore.New(redisstore.Config{
			// redisstore 要求 RESP2 客户端：go-redis 的 FT.* 在
			// RESP3 下解析有缺陷，Options.Protocol: 2 是已验证路径。
			Client: redis.NewClient(&redis.Options{Addr: *redisAddr, Protocol: 2}),
			Index:  "ragdemo", Prefix: "ragdemo/",
			Dim: *dim, Distance: redisstore.Cosine,
			// ingestor 的版本/文档键与演示的 kind 元数据须声明才可过滤。
			Metadata: []redisstore.MetadataField{
				{Name: "_rag_document_id", Type: redisstore.FieldTag},
				{Name: "_rag_version", Type: redisstore.FieldTag},
				{Name: "kind", Type: redisstore.FieldTag},
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := store.EnsureIndex(ctx); err != nil {
			log.Fatal(err)
		}
		vectorStore, keywordStore = store.VectorStore(), store.KeywordSearch()
		log.Printf("向量与关键词后端：Redis Stack %s", *redisAddr)
	} else {
		mem := &eval.MemoryVectorStore{}
		vectorStore, keywordStore = mem, eval.MemoryKeywordSearch{Store: mem}
		log.Print("后端：内存存储（-redis 可切 Redis Stack）")
	}

	// 租户钉死：向量走 Scoped（强制 namespace=ctx 租户），关键词经
	// 保留键 _ns 注入同一租户——两条检索路径都不可越界。
	scopedStore := retrieval.Scoped(vectorStore)
	scopedKeyword := runnable.Func[retrieval.TextSearchRequest, []retrieval.SearchHit](
		func(ctx context.Context, req retrieval.TextSearchRequest, o ...runnable.Option) ([]retrieval.SearchHit, error) {
			if scope, ok := tenant.FromContext(ctx); ok {
				if req.Filters == nil {
					req.Filters = retrieval.Metadata{}
				}
				req.Filters["_ns"] = scope.TenantID
			}
			return keywordStore.Invoke(ctx, req, o...)
		})

	searcher, err := retrieval.NewHybridSearcher(retrieval.HybridSearchConfig{
		Embedder: embedder, VectorStore: scopedStore, KeywordSearch: scopedKeyword,
		EmbeddingModel: *embedModel, VectorTopK: 5, KeywordTopK: 5, FinalTopK: 5,
	})
	if err != nil {
		log.Fatal(err)
	}

	// ---- 入库：loader → chunker → embed → upsert（检查点可恢复） ----
	chunker, err := retrieval.NewTextChunker(retrieval.ChunkConfig{MaxTokens: 80, OverlapTokens: 10}, nil)
	if err != nil {
		log.Fatal(err)
	}
	ingestor, err := retrieval.NewIngestor(retrieval.IngestorConfig{
		Loader:   loaderByMIME(),
		Chunker:  runnable.Func[retrieval.Document, []retrieval.Chunk](chunker.Invoke),
		Embedder: embedder, Store: scopedStore,
		EmbeddingModel: *embedModel, EmbeddingDimensions: *dim,
		EmbeddingBatchSize: 8, Checkpoints: state.NewMemoryStore(),
	})
	if err != nil {
		log.Fatal(err)
	}

	// 租户 acme 入库三篇文档。
	acme, err := tenant.With(ctx, tool.Subject{TenantID: *tenantID, ID: "demo-user"})
	if err != nil {
		log.Fatal(err)
	}
	for _, doc := range demoDocs {
		stats, err := ingestor.Invoke(acme, retrieval.IngestRequest{ID: doc.id, Source: doc.source()})
		if err != nil {
			log.Fatalf("入库 %s 失败: %v", doc.id, err)
		}
		fmt.Printf("入库 %-14s 文档%d 块%d 写入%d 清理%d 未变%v\n",
			doc.id, stats.Documents, stats.Chunks, stats.Upserted, stats.Removed, stats.Unchanged)
	}

	// ---- 检索工具 + Agent ----
	searchTool, err := retrieval.NewSearchTool(searcher, retrieval.SearchToolConfig{TopK: 3})
	if err != nil {
		log.Fatal(err)
	}
	assistant, err := agent.New(ragModel(), agent.WithName("rag-demo"), agent.WithTools(searchTool))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n=== 租户 %s 提问：%s ===\n", *tenantID, *question)
	out, err := assistant.Invoke(acme, []schema.Message{schema.UserMessage(*question)})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out[len(out)-1].JoinedText("\n"))

	// ---- 租户隔离演示：别的租户查不到 acme 的文档 ----
	other, _ := tenant.With(ctx, tool.Subject{TenantID: "other-co", ID: "demo-user"})
	hits, err := searcher.Invoke(other, retrieval.HybridQuery{Text: *question, TopK: 3})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== 租户 other-co 同查询命中数：%d（应为 0，数据按租户隔离）===\n", len(hits))
}

// demoDoc 是示例文档。
type demoDoc struct {
	id   string
	mime string
	body string
}

func (d demoDoc) source() retrieval.Source {
	return retrieval.Source{
		Name: d.id, MIME: d.mime, Body: strings.NewReader(d.body),
		Metadata: retrieval.Metadata{"kind": "doc"},
	}
}

var demoDocs = []demoDoc{
	{"guide-md", "text/markdown", "# 租户检索\n\ngoHarness 的检索按租户隔离：向量命名空间与关键词过滤\n都来自请求的租户作用域，模型不能指定查谁的数据。\n\n## 入库\n\n文档经加载器切分、嵌入后写入向量库，检查点支持按批次恢复。"},
	{"guide-txt", "text/plain", "goHarness 的检索工具返回编号引用文本，并附结构化引用块，\n前端据此渲染来源。空结果返回明确文案，不让模型编造。"},
	{"guide-html", "text/html", "<html><body><h1>重排</h1><p>检索可接 HTTP 重排服务，结果按分数降序并保持原文档序号。</p><script>bad()</script></body></html>"},
}

// loaderByMIME 按 Source.MIME 选加载器；框架只收字节流，不抓 URL。
func loaderByMIME() loader.Loader {
	text, md, html := loader.Text(), loader.Markdown(), loader.HTML()
	return runnable.Func[retrieval.Source, []retrieval.Document](
		func(ctx context.Context, s retrieval.Source, o ...runnable.Option) ([]retrieval.Document, error) {
			switch {
			case strings.HasPrefix(s.MIME, "text/markdown"):
				return md.Invoke(ctx, s, o...)
			case strings.HasPrefix(s.MIME, "text/html"):
				return html.Invoke(ctx, s, o...)
			default:
				return text.Invoke(ctx, s, o...)
			}
		})
}

// ragModel 是脚本化演示模型：第一轮把用户问题作为检索工具调用发出，
// 第二轮（收到工具结果后）把编号引用文本拼进最终答案。
func ragModel() llm.ChatModel {
	var calls int
	return llm.NewHandlerModel(func(ctx context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		calls++
		last := req.Messages[len(req.Messages)-1]
		if calls == 1 && last.Role != schema.RoleTool {
			args, _ := json.Marshal(map[string]any{"query": last.JoinedText(" "), "top_k": 3})
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("rag-1", "search", string(args)))}, nil
		}
		// 第二轮：工具结果正文在 FunctionToolResult.ContentBlocks。
		var answer strings.Builder
		answer.WriteString("根据检索到的内容：\n")
		for _, r := range last.FunctionToolResults() {
			for _, b := range r.ContentBlocks {
				if b.Text != nil {
					answer.WriteString(b.Text.Text)
					answer.WriteByte('\n')
				}
			}
		}
		return []schema.Message{schema.AssistantMessage(answer.String())}, nil
	})
}
