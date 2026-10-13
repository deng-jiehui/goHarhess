package retrieval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/extensions/tenant"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// Retriever 是检索工具背后的检索面：混合检索器或任何同签名实现。
type Retriever = runnable.Runnable[HybridQuery, []SearchHit]

// CitationsBlockKind 是结构化引用在工具结果里的 Opaque 块类型名。
const CitationsBlockKind = "retrieval.citations"

// SearchToolArgs 是检索工具的参数面；租户不在其中，永远来自 ctx 作用域。
type SearchToolArgs struct {
	Query string `json:"query" jsonschema:"检索问题"`
	TopK  int    `json:"top_k,omitempty" jsonschema:"返回条数上限"`
}

// Citation 是一条结构化引用：前端据此渲染来源。
type Citation struct {
	ID         string   `json:"id"`
	DocumentID string   `json:"document_id,omitempty"`
	Score      float64  `json:"score"`
	Source     string   `json:"source,omitempty"`
	Metadata   Metadata `json:"metadata,omitempty"`
}

type SearchToolConfig struct {
	Index       string
	TopK        int // 默认 10
	MaxTokens   int // 引用文本预算；0 用 DefaultMaxReferenceTokens
	Counter     TokenCounter
	Name        string // 默认 "search"
	Description string
	// FilterBuilder 把调用方静态过滤映射到 HybridQuery.Filter；可选。
	FilterBuilder FileFilterBuilder
}

// NewSearchTool 构造只读检索工具：返回带编号的引用文本加一个
// CitationsBlockKind Opaque 块携带结构化引用。空结果返回明确文案，
// 不让模型自行编造。Searcher 建议用 Scoped 包装——命名空间取自 ctx
// 的租户作用域，模型无法指定查哪个租户。
func NewSearchTool(searcher Retriever, cfg SearchToolConfig) (*tool.FunctionTool, error) {
	if searcher == nil {
		return nil, errors.New("search tool requires a searcher")
	}
	if cfg.TopK == 0 {
		cfg.TopK = 10
	}
	if cfg.TopK < 1 {
		return nil, errors.New("search tool TopK must be greater than zero")
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = DefaultMaxReferenceTokens
	}
	if cfg.Counter == nil {
		cfg.Counter = ApproxTokenCounter{}
	}
	name := cfg.Name
	if name == "" {
		name = "search"
	}
	description := cfg.Description
	if description == "" {
		description = "检索知识库并返回带来源编号的相关片段"
	}
	return tool.New[SearchToolArgs, tool.Result](name, description,
		func(ctx context.Context, args SearchToolArgs) (tool.Result, error) {
			return runSearchTool(ctx, searcher, cfg, args)
		}, tool.ReadOnly()), nil
}

func runSearchTool(ctx context.Context, searcher Retriever, cfg SearchToolConfig, args SearchToolArgs) (tool.Result, error) {
	if ctx == nil {
		return tool.Result{}, errors.New("search tool context cannot be nil")
	}
	query := HybridQuery{
		Text: strings.TrimSpace(args.Query), Index: cfg.Index,
		TopK: cfg.TopK,
	}
	if args.TopK > 0 {
		query.TopK = args.TopK
	}
	if scope, ok := tenant.FromContext(ctx); ok {
		query.Namespace = scope.TenantID
	}
	if cfg.FilterBuilder != nil {
		query.Filter = cfg.FilterBuilder(nil)
	}
	hits, err := searcher.Invoke(ctx, query)
	if err != nil {
		return tool.Result{}, err
	}
	if len(hits) == 0 {
		return tool.TextResult("没有找到相关内容。"), nil
	}
	var (
		builder   strings.Builder
		citations []Citation
		used      int
	)
	for index, hit := range hits {
		header := fmt.Sprintf("[Reference %d id=%s source=%s score=%.6f]\n", index+1, hit.ID, hit.Source, hit.Score)
		headerTokens := cfg.Counter.Count(header)
		if used+headerTokens >= cfg.MaxTokens {
			break
		}
		remaining := cfg.MaxTokens - used - headerTokens
		text := truncateToTokens(hit.Text, remaining, cfg.Counter)
		if strings.TrimSpace(text) == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(header)
		builder.WriteString(text)
		used += headerTokens + cfg.Counter.Count(text)
		docID, _ := hit.Metadata["_rag_document_id"].(string)
		citations = append(citations, Citation{
			ID: hit.ID, DocumentID: docID, Score: hit.Score,
			Source: hit.Source, Metadata: hit.Metadata,
		})
		if used >= cfg.MaxTokens {
			break
		}
	}
	if len(citations) == 0 {
		return tool.TextResult("没有找到相关内容。"), nil
	}
	raw, err := json.Marshal(citations)
	if err != nil {
		return tool.Result{}, fmt.Errorf("search tool citations: %w", err)
	}
	return tool.Result{ContentBlocks: []schema.ContentBlock{
		schema.NewTextBlock(builder.String()),
		schema.NewOpaqueBlock(CitationsBlockKind, raw),
	}}, nil
}
