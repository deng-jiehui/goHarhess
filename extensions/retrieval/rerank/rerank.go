// Package rerank 提供通用 HTTP 重排客户端：不同重排服务（Cohere、Jina、
// TEI 等）的请求与响应字段名不一致，用 Wire 的可配置映射适配，
// 不为每家写专用代码。复用 llm 的抖动重试与 Retry-After 语义。
package rerank

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
)

const providerName = "rerank"

// maxResponseBody 限制响应读取上限。
const maxResponseBody = 64 << 20

// Wire 是请求与响应的字段名映射：各服务字段名不同（documents/docs、
// relevance_score/score），默认值对齐 Cohere /rerank 协议。
type Wire struct {
	// 请求字段名。
	ModelField     string // 默认 "model"
	QueryField     string // 默认 "query"
	DocumentsField string // 默认 "documents"
	TopKField      string // 默认 "top_n"；设为 "-" 则不发送 top_k
	// 响应字段名。
	ResultsField  string // 默认 "results"
	IndexField    string // 默认 "index"
	ScoreField    string // 默认 "relevance_score"
	DocumentField string // 默认 "document"；空表示响应不回带文档
}

func (w Wire) withDefaults() Wire {
	if w.ModelField == "" {
		w.ModelField = "model"
	}
	if w.QueryField == "" {
		w.QueryField = "query"
	}
	if w.DocumentsField == "" {
		w.DocumentsField = "documents"
	}
	if w.TopKField == "" {
		w.TopKField = "top_n"
	}
	if w.ResultsField == "" {
		w.ResultsField = "results"
	}
	if w.IndexField == "" {
		w.IndexField = "index"
	}
	if w.ScoreField == "" {
		w.ScoreField = "relevance_score"
	}
	return w
}

// Config 是重排客户端配置；Endpoint 为完整的 rerank URL。
type Config struct {
	Endpoint string
	APIKey   string
	Model    string
	Wire     Wire
	Pool     *httpclient.Pool
	Timeout  time.Duration
	Retry    llm.RetryConfig
}

type client struct {
	cfg  Config
	wire Wire
	http *http.Client
}

// New 校验配置并返回 Runnable 形态的重排客户端。
func New(cfg Config) (runnable.Runnable[retrieval.RerankRequest, []retrieval.RerankResult], error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("rerank client requires an endpoint")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("rerank Timeout cannot be negative")
	}
	return &client{
		cfg:  cfg,
		wire: cfg.Wire.withDefaults(),
		http: httpclient.NewClient(httpclient.ClientConfig{Pool: cfg.Pool, Timeout: cfg.Timeout}),
	}, nil
}

func (c *client) Invoke(ctx context.Context, req retrieval.RerankRequest, _ ...runnable.Option) ([]retrieval.RerankResult, error) {
	if ctx == nil {
		return nil, errors.New("rerank invoke context cannot be nil")
	}
	if len(req.Documents) == 0 {
		return []retrieval.RerankResult{}, nil
	}
	model := req.Model
	if model == "" {
		model = c.cfg.Model
	}
	var results []retrieval.RerankResult
	err := llm.CallWithRetry(ctx, c.cfg.Retry, nil, func() error {
		var callErr error
		results, callErr = c.call(ctx, model, req)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	// 服务端返回顺序不可信：按分数降序稳定排序，保留原始下标。
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
	return results, nil
}

func (c *client) Stream(ctx context.Context, input retrieval.RerankRequest, options ...runnable.Option) (*runnable.StreamReader[[]retrieval.RerankResult], error) {
	return runnable.StreamFromInvoke(ctx, input, c.Invoke, options...)
}

func (c *client) Collect(ctx context.Context, input *runnable.StreamReader[retrieval.RerankRequest], options ...runnable.Option) ([]retrieval.RerankResult, error) {
	return runnable.CollectFromInvoke(ctx, input, c.Invoke, options...)
}

func (c *client) Transform(ctx context.Context, input *runnable.StreamReader[retrieval.RerankRequest], options ...runnable.Option) (*runnable.StreamReader[[]retrieval.RerankResult], error) {
	return runnable.TransformFromInvoke(ctx, input, c.Invoke, options...)
}

func (*client) NodeKind() runnable.NodeKind { return runnable.NodeKindRunnable }

func (c *client) call(ctx context.Context, model string, req retrieval.RerankRequest) ([]retrieval.RerankResult, error) {
	docs := make([]any, len(req.Documents))
	for i, doc := range req.Documents {
		// 文档按对象发送，保留 ID 与元数据供服务端/回带使用。
		docs[i] = map[string]any{"id": doc.ID, "text": doc.Text, "metadata": doc.Metadata}
	}
	body := map[string]any{
		c.wire.ModelField:     model,
		c.wire.QueryField:     req.Query,
		c.wire.DocumentsField: docs,
	}
	if c.wire.TopKField != "-" && req.TopK > 0 {
		body[c.wire.TopKField] = req.TopK
	}
	for key, value := range req.Raw {
		body[key] = value
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal rerank request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build rerank request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("rerank request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, fmt.Errorf("read rerank response: %w", err)
	}
	if int64(len(data)) > maxResponseBody {
		return nil, fmt.Errorf("rerank response exceeds %d bytes", maxResponseBody)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, statusError(resp, data)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("decode rerank response: %w", err)
	}
	return c.decodeResults(parsed, req)
}

// decodeResults 按 Wire 映射解析响应；文档字段存在时回带原引用。
func (c *client) decodeResults(parsed map[string]any, req retrieval.RerankRequest) ([]retrieval.RerankResult, error) {
	rawResults, ok := parsed[c.wire.ResultsField]
	if !ok {
		return nil, fmt.Errorf("rerank response missing %q field", c.wire.ResultsField)
	}
	items, ok := rawResults.([]any)
	if !ok {
		return nil, fmt.Errorf("rerank %q field is not an array", c.wire.ResultsField)
	}
	results := make([]retrieval.RerankResult, 0, len(items))
	for i, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("rerank result %d is not an object", i)
		}
		index, err := wireInt(entry[c.wire.IndexField])
		if err != nil {
			return nil, fmt.Errorf("rerank result %d index: %w", i, err)
		}
		score, err := wireFloat(entry[c.wire.ScoreField])
		if err != nil {
			return nil, fmt.Errorf("rerank result %d score: %w", i, err)
		}
		result := retrieval.RerankResult{Index: index, Score: score}
		// 服务回带文档文本/对象时按原始下标对齐原引用；否则只给下标。
		if c.wire.DocumentField != "" {
			if rawDoc, ok := entry[c.wire.DocumentField]; ok && rawDoc != nil {
				result.Document = decodeWireDocument(rawDoc)
			}
		}
		if index >= 0 && index < len(req.Documents) {
			if result.Document == nil {
				doc := req.Documents[index]
				result.Document = &doc
			}
			result.ID = req.Documents[index].ID
		}
		results = append(results, result)
	}
	return results, nil
}

func decodeWireDocument(raw any) *retrieval.RerankDocument {
	doc := &retrieval.RerankDocument{}
	switch v := raw.(type) {
	case string:
		doc.Text = v
	case map[string]any:
		if text, ok := v["text"].(string); ok {
			doc.Text = text
		}
		if id, ok := v["id"].(string); ok {
			doc.ID = id
		}
		if meta, ok := v["metadata"].(map[string]any); ok {
			doc.Metadata = meta
		}
	}
	return doc
}

func wireInt(raw any) (int, error) {
	switch v := raw.(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	default:
		return 0, fmt.Errorf("expected number, got %T", raw)
	}
}

func wireFloat(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", raw)
	}
}

func statusError(resp *http.Response, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 1024 {
		message = message[:1024]
	}
	return llm.NewHTTPStatusError(providerName, resp.StatusCode, resp.Status,
		message, llm.ParseRetryAfter(resp.Header.Get("Retry-After")))
}

var _ runnable.Runnable[retrieval.RerankRequest, []retrieval.RerankResult] = (*client)(nil)
