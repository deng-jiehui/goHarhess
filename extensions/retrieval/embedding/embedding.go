// Package embedding 提供 OpenAI 兼容 /embeddings 协议的嵌入客户端：
// 自动分批、重试（复用 llm 的抖动与 Retry-After 语义）、限流器由应用注入。
// 不引入厂商 SDK，自托管推理服务一般兼容同一协议。
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
)

const providerName = "embedding"

// maxResponseBody 限制响应读取上限，防止异常大响应撑爆内存。
const maxResponseBody = 64 << 20

// Limiter 是应用注入的限流接口，可对接 LLM 网关或自有实现。
// 每次出站 HTTP 调用（每个嵌入批次）前调用一次。
type Limiter interface {
	Wait(ctx context.Context) error
}

// Config 是嵌入客户端配置；Endpoint 为完整的 embeddings URL
// （如 https://api.openai.com/v1/embeddings）。
type Config struct {
	Endpoint   string
	APIKey     string
	Model      string
	Dimensions int
	// BatchSize 是单次请求的输入条数上限；<=0 表示不切批（整单发送）。
	BatchSize int
	Pool      *httpclient.Pool
	Timeout   time.Duration
	Retry     llm.RetryConfig
	Limiter   Limiter
}

type client struct {
	cfg    Config
	http   *http.Client
	policy llm.RetryConfig
}

// New 校验配置并返回 Runnable 形态的嵌入客户端。
func New(cfg Config) (runnable.Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse], error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("embedding client requires an endpoint")
	}
	if cfg.BatchSize < 0 {
		return nil, errors.New("embedding BatchSize cannot be negative")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("embedding Timeout cannot be negative")
	}
	return &client{
		cfg:    cfg,
		http:   httpclient.NewClient(httpclient.ClientConfig{Pool: cfg.Pool, Timeout: cfg.Timeout}),
		policy: cfg.Retry,
	}, nil
}

// request/response 是 OpenAI 兼容协议的线格式。
type wireRequest struct {
	Model      string         `json:"model,omitempty"`
	Input      []string       `json:"input"`
	Dimensions int            `json:"dimensions,omitempty"`
	Extra      map[string]any `json:"-"`
}

type wireResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func (c *client) Invoke(ctx context.Context, req retrieval.EmbeddingRequest, _ ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	if ctx == nil {
		return retrieval.EmbeddingResponse{}, errors.New("embedding invoke context cannot be nil")
	}
	if len(req.Inputs) == 0 {
		return retrieval.EmbeddingResponse{Vectors: [][]float32{}}, nil
	}
	model := req.Model
	if model == "" {
		model = c.cfg.Model
	}
	dimensions := req.Dimensions
	if dimensions == 0 {
		dimensions = c.cfg.Dimensions
	}
	// 切批保持输入顺序：逐段调用后按原序拼接。
	out := retrieval.EmbeddingResponse{Vectors: make([][]float32, len(req.Inputs))}
	for start := 0; start < len(req.Inputs); {
		end := len(req.Inputs)
		if c.cfg.BatchSize > 0 && start+c.cfg.BatchSize < end {
			end = start + c.cfg.BatchSize
		}
		inputs := req.Inputs[start:end]
		var batch wireResponse
		err := llm.CallWithRetry(ctx, c.policy, nil, func() error {
			if c.cfg.Limiter != nil {
				if err := c.cfg.Limiter.Wait(ctx); err != nil {
					return err
				}
			}
			var callErr error
			batch, callErr = c.call(ctx, model, dimensions, inputs, req.Raw)
			return callErr
		})
		if err != nil {
			return retrieval.EmbeddingResponse{}, err
		}
		// 返回向量数必须与输入条数一致，不静默截断。
		if len(batch.Data) != len(inputs) {
			return retrieval.EmbeddingResponse{}, fmt.Errorf(
				"embedding endpoint returned %d vectors for %d inputs", len(batch.Data), len(inputs))
		}
		for _, item := range batch.Data {
			if item.Index < 0 || item.Index >= len(inputs) {
				return retrieval.EmbeddingResponse{}, fmt.Errorf(
					"embedding endpoint returned out-of-range index %d", item.Index)
			}
			out.Vectors[start+item.Index] = append([]float32(nil), item.Embedding...)
		}
		out.Usage.InputTokens += batch.Usage.PromptTokens
		start = end
	}
	return out, nil
}

func (c *client) Stream(ctx context.Context, input retrieval.EmbeddingRequest, options ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.StreamFromInvoke(ctx, input, c.Invoke, options...)
}

func (c *client) Collect(ctx context.Context, input *runnable.StreamReader[retrieval.EmbeddingRequest], options ...runnable.Option) (retrieval.EmbeddingResponse, error) {
	return runnable.CollectFromInvoke(ctx, input, c.Invoke, options...)
}

func (c *client) Transform(ctx context.Context, input *runnable.StreamReader[retrieval.EmbeddingRequest], options ...runnable.Option) (*runnable.StreamReader[retrieval.EmbeddingResponse], error) {
	return runnable.TransformFromInvoke(ctx, input, c.Invoke, options...)
}

func (*client) NodeKind() runnable.NodeKind { return runnable.NodeKindRunnable }

func (c *client) call(ctx context.Context, model string, dimensions int, inputs []string, extra map[string]any) (wireResponse, error) {
	body := map[string]any{
		"model": model,
		"input": inputs,
	}
	if dimensions > 0 {
		body["dimensions"] = dimensions
	}
	for key, value := range extra {
		body[key] = value
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return wireResponse{}, fmt.Errorf("marshal embedding request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return wireResponse{}, fmt.Errorf("build embedding request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return wireResponse{}, fmt.Errorf("embedding request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return wireResponse{}, fmt.Errorf("read embedding response: %w", err)
	}
	if int64(len(data)) > maxResponseBody {
		return wireResponse{}, fmt.Errorf("embedding response exceeds %d bytes", maxResponseBody)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return wireResponse{}, statusError(resp, data)
	}
	var parsed wireResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return wireResponse{}, fmt.Errorf("decode embedding response: %w", err)
	}
	if parsed.Error != nil {
		return wireResponse{}, fmt.Errorf("embedding API error: %s", parsed.Error.Message)
	}
	return parsed, nil
}

// statusError 把非 2xx 响应转成携带 StatusCode/RetryAfter 的 llm.HTTPStatusError。
func statusError(resp *http.Response, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 1024 {
		message = message[:1024]
	}
	return llm.NewHTTPStatusError(providerName, resp.StatusCode, resp.Status,
		message, llm.ParseRetryAfter(resp.Header.Get("Retry-After")))
}

var _ runnable.Runnable[retrieval.EmbeddingRequest, retrieval.EmbeddingResponse] = (*client)(nil)
