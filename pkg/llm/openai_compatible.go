package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/goharness/goharness/pkg/httpclient"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

const (
	defaultOpenAIBaseURL    = "https://api.openai.com/v1"
	defaultDeepSeekBaseURL  = "https://api.deepseek.com"
	defaultDashScopeBaseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
	// DefaultMaxResponseBytes 是非流式响应体的默认读取上限（32 MiB）。
	// 超限即调用失败，截断内容不会被当作完整响应解析。
	DefaultMaxResponseBytes int64 = 32 << 20
)

// OpenAICompatibleConfig configures providers that expose the Chat
// Completions protocol. OpenAI, DeepSeek and several other vendors can share
// this adapter by changing BaseURL and Model.
type OpenAICompatibleConfig struct {
	Model                   string
	BaseURL                 string
	APIKey                  string
	GenerateConfig          GenerateConfig
	Client                  *http.Client
	Headers                 map[string]string
	Capabilities            Capabilities
	IncludeReasoningContent bool
	// MaxResponseBytes 是非流式响应体的读取上限；零值或负值使用
	// DefaultMaxResponseBytes。
	MaxResponseBytes int64
}

// OpenAICompatibleModel implements the OpenAI Chat Completions wire format.
// It owns no per-request transport; Client is intended to be long-lived and
// shared so net/http can reuse idle connections.
type OpenAICompatibleModel struct {
	model                   string
	endpoint                string
	apiKey                  string
	generateConfig          GenerateConfig
	client                  *http.Client
	headers                 map[string]string
	capabilities            Capabilities
	includeReasoningContent bool
	maxResponseBytes        int64
}

func init() {
	MustRegister("openai-compatible", openAICompatibleFactory(defaultOpenAIBaseURL, false))
	MustRegister("openai", openAICompatibleFactory(defaultOpenAIBaseURL, false))
	MustRegister("deepseek", openAICompatibleFactory(defaultDeepSeekBaseURL, true))
	MustRegister("dashscope", openAICompatibleFactory(defaultDashScopeBaseURL, true))
	MustRegister("qwen", openAICompatibleFactory(defaultDashScopeBaseURL, true))
}

func openAICompatibleFactory(defaultBaseURL string, reasoning bool) Factory {
	return func(cfg Config) (ChatModel, error) {
		baseURL := strings.TrimSpace(cfg.ModelServer)
		if baseURL == "" {
			baseURL = rawString(cfg.Raw, "base_url")
		}
		if baseURL == "" {
			baseURL = defaultBaseURL
		}
		return newOpenAICompatibleModel(OpenAICompatibleConfig{
			Model:                   cfg.Model,
			BaseURL:                 baseURL,
			APIKey:                  cfg.APIKey,
			GenerateConfig:          cfg.GenerateConfig,
			Client:                  cfg.HTTPClient,
			Headers:                 rawHeaders(cfg.Raw),
			IncludeReasoningContent: reasoning || rawBool(cfg.Raw, "include_reasoning_content"),
			MaxResponseBytes:        rawInt64(cfg.Raw, "max_response_bytes"),
		}, cfg.Capabilities)
	}
}

// NewOpenAICompatibleModel creates a model using a reusable HTTP client. A
// nil client receives the framework's pooled default client.
// 零值 Capabilities 采用默认声明（JSONSchema 默认关闭）；需要显式全 false
// 时经 llm.Config.Capabilities 指针传入。
func NewOpenAICompatibleModel(cfg OpenAICompatibleConfig) (*OpenAICompatibleModel, error) {
	if cfg.Capabilities != (Capabilities{}) {
		return newOpenAICompatibleModel(cfg, &cfg.Capabilities)
	}
	return newOpenAICompatibleModel(cfg, nil)
}

// newOpenAICompatibleModel 区分「未声明」与「显式声明」：declared 为 nil 时
// 采用默认能力，非 nil 时按内容原样使用（包括全 false）。
func newOpenAICompatibleModel(cfg OpenAICompatibleConfig, declared *Capabilities) (*OpenAICompatibleModel, error) {
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("OpenAI-compatible model requires a model name")
	}
	endpoint, err := chatCompletionsEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	capabilities := Capabilities{
		Streaming:  true,
		ToolCalls:  true,
		JSONSchema: false,
		Reasoning:  cfg.IncludeReasoningContent,
	}
	if declared != nil {
		capabilities = *declared
	}
	maxResponseBytes := cfg.MaxResponseBytes
	if maxResponseBytes <= 0 {
		maxResponseBytes = DefaultMaxResponseBytes
	}
	if err := checkReservedGenerateKeys(cfg.GenerateConfig); err != nil {
		return nil, err
	}
	return &OpenAICompatibleModel{
		model:                   model,
		endpoint:                endpoint,
		apiKey:                  strings.TrimSpace(cfg.APIKey),
		generateConfig:          MergeGenerateConfig(cfg.GenerateConfig, nil),
		client:                  httpclient.NewClient(httpclient.ClientConfig{Client: cfg.Client}),
		headers:                 cloneHeaders(cfg.Headers),
		capabilities:            capabilities,
		includeReasoningContent: cfg.IncludeReasoningContent,
		maxResponseBytes:        maxResponseBytes,
	}, nil
}

// Capabilities reports the feature set declared for this model endpoint.
func (m *OpenAICompatibleModel) Capabilities() Capabilities {
	return m.capabilities
}

// ModelInfo exposes non-secret endpoint metadata to lifecycle callbacks.
func (m *OpenAICompatibleModel) ModelInfo() ModelInfo {
	provider := "openai-compatible"
	if endpoint, err := url.Parse(m.endpoint); err == nil {
		switch strings.ToLower(endpoint.Hostname()) {
		case "api.openai.com":
			provider = "openai"
		case "api.deepseek.com":
			provider = "deepseek"
		case "dashscope.aliyuncs.com":
			provider = "dashscope"
		}
	}
	return ModelInfo{
		Name:      m.model,
		Provider:  provider,
		ModelType: "openai-compatible",
		Endpoint:  m.endpoint,
	}
}

// HTTPClient exposes the shared client for diagnostics and graceful shutdown.
func (m *OpenAICompatibleModel) HTTPClient() *http.Client {
	return m.client
}

func (m *OpenAICompatibleModel) request(ctx context.Context, input Request, opts Options, streaming bool) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("OpenAI-compatible model context cannot be nil")
	}
	if streaming && !m.capabilities.Streaming {
		return nil, errors.New("OpenAI-compatible model does not support streaming")
	}
	if len(input.Functions) > 0 && !m.capabilities.ToolCalls {
		return nil, errors.New("OpenAI-compatible model does not support tool calls")
	}
	if opts.ResponseFormat != nil && !m.capabilities.JSONSchema {
		return nil, fmt.Errorf("model %q: %w (JSONSchema capability not declared)", m.model, ErrResponseFormatUnsupported)
	}

	payload, err := m.requestPayload(input.Messages, input.Functions, opts, streaming)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode OpenAI-compatible request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build OpenAI-compatible request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if m.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+m.apiKey)
	}
	for key, value := range m.headers {
		request.Header.Set(key, value)
	}

	response, err := m.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call OpenAI-compatible API: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		return nil, providerHTTPError(response)
	}
	return response, nil
}

func (m *OpenAICompatibleModel) Invoke(ctx context.Context, input Request, options ...runnable.Option) (Response, error) {
	resolved, remaining, err := resolveModelOptions(options)
	if err != nil {
		return Response{}, fmt.Errorf("resolve model options: %w", err)
	}
	if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
		return Response{}, fmt.Errorf("resolve model options: %w", err)
	}
	response, err := m.request(ctx, input, resolved, false)
	if err != nil {
		return Response{}, err
	}
	defer response.Body.Close()
	data, err := readBoundedResponseBody(response.Body, m.maxResponseBytes)
	if err != nil {
		return Response{}, err
	}
	messages, usage, finishReason, err := decodeOpenAIResponse(data, false)
	if err != nil {
		return Response{}, err
	}
	return Response{Messages: messages, Usage: usage, FinishReason: finishReason}, nil
}

var _ ChatModel = (*OpenAICompatibleModel)(nil)

func (m *OpenAICompatibleModel) Stream(ctx context.Context, input Request, options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	resolved, remaining, err := resolveModelOptions(options)
	if err != nil {
		return nil, fmt.Errorf("resolve model options: %w", err)
	}
	if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
		return nil, fmt.Errorf("resolve model options: %w", err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	response, err := m.request(streamCtx, input, resolved, true)
	if err != nil {
		cancel()
		return nil, err
	}
	return responseReaderFromWire(ctx, m.streamEvents(streamCtx, response.Body), cancel)
}

func (m *OpenAICompatibleModel) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return Response{}, err
	}
	return m.Invoke(ctx, value, options...)
}

func (m *OpenAICompatibleModel) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Response], error) {
	value, err := input.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return m.Stream(ctx, value, options...)
}

// reservedGenerateKeys 由框架在请求体中写入或由工具声明生成，出现在
// GenerateConfig 中会静默覆盖字段或丢弃工具定义，一律拒绝。
var reservedGenerateKeys = map[string]struct{}{
	"model": {}, "messages": {}, "stream": {}, "tools": {}, "functions": {},
}

// ErrVisionUnsupported 报告向未声明 Vision 能力的模型发送图片输入。
var ErrVisionUnsupported = errors.New("model does not support image input")

// encodeOpenAIImagePart 把图片块编成 OpenAI 的 image_url 部分：
// http(s) URI 原样传出；内联 Data（JSON 字符串承载的 base64）编成
// data URI；本地 file:// 等 URI 在适配器层不解析（由发送前的解析器
// 负责），遇到未解析的本地 URI 返回明确错误。
func encodeOpenAIImagePart(messageIndex, blockIndex int, media *schema.MediaContent) (map[string]any, error) {
	if media == nil {
		return nil, fmt.Errorf("messages[%d].content_blocks[%d]: image block has no content", messageIndex, blockIndex)
	}
	uri := strings.TrimSpace(media.URI)
	if uri != "" {
		if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
			return map[string]any{"type": "image_url", "image_url": map[string]any{"url": uri}}, nil
		}
		return nil, fmt.Errorf("messages[%d].content_blocks[%d]: image URI %q is not an http(s) URL; resolve local URIs to data URIs before sending", messageIndex, blockIndex, uri)
	}
	if len(media.Data) > 0 {
		var encoded string
		if err := json.Unmarshal(media.Data, &encoded); err != nil {
			return nil, fmt.Errorf("messages[%d].content_blocks[%d]: image inline data must be a JSON base64 string", messageIndex, blockIndex)
		}
		mimeType := media.MIMEType
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		return map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:" + mimeType + ";base64," + encoded}}, nil
	}
	return nil, fmt.Errorf("messages[%d].content_blocks[%d]: image block has neither URI nor inline data", messageIndex, blockIndex)
}

func checkReservedGenerateKeys(config GenerateConfig) error {
	for key := range config {
		if _, reserved := reservedGenerateKeys[key]; reserved {
			return fmt.Errorf("%w: %q", ErrReservedGenerateKey, key)
		}
	}
	return nil
}

func (m *OpenAICompatibleModel) requestPayload(messages []schema.Message, functions []schema.FunctionDefinition, opts Options, streaming bool) (map[string]any, error) {
	payload := map[string]any{}
	if err := checkReservedGenerateKeys(opts.ExtraGenerateConfig); err != nil {
		return nil, err
	}
	merged := MergeGenerateConfig(m.generateConfig, opts.ExtraGenerateConfig)
	if opts.ResponseFormat != nil && merged["response_format"] != nil {
		return nil, ErrResponseFormatConflict
	}
	for key, value := range merged {
		payload[key] = value
	}
	if opts.ResponseFormat != nil {
		if err := validateResponseFormat(opts.ResponseFormat); err != nil {
			return nil, err
		}
		payload["response_format"] = encodeResponseFormat(*opts.ResponseFormat)
	}
	payload["model"] = m.model
	encodedMessages, err := encodeOpenAIMessages(messages, m.includeReasoningContent, m.capabilities.Vision)
	if err != nil {
		return nil, fmt.Errorf("encode OpenAI-compatible messages: %w", err)
	}
	payload["messages"] = encodedMessages
	payload["stream"] = streaming
	if len(functions) > 0 && payload["tools"] == nil && payload["functions"] == nil {
		payload["tools"] = encodeOpenAITools(functions)
	}
	return payload, nil
}

func (m *OpenAICompatibleModel) streamEvents(ctx context.Context, body io.ReadCloser) <-chan wireEvent {
	out := make(chan wireEvent)
	rawEvents := parseSSE(ctx, body)
	go func() {
		defer close(out)
		defer body.Close()
		for event := range rawEvents {
			if event.err != nil {
				sendWireEvent(ctx, out, wireEvent{err: event.err})
				return
			}
			data := strings.TrimSpace(event.data)
			if data == "" {
				continue
			}
			if data == "[DONE]" {
				sendWireEvent(ctx, out, wireEvent{kind: wireEventDone})
				return
			}
			messages, usage, finishReason, err := decodeOpenAIResponse([]byte(data), true)
			if err != nil {
				sendWireEvent(ctx, out, wireEvent{err: err})
				return
			}
			if len(messages) > 0 || usage != nil || finishReason != "" {
				var envelope openAIResponse
				if err := json.Unmarshal([]byte(data), &envelope); err != nil {
					sendWireEvent(ctx, out, wireEvent{err: fmt.Errorf("decode OpenAI-compatible stream metadata: %w", err)})
					return
				}
				kind := wireEventDelta
				if len(messages) == 0 {
					kind = wireEventUsage
				}
				if !sendWireEvent(ctx, out, wireEvent{
					kind:         kind,
					messages:     messages,
					usage:        usage,
					finishReason: finishReason,
					responseID:   envelope.ID,
					model:        envelope.Model,
				}) {
					return
				}
			}
		}
		sendWireEvent(ctx, out, wireEvent{err: errors.New("OpenAI-compatible stream closed before [DONE]")})
	}()
	return out
}

func chatCompletionsEndpoint(baseURL string) (string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return "", errors.New("OpenAI-compatible provider requires a base URL")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid OpenAI-compatible base URL %q", baseURL)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(parsed.Path, "/chat/completions") {
		if parsed.Path == "" {
			parsed.Path = "/chat/completions"
		} else {
			parsed.Path += "/chat/completions"
		}
	}
	return parsed.String(), nil
}

type openAIResponse struct {
	ID      string         `json:"id"`
	Model   string         `json:"model"`
	Choices []openAIChoice `json:"choices"`
	Usage   *openAIUsage   `json:"usage,omitempty"`
}

// maxOpenAIToolCallIndex bounds provider-controlled sparse slot allocation.
const maxOpenAIToolCallIndex = 1024

type openAIChoice struct {
	Index        int           `json:"index"`
	Message      openAIMessage `json:"message"`
	Delta        openAIMessage `json:"delta"`
	FinishReason *string       `json:"finish_reason"`
}

type openAIMessage struct {
	Role             string              `json:"role"`
	Content          json.RawMessage     `json:"content"`
	ReasoningContent json.RawMessage     `json:"reasoning_content"`
	Reasoning        json.RawMessage     `json:"reasoning"`
	Thinking         json.RawMessage     `json:"thinking"`
	Thought          json.RawMessage     `json:"thought"`
	Refusal          json.RawMessage     `json:"refusal"`
	Name             string              `json:"name"`
	FunctionCall     *openAIFunctionCall `json:"function_call,omitempty"`
	ToolCalls        []openAIToolCall    `json:"tool_calls,omitempty"`
}

type openAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAIToolCall struct {
	Index    int                `json:"index"`
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIFunctionCall `json:"function"`
}

type openAIUsage struct {
	PromptTokens           int `json:"prompt_tokens"`
	CompletionTokens       int `json:"completion_tokens"`
	TotalTokens            int `json:"total_tokens"`
	CompletionTokenDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
	PromptTokenDetails struct {
		CachedTokens     int `json:"cached_tokens"`
		CacheWriteTokens int `json:"cache_write_tokens"`
	} `json:"prompt_tokens_details"`
}

func decodeOpenAIResponse(data []byte, delta bool) ([]schema.Message, *Usage, string, error) {
	var response openAIResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, nil, "", fmt.Errorf("decode OpenAI-compatible response: %w", err)
	}
	var messages []schema.Message
	finishReason := ""
	for _, choice := range response.Choices {
		if choice.Index != 0 {
			return nil, nil, "", fmt.Errorf("unsupported OpenAI-compatible choice index %d", choice.Index)
		}
		value := choice.Message
		if delta {
			value = choice.Delta
		}
		converted, err := openAIMessageToSchema(value, delta)
		if err != nil {
			return nil, nil, "", err
		}
		messages = append(messages, converted...)
		if choice.FinishReason != nil {
			finishReason = *choice.FinishReason
		}
	}
	usage := decodeUsage(response.Usage)
	if !delta {
		messages = attachResponseMeta(messages, response.ID, response.Model, finishReason, usage)
	}
	return messages, usage, finishReason, nil
}

func openAIMessageToSchema(value openAIMessage, indexed bool) ([]schema.Message, error) {
	role := parseRole(value.Role)
	contentBlocks, contentText, contentArray := decodeOpenAIContentBlocks(value.Content)
	content := contentText
	reasoning := flexibleText(value.ReasoningContent)
	if reasoning == "" {
		reasoning = flexibleText(value.Reasoning)
	}
	if reasoning == "" {
		reasoning = flexibleText(value.Thinking)
	}
	if reasoning == "" {
		reasoning = flexibleText(value.Thought)
	}
	if role == "" && (len(contentBlocks) > 0 || content != "" || reasoning != "" || len(value.ToolCalls) > 0 || value.FunctionCall != nil) {
		role = schema.RoleAssistant
	}
	blocks := make([]schema.ContentBlock, 0, len(contentBlocks)+2+len(value.ToolCalls))
	blocks = append(blocks, contentBlocks...)
	if indexed {
		if !contentArray {
			blocks = make([]schema.ContentBlock, 2)
			if reasoning != "" {
				blocks[0] = schema.NewReasoningBlock(reasoning, "")
			}
			if content != "" {
				blocks[1] = schema.NewTextBlock(content)
			}
		} else if reasoning != "" {
			blocks = append([]schema.ContentBlock{schema.NewReasoningBlock(reasoning, "")}, blocks...)
		}
	} else {
		if reasoning != "" {
			blocks = append(blocks, schema.NewReasoningBlock(reasoning, ""))
		}
		if content != "" && len(contentBlocks) == 0 {
			blocks = append(blocks, schema.NewTextBlock(content))
		}
	}
	base := schema.NewMessage(role, blocks...)
	if refusal := flexibleText(value.Refusal); refusal != "" {
		if base.Extra == nil {
			base.Extra = map[string]any{}
		}
		base.Extra["llm.refusal"] = refusal
	}
	if indexed && contentArray {
		if value.FunctionCall != nil || len(value.ToolCalls) > 0 {
			return nil, errors.New("stream delta content array cannot be combined with function_call or tool_calls")
		}
		if base.Extra == nil {
			base.Extra = map[string]any{}
		}
		base.Extra[openAIAppendBlocksExtraKey] = len(blocks)
	}

	if value.FunctionCall != nil {
		base.ContentBlocks = append(base.ContentBlocks, schema.NewFunctionToolCallBlock("", value.FunctionCall.Name, value.FunctionCall.Arguments))
		return []schema.Message{base}, nil
	}
	if len(value.ToolCalls) == 0 {
		if base.Role == "" && len(base.ContentBlocks) == 0 && content == "" && reasoning == "" {
			return nil, nil
		}
		return []schema.Message{base}, nil
	}

	highest := len(value.ToolCalls) - 1
	if indexed {
		highest = 0
		seen := map[int]struct{}{}
		for _, toolCall := range value.ToolCalls {
			if toolCall.Index < 0 {
				return nil, fmt.Errorf("invalid tool call index %d", toolCall.Index)
			}
			if toolCall.Index > maxOpenAIToolCallIndex {
				return nil, fmt.Errorf("invalid tool call index %d: exceeds maximum %d", toolCall.Index, maxOpenAIToolCallIndex)
			}
			if _, exists := seen[toolCall.Index]; exists {
				return nil, fmt.Errorf("duplicate tool call index %d", toolCall.Index)
			}
			seen[toolCall.Index] = struct{}{}
			highest = max(highest, toolCall.Index)
		}
	}
	toolOffset := len(blocks)
	if indexed && !contentArray {
		toolOffset = 2
	}
	for len(base.ContentBlocks) < toolOffset+highest+1 {
		base.ContentBlocks = append(base.ContentBlocks, schema.ContentBlock{})
	}
	for order, toolCall := range value.ToolCalls {
		index := order
		if indexed {
			index = toolCall.Index
		}
		blockIndex := toolOffset + index
		base.ContentBlocks[blockIndex] = schema.NewFunctionToolCallBlock(toolCall.ID, toolCall.Function.Name, toolCall.Function.Arguments)
	}
	return []schema.Message{base}, nil
}

func decodeOpenAIContentBlocks(raw json.RawMessage) ([]schema.ContentBlock, string, bool) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, "", false
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if text == "" {
			return nil, "", false
		}
		return []schema.ContentBlock{schema.NewTextBlock(text)}, text, false
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return nil, "", false
	}
	blocks := make([]schema.ContentBlock, 0, len(parts))
	var joined strings.Builder
	for _, part := range parts {
		var fields map[string]json.RawMessage
		if json.Unmarshal(part, &fields) != nil {
			blocks = append(blocks, schema.NewOpaqueBlock("openai.unknown", part))
			continue
		}
		var typ string
		_ = json.Unmarshal(fields["type"], &typ)
		switch typ {
		case "text", "input_text", "output_text":
			var s string
			_ = json.Unmarshal(fields["text"], &s)
			blocks = append(blocks, schema.NewTextBlock(s))
			joined.WriteString(s)
		case "image_url":
			var image struct {
				URL string `json:"url"`
			}
			_ = json.Unmarshal(fields["image_url"], &image)
			blocks = append(blocks, schema.NewImageBlock(image.URL, ""))
		default:
			blocks = append(blocks, schema.NewOpaqueBlock("openai."+typ, part))
		}
	}
	return blocks, joined.String(), true
}

func parseRole(value string) schema.Role {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "system":
		return schema.RoleSystem
	case "user":
		return schema.RoleUser
	case "assistant":
		return schema.RoleAssistant
	case "tool":
		return schema.RoleTool
	case "function":
		return schema.RoleTool
	default:
		return ""
	}
}

func flexibleText(raw json.RawMessage) string {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var item map[string]json.RawMessage
	if json.Unmarshal(raw, &item) == nil {
		for _, key := range []string{"text", "content", "reasoning", "thinking", "thought"} {
			if value, ok := item[key]; ok {
				if text := flexibleText(value); text != "" {
					return text
				}
			}
		}
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) == nil {
		var builder strings.Builder
		for _, part := range items {
			builder.WriteString(flexibleText(part))
		}
		return builder.String()
	}
	return ""
}

func decodeUsage(value *openAIUsage) *Usage {
	if value == nil {
		return nil
	}
	return &Usage{
		InputTokens:      value.PromptTokens,
		OutputTokens:     value.CompletionTokens,
		TotalTokens:      value.TotalTokens,
		ReasoningTokens:  value.CompletionTokenDetails.ReasoningTokens,
		CacheReadTokens:  value.PromptTokenDetails.CachedTokens,
		CacheWriteTokens: value.PromptTokenDetails.CacheWriteTokens,
	}
}

// encodeResponseFormat 把类型化输出格式编码为 OpenAI-compatible
// response_format 请求字段。
func encodeResponseFormat(format ResponseFormat) map[string]any {
	schemaField := map[string]any{
		"name":   format.Name,
		"strict": format.Strict,
		"schema": format.Schema,
	}
	if format.Description != "" {
		schemaField["description"] = format.Description
	}
	return map[string]any{
		"type":        "json_schema",
		"json_schema": schemaField,
	}
}

func encodeOpenAIMessages(messages []schema.Message, includeReasoning bool, vision bool) ([]map[string]any, error) {
	encoded := make([]map[string]any, 0, len(messages))
	for messageIndex, message := range messages {
		role := string(message.Role)
		if message.Role == schema.RoleTool {
			role = "tool"
		}
		item := map[string]any{
			"role": role,
		}
		var text, reasoning strings.Builder
		// parts 非空时 content 走数组形式：文本与图片按块顺序混排；
		// 纯文本消息保持字符串形式不变。
		var parts []map[string]any
		flushText := func() {
			if text.Len() > 0 {
				parts = append(parts, map[string]any{"type": "text", "text": text.String()})
				text.Reset()
			}
		}
		seenCall := false
		toolCalls := make([]map[string]any, 0)
		var result *schema.FunctionToolResult
		for blockIndex, block := range message.ContentBlocks {
			switch block.Type {
			case schema.ContentBlockTypeText:
				if seenCall {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: unsupported interleaved text after tool call", messageIndex, blockIndex)
				}
				if block.Text != nil {
					text.WriteString(block.Text.Text)
				}
			case schema.ContentBlockTypeImage:
				if !vision {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: %w (Vision capability not declared)", messageIndex, blockIndex, ErrVisionUnsupported)
				}
				if seenCall {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: image after tool call is not representable", messageIndex, blockIndex)
				}
				part, err := encodeOpenAIImagePart(messageIndex, blockIndex, block.Image)
				if err != nil {
					return nil, err
				}
				flushText()
				parts = append(parts, part)
			case schema.ContentBlockTypeReasoning:
				if !includeReasoning {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: reasoning cannot be omitted", messageIndex, blockIndex)
				}
				if seenCall {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: reasoning order is not representable", messageIndex, blockIndex)
				}
				if block.Reasoning != nil {
					reasoning.WriteString(block.Reasoning.Text)
				}
			case schema.ContentBlockTypeFunctionToolCall:
				if block.FunctionToolCall != nil {
					seenCall = true
					call := block.FunctionToolCall
					toolCalls = append(toolCalls, map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": call.Arguments}})
				}
			case schema.ContentBlockTypeFunctionToolResult:
				if result != nil || block.FunctionToolResult == nil {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: unsupported block type %q", messageIndex, blockIndex, block.Type)
				}
				result = block.FunctionToolResult
			default:
				return nil, fmt.Errorf("messages[%d].content_blocks[%d]: unsupported block type %q", messageIndex, blockIndex, block.Type)
			}
		}
		if parts != nil {
			flushText()
			item["content"] = parts
		} else {
			item["content"] = text.String()
		}
		if reasoning.Len() > 0 {
			item["reasoning_content"] = reasoning.String()
		}
		if len(toolCalls) > 0 {
			item["tool_calls"] = toolCalls
		}
		if result != nil && message.Role != schema.RoleTool {
			return nil, fmt.Errorf("messages[%d]: function_tool_result block requires tool role", messageIndex)
		}
		if message.Role == schema.RoleTool {
			if result == nil {
				return nil, fmt.Errorf("messages[%d]: tool message requires one function_tool_result block", messageIndex)
			}
			item["tool_call_id"] = result.CallID
			var nested strings.Builder
			for blockIndex, block := range result.ContentBlocks {
				if block.Type != schema.ContentBlockTypeText || block.Text == nil || blockIndex > 0 {
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: unsupported function tool result block type %q: OpenAI-compatible tool messages accept text only; put images in a following user message", messageIndex, blockIndex, block.Type)
				}
				nested.WriteString(block.Text.Text)
			}
			item["content"] = text.String() + nested.String()
		}
		encoded = append(encoded, item)
	}
	return encoded, nil
}

func attachResponseMeta(messages []schema.Message, responseID, model, finishReason string, usage *Usage) []schema.Message {
	cloned := schema.CloneMessages(messages)
	if len(cloned) == 1 {
		blocks := make([]schema.ContentBlock, 0, len(cloned[0].ContentBlocks))
		for _, block := range cloned[0].ContentBlocks {
			if block.Type != "" {
				blocks = append(blocks, block)
			}
		}
		cloned[0].ContentBlocks = blocks
		cloned[0].ResponseMeta = &schema.ResponseMeta{ID: responseID, Model: model, FinishReason: finishReason, Usage: cloneUsage(usage)}
	}
	return cloned
}

func encodeOpenAITools(functions []schema.FunctionDefinition) []map[string]any {
	tools := make([]map[string]any, 0, len(functions))
	for _, function := range functions {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        function.Name,
				"description": function.Description,
				"parameters":  function.Parameters,
			},
		})
	}
	return tools
}

// HTTPStatusError preserves the upstream status for retry and observability
// policies while keeping the human-readable provider error intact.
type HTTPStatusError struct {
	Provider   string
	Status     int
	StatusText string
	Message    string
	// retryAfter 是响应 Retry-After 头解析出的建议等待；0 表示未提供。
	retryAfter time.Duration
}

func (e *HTTPStatusError) Error() string {
	status := e.StatusText
	if status == "" {
		status = fmt.Sprintf("HTTP %d", e.Status)
	}
	if e.Message == "" {
		return fmt.Sprintf("%s API returned %s", e.Provider, status)
	}
	return fmt.Sprintf("%s API returned %s: %s", e.Provider, status, e.Message)
}

func (e *HTTPStatusError) StatusCode() int { return e.Status }

// RetryAfter 返回服务端建议的重试等待；未提供或为 0 时重试器按退避计算。
func (e *HTTPStatusError) RetryAfter() time.Duration { return e.retryAfter }

// NewHTTPStatusError 供复用 llm 重试语义的非 ChatModel 客户端构造状态错误。
// retryAfter 传 0 表示上游未给建议。
func NewHTTPStatusError(provider string, status int, statusText, message string, retryAfter time.Duration) *HTTPStatusError {
	return &HTTPStatusError{Provider: provider, Status: status, StatusText: statusText, Message: message, retryAfter: retryAfter}
}

// ParseRetryAfter 解析 Retry-After 头（秒数或 HTTP 日期）；无法解析返回 0。
func ParseRetryAfter(value string) time.Duration { return parseRetryAfter(value) }

// readBoundedResponseBody 最多读取 limit+1 字节；超过 limit 即判定响应过大，
// 返回明确的 ResponseTooLargeError，截断数据绝不进入响应解码。
func readBoundedResponseBody(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read OpenAI-compatible response: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, &ResponseTooLargeError{Provider: "OpenAI-compatible", Limit: limit}
	}
	return data, nil
}

func providerHTTPError(response *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("OpenAI-compatible API returned %s (read error: %w)", response.Status, err)
	}
	body := strings.TrimSpace(string(data))
	return &HTTPStatusError{Provider: "OpenAI-compatible", Status: response.StatusCode, StatusText: response.Status, Message: body, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
}

// parseRetryAfter 解析 Retry-After 头的两种形式：秒数或 HTTP 日期；
// 无法解析或为负时返回 0。
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := time.Until(at); d > 0 {
			return d
		}
	}
	return 0
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func rawString(raw map[string]any, key string) string {
	if raw == nil {
		return ""
	}
	value, _ := raw[key].(string)
	return strings.TrimSpace(value)
}

func rawBool(raw map[string]any, key string) bool {
	if raw == nil {
		return false
	}
	value, _ := raw[key].(bool)
	return value
}

// rawInt64 读取配置文件中的整数值；容忍 JSON 数字常见的 float64。
func rawInt64(raw map[string]any, key string) int64 {
	if raw == nil {
		return 0
	}
	switch value := raw[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case float64:
		return int64(value)
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	}
	return 0
}

func rawHeaders(raw map[string]any) map[string]string {
	if raw == nil {
		return nil
	}
	headers := map[string]string{}
	switch values := raw["headers"].(type) {
	case map[string]string:
		for key, value := range values {
			headers[key] = value
		}
	case map[string]any:
		for key, value := range values {
			if text, ok := value.(string); ok {
				headers[key] = text
			}
		}
	}
	return headers
}
