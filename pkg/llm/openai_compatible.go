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
	"strings"

	"goharness/pkg/httpclient"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const (
	defaultOpenAIBaseURL    = "https://api.openai.com/v1"
	defaultDeepSeekBaseURL  = "https://api.deepseek.com"
	defaultDashScopeBaseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
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
		return NewOpenAICompatibleModel(OpenAICompatibleConfig{
			Model:                   cfg.Model,
			BaseURL:                 baseURL,
			APIKey:                  cfg.APIKey,
			GenerateConfig:          cfg.GenerateConfig,
			Client:                  cfg.HTTPClient,
			Headers:                 rawHeaders(cfg.Raw),
			IncludeReasoningContent: reasoning || rawBool(cfg.Raw, "include_reasoning_content"),
		})
	}
}

// NewOpenAICompatibleModel creates a model using a reusable HTTP client. A
// nil client receives the framework's pooled default client.
func NewOpenAICompatibleModel(cfg OpenAICompatibleConfig) (*OpenAICompatibleModel, error) {
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, errors.New("OpenAI-compatible model requires a model name")
	}
	endpoint, err := chatCompletionsEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	capabilities := cfg.Capabilities
	if capabilities == (Capabilities{}) {
		capabilities = Capabilities{
			Streaming:  true,
			ToolCalls:  true,
			JSONSchema: true,
			Reasoning:  cfg.IncludeReasoningContent,
		}
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

func (m *OpenAICompatibleModel) request(ctx context.Context, input Request, streaming bool) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("OpenAI-compatible model context cannot be nil")
	}
	if streaming && !m.capabilities.Streaming {
		return nil, errors.New("OpenAI-compatible model does not support streaming")
	}
	if len(input.Options.Functions) > 0 && !m.capabilities.ToolCalls {
		return nil, errors.New("OpenAI-compatible model does not support tool calls")
	}

	payload, err := m.requestPayload(input.Messages, input.Options, streaming)
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

func (m *OpenAICompatibleModel) Invoke(ctx context.Context, input Request, _ ...runnable.Option) (Response, error) {
	response, err := m.request(ctx, input, false)
	if err != nil {
		return Response{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return Response{}, fmt.Errorf("read OpenAI-compatible response: %w", err)
	}
	messages, usage, finishReason, err := decodeOpenAIResponse(data, false)
	if err != nil {
		return Response{}, err
	}
	return Response{Messages: messages, Usage: usage, FinishReason: finishReason}, nil
}

var _ ChatModel = (*OpenAICompatibleModel)(nil)

func (m *OpenAICompatibleModel) Stream(ctx context.Context, input Request, _ ...runnable.Option) (*runnable.StreamReader[Response], error) {
	streamCtx, cancel := context.WithCancel(ctx)
	response, err := m.request(streamCtx, input, true)
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

func (m *OpenAICompatibleModel) requestPayload(messages []schema.Message, opts ChatOptions, streaming bool) (map[string]any, error) {
	payload := map[string]any{}
	for key, value := range MergeGenerateConfig(m.generateConfig, opts.ExtraGenerateConfig) {
		payload[key] = value
	}
	payload["model"] = m.model
	encodedMessages, err := encodeOpenAIMessages(messages, m.includeReasoningContent)
	if err != nil {
		return nil, fmt.Errorf("encode OpenAI-compatible messages: %w", err)
	}
	payload["messages"] = encodedMessages
	payload["stream"] = streaming
	if len(opts.Functions) > 0 && payload["tools"] == nil && payload["functions"] == nil {
		payload["tools"] = encodeOpenAITools(opts.Functions)
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
	if indexed && contentArray {
		if value.FunctionCall != nil || len(value.ToolCalls) > 0 {
			return nil, errors.New("stream delta content array cannot be combined with function_call or tool_calls")
		}
		base.Extra = map[string]any{openAIAppendBlocksExtraKey: len(blocks)}
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

func encodeOpenAIMessages(messages []schema.Message, includeReasoning bool) ([]map[string]any, error) {
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
		item["content"] = text.String()
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
					return nil, fmt.Errorf("messages[%d].content_blocks[%d]: unsupported function tool result block type %q", messageIndex, blockIndex, block.Type)
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

func providerHTTPError(response *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("OpenAI-compatible API returned %s (read error: %w)", response.Status, err)
	}
	body := strings.TrimSpace(string(data))
	return &HTTPStatusError{Provider: "OpenAI-compatible", Status: response.StatusCode, StatusText: response.Status, Message: body}
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
