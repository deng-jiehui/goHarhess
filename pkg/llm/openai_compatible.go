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
	return func(cfg Config) (Model, error) {
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

	payload := m.requestPayload(input.Messages, input.Options, streaming)
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

var _ Model = (*OpenAICompatibleModel)(nil)

func (m *OpenAICompatibleModel) Stream(ctx context.Context, input Request, _ ...runnable.Option) (runnable.Stream[Event], error) {
	response, err := m.request(ctx, input, true)
	if err != nil {
		return nil, err
	}
	events := m.streamEvents(ctx, response.Body)
	if !input.Options.DeltaStream {
		events = snapshotWireEvents(ctx, events)
	}
	return standardizeWireEvents(ctx, events), nil
}

func (m *OpenAICompatibleModel) requestPayload(messages []schema.Message, opts ChatOptions, streaming bool) map[string]any {
	payload := map[string]any{}
	for key, value := range MergeGenerateConfig(m.generateConfig, opts.ExtraGenerateConfig) {
		payload[key] = value
	}
	payload["model"] = m.model
	payload["messages"] = encodeOpenAIMessages(messages, m.includeReasoningContent)
	payload["stream"] = streaming
	if len(opts.Functions) > 0 && payload["tools"] == nil && payload["functions"] == nil {
		payload["tools"] = encodeOpenAITools(opts.Functions)
	}
	return payload
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
			if len(messages) > 0 || usage != nil {
				kind := wireEventDelta
				if len(messages) == 0 {
					kind = wireEventUsage
				}
				if !sendWireEvent(ctx, out, wireEvent{
					kind:         kind,
					messages:     messages,
					usage:        usage,
					finishReason: finishReason,
				}) {
					return
				}
			}
		}
		sendWireEvent(ctx, out, wireEvent{kind: wireEventDone})
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
	Choices []openAIChoice `json:"choices"`
	Usage   *openAIUsage   `json:"usage,omitempty"`
}

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
	return messages, decodeUsage(response.Usage), finishReason, nil
}

func openAIMessageToSchema(value openAIMessage, indexed bool) ([]schema.Message, error) {
	role := parseRole(value.Role)
	content := flexibleText(value.Content)
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
	if role == "" && (content != "" || reasoning != "" || len(value.ToolCalls) > 0 || value.FunctionCall != nil) {
		role = schema.RoleAssistant
	}
	base := schema.Message{Role: role, Content: schema.TextContent(content), ReasoningContent: schema.TextContent(reasoning)}
	if value.Name != "" {
		base.Name = value.Name
	}

	if value.FunctionCall != nil {
		call := &schema.FunctionCall{Name: value.FunctionCall.Name, Arguments: value.FunctionCall.Arguments}
		base.FunctionCall = call
		return []schema.Message{base}, nil
	}
	if len(value.ToolCalls) == 0 {
		if base.Role == "" && base.Content.IsZero() && base.ReasoningContent.IsZero() {
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
			if _, exists := seen[toolCall.Index]; exists {
				return nil, fmt.Errorf("duplicate tool call index %d", toolCall.Index)
			}
			seen[toolCall.Index] = struct{}{}
			highest = max(highest, toolCall.Index)
		}
	}
	messages := make([]schema.Message, highest+1)
	messages[0] = base
	for order, toolCall := range value.ToolCalls {
		index := order
		if indexed {
			index = toolCall.Index
		}
		target := messages[index]
		if target.Role == "" {
			target.Role = schema.RoleAssistant
		}
		target.FunctionCall = &schema.FunctionCall{
			Name:      toolCall.Function.Name,
			Arguments: toolCall.Function.Arguments,
		}
		if toolCall.ID != "" {
			if target.Extra == nil {
				target.Extra = map[string]any{}
			}
			target.Extra["function_id"] = toolCall.ID
		}
		messages[index] = target
	}
	return messages, nil
}

func parseRole(value string) schema.Role {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "system":
		return schema.RoleSystem
	case "user":
		return schema.RoleUser
	case "assistant":
		return schema.RoleAssistant
	case "tool", "function":
		return schema.RoleFunction
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
		PromptTokens:     value.PromptTokens,
		CompletionTokens: value.CompletionTokens,
		TotalTokens:      value.TotalTokens,
		ReasoningTokens:  value.CompletionTokenDetails.ReasoningTokens,
	}
}

func encodeOpenAIMessages(messages []schema.Message, includeReasoning bool) []map[string]any {
	encoded := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		role := string(message.Role)
		if message.Role == schema.RoleFunction {
			role = "tool"
		}
		item := map[string]any{
			"role":    role,
			"content": message.Content,
		}
		if message.Name != "" {
			item["name"] = message.Name
		}
		if message.Role == schema.RoleFunction {
			item["tool_call_id"] = message.FunctionID("")
		}
		if includeReasoning && !message.ReasoningContent.IsZero() {
			item["reasoning_content"] = message.ReasoningContent
		}
		if message.FunctionCall != nil {
			item["tool_calls"] = []map[string]any{{
				"id":   message.FunctionID("call-1"),
				"type": "function",
				"function": map[string]any{
					"name":      message.FunctionCall.Name,
					"arguments": message.FunctionCall.Arguments,
				},
			}}
		}
		encoded = append(encoded, item)
	}
	return encoded
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
