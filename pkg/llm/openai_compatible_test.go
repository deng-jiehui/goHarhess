package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

func TestOpenAICompatibleStreamingStandardizesReasoningAndToolDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["model"] != "deepseek-chat" || payload["stream"] != true {
			t.Fatalf("payload = %#v", payload)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"id\":\"resp-stream\",\"model\":\"deepseek-chat\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think \"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"step\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: " + `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"search","arguments":"{\"text\":\"hel"}}]}}]}` + "\n\n"))
		_, _ = writer.Write([]byte("data: " + `{"choices":[{"index":0,"finish_reason":"tool_calls","delta":{"tool_calls":[{"index":0,"function":{"arguments":"lo\"}"}}]}}]}` + "\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"total_tokens\":7,\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	model, err := llm.New(llm.Config{
		ModelType:   "deepseek",
		Model:       "deepseek-chat",
		ModelServer: server.URL + "/v1",
		APIKey:      "test-key",
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), llm.Request{
		Messages: []schema.Message{schema.UserMessage("hello")},
	}, llm.WithDeltaStream(true))
	if err != nil {
		t.Fatal(err)
	}
	response, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	messages := response.Messages
	if len(messages) != 1 {
		t.Fatalf("messages = %#v, want one normalized assistant message", messages)
	}
	if messages[0].JoinedText("") != "answer" {
		t.Fatalf("content = %q", messages[0].JoinedText(""))
	}
	reasoning := messages[0].ReasoningBlocks()
	if len(reasoning) != 1 || reasoning[0].Text != "think step" {
		t.Fatalf("reasoning = %#v", reasoning)
	}
	calls := messages[0].FunctionToolCalls()
	if len(calls) != 1 || calls[0].ID != "call-1" || calls[0].Name != "search" || calls[0].Arguments != `{"text":"hello"}` {
		t.Fatalf("function calls = %#v", calls)
	}
	if response.Usage == nil || response.Usage.TotalTokens != 7 || response.Usage.ReasoningTokens != 2 {
		t.Fatalf("usage = %#v", response.Usage)
	}
	meta := messages[0].ResponseMeta
	if meta == nil || meta.ID != "resp-stream" || meta.Model != "deepseek-chat" || meta.FinishReason != "tool_calls" || meta.Usage == nil || meta.Usage.TotalTokens != 7 {
		t.Fatalf("response meta = %#v", meta)
	}
}

func TestOpenAICompatibleNonStreamingAndCapabilities(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok","thinking":"private thought"}}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	defer server.Close()

	model, err := llm.New(llm.Config{
		ModelType:   "openai-compatible",
		Model:       "test-model",
		ModelServer: server.URL,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := model.Capabilities()
	if !capabilities.Streaming || !capabilities.ToolCalls || !capabilities.JSONSchema {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	response, err := model.Invoke(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Messages) != 1 {
		t.Fatalf("response = %#v", response)
	}
	reasoning := response.Messages[0].ReasoningBlocks()
	if response.Messages[0].JoinedText("") != "ok" || len(reasoning) != 1 || reasoning[0].Text != "private thought" {
		t.Fatalf("message = %#v", response.Messages[0])
	}
	if response.Usage == nil || response.Usage.TotalTokens != 3 {
		t.Fatalf("usage = %#v", response.Usage)
	}
}

func TestOpenAICompatibleCanUseCustomHeaders(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotHeader = request.Header.Get("X-Provider")
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[]}`))
	}))
	defer server.Close()

	model, err := llm.New(llm.Config{
		ModelType:   "openai-compatible",
		Model:       "model",
		ModelServer: server.URL,
		Raw:         map[string]any{"headers": map[string]string{"X-Provider": "test"}},
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(gotHeader) != "test" {
		t.Fatalf("custom header = %q", gotHeader)
	}
}

func TestOpenAICompatibleStreamingPreservesSparseToolCallIndexes(t *testing.T) {
	frames := []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-0","function":{"name":"first","arguments":"{\"value\":\""}},{"index":1,"id":"call-1","function":{"name":"second","arguments":"{\"value\":\""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"two\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"one\"}"}}]}}]}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		for _, frame := range frames {
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", frame)
		}
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
		Model: "indexed", BaseURL: server.URL, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), llm.Request{}, llm.WithDeltaStream(true))
	if err != nil {
		t.Fatal(err)
	}
	response, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	messages := response.Messages
	if len(messages) != 1 {
		t.Fatalf("messages=%#v", messages)
	}
	calls := messages[0].FunctionToolCalls()
	if len(calls) != 2 || calls[0].ID != "call-0" || calls[0].Name != "first" || calls[0].Arguments != `{"value":"one"}` {
		t.Fatalf("first call=%#v", calls)
	}
	if calls[1].ID != "call-1" || calls[1].Name != "second" || calls[1].Arguments != `{"value":"two"}` {
		t.Fatalf("second call=%#v", calls)
	}
}

func TestOpenAICompatibleStreamingRejectsEOFBeforeDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n"))
	}))
	defer server.Close()

	model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
		Model: "incomplete-stream", BaseURL: server.URL, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), llm.Request{}, llm.WithDeltaStream(true))
	if err != nil {
		t.Fatal(err)
	}
	_, streamErr := stream.Collect(context.Background())
	if streamErr == nil || !strings.Contains(streamErr.Error(), "before [DONE]") {
		t.Fatalf("stream error=%v", streamErr)
	}
}

func TestOpenAICompatibleRejectsMultipleChoices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"index":1,"message":{"role":"assistant","content":"alternate"}}]}`))
	}))
	defer server.Close()
	model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
		Model: "single-choice", BaseURL: server.URL, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), llm.Request{})
	if err == nil || !strings.Contains(err.Error(), "choice index 1") {
		t.Fatalf("choice error=%v", err)
	}
}

func TestOpenAICompatibleRejectsInvalidToolCallIndexes(t *testing.T) {
	cases := map[string]string{
		"negative":  `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":-1,"function":{"name":"bad","arguments":"{}"}}]}}]}`,
		"duplicate": `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"first","arguments":"{}"}},{"index":0,"function":{"name":"second","arguments":"{}"}}]}}]}`,
		"oversized": `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1000000,"function":{"name":"bad","arguments":"{}"}}]}}]}`,
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(writer, "data: %s\n\ndata: [DONE]\n\n", frame)
			}))
			defer server.Close()
			model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
				Model: "invalid-index", BaseURL: server.URL, Client: server.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := model.Stream(context.Background(), llm.Request{}, llm.WithDeltaStream(true))
			if err != nil {
				t.Fatal(err)
			}
			_, streamErr := stream.Collect(context.Background())
			if streamErr == nil || !strings.Contains(streamErr.Error(), "tool call index") {
				t.Fatalf("stream error=%v", streamErr)
			}
		})
	}
}

// 非流式响应超过配置的字节上限时停止读取，返回明确的过大错误，
// 截断内容不会被当作完整响应解析。
func TestOpenAICompatibleRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		// 有效 JSON 前缀 + 超长填充：即使截断后仍可被解码的内容也绝不能进入解析。
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"`))
		_, _ = writer.Write([]byte(strings.Repeat("x", 4096)))
		_, _ = writer.Write([]byte(`"}}]}`))
	}))
	defer server.Close()

	model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
		Model: "test-model", BaseURL: server.URL, Client: server.Client(),
		MaxResponseBytes: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), llm.Request{})
	var tooLarge *llm.ResponseTooLargeError
	if !errors.As(err, &tooLarge) || !errors.Is(err, llm.ErrResponseTooLarge) {
		t.Fatalf("err=%v", err)
	}
	if tooLarge.Limit != 64 {
		t.Fatalf("limit=%d", tooLarge.Limit)
	}
	if !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("error lacks size hint: %v", err)
	}
}

// 上限边界：恰好等于上限通过，超过一字节即失败；上限可配置。
func TestOpenAICompatibleResponseLimitBoundary(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(body)
	}))
	defer server.Close()

	build := func(limit int64) *llm.OpenAICompatibleModel {
		model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
			Model: "test-model", BaseURL: server.URL, Client: server.Client(),
			MaxResponseBytes: limit,
		})
		if err != nil {
			t.Fatal(err)
		}
		return model
	}
	response, err := build(int64(len(body))).Invoke(context.Background(), llm.Request{})
	if err != nil || len(response.Messages) != 1 {
		t.Fatalf("at-limit response failed: %v", err)
	}
	if _, err := build(int64(len(body))-1).Invoke(context.Background(), llm.Request{}); !errors.Is(err, llm.ErrResponseTooLarge) {
		t.Fatalf("over-limit err=%v", err)
	}
	// 零值回落默认上限，正常响应不受影响。
	response, err = build(0).Invoke(context.Background(), llm.Request{})
	if err != nil || response.Messages[0].JoinedText("") != "ok" {
		t.Fatalf("default-limit response failed: %v", err)
	}
}
