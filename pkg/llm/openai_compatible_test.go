package llm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"goharness/pkg/llm"
	"goharness/pkg/schema"
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
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think \"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"step\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: " + `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"search","arguments":"hel"}}]}}]}` + "\n\n"))
		_, _ = writer.Write([]byte("data: " + `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"lo"}}]}}]}` + "\n\n"))
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
		Options:  llm.ChatOptions{DeltaStream: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	accumulator := llm.NewDeltaAccumulator(true)
	var usage *llm.Usage
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Usage != nil {
			usage = item.Value.Usage
		}
		if item.Value.Kind == llm.EventDelta && len(item.Value.Messages) > 0 {
			if _, err := accumulator.Apply(item.Value); err != nil {
				t.Fatal(err)
			}
		}
	}
	messages := accumulator.Messages()
	if len(messages) != 1 {
		t.Fatalf("messages = %#v, want one normalized assistant message", messages)
	}
	if messages[0].Text() != "answer" {
		t.Fatalf("content = %q", messages[0].Text())
	}
	if messages[0].ReasoningContent.String() != "think step" {
		t.Fatalf("reasoning = %q", messages[0].ReasoningContent.String())
	}
	if messages[0].FunctionCall == nil || messages[0].FunctionCall.Name != "search" || messages[0].FunctionCall.Arguments != "hello" {
		t.Fatalf("function call = %#v", messages[0].FunctionCall)
	}
	if messages[0].FunctionID("") != "call-1" {
		t.Fatalf("function id = %q", messages[0].FunctionID(""))
	}
	if usage == nil || usage.TotalTokens != 7 || usage.ReasoningTokens != 2 {
		t.Fatalf("usage = %#v", usage)
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
	if response.Messages[0].Text() != "ok" || response.Messages[0].ReasoningContent.String() != "private thought" {
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
	stream, err := model.Stream(context.Background(), llm.Request{Options: llm.ChatOptions{DeltaStream: true}})
	if err != nil {
		t.Fatal(err)
	}
	accumulator := llm.NewDeltaAccumulator(true)
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind == llm.EventDelta {
			if _, err := accumulator.Apply(item.Value); err != nil {
				t.Fatal(err)
			}
		}
	}
	messages := accumulator.Messages()
	if len(messages) != 2 {
		t.Fatalf("messages=%#v", messages)
	}
	if messages[0].FunctionCall == nil || messages[0].FunctionCall.Name != "first" || messages[0].FunctionCall.Arguments != `{"value":"one"}` || messages[0].FunctionID("") != "call-0" {
		t.Fatalf("first call=%#v", messages[0])
	}
	if messages[1].FunctionCall == nil || messages[1].FunctionCall.Name != "second" || messages[1].FunctionCall.Arguments != `{"value":"two"}` || messages[1].FunctionID("") != "call-1" {
		t.Fatalf("second call=%#v", messages[1])
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
			stream, err := model.Stream(context.Background(), llm.Request{Options: llm.ChatOptions{DeltaStream: true}})
			if err != nil {
				t.Fatal(err)
			}
			var streamErr error
			for item := range stream {
				if item.Err != nil {
					streamErr = item.Err
				}
			}
			if streamErr == nil || !strings.Contains(streamErr.Error(), "tool call index") {
				t.Fatalf("stream error=%v", streamErr)
			}
		})
	}
}
