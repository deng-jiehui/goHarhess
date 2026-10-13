package llm_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

// captureChatServer 捕获一次 /chat/completions 请求体并返回最小合法响应。
func captureChatServer(t *testing.T, payload *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(payload); err != nil {
			t.Fatal(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
}

func visionModel(t *testing.T, baseURL string, vision bool) *llm.OpenAICompatibleModel {
	t.Helper()
	model, err := llm.NewOpenAICompatibleModel(llm.OpenAICompatibleConfig{
		Model:        "m",
		BaseURL:      baseURL,
		Capabilities: llm.Capabilities{Streaming: true, ToolCalls: true, Vision: vision},
	})
	if err != nil {
		t.Fatal(err)
	}
	return model
}

func TestOpenAICompatibleEncodesMixedTextAndImageInOrder(t *testing.T) {
	var payload map[string]any
	server := captureChatServer(t, &payload)
	defer server.Close()
	model := visionModel(t, server.URL, true)
	message := schema.NewMessage(schema.RoleUser,
		schema.NewTextBlock("before "),
		schema.NewImageBlock("https://cdn.test/a.png", "image/png"),
		schema.NewTextBlock(" after"),
		schema.NewImageBlock("", "image/jpeg"))
	message.ContentBlocks[3].Image.Data = json.RawMessage(strconv.Quote(base64.StdEncoding.EncodeToString([]byte("PNG"))))
	_, err := model.Invoke(context.Background(), llm.Request{Messages: []schema.Message{message}})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := payload["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("messages = %#v", payload["messages"])
	}
	content, _ := messages[0].(map[string]any)["content"].([]any)
	if len(content) != 4 {
		t.Fatalf("content = %#v", messages[0])
	}
	if content[0].(map[string]any)["type"] != "text" || content[0].(map[string]any)["text"] != "before " {
		t.Fatalf("part0 = %#v", content[0])
	}
	part1 := content[1].(map[string]any)
	if part1["type"] != "image_url" || part1["image_url"].(map[string]any)["url"] != "https://cdn.test/a.png" {
		t.Fatalf("part1 = %#v", part1)
	}
	if content[2].(map[string]any)["text"] != " after" {
		t.Fatalf("part2 = %#v", content[2])
	}
	want := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString([]byte("PNG"))
	if got := content[3].(map[string]any)["image_url"].(map[string]any)["url"]; got != want {
		t.Fatalf("part3 url = %q, want %q", got, want)
	}
}

func TestOpenAICompatiblePureTextContentStaysString(t *testing.T) {
	var payload map[string]any
	server := captureChatServer(t, &payload)
	defer server.Close()
	model := visionModel(t, server.URL, true)
	_, err := model.Invoke(context.Background(), llm.Request{Messages: []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewTextBlock("plain")),
	}})
	if err != nil {
		t.Fatal(err)
	}
	messages, _ := payload["messages"].([]any)
	if _, ok := messages[0].(map[string]any)["content"].(string); !ok {
		t.Fatalf("content = %#v, want string form", messages[0])
	}
}

func TestOpenAICompatibleRejectsImageWithoutVision(t *testing.T) {
	server := captureChatServer(t, new(map[string]any))
	defer server.Close()
	model := visionModel(t, server.URL, false)
	_, err := model.Invoke(context.Background(), llm.Request{Messages: []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewImageBlock("https://cdn.test/a.png", "image/png")),
	}})
	if !errors.Is(err, llm.ErrVisionUnsupported) {
		t.Fatalf("err = %v, want ErrVisionUnsupported", err)
	}
}

func TestOpenAICompatibleRejectsUnresolvedLocalImageURI(t *testing.T) {
	server := captureChatServer(t, new(map[string]any))
	defer server.Close()
	model := visionModel(t, server.URL, true)
	_, err := model.Invoke(context.Background(), llm.Request{Messages: []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewImageBlock("file:///tmp/a.png", "image/png")),
	}})
	if err == nil || !strings.Contains(err.Error(), "file:///tmp/a.png") {
		t.Fatalf("err = %v, want URI in message", err)
	}
}

func TestOpenAICompatibleRejectsImageInToolResult(t *testing.T) {
	server := captureChatServer(t, new(map[string]any))
	defer server.Close()
	model := visionModel(t, server.URL, true)
	result := schema.NewMessage(schema.RoleTool, schema.ContentBlock{
		Type: schema.ContentBlockTypeFunctionToolResult,
		FunctionToolResult: &schema.FunctionToolResult{
			CallID:        "c1",
			ContentBlocks: []schema.ContentBlock{schema.NewImageBlock("https://cdn.test/a.png", "image/png")},
		},
	})
	_, err := model.Invoke(context.Background(), llm.Request{Messages: []schema.Message{result}})
	if err == nil || !strings.Contains(err.Error(), "tool") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAICompatibleImageEncodingSharedByStreamAndInvoke(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		var payload map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if streaming {
				writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
		}))
		model := visionModel(t, server.URL, true)
		request := llm.Request{Messages: []schema.Message{
			schema.NewMessage(schema.RoleUser, schema.NewTextBlock("look"), schema.NewImageBlock("https://cdn.test/a.png", "image/png")),
		}}
		if streaming {
			reader, err := model.Stream(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := reader.Recv(); err != nil {
					break
				}
			}
		} else if _, err := model.Invoke(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		server.Close()
		messages, _ := payload["messages"].([]any)
		content, _ := messages[0].(map[string]any)["content"].([]any)
		if len(content) != 2 || content[1].(map[string]any)["type"] != "image_url" {
			t.Fatalf("streaming=%v content = %#v", streaming, content)
		}
	}
}
