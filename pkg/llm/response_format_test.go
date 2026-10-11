package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

var testSchemaBytes = json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false}`)

func TestResponseFormatCloneIndependent(t *testing.T) {
	format := llm.ResponseFormat{
		Name:        "answer",
		Description: "desc",
		Schema:      testSchemaBytes,
		Strict:      true,
	}
	cloned := format.Clone()
	cloned.Schema[0] = ' '
	cloned.Name = "changed"
	if string(format.Schema) != string(testSchemaBytes) || format.Name != "answer" {
		t.Fatalf("clone shares mutable state: %#v", format)
	}
}

func TestWithResponseFormatNilClears(t *testing.T) {
	resolved, _, err := llm.PrepareOptions([]runnable.Option{
		llm.WithResponseFormat(&llm.ResponseFormat{Name: "x", Schema: testSchemaBytes}),
		llm.WithResponseFormat(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ResponseFormat != nil {
		t.Fatalf("expected nil format, got %#v", resolved.ResponseFormat)
	}
}

func TestWithResponseFormatFreezesCallerValue(t *testing.T) {
	format := &llm.ResponseFormat{
		Name:   "answer",
		Schema: append(json.RawMessage(nil), testSchemaBytes...),
	}
	option := llm.WithResponseFormat(format)
	// 创建选项后修改原值，已冻结格式不受影响。
	format.Name = "mutated"
	format.Schema[0] = ' '

	resolved, _, err := llm.PrepareOptions([]runnable.Option{option})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ResponseFormat == nil || resolved.ResponseFormat.Name != "answer" || string(resolved.ResponseFormat.Schema) != string(testSchemaBytes) {
		t.Fatalf("resolved format = %#v", resolved.ResponseFormat)
	}
	// 应用后再修改解析结果的 Schema，不影响下一次应用。
	resolved.ResponseFormat.Name = "mutated-again"
	again, _, err := llm.PrepareOptions([]runnable.Option{option})
	if err != nil {
		t.Fatal(err)
	}
	if again.ResponseFormat.Name != "answer" {
		t.Fatalf("resolved format mutated across applications: %#v", again.ResponseFormat)
	}
}

func TestOptionsCloneCopiesResponseFormat(t *testing.T) {
	opts := llm.Options{ResponseFormat: &llm.ResponseFormat{Name: "a", Schema: testSchemaBytes}}
	cloned := opts.Clone()
	if cloned.ResponseFormat == opts.ResponseFormat {
		t.Fatal("clone shares ResponseFormat pointer")
	}
	cloned.ResponseFormat.Name = "b"
	if opts.ResponseFormat.Name != "a" {
		t.Fatalf("clone mutated original: %#v", opts.ResponseFormat)
	}
}

func newFormatFixture(t *testing.T, onRequest func(map[string]any)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if onRequest != nil {
			onRequest(payload)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"{\"a\":\"ok\"}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	t.Cleanup(server.Close)
	return server
}

func formatModel(t *testing.T, server *httptest.Server, capabilities *llm.Capabilities) llm.ChatModel {
	t.Helper()
	model, err := llm.New(llm.Config{
		ModelType:    "openai-compatible",
		Model:        "test-model",
		ModelServer:  server.URL,
		HTTPClient:   server.Client(),
		Capabilities: capabilities,
	})
	if err != nil {
		t.Fatal(err)
	}
	return model
}

func TestOpenAICompatibleResponseFormatWireShape(t *testing.T) {
	var payload map[string]any
	server := newFormatFixture(t, func(p map[string]any) { payload = p })
	model := formatModel(t, server, &llm.Capabilities{Streaming: true, ToolCalls: true, JSONSchema: true})

	format := &llm.ResponseFormat{
		Name:        "answer",
		Description: "structured answer",
		Schema:      testSchemaBytes,
		Strict:      true,
	}
	_, err := model.Invoke(context.Background(), llm.Request{
		Messages: []schema.Message{schema.UserMessage("q")},
	}, llm.WithResponseFormat(format))
	if err != nil {
		t.Fatal(err)
	}
	rf, ok := payload["response_format"].(map[string]any)
	if !ok || rf["type"] != "json_schema" {
		t.Fatalf("response_format = %#v", payload["response_format"])
	}
	js, ok := rf["json_schema"].(map[string]any)
	if !ok || js["name"] != "answer" || js["strict"] != true || js["description"] != "structured answer" {
		t.Fatalf("json_schema = %#v", js)
	}
	var wantSchema any
	if err := json.Unmarshal(testSchemaBytes, &wantSchema); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(js["schema"], wantSchema) {
		t.Fatalf("schema = %#v", js["schema"])
	}
}

func TestOpenAICompatibleResponseFormatOmittedByDefault(t *testing.T) {
	var payload map[string]any
	server := newFormatFixture(t, func(p map[string]any) { payload = p })
	model := formatModel(t, server, nil)
	if _, err := model.Invoke(context.Background(), llm.Request{}); err != nil {
		t.Fatal(err)
	}
	if _, exists := payload["response_format"]; exists {
		t.Fatalf("response_format present without typed format: %#v", payload)
	}
}

func TestOpenAICompatibleResponseFormatCapabilityRejected(t *testing.T) {
	server := newFormatFixture(t, nil)
	// 显式全 false 声明不应回落为默认能力。
	model := formatModel(t, server, &llm.Capabilities{})
	if model.Capabilities().JSONSchema {
		t.Fatal("explicit all-false capabilities fell back to defaults")
	}
	_, err := model.Invoke(context.Background(), llm.Request{}, llm.WithResponseFormat(&llm.ResponseFormat{
		Name:   "a",
		Schema: testSchemaBytes,
	}))
	if !errors.Is(err, llm.ErrResponseFormatUnsupported) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAICompatibleResponseFormatDefaultCapabilityOff(t *testing.T) {
	server := newFormatFixture(t, nil)
	model := formatModel(t, server, nil)
	if model.Capabilities().JSONSchema {
		t.Fatal("default JSONSchema capability should be false")
	}
	_, err := model.Invoke(context.Background(), llm.Request{}, llm.WithResponseFormat(&llm.ResponseFormat{
		Name:   "a",
		Schema: testSchemaBytes,
	}))
	if !errors.Is(err, llm.ErrResponseFormatUnsupported) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAICompatibleResponseFormatGenerateConfigConflict(t *testing.T) {
	server := newFormatFixture(t, nil)
	model, err := llm.New(llm.Config{
		ModelType:    "openai-compatible",
		Model:        "test-model",
		ModelServer:  server.URL,
		HTTPClient:   server.Client(),
		Capabilities: &llm.Capabilities{JSONSchema: true},
		GenerateConfig: llm.GenerateConfig{
			"response_format": map[string]any{"type": "json_object"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), llm.Request{}, llm.WithResponseFormat(&llm.ResponseFormat{
		Name:   "a",
		Schema: testSchemaBytes,
	}))
	if !errors.Is(err, llm.ErrResponseFormatConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAICompatibleResponseFormatExtraConfigConflict(t *testing.T) {
	server := newFormatFixture(t, nil)
	model := formatModel(t, server, &llm.Capabilities{JSONSchema: true})
	_, err := model.Invoke(context.Background(), llm.Request{},
		llm.WithResponseFormat(&llm.ResponseFormat{Name: "a", Schema: testSchemaBytes}),
		llm.WithGenerateConfig(map[string]any{"response_format": map[string]any{"type": "json_object"}}),
	)
	if !errors.Is(err, llm.ErrResponseFormatConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenAICompatibleRefusalPreservedInMessageExtra(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":null,"refusal":"cannot comply"},"finish_reason":"stop"}]}`))
	}))
	t.Cleanup(server.Close)
	model := formatModel(t, server, nil)
	response, err := model.Invoke(context.Background(), llm.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Messages) != 1 {
		t.Fatalf("messages = %#v", response.Messages)
	}
	if response.Messages[0].Extra["llm.refusal"] != "cannot comply" {
		t.Fatalf("refusal extra = %#v", response.Messages[0].Extra)
	}
}

func TestStrictFormatRejectsCompositeBypass(t *testing.T) {
	requests := 0
	server := newFormatFixture(t, func(map[string]any) { requests++ })
	model := formatModel(t, server, &llm.Capabilities{Streaming: true, ToolCalls: true, JSONSchema: true})

	cases := map[string]string{
		"anyOf non-strict object": `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false,"anyOf":[{"type":"object","properties":{"b":{"type":"string"}},"required":["b"]}]}`,
		"nullable object union":   `{"type":["object","null"],"properties":{"a":{"type":"string"}},"required":["a"]}`,
		"oneOf missing required":  `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false,"oneOf":[{"type":"object","properties":{"b":{"type":"string"}},"additionalProperties":false}]}`,
		"defs non-strict object":  `{"type":"object","properties":{"a":{"$ref":"#/$defs/x"}},"required":["a"],"additionalProperties":false,"$defs":{"x":{"type":"object","properties":{"y":{"type":"string"}}}}}`,
		"tuple items non-strict":  `{"type":"object","properties":{"a":{"type":"array","prefixItems":[{"type":"object","properties":{"y":{"type":"string"}}}]}},"required":["a"],"additionalProperties":false}`,
	}
	for name, raw := range cases {
		requests = 0
		format := &llm.ResponseFormat{Name: "f", Schema: json.RawMessage(raw), Strict: true}
		_, err := model.Invoke(context.Background(), llm.Request{}, llm.WithResponseFormat(format))
		if err == nil {
			t.Fatalf("%s: non-strict composite schema accepted", name)
		}
		if requests != 0 {
			t.Fatalf("%s: invalid format reached the wire", name)
		}
	}
}

func TestStrictFormatAcceptsValidUnions(t *testing.T) {
	requests := 0
	server := newFormatFixture(t, func(map[string]any) { requests++ })
	model := formatModel(t, server, &llm.Capabilities{Streaming: true, ToolCalls: true, JSONSchema: true})

	cases := map[string]string{
		"nullable strict object": `{"type":["object","null"],"properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false}`,
		"anyOf strict objects":   `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false,"anyOf":[{"type":"object","properties":{"b":{"type":"string"}},"required":["b"],"additionalProperties":false}]}`,
		"anyOf mixed types":      `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"],"additionalProperties":false,"anyOf":[{"type":"string"},{"type":"object","properties":{"b":{"type":"string"}},"required":["b"],"additionalProperties":false}]}`,
	}
	for name, raw := range cases {
		requests = 0
		format := &llm.ResponseFormat{Name: "f", Schema: json.RawMessage(raw), Strict: true}
		if _, err := model.Invoke(context.Background(), llm.Request{}, llm.WithResponseFormat(format)); err != nil {
			t.Fatalf("%s: valid strict composite rejected: %v", name, err)
		}
		if requests != 1 {
			t.Fatalf("%s: expected one request, got %d", name, requests)
		}
	}
}
