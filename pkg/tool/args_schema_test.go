package tool_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func paramsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "minLength": 1},
			"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 100},
			"tags":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"required":             []any{"query"},
		"additionalProperties": false,
	}
}

func assistantCall(id, name, args string) []schema.Message {
	return []schema.Message{
		schema.NewMessage(schema.RoleUser, schema.NewTextBlock("go")),
		schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, name, args)),
	}
}

// 参数不满足 schema 时不调用工具本体，返回 isError 结果供模型自纠。
func TestToolsNodeRejectsArgsViolatingSchema(t *testing.T) {
	var calls atomic.Int32
	search := tool.NewFunctionTool(tool.Spec{Name: "search", Parameters: paramsSchema()}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{search}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		args string
	}{
		{"missing required", `{"limit": 3}`},
		{"wrong type", `{"query": "q", "limit": "three"}`},
		{"out of range", `{"query": "q", "limit": 0}`},
		{"additional property", `{"query": "q", "admin": true}`},
		{"bad nested item", `{"query": "q", "tags": ["a", 1]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			out, err := node.Invoke(context.Background(), assistantCall("call-1", "search", tc.args))
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != before {
				t.Fatal("tool handler ran with invalid arguments")
			}
			if len(out) != 1 {
				t.Fatalf("messages=%d", len(out))
			}
			result := out[0].ContentBlocks[0].FunctionToolResult
			if result == nil || !result.IsError {
				t.Fatalf("expected isError result, got %#v", out[0].ContentBlocks[0])
			}
			text := result.ContentBlocks[0].Text.Text
			if !strings.Contains(text, "schema") {
				t.Fatalf("error message lacks schema hint: %q", text)
			}
		})
	}
}

// 合法参数照常执行，Args 原样到达 handler。
func TestToolsNodeExecutesArgsMatchingSchema(t *testing.T) {
	search := tool.NewFunctionTool(tool.Spec{Name: "search", Parameters: paramsSchema()}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
		if input.Args["query"] != "go" || input.Args["limit"] != float64(5) {
			return tool.Result{}, errors.New("unexpected args")
		}
		return tool.TextResult("done"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{search}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(context.Background(), assistantCall("call-1", "search", `{"query":"go","limit":5}`))
	if err != nil {
		t.Fatal(err)
	}
	result := out[0].ContentBlocks[0].FunctionToolResult
	if result == nil || result.IsError || result.ContentBlocks[0].Text.Text != "done" {
		t.Fatalf("unexpected result %#v", result)
	}
}

// 未声明 Parameters 的工具回落到 EmptyParameters：任意 JSON 对象可用。
func TestToolsNodeDefaultsToPermissiveEmptyParameters(t *testing.T) {
	var calls atomic.Int32
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{echo}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(context.Background(), assistantCall("call-1", "echo", `{"anything": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || out[0].ContentBlocks[0].FunctionToolResult.IsError {
		t.Fatalf("calls=%d result=%#v", calls.Load(), out[0])
	}
}

// 注册期预编译：非法 schema 在 NewToolsNode 就失败，不等到运行期。
func TestToolsNodeRejectsInvalidSchemaAtRegistration(t *testing.T) {
	bad := tool.NewFunctionTool(tool.Spec{
		Name:       "bad",
		Parameters: map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"$ref": "#/definitions/missing"}}},
	}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	if _, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{bad}}); err == nil || !strings.Contains(err.Error(), `"bad"`) {
		t.Fatalf("expected registration error naming the tool, got %v", err)
	}
}

// 校验失败面向模型是可自纠的文本；直调 Runtime 的调用方拿到可分类的错误。
func TestInvalidArgsErrorCarriesSentinel(t *testing.T) {
	probe := tool.NewFunctionTool(tool.Spec{Name: "probe", Parameters: paramsSchema()}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{probe}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := node.Invoke(context.Background(), assistantCall("call-1", "probe", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	text := out[0].ContentBlocks[0].FunctionToolResult.ContentBlocks[0].Text.Text
	if !strings.Contains(text, `tool "probe" arguments violate the declared schema`) {
		t.Fatalf("unexpected message: %q", text)
	}
}

// 注册快照与模型定义完全隔离：注册后修改调用方原参数表、Binding.Spec()
// 与 Definitions() 的返回值，都不能改变校验规则或导出的定义。
func TestRegistrationSnapshotIsolatedFromMutableParams(t *testing.T) {
	params := paramsSchema()
	var calls atomic.Int32
	search := tool.NewFunctionTool(tool.Spec{Name: "search", Parameters: params}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult("ok"), nil
	})
	node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{search}})
	if err != nil {
		t.Fatal(err)
	}
	// 调用方清空原参数表的 required。
	params["required"] = []any{}
	// 通过 Spec() 与 Definitions() 拿到的副本篡改 rules。
	runtime, err := tool.NewRuntime(nil)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := runtime.Register(search)
	if err != nil {
		t.Fatal(err)
	}
	binding.Spec().Parameters["required"] = []any{}
	defs := node.Definitions()
	defs[0].Parameters["required"] = []any{}
	if props, ok := defs[0].Parameters["properties"].(map[string]any); ok {
		props["query"] = map[string]any{"type": "number"}
	}
	// 导出的定义仍要求 query 必填、类型为 string。
	fresh := node.Definitions()[0].Parameters
	required, _ := fresh["required"].([]any)
	if len(required) != 1 || required[0] != "query" {
		t.Fatalf("exported required=%v", fresh["required"])
	}
	if props, ok := fresh["properties"].(map[string]any); !ok {
		t.Fatal("properties missing")
	} else if q, ok := props["query"].(map[string]any); !ok || q["type"] != "string" {
		t.Fatalf("exported query=%v", props["query"])
	}
	// 校验规则不变：{} 仍拒绝，合法参数仍执行。
	out, err := node.Invoke(context.Background(), assistantCall("call-1", "search", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out[0].ContentBlocks[0].FunctionToolResult.IsError {
		t.Fatal("mutated exports loosened validation")
	}
	if _, err := node.Invoke(context.Background(), assistantCall("call-2", "search", `{"query":"go"}`)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

// $schema 版本在注册期检查：不支持的版本直接拒绝，支持的版本正常工作。
func TestSchemaVersionCheckedAtRegistration(t *testing.T) {
	build := func(schemaURI string) tool.Tool {
		params := paramsSchema()
		if schemaURI != "" {
			params["$schema"] = schemaURI
		}
		return tool.NewFunctionTool(tool.Spec{Name: "versioned", Parameters: params}, func(context.Context, tool.CallInput) (tool.Result, error) {
			return tool.TextResult("ok"), nil
		})
	}
	supported := []string{
		"",
		"http://json-schema.org/draft-07/schema#",
		"https://json-schema.org/draft-07/schema#",
		"https://json-schema.org/draft/2020-12/schema",
	}
	for _, uri := range supported {
		node, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{build(uri)}})
		if err != nil {
			t.Fatalf("$schema=%q: %v", uri, err)
		}
		out, err := node.Invoke(context.Background(), assistantCall("c", "versioned", `{"query":"go"}`))
		if err != nil || out[0].ContentBlocks[0].FunctionToolResult.IsError {
			t.Fatalf("$schema=%q execute failed: %v %#v", uri, err, out[0])
		}
	}
	unsupported := []string{
		"https://json-schema.org/draft/2019-09/schema",
		"http://json-schema.org/draft-04/schema#",
	}
	for _, uri := range unsupported {
		if _, err := tool.NewToolsNode(tool.ToolNodeConfig{Tools: []tool.Tool{build(uri)}}); err == nil ||
			!strings.Contains(err.Error(), "versioned") || !strings.Contains(err.Error(), "unsupported") {
			t.Fatalf("$schema=%q expected registration error, got %v", uri, err)
		}
	}
}

// 直调 Runtime.Execute 时校验同样生效：返回 *InvalidArgsError，
// 可用 errors.Is 分类，且不执行工具本体。
func TestScopeExecuteRejectsInvalidArgs(t *testing.T) {
	var calls atomic.Int32
	probe := tool.NewFunctionTool(tool.Spec{Name: "probe", Parameters: paramsSchema()}, func(context.Context, tool.CallInput) (tool.Result, error) {
		calls.Add(1)
		return tool.TextResult("ok"), nil
	})
	runtime, err := tool.NewRuntime(nil)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := runtime.Register(probe)
	if err != nil {
		t.Fatal(err)
	}
	scope := runtime.NewScope(1)
	_, err = scope.Execute(context.Background(), binding, tool.CallInput{Args: map[string]any{"limit": -1}})
	var invalid *tool.InvalidArgsError
	if !errors.As(err, &invalid) || !errors.Is(err, tool.ErrInvalidToolArgs) {
		t.Fatalf("err=%v", err)
	}
	if invalid.ToolName != "probe" {
		t.Fatalf("tool name=%q", invalid.ToolName)
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran with invalid args")
	}
	// 校验失败不参与只读重试语义外的任何重跑：合法参数仍正常执行。
	if _, err := scope.Execute(context.Background(), binding, tool.CallInput{Args: map[string]any{"query": "x"}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}
