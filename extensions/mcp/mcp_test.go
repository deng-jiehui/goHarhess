package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	sdkclient "github.com/mark3labs/mcp-go/client"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	mcpext "github.com/goharness/goharness/extensions/mcp"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func TestMCPResultPreservesContentThenStructuredPayload(t *testing.T) {
	serverValue := server.NewMCPServer("mixed-server", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(sdkmcp.NewTool("mixed"), func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		result := sdkmcp.NewToolResultText("first")
		result.StructuredContent = map[string]any{"page": 3, "source": "citation"}
		return result, nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, err := tools[0].Invoke(context.Background(), tool.CallInput{Args: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || len(result.ContentBlocks) != 2 || result.ContentBlocks[0].Text == nil || result.ContentBlocks[0].Text.Text != "first" {
		t.Fatalf("result = %#v", result)
	}
	structured := result.ContentBlocks[1]
	if structured.Type != schema.ContentBlockTypeOpaque || structured.Opaque == nil || structured.Opaque.Kind != "mcp.structured" {
		t.Fatalf("structured block = %#v", structured)
	}
	var payload map[string]any
	if err := json.Unmarshal(structured.Opaque.Raw, &payload); err != nil || payload["page"] != float64(3) || payload["source"] != "citation" {
		t.Fatalf("structured payload = %s, err=%v", structured.Opaque.Raw, err)
	}
}

func TestMCPAdapterExtractsSchemaAndRunsThroughToolsNode(t *testing.T) {
	serverValue := server.NewMCPServer(
		"test-server",
		"1.0.0",
		server.WithToolCapabilities(true),
	)
	serverValue.AddTool(sdkmcp.NewTool(
		"echo",
		sdkmcp.WithDescription("echo a value"),
		sdkmcp.WithString("value", sdkmcp.Required(), sdkmcp.Description("value to echo")),
		sdkmcp.WithReadOnlyHintAnnotation(true),
	), func(_ context.Context, request sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultText("echo:" + request.GetString("value", "")), nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk, ClientName: "test-client"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	node, err := client.NewToolsNode(context.Background(), tool.ToolNodeConfig{Name: "mcp-tools", MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	messages := []schema.Message{schema.UserMessage("say x"), mcpCall("echo", `{"value":"x"}`, "call-1")}
	output, err := node.Invoke(context.Background(), messages)
	if err != nil {
		t.Fatal(err)
	}
	results := output[0].FunctionToolResults()
	if len(output) != 1 || len(results) != 1 || results[0].CallID != "call-1" || len(results[0].ContentBlocks) != 1 || results[0].ContentBlocks[0].Text.Text != "echo:x" {
		t.Fatalf("output = %#v", output)
	}
	if output[0].Role != schema.RoleTool || results[0].IsError {
		t.Fatalf("tool message = %#v", output[0])
	}

	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 {
		t.Fatalf("tools = %#v", tools)
	}
	definition := tools[0].Spec().Definition()
	properties, ok := definition.Parameters["properties"].(map[string]any)
	if !ok || properties["value"] == nil {
		t.Fatalf("MCP schema = %#v", definition.Parameters)
	}
}

func TestMCPToolErrorRemainsModelVisible(t *testing.T) {
	serverValue := server.NewMCPServer("error-server", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(sdkmcp.NewTool("fail"), func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultError("business failure"), nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk})
	if err != nil {
		t.Fatal(err)
	}
	node, err := client.NewToolsNode(context.Background(), tool.ToolNodeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := node.Invoke(context.Background(), []schema.Message{mcpCall("fail", `{}`, "fail-1")})
	if err != nil {
		t.Fatal(err)
	}
	results := output[0].FunctionToolResults()
	if len(output) != 1 || len(results) != 1 || !results[0].IsError || results[0].ContentBlocks[0].Text.Text != "business failure" {
		t.Fatalf("error output = %#v", output)
	}
}

func TestMCPToolsRunInReactAgentAppendOnlyLoop(t *testing.T) {
	serverValue := server.NewMCPServer("agent-server", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(sdkmcp.NewTool("lookup", sdkmcp.WithString("key", sdkmcp.Required())), func(_ context.Context, request sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultText("value:" + request.GetString("key", "")), nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := client.Tools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		messages := make([]schema.Message, 0, len(request.Messages))
		for _, message := range request.Messages {
			if !contextengine.IsSystemReminder(message) {
				messages = append(messages, message)
			}
		}
		current := calls.Add(1)
		if len(request.Functions) != 1 || request.Functions[0].Name != "lookup" {
			return nil, fmt.Errorf("model tool definitions = %#v", request.Functions)
		}
		if current == 1 {
			return []schema.Message{mcpCall("lookup", `{"key":"alpha"}`, "lookup-1")}, nil
		}
		if len(messages) < 3 {
			return nil, fmt.Errorf("second turn messages = %#v", messages)
		}
		call := messages[len(messages)-2]
		observation := messages[len(messages)-1]
		calls := call.FunctionToolCalls()
		if call.Role != schema.RoleAssistant || len(calls) != 1 {
			return nil, fmt.Errorf("tool call message = %#v", call)
		}
		results := observation.FunctionToolResults()
		if observation.Role != schema.RoleTool || len(results) != 1 || results[0].CallID != "lookup-1" || results[0].ContentBlocks[0].Text.Text != "value:alpha" {
			return nil, fmt.Errorf("tool result message = %#v", observation)
		}
		return []schema.Message{schema.AssistantMessage("final:value:alpha")}, nil
	})
	value, err := agent.New(model, agent.WithName("mcp-agent"), agent.WithTools(tools...))
	if err != nil {
		t.Fatal(err)
	}
	output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("lookup alpha")})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(output) != 3 || output[2].JoinedText("") != "final:value:alpha" {
		t.Fatalf("calls=%d output=%#v", calls.Load(), output)
	}
}

func mcpCall(name, arguments, id string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, name, arguments))
}
