package mcp_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	sdkclient "github.com/mark3labs/mcp-go/client"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	mcpext "goharness/extensions/mcp"
	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/tool"
)

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
	if len(output) != 1 || output[0].Text() != "echo:x" || output[0].FunctionID("") != "call-1" {
		t.Fatalf("output = %#v", output)
	}
	if output[0].Role != schema.RoleFunction || output[0].Extra["tool_error"] != false {
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
	if len(output) != 1 || output[0].Extra["tool_error"] != true || output[0].Text() != "business failure" {
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
	model := llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, options llm.ChatOptions) ([]schema.Message, error) {
		current := calls.Add(1)
		if len(options.Functions) != 1 || options.Functions[0].Name != "lookup" {
			return nil, fmt.Errorf("model tool definitions = %#v", options.Functions)
		}
		if current == 1 {
			return []schema.Message{mcpCall("lookup", `{"key":"alpha"}`, "lookup-1")}, nil
		}
		if len(messages) < 3 {
			return nil, fmt.Errorf("second turn messages = %#v", messages)
		}
		call := messages[len(messages)-2]
		observation := messages[len(messages)-1]
		if call.Role != schema.RoleAssistant || call.FunctionID("") != "lookup-1" {
			return nil, fmt.Errorf("tool call message = %#v", call)
		}
		if observation.Role != schema.RoleFunction || observation.FunctionID("") != "lookup-1" || observation.Text() != "value:alpha" {
			return nil, fmt.Errorf("tool result message = %#v", observation)
		}
		return []schema.Message{schema.AssistantMessage("final:value:alpha")}, nil
	})
	value, err := agent.New(agent.BaseConfig{Name: "mcp-agent", Model: model, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	output, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("lookup alpha")})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(output) != 3 || output[2].Text() != "final:value:alpha" {
		t.Fatalf("calls=%d output=%#v", calls.Load(), output)
	}
}

func mcpCall(name, arguments, id string) schema.Message {
	message := schema.AssistantMessage("")
	message.FunctionCall = &schema.FunctionCall{Name: name, Arguments: arguments}
	message.Extra = map[string]any{"function_id": id}
	return message
}
