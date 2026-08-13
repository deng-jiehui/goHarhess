package mcp_test

import (
	"context"
	"testing"

	sdkclient "github.com/mark3labs/mcp-go/client"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	mcpext "goharness/extensions/mcp"
	"goharness/pkg/callback"
	"goharness/pkg/tool"
)

func TestMCPCallbackReportsProviderNeutralOperations(t *testing.T) {
	serverValue := server.NewMCPServer("callback-server", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(sdkmcp.NewTool("echo", sdkmcp.WithString("value")), func(_ context.Context, request sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultText(request.GetString("value", "")), nil
	})
	sdk, err := sdkclient.NewInProcessClient(serverValue)
	if err != nil {
		t.Fatal(err)
	}
	client, err := mcpext.NewClient(mcpext.Config{SDKClient: sdk, ClientName: "callback-client"})
	if err != nil {
		t.Fatal(err)
	}
	var starts, ends []mcpext.Operation
	observer := mcpext.CallbackFuncs{CallbackName: "mcp", Start: func(_ context.Context, request mcpext.CallbackRequest) error {
		if err := request.Validate(); err != nil {
			t.Error(err)
		}
		starts = append(starts, request.Operation)
		return nil
	}, End: func(_ context.Context, response mcpext.CallbackResponse) error {
		ends = append(ends, response.Operation)
		return nil
	}}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	ctx := callback.WithManager(context.Background(), manager)
	tools, err := client.RefreshTools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tools[0].Invoke(ctx, tool.CallInput{Args: map[string]any{"value": "x"}}); err != nil {
		t.Fatal(err)
	}
	want := []mcpext.Operation{mcpext.OperationInitialize, mcpext.OperationListTools, mcpext.OperationCallTool}
	if len(starts) != len(want) || len(ends) != len(want) {
		t.Fatalf("starts=%v ends=%v", starts, ends)
	}
	for index := range want {
		if starts[index] != want[index] || ends[index] != want[index] {
			t.Fatalf("starts=%v ends=%v", starts, ends)
		}
	}
}

func TestMCPCallbackCloneIsolatesArguments(t *testing.T) {
	request := mcpext.CallbackRequest{Operation: mcpext.OperationCallTool, CallTool: &mcpext.CallToolRequest{Name: "echo", Args: map[string]any{"nested": map[string]any{"owner": "original"}}}}
	cloned := request.Clone()
	cloned.CallTool.Args["nested"].(map[string]any)["owner"] = "changed"
	if request.CallTool.Args["nested"].(map[string]any)["owner"] != "original" {
		t.Fatal("MCP callback argument mutation leaked")
	}
}
