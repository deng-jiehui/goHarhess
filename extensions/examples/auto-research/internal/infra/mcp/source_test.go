package mcp

import (
	"context"
	"testing"

	mcpext "github.com/goharness/goharness/extensions/mcp"
	sdkclient "github.com/mark3labs/mcp-go/client"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/goharness/autoresearch/internal/domain"
)

func TestMCPSourceCallsDiscoveredToolAndNormalizesResult(t *testing.T) {
	serverValue := server.NewMCPServer("fixture", "1.0.0", server.WithToolCapabilities(true))
	serverValue.AddTool(sdkmcp.NewTool("search", sdkmcp.WithString("query")), func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
		return sdkmcp.NewToolResultStructured(map[string]any{"papers": []any{map[string]any{
			"id": "2401.00001v2", "title": "Fixture Research", "published": "2024-01-02", "summary": "摘要",
		}}}, ""), nil
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
	source, err := NewMCPSource(client, MCPSourceConfig{Provider: "arxiv", SearchTool: "search"})
	if err != nil {
		t.Fatal(err)
	}
	items, err := source.Search(context.Background(), domain.PaperQuery{Text: "fixture"})
	if err != nil || len(items) != 1 || items[0].SourceKey != "arxiv:2401.00001" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}
