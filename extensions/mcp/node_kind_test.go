package mcp_test

import (
	"testing"

	mcpext "goharness/extensions/mcp"
	"goharness/pkg/runnable"
)

func TestDynamicToolsNodeDeclaresMCPNodeKind(t *testing.T) {
	var node *mcpext.DynamicToolsNode
	if node.NodeKind() != runnable.NodeKindMCP {
		t.Fatalf("node kind = %q", node.NodeKind())
	}
}
