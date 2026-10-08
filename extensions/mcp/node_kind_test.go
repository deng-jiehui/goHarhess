package mcp_test

import (
	"testing"

	mcpext "github.com/goharness/goharness/extensions/mcp"
	"github.com/goharness/goharness/pkg/runnable"
)

func TestDynamicToolsNodeDeclaresMCPNodeKind(t *testing.T) {
	var node *mcpext.DynamicToolsNode
	if node.NodeKind() != runnable.NodeKindMCP {
		t.Fatalf("node kind = %q", node.NodeKind())
	}
}
