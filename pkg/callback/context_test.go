package callback_test

import (
	"context"
	"testing"

	"goharness/pkg/callback"
)

func TestBeginPropagatesTraceAndClonesMetadata(t *testing.T) {
	ctx := callback.WithTraceID(context.Background(), "trace-upstream")
	ctx = callback.WithMetadata(ctx, map[string]any{
		"tenant": "tenant-a",
		"nested": map[string]any{"value": "original"},
	})
	agentCtx, agentOperation := callback.Begin(ctx, "graph", "assistant", map[string]any{"request": "r-1"})
	toolCtx, toolOperation := callback.Begin(agentCtx, "tool", "search", map[string]any{"tool.read_only": true})

	if agentOperation.Trace.TraceID != "trace-upstream" || toolOperation.Trace.TraceID != "trace-upstream" {
		t.Fatalf("trace IDs = %#v / %#v", agentOperation.Trace, toolOperation.Trace)
	}
	if agentOperation.Trace.RunID == "" || toolOperation.Trace.RunID != agentOperation.Trace.RunID {
		t.Fatalf("run IDs = %#v / %#v", agentOperation.Trace, toolOperation.Trace)
	}
	if toolOperation.Trace.ParentSpanID != agentOperation.Trace.SpanID {
		t.Fatalf("tool parent = %q, want %q", toolOperation.Trace.ParentSpanID, agentOperation.Trace.SpanID)
	}
	metadata := callback.MetadataFromContext(toolCtx)
	if metadata["tenant"] != "tenant-a" || metadata["request"] != "r-1" || metadata["tool.read_only"] != true {
		t.Fatalf("metadata = %#v", metadata)
	}
	metadata["nested"].(map[string]any)["value"] = "changed"
	again := callback.MetadataFromContext(toolCtx)
	if again["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("context metadata was mutated: %#v", again)
	}
}

func TestNestedScopesReuseOneManagerOwnershipTree(t *testing.T) {
	manager, err := callback.NewManager(callback.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root, rootScope := callback.EnterScope(context.Background())
	root = callback.WithManager(root, manager)
	child, childScope := callback.EnterScope(root)
	resolved, ok := callback.ManagerFromContext(child)
	if !rootScope.Root || rootScope.Depth != 0 || childScope.Root || childScope.Depth != 1 {
		t.Fatalf("root=%#v child=%#v", rootScope, childScope)
	}
	if !ok || resolved != manager {
		t.Fatal("nested scope replaced the root Manager")
	}
}
