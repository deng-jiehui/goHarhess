package callback_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/callback"
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

// WithMetadata 在写入时克隆传入值：之后修改调用方的 map、slice 或嵌套
// 容器不影响 ctx 中已存的快照；读取结果同样是独立副本。
func TestWithMetadataClonesMutableInputs(t *testing.T) {
	labels := map[string]string{"team": "a"}
	tags := []string{"x"}
	nested := map[string]any{"list": []string{"one"}, "inner": map[string]string{"k": "v"}}
	ctx := callback.WithMetadata(context.Background(), map[string]any{
		"labels": labels,
		"tags":   tags,
		"nested": nested,
	})
	labels["team"] = "mutated"
	tags[0] = "mutated"
	nested["list"].([]string)[0] = "mutated"
	nested["inner"].(map[string]string)["k"] = "mutated"

	metadata := callback.MetadataFromContext(ctx)
	if metadata["labels"].(map[string]string)["team"] != "a" ||
		metadata["tags"].([]string)[0] != "x" ||
		metadata["nested"].(map[string]any)["list"].([]string)[0] != "one" ||
		metadata["nested"].(map[string]any)["inner"].(map[string]string)["k"] != "v" {
		t.Fatalf("context metadata was mutated by source changes: %#v", metadata)
	}
	// 修改读取结果同样不回写 ctx。
	metadata["labels"].(map[string]string)["team"] = "mutated"
	if got := callback.MetadataFromContext(ctx)["labels"].(map[string]string)["team"]; got != "a" {
		t.Fatalf("read snapshot mutated context: %v", got)
	}
}

// 派生 ctx 的覆盖与删除只影响该分支：父调用与兄弟分支的快照不变。
func TestDerivedContextMetadataIsolation(t *testing.T) {
	parent := callback.WithMetadata(context.Background(), map[string]any{
		"tenant": "acme",
		"mode":   "base",
		"stale":  "remove-me",
	})
	branch := callback.WithoutMetadata(parent, "stale")
	branch = callback.WithMetadata(branch, map[string]any{"mode": "branch"})
	sibling := callback.WithoutMetadata(parent, "mode")

	parentMetadata := callback.MetadataFromContext(parent)
	if parentMetadata["mode"] != "base" || parentMetadata["stale"] != "remove-me" {
		t.Fatalf("parent metadata mutated: %#v", parentMetadata)
	}
	branchMetadata := callback.MetadataFromContext(branch)
	if branchMetadata["mode"] != "branch" || branchMetadata["tenant"] != "acme" {
		t.Fatalf("branch metadata = %#v", branchMetadata)
	}
	if _, exists := branchMetadata["stale"]; exists {
		t.Fatalf("deleted key survived in branch: %#v", branchMetadata)
	}
	siblingMetadata := callback.MetadataFromContext(sibling)
	if _, exists := siblingMetadata["mode"]; exists {
		t.Fatalf("sibling delete failed: %#v", siblingMetadata)
	}
	if siblingMetadata["stale"] != "remove-me" {
		t.Fatalf("sibling lost unrelated keys: %#v", siblingMetadata)
	}
	// 空删除返回原 ctx。
	if got := callback.WithoutMetadata(parent); got != parent {
		t.Fatal("empty delete should return the same context")
	}
}

// 并行分支各自派生 ctx，覆盖与删除互不影响——这是 Graph 并行分支
// 直接继承父上下文后仍保持隔离的基础。
func TestParallelDerivedContextsDoNotShareMetadata(t *testing.T) {
	parent := callback.WithMetadata(context.Background(), map[string]any{
		"shared": map[string]any{"owner": "parent"},
		"tag":    "root",
	})
	results := make(chan map[string]any, 2)
	derive := func(mutate func(context.Context) context.Context) {
		metadata := callback.MetadataFromContext(mutate(parent))
		metadata["shared"].(map[string]any)["owner"] = "local-mutation"
		results <- callback.MetadataFromContext(mutate(parent))
	}
	go derive(func(ctx context.Context) context.Context {
		return callback.WithMetadata(ctx, map[string]any{"tag": "a"})
	})
	go derive(func(ctx context.Context) context.Context {
		return callback.WithoutMetadata(ctx, "tag")
	})
	first, second := <-results, <-results
	for _, snapshot := range []map[string]any{first, second} {
		if snapshot["shared"].(map[string]any)["owner"] != "parent" {
			t.Fatalf("branch snapshot mutated shared value: %#v", snapshot)
		}
	}
	tags := map[bool]int{true: 0, false: 0}
	for _, snapshot := range []map[string]any{first, second} {
		_, exists := snapshot["tag"]
		tags[exists]++
	}
	if tags[true] != 1 || tags[false] != 1 {
		t.Fatalf("branch tag states = %v/%v", first, second)
	}
	if got := callback.MetadataFromContext(parent)["tag"]; got != "root" {
		t.Fatalf("parent tag mutated: %v", got)
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
