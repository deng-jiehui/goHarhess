package graph_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type dispatchProbeConfig struct {
	Tag string
}

func withProbeTag(tag string) runnable.Option {
	return runnable.WrapOption(func(c *dispatchProbeConfig) { c.Tag = tag })
}

// probeNode 解析自己的配置类型并记录；不声明类型时只能接收定向选项。
type probeNode struct {
	mu   sync.Mutex
	tags []string
}

func (n *probeNode) seen() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.tags...)
}

func (n *probeNode) runnable() runnable.Runnable[string, string] {
	return runnable.Func[string, string](func(_ context.Context, input string, options ...runnable.Option) (string, error) {
		resolved, _, err := runnable.ResolveComponentOptions(dispatchProbeConfig{}, options...)
		if err != nil {
			return "", err
		}
		if resolved.Tag != "" {
			n.mu.Lock()
			n.tags = append(n.tags, resolved.Tag)
			n.mu.Unlock()
		}
		return input, nil
	})
}

// 深层定向路径只在到达最终目标后剥离：fanout→branch→first 只命中 first，
// 不会把配置扩大到嵌套图内的其他节点。
func TestDeepTargetedPathReachesOnlyFinalNode(t *testing.T) {
	first, second := &probeNode{}, &probeNode{}
	child := graph.NewGraph[string, string]("child")
	for _, item := range []struct {
		id   string
		node *probeNode
	}{{"first", first}, {"second", second}} {
		if err := child.AddNode(item.id, item.node.runnable()); err != nil {
			t.Fatal(err)
		}
	}
	if err := child.AddEdge(graph.Start, "first"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("first", "second"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("second", graph.End); err != nil {
		t.Fatal(err)
	}
	compiledChild := mustCompile(t, child)

	g := graph.NewGraph[string, string]("parent")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{{
		Name: "nested", Node: compiledChild,
		Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
		Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil },
	}}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)

	if _, err := mustCompile(t, g).Invoke(context.Background(), "base",
		runnable.ForPath([]string{"fanout", "nested", "first"}, withProbeTag("hit")),
	); err != nil {
		t.Fatal(err)
	}
	if tags := first.seen(); len(tags) != 1 || tags[0] != "hit" {
		t.Fatalf("first tags=%v", tags)
	}
	if tags := second.seen(); len(tags) != 0 {
		t.Fatalf("second unexpectedly received config: %v", tags)
	}
}

// 元数据统一经 ctx 传递：调用方在运行上下文上设置的标签沿调度继承到
// 节点作用域，Graph 不再提供 Option 入口，也不做任何重放与覆盖。
func TestRunContextMetadataReachesNodes(t *testing.T) {
	var observed map[string]any
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		observed = callback.MetadataFromContext(ctx)
		return input, nil
	})
	g := graph.NewGraph[string, string]("meta-ctx")
	if err := g.AddNode("node", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	runCtx := callback.WithMetadata(context.Background(), map[string]any{
		"tenant": "global",
		"mode":   "run",
	})
	if _, err := mustCompile(t, g).Invoke(runCtx, "in"); err != nil {
		t.Fatal(err)
	}
	if observed["tenant"] != "global" || observed["mode"] != "run" {
		t.Fatalf("metadata = %#v", observed)
	}
	// 读到的快照与运行上下文互相隔离。
	observed["tenant"] = "mutated"
	if got := callback.MetadataFromContext(runCtx)["tenant"]; got != "global" {
		t.Fatalf("context metadata mutated: %v", got)
	}
}

// 并行分支直接继承父运行上下文的分支标签与节点标签；分支需要自己的
// 标签时创建派生 ctx，只影响该分支及其下游，不污染兄弟分支。
func TestParallelBranchMetadataIsolationViaDerivedContext(t *testing.T) {
	var innerSeen, plainSeen map[string]any
	var mu sync.Mutex
	inner := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mu.Lock()
		innerSeen = callback.MetadataFromContext(ctx)
		mu.Unlock()
		return input, nil
	})
	// relay 分支在调用内部组件前创建派生 ctx 追加分支标签。
	relay := &relayBranch{inner: inner, derive: func(ctx context.Context) context.Context {
		return callback.WithMetadata(ctx, map[string]any{"branch": "relayed"})
	}}
	plain := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mu.Lock()
		plainSeen = callback.MetadataFromContext(ctx)
		mu.Unlock()
		return input, nil
	})
	g := graph.NewGraph[string, string]("branch-meta")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{
		{Name: "relayed", Node: relay,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }},
		{Name: "plain", Node: plain,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if branch, exists := callback.MetadataFromContext(ctx)["branch"]; exists {
			t.Fatalf("branch tag leaked to downstream: %v", branch)
		}
		return input, nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)
	runCtx := callback.WithMetadata(context.Background(), map[string]any{"tenant": "global"})
	if _, err := mustCompile(t, g).Invoke(runCtx, "base"); err != nil {
		t.Fatal(err)
	}
	if innerSeen["tenant"] != "global" || innerSeen["branch"] != "relayed" {
		t.Fatalf("inner metadata = %#v", innerSeen)
	}
	if plainSeen["tenant"] != "global" {
		t.Fatalf("plain metadata = %#v", plainSeen)
	}
	if _, exists := plainSeen["branch"]; exists {
		t.Fatalf("derived ctx tag leaked to sibling: %#v", plainSeen)
	}
}

// 转发型分支可继续消费更深的路径段；非复合分支在路径过深时入口报错。
func TestParallelBranchRelayConsumesDeeperPath(t *testing.T) {
	inner := &probeNode{}
	relay := &relayBranch{inner: inner.runnable()}
	g := graph.NewGraph[string, string]("relay-branch")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	leaf := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{
		{Name: "relayed", Node: relay,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }},
		{Name: "plain", Node: leaf,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)
	compiled := mustCompile(t, g)

	if _, err := compiled.Invoke(context.Background(), "base",
		runnable.ForPath([]string{"fanout", "relayed", "inner"}, withProbeTag("deep")),
	); err != nil {
		t.Fatal(err)
	}
	if tags := inner.seen(); len(tags) != 1 || tags[0] != "deep" {
		t.Fatalf("relay inner tags=%v", tags)
	}
	if _, err := compiled.Invoke(context.Background(), "base",
		runnable.ForPath([]string{"fanout", "plain", "inner"}, withProbeTag("nope")),
	); err == nil || !strings.Contains(err.Error(), "not a composite") {
		t.Fatalf("Invoke error = %v", err)
	}
}

// relayBranch 是声明 OptionRelay 的测试组件：内部只有一个名为 inner 的
// 组件，消费匹配的路径段后把剩余选项交给它；derive 可在转发前派生 ctx。
type relayBranch struct {
	inner  runnable.Runnable[string, string]
	derive func(context.Context) context.Context
}

func (*relayBranch) OptionRelay() bool { return true }

func (r *relayBranch) Invoke(ctx context.Context, input string, options ...runnable.Option) (string, error) {
	if r.derive != nil {
		ctx = r.derive(ctx)
	}
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return "", err
	}
	for index := range specs {
		if len(specs[index].Path) > 0 && specs[index].Path[0] == "inner" {
			specs[index].Path = specs[index].Path[1:]
		}
	}
	return r.inner.Invoke(ctx, input, runnable.OptionsFromSpecs(specs)...)
}
func (r *relayBranch) Stream(ctx context.Context, input string, options ...runnable.Option) (*runnable.StreamReader[string], error) {
	return runnable.StreamFromInvoke(ctx, input, r.Invoke, options...)
}
func (r *relayBranch) Collect(ctx context.Context, in *runnable.StreamReader[string], options ...runnable.Option) (string, error) {
	return runnable.CollectFromInvoke(ctx, in, r.Invoke, options...)
}
func (r *relayBranch) Transform(ctx context.Context, in *runnable.StreamReader[string], options ...runnable.Option) (*runnable.StreamReader[string], error) {
	return runnable.TransformFromInvoke(ctx, in, r.Invoke, options...)
}
