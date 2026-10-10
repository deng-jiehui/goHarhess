package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/goharness/goharness/pkg/runnable"
)

type fpState struct {
	Value string `json:"value"`
}

// 阶段四补查 P1：整图指纹必须对「完全相同的定义」稳定——路由来源曾按
// map 随机序写入指纹条目，同一图多次编译产生不同 revision，恢复被
// ErrGraphRevisionMismatch 拒绝。按相同定义重建多次，revision 必须一致。
func TestFingerprintStableAcrossIdenticalRebuilds(t *testing.T) {
	build := func(t *testing.T) string {
		t.Helper()
		g := NewGraph[string, string]("fp")
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(g.SetStateFactory(func(_ context.Context, in string) (*fpState, error) {
			return &fpState{Value: in}, nil
		}))
		node := func(name string) {
			must(g.AddNode(name, runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), WithPreHandler(func(_ context.Context, s *fpState) (string, error) { return s.Value, nil })))
		}
		for _, n := range []string{"src", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10", "a", "b", "c", "d"} {
			node(n)
		}
		must(g.AddEdge(Start, "src"))
		// 十个路由来源、各带多个目标：来源与目标的遍历顺序都不能影响指纹。
		route := func(targets ...string) *runtimeRoute {
			r := newRuntimeRoute(func(_ context.Context, s string) (string, error) { return targets[0], nil })
			for _, tg := range targets {
				r.targets[tg] = true
			}
			return r
		}
		must(g.addRuntimeRoute("src", route("r1", "a", "b")))
		for i, from := range []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"} {
			next := "r" + fmt.Sprint(i+2)
			must(g.addRuntimeRoute(from, route(next, "a", "b")))
		}
		must(g.addRuntimeRoute("r10", route(End, "c", "d")))
		must(g.AddEdge("a", End))
		must(g.AddEdge("b", End))
		must(g.AddEdge("c", End))
		must(g.AddEdge("d", End))
		app, err := g.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		revision, resumableErr := app.runtime.recoveryMetadata()
		if resumableErr != nil {
			t.Fatalf("metadata: %v", resumableErr)
		}
		return revision
	}
	first := build(t)
	for i := 0; i < 32; i++ {
		if got := build(t); got != first {
			t.Fatalf("rebuild %d produced different fingerprint: %q vs %q", i, got, first)
		}
	}
}
