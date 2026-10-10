package graph_test

import (
	"context"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// 阶段四补查 P1：并发上限为 1 时父节点并发调用两个子图，先结束的
// 子调用不能把父节点让出的名额提前取回——否则仍在启动的子调用等
// 名额、父节点等子调用，互相卡死。全部借用归还后父名额才恢复。
//
// 通道编排保证确定性：第一个子图等「第二个已启动」才返回；第二个的
// StateFactory 在「第一个整体返回」前阻塞，使它的内层额度领取晚于
// 第一个 resume 的取回动作——这正是偷名额的时序窗口。
func concurrentSlotChildren(t *testing.T, checkpoint bool) {
	t.Helper()
	secondStarted := make(chan struct{})
	firstReturned := make(chan struct{})
	factory := func(_ context.Context, text string) (*inputState, error) {
		return &inputState{Text: text}, nil
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	build := func(name string, node runnable.Runnable[string, string], fac func(context.Context, string) (*inputState, error)) *graph.CompiledGraph[string, string] {
		g := graph.NewGraph[string, string](name)
		must(g.SetStateFactory(fac))
		must(g.AddNode("work", node, graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
		must(g.AddEdge(graph.Start, "work"))
		must(g.AddEdge("work", graph.End))
		return mustCompile(t, g)
	}
	first := build("slot-first", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		select {
		case <-secondStarted:
			return s + ":first", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}), factory)
	second := build("slot-second", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s + ":second", nil
	}), func(ctx context.Context, text string) (*inputState, error) {
		close(secondStarted)
		select {
		case <-firstReturned:
			return factory(ctx, text)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	parent := graph.NewGraph[string, string]("slot-parent")
	must(parent.SetStateFactory(factory))
	must(parent.AddNode("fan", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		errs := make(chan error, 2)
		outs := make(chan string, 2)
		go func() {
			out, err := first.Invoke(ctx, s+"-0")
			close(firstReturned)
			outs <- out
			errs <- err
		}()
		go func() {
			out, err := second.Invoke(ctx, s+"-1")
			outs <- out
			errs <- err
		}()
		firstErr, secondErr := <-errs, <-errs
		if firstErr != nil {
			return "", firstErr
		}
		if secondErr != nil {
			return "", secondErr
		}
		return <-outs + "|" + <-outs, nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })))
	must(parent.AddEdge(graph.Start, "fan"))
	must(parent.AddEdge("fan", graph.End))
	must(parent.SetMaxConcurrency(1))
	compiled := mustCompile(t, parent)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var options []runnable.Option
	if checkpoint {
		options = append(options, graph.WithCheckpoint(state.NewMemoryStore(), "slots"))
	}
	out, err := compiled.Invoke(ctx, "root", options...)
	if err != nil {
		t.Fatalf("两个子图未能完成（名额互等）：%v", err)
	}
	if out != "root-0:first|root-1:second" && out != "root-1:second|root-0:first" {
		t.Fatalf("out=%q", out)
	}
}

func TestNestedConcurrentCallsWithoutCheckpoint(t *testing.T) {
	concurrentSlotChildren(t, false)
}

func TestNestedConcurrentCallsWithCheckpoint(t *testing.T) {
	concurrentSlotChildren(t, true)
}
