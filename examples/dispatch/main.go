package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

// 动态任务分派：plan 完成后按当前 State 生成任务清单，框架为每个成员
// 创建独立任务并冻结其输入；成员持有局部 State，全部到达后按清单顺序
// 汇合到 collect。清单为空时直接进入汇合。
type workState struct {
	Brief string
	Parts []string
}

func main() {
	g := graph.NewGraph[string, string]("dispatch")
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, brief string) (*workState, error) {
		return &workState{Brief: brief}, nil
	}))
	// 成员局部 State 的增量按任务顺序并入候选 State。
	must(g.SetStepMerger(func(_ context.Context, base *workState, results []graph.StepResult[*workState]) error {
		previous := len(base.Parts)
		for _, result := range results {
			base.Parts = append(base.Parts, result.State.Parts[previous:]...)
		}
		return nil
	}))

	must(g.AddNode("plan", runnable.Func[string, string](func(_ context.Context, brief string, _ ...runnable.Option) (string, error) {
		return brief, nil
	}), graph.WithPreHandler(func(_ context.Context, s *workState) (string, error) { return s.Brief, nil })))
	must(g.AddNode("worker", runnable.Func[string, string](func(_ context.Context, word string, _ ...runnable.Option) (string, error) {
		return "done:" + word, nil
	}), graph.WithPreHandler(func(_ context.Context, s *workState) (string, error) {
		// 成员入口任务用冻结输入直接调用；再次经过时按局部 State 重建。
		return s.Brief, nil
	}), graph.WithPostHandler(func(_ context.Context, s *workState, out string) error {
		s.Parts = append(s.Parts, out)
		return nil
	})))
	must(g.AddNode("collect", runnable.Func[string, string](func(_ context.Context, joined string, _ ...runnable.Option) (string, error) {
		return joined, nil
	}), graph.WithPreHandler(func(_ context.Context, s *workState) (string, error) {
		return strings.Join(s.Parts, " | "), nil
	})))

	// plan 完成后由清单生成器决定本组任务；成员按声明顺序汇合到 collect。
	must(graph.AddDispatch[string, string](g, "plan", []string{"worker"}, "collect",
		func(_ context.Context, s *workState) ([]graph.DispatchTask, error) {
			var tasks []graph.DispatchTask
			for _, word := range strings.Fields(s.Brief) {
				tasks = append(tasks, graph.Send("worker", word))
			}
			return tasks, nil
		}))
	must(g.AddEdge(graph.Start, "plan"))
	must(g.AddEdge("worker", "collect"))
	must(g.AddEdge("collect", graph.End))

	compiled, err := g.Compile()
	must(err)
	result, err := compiled.Invoke(context.Background(), "alpha beta gamma")
	must(err)
	fmt.Println(result)

	empty, err := compiled.Invoke(context.Background(), "  ")
	must(err)
	fmt.Printf("empty manifest: %q\n", empty)
}
