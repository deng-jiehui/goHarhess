package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type researchState struct {
	Topic   string
	Results []string
}

func main() {
	g := graph.NewGraph[string, string]("research")
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, topic string) (*researchState, error) {
		return &researchState{Topic: topic}, nil
	}))
	must(g.SetMaxConcurrency(2))
	must(g.SetStepMerger(func(_ context.Context, base *researchState, results []graph.StepResult[*researchState]) error {
		// 每个任务拿到相同入口，只合并各任务在这一轮新加的结果。
		previous := len(base.Results)
		for _, result := range results {
			base.Results = append(base.Results, result.State.Results[previous:]...)
		}
		return nil
	}))
	for _, name := range []string{"news", "papers", "download"} {
		label := name
		must(g.AddNode(label, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return label + ": " + input, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *researchState) (string, error) { return s.Topic, nil }),
			graph.WithPostHandler(func(_ context.Context, s *researchState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		))
	}
	must(g.AddNode("summary", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil }),
		graph.WithPreHandler(func(_ context.Context, s *researchState) (string, error) { return strings.Join(s.Results, "\n"), nil }),
	))
	must(g.AddFork(graph.Start, "news", "papers"))
	must(g.AddJoin(graph.Start, "summary"))
	must(g.AddEdge("news", "summary"))
	must(g.AddEdge("papers", "download"))
	must(g.AddEdge("download", "summary"))
	must(g.AddEdge("summary", graph.End))
	compiled, err := g.Compile()
	must(err)
	result, err := compiled.Invoke(context.Background(), "Graph 原生分支")
	must(err)
	fmt.Println(result)
}
