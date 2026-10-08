package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type formState struct{ Greeting string }

func main() {
	ctx := context.Background()
	g := graph.NewGraph[string, string]("interactive-form")
	check(g.SetStateFactory(func(_ context.Context, input string) (*formState, error) { return &formState{Greeting: input}, nil }))
	check(g.AddNode("form", runnable.Func[string, string](func(ctx context.Context, greeting string, _ ...runnable.Option) (string, error) {
		name, err := graph.Interrupt(ctx, map[string]any{"field": "name", "prompt": "请输入姓名"})
		if err != nil {
			return "", err
		}
		language, err := graph.Interrupt(ctx, map[string]any{"field": "language", "choices": []string{"中文", "English"}})
		if err != nil {
			return "", err
		}
		// 校验并解释输入是节点的职责，Graph 不限定为批准/拒绝。
		person, ok := name.(string)
		if !ok {
			return "", errors.New("姓名必须为字符串")
		}
		switch language {
		case "中文":
			return greeting + "，" + person, nil
		case "English":
			return "Hello, " + person, nil
		default:
			return "", errors.New("不支持的语言")
		}
	}), graph.WithPreHandler(func(_ context.Context, s *formState) (string, error) { return s.Greeting, nil })))
	check(g.AddEdge(graph.Start, "form"))
	check(g.AddEdge("form", graph.End))
	compiled, err := g.Compile()
	check(err)
	option := graph.WithCheckpoint(state.NewMemoryStore(), "form-1")
	output, err := compiled.Invoke(ctx, "你好", option)
	// 模拟两次独立请求；服务端实际从用户或外部系统接收 ID 和输入。
	for _, answer := range []any{"Ada", "English"} {
		var suspended *graph.InterruptedError
		if !errors.As(err, &suspended) {
			log.Fatalf("预期挂起，实际为 %v", err)
		}
		question := suspended.PendingInterrupts()[0]
		fmt.Printf("等待输入：%s\n", question.Payload)
		output, err = compiled.Resume(ctx, option, graph.WithResume(question.ID, answer))
	}
	check(err)
	fmt.Println(output)
}
func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
