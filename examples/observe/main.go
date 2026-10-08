// observe 演示统一观测：一行 observe.With + observe.Slog 把一次运行的
// 调用树、耗时与 token 用量打成缩进日志。
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

func main() {
	// 一行接入：Slog observer 把 run/node/llm/tool 事件打印为缩进调用树。
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := observe.With(context.Background(), observe.Slog(logger))

	echo := tool.NewFunctionTool(tool.Spec{
		Name:        "echo",
		Description: "Return the text field from arguments.",
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{"text": map[string]any{"type": "string"}},
			"required":   []string{"text"},
		},
	}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		return tool.TextResult(fmt.Sprint(input.Args["text"])), nil
	})
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("1", "echo", `{"text":"hello"}`))},
		[]schema.Message{schema.AssistantMessage("done")},
	)
	value, err := agent.New(agent.BaseConfig{Name: "demo", Model: model, Tools: []tool.Tool{echo}})
	must(err)
	messages, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("say hello")})
	must(err)
	fmt.Println("last:", messages[len(messages)-1].JoinedText(""))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
