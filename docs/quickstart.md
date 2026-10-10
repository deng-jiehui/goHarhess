# 快速上手

本指南面向希望快速构建 Agent 的 Go 工程师。goHarness 的核心不是应用注册器，
而是一个以强类型 Graph 为中心的 LLM 编排框架：ChatModel、ToolNode、MCP、
Context Engine 和 Agent 都遵循统一的 Runnable 契约，可以按需组合成业务图。

## 环境准备

- Go 1.24 或更高版本。
- 使用任一 OpenAI-compatible 服务时，准备模型地址、模型名与 API Key。
- Windows 上运行竞态检测时，安装 MinGW-w64 GCC，并将其 `bin` 目录加入 `PATH`。

```powershell
go test -count=1 ./...
go vet ./...
```

可先运行内置示例：

```powershell
go run ./examples/basic
go run ./examples/graph
```

## 五分钟接入

最快的方式是使用 Agent 快捷装配层。下面的代码创建 ChatModel 和一个工具，框架会
自动把它们装配成 ReAct 图，然后直接调用 Agent。应用层只需持有 Agent 或
`map[string]agent.Agent`，不需要框架注册器。

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/goharness/goharness/pkg/agent"
    "github.com/goharness/goharness/pkg/llm"
    "github.com/goharness/goharness/pkg/schema"
    "github.com/goharness/goharness/pkg/tool"
)

func main() {
    model, err := llm.New(llm.Config{
        ModelType:   "openai-compatible",
        Model:       "your-model",
        ModelServer: "https://provider.example.com/v1",
        APIKey:      os.Getenv("LLM_API_KEY"),
    })
    if err != nil {
        log.Fatal(err)
    }

    echo := tool.NewFunctionTool(tool.Spec{
        Name:        "echo",
        Description: "返回 text 参数。",
        Parameters: map[string]any{
            "type": "object",
            "properties": map[string]any{
                "text": map[string]any{"type": "string"},
            },
            "required": []string{"text"},
        },
    }, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
        return tool.TextResult(input.Args["text"].(string)), nil
    })

    assistant, err := agent.New(model,
        agent.WithName("assistant"),
        agent.WithSystemMessage("You are a helpful assistant."),
        agent.WithTools(echo),
        agent.WithMaxLLMCalls(4),
    )
    if err != nil {
        log.Fatal(err)
    }

    output, err := assistant.Invoke(context.Background(), []schema.Message{
        schema.NewMessage(schema.RoleUser, schema.NewTextBlock("请调用 echo 工具返回 hello。")),
    })
    if err != nil {
        log.Fatal(err)
    }
    for _, message := range output {
        log.Printf("%s: %s", message.Role, message.JoinedText(""))
    }
}
```

`agent.New` 会按配置自动选择图配方：未配置工具时创建 Basic Agent；配置
`WithTools` 后创建 ReAct Agent。主管路由（Supervisor/Router）与群聊编排
不是预置配方，可以用 `graph.NewState` 门面自行组合，参考
`examples/routing` 与 `examples/groupchat`。

如果应用需要注册多个可命名 Agent，使用普通 Go map，并在 HTTP、RPC 或任务入口按名称路由：

```go
agents := map[string]agent.Agent{
    "assistant":  assistant,
    "researcher": researcher,
}
selected, ok := agents[agentName]
if !ok {
    return fmt.Errorf("unknown agent %q", agentName)
}
reply, err := selected.Invoke(ctx, messages)
```

注册、鉴权、限流和多副本调度属于应用层；框架只负责 Runnable、Graph 和 Agent 执行。

## 上线检查清单

- 明确每个 Agent 的最大模型调用数、工具超时、工具并发和 Graph 最大步数。
- 所有可写工具都通过 Middleware 执行认证、租户隔离、权限校验和必要的幂等保护。
- 以 `Context` 传递 Trace、请求取消和 `tool.Subject`，不把敏感数据拼入 Prompt。
- 流式消费者只在 `Recv` 返回 `io.EOF`、且已收到 `KindResult` 最终结果后保存、扣费或声明成功；`Recv` 返回的非 EOF 错误立即作为失败处理。
- 自定义流适配器应在 Reader 关闭时取消上游；首个数据后的流错误不得自动重试。
- Observer 保持短小、可取消；失败上报接入自己的日志、告警或死信流程。
- 为会话和执行配置统一的耐久 `state.Store`，验证版本冲突、过期、挂起、保存失败、完成结果复用及外部工具幂等。
- 分别在核心模块和已启用的 `extensions` 模块执行单测、`go vet` 与竞态检测。
- 先压测模型限流、工具并发、SSE 中断、网络重试与下游存储故障，再逐步放量。
- 为缓存装饰器验证键隔离、失效、截断流、并发请求和调用方取消语义后再启用。

## 延伸阅读

- [核心概念](concepts.md)
- [Agent 装配](agent.md)
- [工具运行时](tool-runtime.md)
- [模型 Provider](llm-providers.md)
- [观测](observability.md)
- [检查点与中断](checkpoints-interrupts.md)
- [流式输出](public-stream.md)
- [Cookbook](cookbook.md)
