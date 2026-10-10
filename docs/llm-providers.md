# 模型

框架通过 `llm.ChatModel` 执行模型，它组合了完整的 `runnable.Runnable[llm.Request, llm.Response]` 契约、能力声明和安全的模型元数据。Provider 线路方法保持为私有实现。

## ChatModel 契约

`llm.ChatModel` 同时实现：

```go
runnable.Runnable[llm.Request, llm.Response]
```

因此模型既能交给 Agent 快捷装配，也能作为普通 Runnable 使用。接入自定义 Graph 时，
使用 `llm.NewChatModelNode(model)` 包装 provider 模型；该公开节点负责发射 ChatModel
start/end/error 观测事件（`observe.KindLLM`），并让 Graph 快照将其识别为 `chat_model`。直接调用
模型时，消息、工具定义和生成参数都通过 provider-neutral 的 `llm.Request` 传入：

```go
response, err := model.Invoke(ctx, llm.Request{
    Messages:  []schema.Message{schema.NewMessage(schema.RoleUser, schema.NewTextBlock("查询订单状态"))},
    Functions: toolsNode.Definitions(),
}, llm.WithGenerateConfig(llm.GenerateConfig{"max_tokens": 1024}))
```

工具定义是 `llm.Request.Functions` 字段；`DeltaStream`、`max_tokens` 等运行
配置统一通过 `...runnable.Option` 传入（`llm.WithDeltaStream`、
`llm.WithMaxTokens`、`llm.WithSeed`、`llm.WithGenerateConfig`），由模型入口
解析一次。

Provider 只负责把不同厂商的协议转换成统一的 Message、ContentBlocks、Usage 和 Response；
Agent 图不依赖 OpenAI、DeepSeek 或 Qwen 的私有响应结构。

## OpenAI-compatible Provider

内置适配器注册为 `openai-compatible` 和 `openai`。DeepSeek 注册为 `deepseek`，复用同一适配器，默认使用 DeepSeek 端点并启用 reasoning-content 输入。DashScope/Qwen 注册为 `dashscope` 和 `qwen`，默认使用 DashScope 的 OpenAI-compatible 端点。

| `ModelType` | 默认适配 |
| --- | --- |
| `openai-compatible`、`openai` | 通用 OpenAI-compatible 服务，需按服务填写 `ModelServer` |
| `deepseek` | DeepSeek OpenAI-compatible API |
| `dashscope`、`qwen` | DashScope/Qwen OpenAI-compatible API |

```go
model, err := llm.New(llm.Config{
    ModelType: "deepseek",
    Model:     "deepseek-chat",
    APIKey:    os.Getenv("DEEPSEEK_API_KEY"),
})
```

任何 OpenAI-compatible 服务都可以使用 `ModelType: "openai-compatible"`，并将 `ModelServer` 设置为其 API 基础 URL。多个模型需要共享同一连接池时，可以提供共享的 `Config.HTTPClient`。

非流式响应体受 `OpenAICompatibleConfig.MaxResponseBytes`（或配置键 `max_response_bytes`）限制，默认 `llm.DefaultMaxResponseBytes`（32 MiB）。超限立即返回 `*llm.ResponseTooLargeError`（`errors.Is(err, llm.ErrResponseTooLarge)`），属调用失败——截断字节不会被当作完整响应解析。流式 SSE 按事件帧处理，不受此限。

未注入客户端时，模型 Provider 使用 `pkg/httpclient.DefaultPool`。同一 Transport 也用于检索和推理 REST 服务；零超时 LLM 客户端仍适合长时间 SSE 流。

不要将 API Key 写入 `observe.Event`、Checkpoint、日志或 Graph State。

## ChatModel 流生命周期

`Model.Stream` 返回 `*runnable.StreamReader[llm.Response]`，Reader 只发送一次完整 Response。
Provider 收到完整上游终止标记后才提交该结果；SSE 提前关闭、Reader 没有产生结果或响应
无法完整归约时返回 `llm.ErrStreamTruncated`，不能提交部分响应。

模型增量不进入 Reader，而是发布到 `pkg/stream` 公共事件流。在图或 Agent 运行中，框架自动
完成发布；直接使用裸模型时可用 `stream.Run` 开启作用域，`Reader.Recv` 返回
`Event{Kind: KindMessage}` 增量与最终结果：

```go
events, err := stream.Run(ctx, func(run context.Context) (llm.Response, error) {
    inner, err := model.Stream(run, request)
    if err != nil {
        return llm.Response{}, err
    }
    return llm.CollectResponse(run, inner)
})
if err != nil {
    return err
}
for {
    event, err := events.Recv()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    if event.Kind == stream.KindMessage && event.Message != nil {
        renderPartialResponse(ctx, event.Message)
    }
    if event.Kind == stream.KindResult && event.Result != nil {
        response = *event.Result
    }
}
```

Retry 只会重试首个实时增量产生前的可重试错误或静默截断；增量产生后失败会直接返回，
避免重复文本、工具调用或计费。调用 `reader.Close()` 会取消上游 SSE/模型生产协程。缓存只
保存完整 Response，不缓存或重放实时增量。

## 规范化流

Provider 将私有线路事件归约为一个完整的 `llm.Response`，通过 `*runnable.StreamReader[llm.Response]` 返回。SSE 分帧、解压和 Provider 分片解码属于包内部细节。实时的部分 Response 通过 `pkg/stream` 公共事件流的 `KindMessage` 增量发布，绝不会进入 Reader 的最终结果数据面。

名为 `reasoning_content`、`reasoning`、`thinking` 或 `thought` 的推理字段会规范化为有序 `Message.ContentBlocks` 切片中的 `schema.NewReasoningBlock` 条目。工具调用参数片段会在最终 Response 提交前合并为 `NewFunctionToolCallBlock` 条目。适配器保留 Block 顺序，并用 `RoleTool` 和 `NewFunctionToolResultBlock` 表示结果。不支持的 Provider 载荷会保留为 `NewOpaqueBlock` 值或产生明确错误；绝不会静默展平或丢弃。

## 能力

每个 `llm.ChatModel` 都通过 `Capabilities()` 声明对流式、工具调用、视觉、JSON-schema 和推理的支持。

## 可选重试

重试默认关闭，可以使用 `llm.NewRetryingModel` 包装任意模型。该装饰器只重试可重试的请求失败，以及首个数据事件前到达的流错误；部分流绝不会重放。

`llm.NewRateLimitedModel` 提供可选的并发限制和最小请求启动间隔。这些装饰器可按任意顺序与应用自己的熔断策略组合。

需要重试或限流时由使用方显式包装模型再交给 `agent.New`：
`llm.NewRetryingModel(llm.NewRateLimitedModel(model, rlCfg), retryCfg)`。
装饰器组合顺序由使用方保证（重试在外层、限流在内层时，每次真实请求都会
重新占用配额）；需要跨 Agent 实例共享限流配额时，包装同一个底层模型再分发
给各 Agent。
