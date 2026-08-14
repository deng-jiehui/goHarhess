# LLM Provider

框架通过 `llm.ChatModel` 执行模型，它组合了完整的 `runnable.Runnable[llm.Request, llm.Response]` 契约、能力声明和安全的模型元数据。Provider 线路方法保持为私有实现。

## OpenAI-compatible Provider

内置适配器注册为 `openai-compatible` 和 `openai`。DeepSeek 注册为 `deepseek`，复用同一适配器，默认使用 DeepSeek 端点并启用 reasoning-content 输入。DashScope/Qwen 注册为 `dashscope` 和 `qwen`，默认使用 DashScope 的 OpenAI-compatible 端点。

```go
model, err := llm.New(llm.Config{
    ModelType:   "deepseek",
    Model:       "deepseek-chat",
    APIKey:      os.Getenv("DEEPSEEK_API_KEY"),
    ModelServer: "https://api.deepseek.com",
})
```

任何 OpenAI-compatible 服务都可以使用 `ModelType: "openai-compatible"`，并将 `ModelServer` 设置为其 API 基础 URL。多个模型需要共享同一连接池时，可以提供共享的 `Config.HTTPClient`。

未注入客户端时，模型 Provider 使用 `pkg/httpclient.DefaultPool`。同一 Transport 也用于检索和推理 REST 服务；零超时 LLM 客户端仍适合长时间 SSE 流。

## 规范化流

Provider 将私有线路事件归约为一个完整的 `llm.Response`，通过 `*runnable.StreamReader[llm.Response]` 返回。SSE 分帧、解压和 Provider 分片解码属于包内部细节。实时的部分 Response 通过类型化 Context EventStream 回调发布，绝不会进入 Reader 的最终结果数据面。

名为 `reasoning_content`、`reasoning`、`thinking` 或 `thought` 的推理字段会规范化为有序 `Message.ContentBlocks` 切片中的 `schema.NewReasoningBlock` 条目。工具调用参数片段会在最终 Response 提交前合并为 `NewFunctionToolCallBlock` 条目。适配器保留 Block 顺序，并用 `RoleTool` 和 `NewFunctionToolResultBlock` 表示结果。不支持的 Provider 载荷会保留为 `NewOpaqueBlock` 值或产生明确错误；绝不会静默展平或丢弃。构造函数和访问器详情请参阅 README 的规范 Message 与 ContentBlocks 章节。

## 能力

每个 `llm.ChatModel` 都通过 `Capabilities()` 声明对流式、工具调用、视觉、JSON-schema 和推理的支持。

## 可选重试

重试默认关闭，可以使用 `llm.NewRetryingModel` 包装任意模型。该装饰器只重试可重试的请求失败，以及首个数据事件前到达的流错误；部分流绝不会重放。

`llm.NewRateLimitedModel` 提供可选的并发限制和最小请求启动间隔。这些装饰器可按任意顺序与应用自己的熔断策略组合。
