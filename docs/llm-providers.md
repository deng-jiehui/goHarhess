# LLM Providers

The framework executes models through `llm.Model`, which combines
`runnable.Runnable[llm.Request, llm.Response]`,
`runnable.Streamer[llm.Request, llm.Event]`, capabilities, and safe model
metadata. Providers expose only `Invoke` and `Stream`; wire methods are private.

## OpenAI-compatible providers

The built-in adapter is registered as `openai-compatible` and `openai`. DeepSeek
is registered as `deepseek` and uses the same adapter with a DeepSeek default
endpoint and reasoning-content input enabled. DashScope/Qwen are registered as
`dashscope` and `qwen` and use DashScope's OpenAI-compatible endpoint by
default.

```go
model, err := llm.New(llm.Config{
    ModelType:   "deepseek",
    Model:       "deepseek-chat",
    APIKey:      os.Getenv("DEEPSEEK_API_KEY"),
    ModelServer: "https://api.deepseek.com",
})
```

Any OpenAI-compatible service can use `ModelType: "openai-compatible"` and set
`ModelServer` to its API base URL. A shared `Config.HTTPClient` may be supplied
when several models should use the same connection pool.

Without an injected client, model providers use `pkg/httpclient.DefaultPool`.
The same Transport is also used by retrieval and inference REST services; its
zero-timeout LLM client remains suitable for long SSE streams.

## Normalized streams

Providers translate private wire events into `llm.Event` values. SSE framing,
decompression, and provider chunk decoding are private package details; callers
only receive `runnable.Stream[llm.Event]`.
Reasoning fields named `reasoning_content`, `reasoning`, `thinking`, or
`thought` are normalized to `schema.Message.ReasoningContent`. Tool-call
argument fragments remain deltas and can be merged with `DeltaAccumulator`.
When a caller does not request delta output, the OpenAI-compatible adapter
emits cumulative message snapshots.

## Capabilities

Every `llm.Model` declares streaming, tool-call, vision, JSON-schema, and
reasoning support through `Capabilities()`.

## Optional retry

Retries are opt-in and can wrap any model with `llm.NewRetryingModel`. The
decorator retries only retryable request failures and stream errors that arrive
before the first data event; partial streams are never replayed.

`llm.NewRateLimitedModel` provides an optional concurrency limit and minimum
request-start interval. These decorators can be composed in either order with
the application's own circuit-breaker policy.
