# HTTP Tools and External APIs

Every tool call managed by `internal/toolruntime` receives a pooled client in
`tool.CallInput.HTTPClient`. The same client is available through
`tool.HTTPClient(ctx)`. Built-in defaults use `httpclient.DefaultPool`, so tool
requests share the framework Transport with LLM, retrieval, embedding and
rerank integrations.

Use request contexts so cancellation and Agent tool-call deadlines reach the
network operation:

```go
search := tool.NewFunctionTool(tool.Spec{
    Name:       "web_search",
    IsReadOnly: true,
}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
    service, err := tool.NewHTTPService(ctx, httpclient.ServiceConfig{
        BaseURL:      "https://google.serper.dev",
        APIKey:       os.Getenv("SERPER_API_KEY"),
        APIKeyHeader: "X-API-KEY",
    })
    if err != nil {
        return tool.Result{}, err
    }
    var response map[string]any
    if err := service.DoJSON(ctx, http.MethodPost, "search", input.Args, &response); err != nil {
        return tool.Result{}, err
    }
    return tool.DataResult(response), nil
})
```

For APIs that need direct response-body access, create a request with the tool
context and execute it with the injected client:

```go
request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
response, err := input.HTTPClient.Do(request)
```

This works for Serper, Bing Search, Google Custom Search, CRM/ERP endpoints,
DingTalk or Feishu bots, GitHub, and other HTTP APIs. Authentication remains in
tool configuration or application secrets; it must not be passed through
model-visible tool arguments.

## Custom pool

The default path needs no configuration. A run can select an isolated pool for
mTLS, a custom proxy or traffic isolation:

```go
ctx := tool.WithHTTPOptions(ctx, tool.HTTPOptions{
    Pool:    dedicatedPool,
    Timeout: 10 * time.Second,
})
messages, err := value.Invoke(ctx, input)
```

Calling `http.Get`, `http.Post`, or a separately constructed client cannot be
transparently intercepted by Go. Such calls are outside framework pool
management. Managed tools should use `CallInput.HTTPClient`, `tool.HTTPClient`,
or `tool.NewHTTPService`.

Read-only tools may use the runtime retry policy. CRM writes, message pushes and
other mutating calls should leave `Spec.IsReadOnly` false to avoid automatic
retries that could duplicate side effects.
