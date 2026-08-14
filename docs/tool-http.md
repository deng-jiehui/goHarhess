# HTTP 工具与外部 API

`tool.Runtime` 管理的每次工具调用都会在
`tool.CallInput.HTTPClient` 中获得连接池客户端。也可通过
`tool.HTTPClient(ctx)`. Built-in defaults use `httpclient.DefaultPool`, so tool
工具请求与 LLM、检索、Embedding 和 Rerank 集成共享框架 Transport。

使用请求 Context，使取消信号和 Agent 工具调用截止时间传递到网络操作：

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

对于需要直接访问响应体的 API，使用工具 Context 创建请求，并通过注入的客户端执行：

```go
request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
response, err := input.HTTPClient.Do(request)
```

这适用于 Serper、Bing Search、Google Custom Search、CRM/ERP 端点、DingTalk 或 Feishu 机器人、GitHub 及其他 HTTP API。认证信息应保留在工具配置或应用密钥中，不得通过模型可见的工具参数传递。

## 自定义连接池

默认路径无需配置。运行时可为 mTLS、自定义代理或流量隔离选择独立连接池：

```go
ctx := tool.WithHTTPOptions(ctx, tool.HTTPOptions{
    Pool:    dedicatedPool,
    Timeout: 10 * time.Second,
})
messages, err := value.Invoke(ctx, input)
```

Go 无法透明拦截 `http.Get`、`http.Post` 或单独构造的客户端。这些调用不受框架连接池管理。受管理的工具应使用 `CallInput.HTTPClient`、`tool.HTTPClient` 或 `tool.NewHTTPService`。

只读工具可以使用 Runtime 重试策略。CRM 写入、消息推送及其他变更调用应保持 `Spec.IsReadOnly` 为 false，以避免自动重试造成副作用重复。
