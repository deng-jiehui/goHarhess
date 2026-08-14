# MCP 工具

核心 `pkg/tool` 包提供 `Tool` 和 `ToolsNode`。`ToolsNode`
接收末尾的 Assistant 函数调用消息，仅并发执行该后缀，并返回有序的工具结果消息，供下一轮模型调用前追加。

MCP 集成位于 `extensions/mcp`，因此核心模块无需依赖
MCP SDK 依赖。它使用 `github.com/mark3labs/mcp-go`，支持
stdio、SSE、可流式 HTTP 以及注入的 SDK 客户端。

```go
client, err := mcp.NewClient(mcp.Config{
    Transport: mcp.TransportStreamableHTTP,
    Endpoint:  "https://example.com/mcp",
    Pool:      httpclient.DefaultPool,
})
if err != nil {
    return err
}
defer client.Close()

toolsNode, err := client.NewToolsNode(ctx, tool.ToolNodeConfig{
    Name:           "mcp-tools",
    MaxConcurrency: 8,
})
if err != nil {
    return err
}

// ToolsNode remains a pure message Runnable. Graph handlers map the
// run-exclusive State to its input and apply its output.
err = agentGraph.AddNode("mcp-tools", toolsNode,
    graph.WithPreHandler(func(
        _ context.Context, state *AgentState,
    ) ([]schema.Message, error) {
        return schema.CloneMessages(state.Messages), nil
    }),
    graph.WithPostHandler(func(
        _ context.Context, state *AgentState, output []schema.Message,
    ) error {
        state.Messages = append(state.Messages, schema.CloneMessages(output)...)
        return nil
    }),
)
```

适配器将 MCP `tools/list` Schema 转换为 `tool.Spec.Parameters`，
并将 MCP `tools/call` 结果转换为 `tool.Result`。MCP `isError` 结果仍是
模型可见内容，而不是传输错误。收到
`notifications/tools/list_changed` 通知会将动态节点标记为 dirty；
下次调用时会刷新 Schema 和路由表。

对于 React Agent，工具节点会自动组装：

```go
tools, err := client.Tools(ctx)
if err != nil {
    return err
}
agentValue, err := agent.NewReactAgent(agent.ReactAgentConfig{
    BaseConfig: agent.BaseConfig{
        Model: llmModel,
        Tools: tools,
    },
})
```

生成的配方包含有界的 `llm -> tools -> llm` 循环。用户无需手动注册第二个工具节点。

`ToolsNode` 对节点级 `MaxConcurrency` 以及每个已注册
tool's `Spec.Concurrency` group limit across all simultaneous invocations.
它与 Agent 共享公开的 `tool.Runtime`，因此 Middleware、重试、超时、
递归和授权策略遵循相同的执行契约。
