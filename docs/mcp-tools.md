# MCP Tools

The core `pkg/tool` package exposes `BaseTool` and `ToolsNode`. `ToolsNode`
accepts the trailing assistant function-call messages, executes only that
suffix concurrently, and returns ordered tool-result messages to append before
the next model turn.

The MCP integration lives in `extensions/mcp` so the core module does not take
on an MCP SDK dependency. It uses `github.com/mark3labs/mcp-go` and supports
stdio, SSE, streamable HTTP, and injected SDK clients.

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

The adapter translates MCP `tools/list` schemas into `tool.Spec.Parameters`
and MCP `tools/call` results into `tool.Result`. MCP `isError` results remain
model-visible content rather than becoming transport errors. A
`notifications/tools/list_changed` notification marks the dynamic node dirty;
the next invocation refreshes the schema and routing table.

For React agents, the tools node is assembled automatically:

```go
tools, err := client.Tools(ctx)
if err != nil {
    return err
}
agentValue, err := react.NewAgent(ctx, react.Config{
    BaseConfig: agent.BaseConfig{
        Model: llmModel,
        Tools: tools,
    },
})
```

The resulting recipe contains the bounded `llm -> tools -> llm` loop. Users do
not need to register a second tools node manually.

`ToolsNode` enforces its node-wide `MaxConcurrency` and each registered
tool's `Spec.Concurrency` group limit across all simultaneous invocations.
Agent middleware, retry, timeout, recursion, and authorization policies are
owned by the Agent runtime; assemble tools through an Agent when those
controls are required.
