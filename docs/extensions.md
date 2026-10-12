# 可选扩展

框架模块包含与 Provider 无关的契约，不选择
缓存后端或 Redis 客户端。可选实现位于嵌套的
`extensions` 模块，应用可以按需选择依赖。

## 检索组件

RAG 数据组件（分块、索引、混合检索、重排）在
`extensions/retrieval` 中，实现 `runnable.Runnable` 契约，
可直接接入图节点。用法与 HTTP 服务示例见
[extensions/retrieval 文档](../extensions/retrieval/README.md)。

## 子 Agent

`extensions/subagent` 让父 Agent 在工具调用里把任务交给同步运行的子 Agent：
`run_subagent` 一次性执行、`spawn_agent`/`followup_task` 会话子 Agent、
`list_agents`/`get_agent`/`close_agent` 管理，审批挂起与恢复由引擎命名空间
执行树传播到根入口。配置与语义见 [Agent 文档的子 Agent 节](agent.md#让模型创建和管理子-agent)，
可运行示例在 `extensions/examples/subagent`。

## 缓存装饰器

缓存扩展装饰 Provider 值并保留规范的
Runnable 契约，因此缓存模型和向量存储可以直接传给
Agent 和 RAG 管道：

```go
import (
    llmcache "github.com/goharness/goharness/extensions/cache/llm"
    retrievalcache "github.com/goharness/goharness/extensions/cache/retrieval"
)

cachedModel, err := llmcache.NewCachedModel(model, llmcache.CacheConfig{
    Store:   memoryCache,
    ModelID: "provider:model",
})

cachedStore, err := retrievalcache.NewCachedVectorStore(vectorStore,
    retrievalcache.VectorCacheConfig{
        Store:   vectorCache,
        StoreID: "documents",
    })
```

扩展包含内存和 Redis 缓存存储。核心包不会导入这些依赖，
应用也可以替换为自定义的 `CacheStore` 或 `VectorCacheStore` 实现。

## 并发压测工具

`extensions/examples/loadtest` 用可配置延迟的假模型对 Agent 做并发压测：
会话数、并发数、同会话争用比例、工具数量、历史长度、`none|memory|redis`
存储与 `async|sync` durability 都可配置；输出端到端与模型/存储各阶段的
P50/P90/P99 分位、错误计数、协程数与内存增量，支持 `-cpuprofile`、
`-memprofile` 与 `-pprofaddr`。`seed` 模式只播种会话历史，用于测量存储
侧内存留存；`-probe` 在测量窗口内探测 Redis PING 延迟。

```powershell
cd extensions
go run ./examples/loadtest -store redis -redisaddr 127.0.0.1:6379 -sessions 64 -concurrency 8 -requests 256 -history 200 -tools 2 -durability sync -probe -json result.json
```

阶段六基线测量见 [benchmarks/pre-stage6](benchmarks/pre-stage6.md)。

## Redis Checkpoint 存储

Redis Checkpoint 持久化同样是可选适配器：

```go
import redisstore "github.com/goharness/goharness/extensions/state/redis"

store, err := redisstore.NewRedisStore(redisstore.RedisStoreConfig{
    Client: redisClient,
    Prefix: "customer-support:checkpoint",
})
```

返回值实现核心 `state.Store` 接口。不需要 Redis 的应用可以继续使用核心 `state.MemoryStore`，或提供
自己的存储，而无需引入 Redis 依赖。

## MCP 工具

MCP 集成位于 `extensions/mcp`，因此核心模块无需依赖
MCP SDK 依赖。它使用 `github.com/mark3labs/mcp-go`，支持
stdio、SSE、可流式 HTTP 以及注入的 SDK 客户端。

适配器将 MCP `tools/list` Schema 转换为 `tool.Spec.Parameters`，
并将 MCP `tools/call` 结果转换为 `tool.Result`。MCP `isError` 结果仍是
模型可见内容，而不是传输错误。收到
`notifications/tools/list_changed` 通知会将动态节点标记为 dirty；
下次调用时会刷新 Schema 和路由表。

### 快速装配进 Agent

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

mcpTools, err := client.Tools(ctx)
if err != nil {
    return err
}

assistant, err := agent.New(model,
    agent.WithName("mcp-assistant"),
    agent.WithTools(mcpTools...),
)
```

`agent.New` 会把 MCP 工具和本地工具按相同的 `tool.Tool` 契约装入 ReAct 图。

### 作为动态 ToolsNode 编入自定义图

```go
mcpNode, err := client.NewToolsNode(ctx, tool.ToolNodeConfig{
    Name:           "mcp-tools",
    MaxConcurrency: 8,
})
```

`mcpNode` 实现 `runnable.NamedRunnable[[]schema.Message, []schema.Message]`。它可以直接通过 `g.AddNode` 接入自定义图，Pre/Post Handler 负责读取和更新
业务 State，MCP 节点本身不需要知道 State 类型。

`ToolsNode` 对节点级 `MaxConcurrency` 以及每个已注册工具的
`Spec.Concurrency` 并发组上限统一生效。它与 Agent 共享公开的 `tool.Runtime`，
因此 Middleware、重试、超时、递归和授权策略遵循相同的执行契约。

## OpenTelemetry

`extensions/otel` 把 `pkg/observe` 事件映射为 OTel span：

```go
ctx = otel.With(ctx, tracer) // 或 observe.With(ctx, otel.Observer(tracer))
```

接入与事件模型见[观测](observability.md)。
