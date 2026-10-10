# 流式输出

Graph 使用 `compiled.StreamEvents(ctx, input, stream.Config{Buffer: 64}, options...)`。
Agent 使用 `agent.StreamEvents(ctx, value, messages, stream.Config{Buffer: 64}, options...)`。
返回 `*stream.Reader[O]`：只公开 `Recv()` 和 `Close()`，内部复用
`runnable.StreamReader[stream.Event[O]]`，不提供混合事件的 Collect。
普通调用继续使用 Invoke，节点之间的 Stream/Collect/Transform 契约保持不变。

Agent 的 `Stream` 返回 `*runnable.StreamReader[[]schema.Message]`，数据面只包含 END 节点的
最终结果。LLM 增量和 Agent 中间状态经 `pkg/stream` 公共事件流发布，因此不会污染 Graph 的
输出类型；`agent.StreamEvents` 一行接入：

```go
events, err := agent.StreamEvents(ctx, assistant, []schema.Message{
    schema.NewMessage(schema.RoleUser, schema.NewTextBlock("总结这份材料。")),
}, stream.Config{}, agent.WithDeltaStream(true))
if err != nil {
    return err
}
defer events.Close()
for {
    event, err := events.Recv()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    switch event.Kind {
    case stream.KindMessage:
        pushModelChunk(ctx, event.Message)
    case stream.KindResult:
        output = *event.Result
    }
}
```

服务端将请求 `Context` 透传到模型、Graph 节点、工具、中间件与 Observer；客户端断开
或上游超时应取消该 Context。对于输出已开始的流，不要自动重试整条请求，以免重复执行
工具或重复计费。

## 四类信息

| Kind | Payload | 消费方式 |
|---|---|---|
| message | MessageUpdate | 按 MessageID 应用 Delta，或接收完整 Snapshot |
| activity | ActivityUpdate | 按 ID 显示、更新、关闭加载提示 |
| lifecycle | Lifecycle | 可选执行轨迹，通过 Config.Lifecycle 开启 |
| result | *O | 本次调用的唯一最终业务结果，不作为文本再次追加 |

事件附带 RunID、Namespace、NodeID、Sequence。Sequence 是汇集后的交付顺序，
不代表并行任务的全局因果关系。ID 为随机生成的不透明标识；不会因进程重启重新从 1 开始。
这不等于提供持久化事件或重放功能。

并行成员的事件 NodeID 使用公开成员身份：动态分派成员为目标节点名；
Namespace 携带成员所属分支名。

成功时 result 后为 EOF。运行错误由 Recv 返回，保留原始错误链；随后 EOF。
已入队事件先交付再返回运行错误；主动 Close 会丢弃尚未交付的事件并取消执行。
取消是协作式的：自定义节点和后台任务必须遵守 Context，并在节点返回前结束其发布者。
队列按事件数量限长，Buffer=0 为无缓冲；满后背压，不静默丢增量。
该限制不是字节级内存配额，单个完整媒体块可能很大。

## 消息协议

`schema.Message` 是完整消息；`schema.MessageDelta` 是构造消息的操作：

1. message_start：提供稳定 MessageID 和 Role。
2. block_start：添加内容块，块索引从 0 连续增长。
3. block_append：向文本、推理、工具参数追加片段。
4. message_metadata：替换响应元数据。
5. message_end：校验完整消息，禁止后续修改。

`schema.Message.ID`、消息事件的 MessageID、Delta.MessageID 保持一致。
前端用这个 ID 合并增量，并在收到最终结果时更新已有消息；不要再次追加相同 ID 的消息。
ResponseMeta.ID 仍然是 Provider 响应 ID，与框架消息身份分开。
缓存命中会重新分配本次输出的消息 ID，同一次调用的快照和结果共享该 ID。

自定义节点推荐使用消息写入句柄：

```go
writer, err := stream.StartMessage(ctx, schema.RoleAssistant)
if err != nil { return schema.Message{}, err }
if err := writer.AppendText("正在查询订单"); err != nil { return schema.Message{}, err }
if err := writer.AppendText("，查询完成。"); err != nil { return schema.Message{}, err }
return writer.End()
```

句柄负责 ID、块索引和消息合并。AddBlock 支持推理、工具调用及完整媒体块，
Append 向指定块追加分片，SetMetadata 写入响应元数据。End 成功后可重复调用，
返回隔离副本而不会重复发布完成事件；完成后追加会报错。
直接 PublishSnapshots 会为调用者拥有的消息切片补齐缺失 ID，已有 ID 不变。

增量不根据字符串前缀猜测快照，不去重相同文字。工具参数允许暂时不是合法 JSON，
message_end 时才执行完整校验。图片、音频、文件等以完整内容块添加，首版不提供媒体字节流。
Provider 的稀疏工具索引转换成首次出现顺序的连续公共块索引。

消费者可使用 `schema.NewMessageAccumulator()`，逐条 `Apply(delta)`，
需要时用 `Message(id)` 获取隔离快照。合并器单消费者使用，采用字符串构建器避免每次追加复制全文。
最终 result 不一定与所有生成过的消息一一对应：Agent 可能筛选、装饰消息或执行多个模型调用。
它是完整的业务返回值，不是增量流重放日志。

只有完整响应的模型通过 ChatModelNode 提供一次 Snapshot；不会伪造逐字输出。
直接使用第三方模型时，应接入 PublishMessage 或自行发布完整快照。

## 业务进度

```go
activity := stream.StartActivity(ctx, "正在搜索订单库...")
orders, err := queryOrders(ctx, input)
activity.End(err)
```

活动使用 ActivityStatus 枚举，生命周期使用 Scope、Phase 枚举。发布入口校验
载荷与类别、ID 一致性、块定位和消息操作顺序；Event.Validate 可校验解码后的事件外壳。
支持 started、updated、completed、failed、canceled。End 幂等。
活动失败不自动决定运行失败，由业务返回值决定。运行结束时框架为尚未结束的活动和
生命周期补充 failed/canceled 终态，再交付最终结果或运行错误；消费者主动 Close 后不再保证交付。
发布操作受运行 Context 和队列背压约束；StartActivity/Update/End 不把展示错误改写成业务错误。

## 模型、缓存和兼容

- OpenAI-compatible Provider 直接发布规范化增量，并由同一份增量构造最终 llm.Response。
- 已发布消息的调用失败后不自动重试，避免重复显示；重试检测不请求完整快照。
- 命中已完成的缓存发布完整 Snapshot。公开流的缓存 miss 不合并其他在途请求，
  以免把一个运行的事件与取消权共享给另一个运行；仍然写入完成缓存。
- Agent 的 result 等待会话提交完成；内部 Graph 不发布额外顶层 result。
- 旧 schema.MessageEvent、callback.EmitStream 与 internal/streamcompat 已在轻量化阶段
  删除；公共流是唯一的消息增量通道。新 UI 只消费公共流；工具观察、路由和群聊结果也发布
  带消息 ID 的公共快照。Provider 不使用前缀猜测累积器。
- 第一版不提供断线重放、持久化事件、任意历史编辑、SSE/WebSocket 服务实现。
  网络适配器只需对 Event 编码，并将终止错误转换为自己的网络错误消息。

运行 `go run ./examples/stream` 可查看正文更新与查询活动交错的 JSON 事件。
该例使用本地模拟节点，不连接数据库，也不代表框架自动并行查询和生成。

## 验证

核心：`go test ./...`、`go vet ./...`。
扩展模块：在 extensions 中运行相同命令。
本轮按项目使用者要求在所有修改完成后统一测试，不进行竞态测试。
局部基准：`go test ./pkg/schema -run '^$' -bench BenchmarkMessageTransport -benchmem`。
