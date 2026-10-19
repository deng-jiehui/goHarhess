# 观测

`pkg/observe` 是唯一的观测入口：`observe.With` 把 `Observer` 注册到运行 Context，
一次运行的 run / node / llm / tool / interrupt / checkpoint 事件全部送达同一观察者
（retrieval、MCP 等扩展使用自定义 `Kind`）。一行接入，即可看到一次运行的调用树、
耗时和 token 用量。

```go
observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
    switch {
    case event.Kind == observe.KindLLM && event.Phase == observe.PhaseEnd:
        if event.Usage != nil {
            log.Printf("tokens=%d", event.Usage.TotalTokens)
        }
    case event.Kind == observe.KindNode && event.Phase == observe.PhaseEnd:
        log.Printf("node=%s step=%v duration=%s",
            event.Name, event.Attrs["graph.step"], event.Duration)
    case event.Kind == observe.KindTool && event.Phase == observe.PhaseError:
        log.Printf("tool=%s err=%v", event.Name, event.Err)
    }
})

ctx = observe.With(ctx, observer)
output, err := assistant.Invoke(ctx, input)
```

## 事件模型

每条事件携带 `Trace{TraceID, RunID, SpanID, ParentSpanID}`、开始时间、耗时与少量结构化
属性（`graph.step`、`graph.node_id`、`graph.node_kind`、`llm.finish_reason`、
`tool.abandoned`、输入输出类型名等）；llm 结束事件带 token 用量。Graph 为每个节点创建
独立子 Span，嵌套 Agent / 子图形成调用树，APM 系统保留节点级瀑布时序。

llm 结束事件中的 `Usage` 由模型适配器从 Provider 响应转换而来，包含输入、输出、总量、
推理和提示缓存 Token（`CacheReadTokens`/`CacheWriteTokens`）。框架不会根据请求内容自行
推算缓存命中；Provider 没有返回缓存字段时，缓存指标保持未知或为零，前端应按字段是否
存在决定显示数值还是 `—`。

事件默认不含请求/响应载荷——需要时用 `observe.CapturePayload(redact)`
显式开启并把脱敏函数交给 `With`。

## 内置接入

- `observe.Slog(logger)` 打印缩进调用树（见 `examples/observe`）。
- `extensions/otel` 的 `otel.Observer(tracer)` 把事件映射为 OTel span：

```go
ctx = otel.With(ctx, tracer) // 或 observe.With(ctx, otel.Observer(tracer))
```

## 同步语义与异步包装

Observer 同步直调、按注册顺序运行，`Invoke` 返回时已产生的事件已处理完毕；它适合审计、
计费和关键日志。Observer 的 panic 被隔离并记录到 `slog`，不会改变业务结果；Observer
接口不返回 error，失败上报由接入方自己排队。

Observer 只做快速记录：整理事件、更新计数、写入本地日志或提交到有界队列，远程发送、
通知等耗时工作交给接入的日志 SDK、告警组件或 `observe.Async` 包装在后台完成。执行期限
结束会触发返回流程，但实际返回仍要等待同步 Observer 结束——慢 Observer 会拖延
`Execute` 返回，框架不强制终止用户观测者。

需要异步上报时用 `observe.Async(observer, buffer, dropWhenFull)` 包装：它有有界队列、
可选的丢弃策略和 `Dropped()` 计数；`Close` 排空后停止。接入组件使用队列时要有容量限制，
队列满时明确拒绝或丢弃并计数，不做无界入队。计费、审计等需要确定性递交的
工作应保持同步 Observer。

工具放弃告警（`AbandonedError`）以 `Kind==tool`、`Phase==error`、
`Attrs["tool.abandoned"]==true` 的事件派发，并在调用方 Context 上发射。

Observer 内部如需在 HTTP 请求取消后继续写审计库，应用自行派生持久化 Context，并设置
自己的上限：

```go
persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
defer cancel()
err := writeAudit(persistCtx, event)
```

## 实时事件流

实时数据（消息增量、业务进度、生命周期通知）不进入观测事件流，而走 `pkg/stream`
公共流（`KindMessage`/`KindActivity`/`KindLifecycle`/`KindResult`）。组件用
`stream.PublishMessage`/`PublishSnapshots`/`PublishActivity` 发布，Agent/Graph
运行期间自动生效；裸组件可用 `stream.Run` 开启作用域，调用方从 `stream.Reader`
逐条 `Recv` 事件。详见[流式输出](public-stream.md)。

观测（observe）与实时流（stream）的分工：observe 是事后审计与指标通道，事件随
生命周期同步递交；stream 是消费侧实时数据通道，受队列背压约束。两者互补，
互不替代。

## 模型 callback 归属

模型观测沿用 `llm.BeginCallback` / `EndCallback` / `ErrorCallback`，统一发送
`observe.KindLLM` 事件。`ChatModelNode` 在执行入口组装模型信息并开启调用，
流式 Reader 归约负责收尾；两者共用包内 callback 对象，终止事件只递交一次。
快照发布失败、上游错误和响应截断均产生 error 事件；主动关闭 Reader 会取消上游，
终止事件由生产协程退出时递交。

独立模型调用也可以复用 `llm.NewChatModelNode(model)`，它已经实现 `ChatModel`，
无需启动 Graph。裸 Provider 不会自动发射这些生命周期事件；手动调用公开 callback
时，由调用方配对开始与终止。`ObserveReaderWithInvocation` 接续已有调用身份，
不会额外发射开始事件。

`observe.Slog` 默认输出 `kind`、`phase`、`trace_id`、`span_id`、`parent_span_id`
和 `run_id`，可以按调用身份关联并发日志；事件元数据不能覆盖这些关联字段。
