# Callback 执行模型设计

## 目标

将 goHarness 的 Callback 从“有界异步队列”改为默认同步直调，保证生命周期事件在
业务调用返回前已经完成递交，并把流式 Chunk 观测与生命周期结算明确分开。

本次改造覆盖 Callback 公共配置、Agent/Graph/LLM/Tool 的事件分发、错误隔离、Chunk
过滤和相关文档测试。Middleware 的同步业务控制职责不变。

## 非目标

- 不把 Callback 变成可拒绝、改写或替代业务结果的 Middleware。
- 不在 Agent 默认路径内创建异步队列、worker、背压或 Flush 生命周期。
- 不恢复已移除的旧 Agent API，也不保留旧 Callback 配置的队列语义。

## 公共模型

### Handler

`callback.Handler` 是同步观测接口：

```go
type Handler interface {
    Handle(context.Context, Event) error
}
```

`CallbackFunc`、`Funcs` 实现 `Handler`。现有 `Callback` 名称如果为了降低一次性迁移
成本保留为类型别名，也只表示同步 Handler，不携带任何旧的异步行为。

Agent 配置使用 `Handlers []callback.Handler` 语义，`WithCallbacks` 的参数类型为
`...callback.Handler`。Middleware 继续通过 `WithMiddlewares` 配置，任何拒绝、输入
输出转换、重试和超时控制都只由 Middleware/`internal/toolruntime` 负责。

### Config

```go
type Config struct {
    Timeout      time.Duration // 单个 Handler 的协作式 deadline；零值表示不限制
    FilterChunks bool          // true 时忽略 agent.chunk 与 llm.chunk
}
```

删除 `QueueSize`、`MaxConcurrency`、`ErrorHandler` 以及相关默认值和容量校验。
`Timeout` 为零时直接使用调用 Context；大于零时为每个 Handler 派生带 deadline 的
Context。Handler 必须尊重 Context 取消；框架不通过后台 Goroutine 强行终止同步
调用。Handler 返回时如果 deadline 已到，分发器记录一个 `TimeoutError`，但不会把
该 Handler 永久禁用，后续事件仍按同步顺序执行。

`SyncDispatcher.Dispatch(ctx, event) error` 返回本次事件中所有 Handler 错误的聚合值，
错误类型保留 Handler 名称和事件快照。Agent、Graph、LLM 和 Tool 的业务路径显式
忽略这个返回值，因此 Callback 失败不会改变业务结果；直接使用 Dispatcher 的应用可以
自行记录或统计该错误。这样错误处理不再依赖隐含的后台 ErrorHandler。

## 组件职责

### SyncDispatcher

`SyncDispatcher` 只负责：保存不可变 Handler 列表、分配事件序号、复制事件快照、按
注册顺序同步执行 Handler，并捕获每个 Handler 的 panic/error 后继续执行后续 Handler。

`Dispatch` 的行为如下：

1. 空 Dispatcher 或空 Handler 列表直接返回。
2. 如果 `FilterChunks` 为 true，丢弃 `EventAgentChunk` 和 `EventLLMChunk` 的观测
   分发，但不影响 Agent/LLM 本身的流和最终指标。
3. 给事件分配单调递增的 `Sequence`，克隆 Metadata/Metrics 后逐个调用 Handler。
4. Handler 使用当前业务 Context；调用方可以在自己的 Handler 内使用
   `context.WithoutCancel(ctx)` 将计费、审计写入与 HTTP 请求取消解耦。
5. 错误和 panic 转换为 `CallbackError`，由 `Dispatch` 聚合返回；绝不能影响业务调用
   或后续 Handler。

同步调用不再有“提交后等待”的窗口，因此删除队列、worker、`Wait`、Dropped 统计和
异步状态机。嵌套 Agent/Graph 仍通过 Context 复用同一个根 Dispatcher；有本地 Handler
时使用轻量的 child dispatcher，先向 parent 转发，再执行本地 Handler，保持现有
scope/trace 关系。

### AsyncObserverManager

异步需求不属于 Agent 默认执行模型。若保留异步观察能力，`AsyncObserverManager` 必须
作为与 `SyncDispatcher` 完全分离的显式 opt-in 组件，使用独立的 `Observer` 接口和
配置，不能复用 `callback.Config`，也不能由 `WithCallbacks` 隐式创建。Agent 默认不
构造它。该组件只负责把克隆后的事件交给用户提供的异步观察器；队列容量、重试、关闭
和丢弃策略由调用方负责，不承诺无损。

推荐的应用侧包装保持最小化：一个 Handler 克隆 Event 后启动用户自己的 Goroutine，
并自行决定缓冲、生命周期、重试和关闭策略。框架不承诺该异步包装的无损递交；需要
无损的计费/审计必须使用同步 Handler。

## 生命周期与流数据

- Agent、LLM、Retriever、Tool、Graph 的 start/end/error 事件均通过 `SyncDispatcher`
  同步执行。
- Agent/LLM 的 Chunk 继续由原有 Stream 管道传递，不依赖 Callback 才能推进流。
- `FilterChunks` 仅控制 Chunk 观测，不改变流内容、顺序、取消和 Done 语义。
- LLM 使用事件中的 Usage 更新累计 `Metrics`，`llm.end` 一次性携带最终 token 统计。
- Agent 的 `agent.end` 携带本次运行累计 chunk、消息、字节和工具调用统计。

## 错误处理

Callback 错误永远不替换 Agent、LLM、Tool 或 Graph 的业务错误。每个 Handler 独立恢复
panic；一个 Handler 失败后仍执行同一事件的后续 Handler。同步错误报告入口只用于日志、
测试或应用自定义告警；框架调用点统一忽略 `Dispatch` 返回值，报告逻辑不再在
Dispatcher 内部偷偷起 Goroutine。

Context 取消仍会传递给同步 Handler，这是共享执行上下文的必要部分。需要在请求取消
后继续写入的 Handler 必须显式派生 `context.WithoutCancel`，而不是依赖框架偷偷脱离
取消链。

## 数据流

```text
Agent/LLM/Tool/Graph 产生生命周期事件
          |
          v
   SyncDispatcher.Dispatch (当前 Goroutine)
          |
    +-----+-------------------+
    |                         |
Handler 1 -> Handler 2 ...   FilterChunks=true 时跳过 chunk
    |
    +--> Dispatch 返回 CallbackError（隔离，不回写业务结果）

LLM StreamReader ----------------------> 下游模型/Agent
             \-- llm.end 携带最终 Usage/Metrics
```

## 迁移与验证

文档和示例移除 `QueueSize`、`MaxConcurrency`、`ErrorHandler` 的 Callback 配置；Agent
自身的工具 `MaxConcurrency` 保持不变。回归测试覆盖：

- Dispatch 在 Handler 返回前阻塞，事件不丢失且按注册顺序执行。
- Handler error/panic 不阻断后续 Handler，`Dispatch` 返回聚合错误但不反向影响业务。
- `Timeout == 0` 不创建 deadline，正值按协作式 deadline 处理。
- `FilterChunks` 对 Agent/LLM Chunk 生效，end 事件和最终 token 指标仍递交。
- 取消 Context 的值可见；需要持久化的 Handler 可显式使用 `WithoutCancel`。
- 嵌套 Agent/Graph 的 parent/child scope、trace 和事件序号保持正确。
- 全仓库 `go test -count=1 ./...`、`go vet ./...`，以及配置 MinGW-w64 GCC 后的
  `go test -race -count=1 ./...`。
