# 现有 callback 整合与缺陷修复

> 更新时间：2026-10-15。按用户确定的方向实施：沿用现有 callback，统一模型调用的生命周期，不增加模型观测装饰器或新的公共观测层。

## 目标与边界

修复模型快照发送失败的终止事件漏报，集中节点和 Reader 的收尾责任，并补齐日志关联身份。

调用链继续为：ChatModelNode / ObserveReader → BeginCallback、EndCallback、ErrorCallback → observe.Event → Observer → 日志、指标或 OTel。

核心不增加依赖；既有公共 callback、ChatModel、Runnable 和强类型实时流接口继续使用。模型生成、重试、限流与缓存策略沿用既有实现。

## 类型与所有权

| 类型 / 接口 | 可见性 | 职责 |
| --- | --- | --- |
| ChatModel | 公共，现有 | 模型执行契约 |
| ChatModelNode | 公共，现有 | 节点描述、模型元数据、执行入口 |
| observe.Invocation | 公共，现有 | 某次调用的身份和只读快照 |
| modelCallback | 包内 | 持有 Invocation，通过同一 finish 入口发出一次终止事件 |
| observe.Event / Observer | 公共，现有 | 统一事件传递与消费 |

包内 modelCallback 没有模型执行方法，也不实现 ChatModel；它只协调现有 callback 的生命周期。

## 实施内容

### 1. 集中模型 callback 收尾

- pkg/llm/callback.go：增加包内开始与收尾辅助对象；以一次性守卫防止重复终止。
- pkg/llm/node.go：开启调用后，启动失败、空 Reader 和流式归约共用同一 callback 对象。
- pkg/llm/response_stream.go：成功、上游错误、响应截断、快照投递失败、输出管道失败统一进入终止处理。
- 终止 callback 先递交，再关闭输出管道。调用方主动取消可以先返回，生产协程退出后完成终止事件。
- 关闭观测时包内 callback 对象为 nil；消息投递与取消清理依旧执行。
- ObserveReaderWithInvocation 接续传入身份，不发第二次开始事件。手工使用公开 callback 的调用方仍负责配对。

### 2. 补日志身份

- pkg/observe/slog.go：输出 kind、phase、trace_id、span_id、parent_span_id、run_id。
- 关联字段取自事件身份，普通 Attrs 不能覆盖它们；其他元数据继续输出。

### 3. 更新接入文档

- docs/observability.md：说明 callback 的节点 / Reader 归属和取消收尾时序。
- 独立调用可复用现有 ChatModelNode，它已经实现 ChatModel，无需启动 Graph。

## 验证

已新增的行为回归：

- 慢消费者使快照投递失败：一次开始、一次错误、零次成功结束；公共流保持慢消费者错误。
- 主动关闭观测 Reader：取消上游并产生取消终止事件。
- JSON 日志含真实调用身份，业务元数据不能覆盖；其他模型元数据保留。

首次运行已确认慢消费者漏报和日志关联测试失败；修改后 pkg/llm、pkg/observe 的完整测试通过。

统一验收命令：

```powershell
go test -count=1 ./...
go vet ./...
# 在 extensions 模块执行同样的两项检查
```

验收包括已有 Graph / Agent 调用树、错误与中断、OTel 收尾、重试和缓存测试。不运行竞态测试。

2026-10-15 验收结果：根模块和 extensions 模块的 `go test -count=1 ./...`、
`go vet ./...` 均通过；慢消费者漏报与日志关联回归通过。未运行竞态测试。

## 后续独立事项

首内容延迟、每次真实请求、限流等待、缓存用量归约及 HTTP 跨服务传播，应分别明确事件和指标口径后沿用现有 callback 扩展。它们不是本次生命周期整合的已完成项，也不要求增加新的模型类型。
