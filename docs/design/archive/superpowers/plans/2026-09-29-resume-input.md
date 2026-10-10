# 通用携带输入恢复实施计划

**目标：** Graph 提供可持久化的动态交互中断，恢复输入由节点解释，Agent 与公共事件流复用该机制。

**架构：** Interrupt 返回类型化控制错误；调度器保存节点入口 State 与顺序化交互记录。恢复按中断 ID 匹配输入，从节点入口重放已回答位置，节点成功提交后清除当前作用域。无人工输入的旧中断保持兼容。

**约束：** 不修改已有未提交工作的无关内容；中文注释；按用户要求集中运行测试一次，不运行竞态测试；不增加外部依赖。不扩展 History、任意 State patch、静态断点。并行分支与嵌套 Graph 内的动态交互明确拒绝。用户允许删除冗余 API，不为兼容而新增过渡层；外部暂停仍有独立用途。

- [x] 新增 `pkg/graph/interrupt_input_test.go`：覆盖非工具场景、多次交互、重启恢复、入口 State 重放、无效 ID、null、内容变化、保存失败及流语义。
- [x] 新增 `pkg/control/interaction.go`、`pkg/graph/interrupt_input.go`：JSON 交互记录、Interrupt、WithResume、读取待处理交互；恢复选项只被目标 Graph 消费。
- [x] 修改 `pkg/graph/continuation.go`、`runtime_execute.go`：检查点 v2 兼容 v1，入口快照、交互记录重放、保存成功后才返回 InterruptedError；提交阶段禁止中断。
- [x] 修改 `pkg/agent/interrupt.go`、`public_stream.go` 及 `pkg/graph/public_stream.go`：WithResume 与 ResumeStreamEvents。
- [x] 修改 `pkg/stream/event.go`、`run.go`、`closure.go`：挂起生命周期与交互摘要，不生成成功 result；默认事件流也能看见待输入通知。
- [x] 添加 Agent 集成测试，更新工具审批示例，新增通用交互示例与文档，注明序列化、重放、副作用、并发恢复边界。
- [x] 格式化并审查变更，集中执行核心与扩展模块测试（不含 race），记录结果。

## 验证结果

核心与扩展各执行 `go test ./...`；默认构建缓存无访问权限，改用临时目录 GOCACHE。集中验证发现选项适配编译错误，修正后仅补跑 `./pkg/graph ./pkg/agent ./internal/agentrecipe ./examples/...` 与扩展 `./mcp`；所有包通过。无竞态测试。独立只读审查发现的后续节点失败无法恢复问题已修复并加入回归测试，复核无阻断问题。

## 对外接口

```go
graph.Interrupt(ctx, payload) (any, error)
graph.WithResume(interruptID, value) runnable.Option
compiled.PendingInterrupts(ctx, graph.WithCheckpoint(store, key)) ([]control.InterruptRecord, error)
agent.WithResume(interruptID, value) agent.RunOption
compiled.ResumeStreamEvents(ctx, config, options...)
agent.ResumeStreamEvents(ctx, value, config, options...)
```

Payload、输入持久化为 JSON；返回的动态数字采用 json.Number，业务结构可通过 JSON 解码。节点内 Interrupt 调用顺序与 Payload 必须稳定。已接受输入的运行逐节点推进检查点，最终标记完成，避免后续失败迫使重做交互节点。
