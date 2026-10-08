# 通用交互中断与携带输入恢复

`graph.Interrupt(ctx, payload)` 用于补充信息、选择方案、人工修订、等待外部系统和审核等场景。Graph 不解释输入的业务含义，工具审批只是一个使用例。

## 发起与恢复

在顺序根 Graph 的节点或 PreHandler 中：

```go
answer, err := graph.Interrupt(ctx, map[string]any{
    "field": "destination",
    "prompt": "请选择目的地",
})
if err != nil {
    return "", err
}
destination, ok := answer.(string)
if !ok {
    return "", errors.New("目的地必须是字符串")
}
return destination, nil
```

运行配置 `graph.WithCheckpoint(store, key)`，Graph 必须有可序列化的 State，每个顺序节点有 PreHandler。首次发起交互时调度器保存检查点，再返回 `*graph.InterruptedError`；保存失败直接返回存储错误。无需保持 goroutine 等待用户。

```go
var interrupted *graph.InterruptedError
if errors.As(err, &interrupted) {
    questions := interrupted.PendingInterrupts()
    // 将 ID、Payload 交给界面或外部系统，之后在新请求中提交输入。
    result, err = compiled.Resume(ctx,
        graph.WithCheckpoint(store, key),
        graph.WithResume(questions[0].ID, "上海"),
    )
}
```

页面刷新或进程重启后，可调用 `compiled.PendingInterrupts(ctx, graph.WithCheckpoint(store, key))` 查询待输入问题。返回独立副本，只包含未回答记录。检查点的 key 应由业务绑定到正确的用户/会话，并在服务端校验操作权限。

Agent 使用 `agent.WithResume(id, value)`，通过 `agent.Resume`、`agent.ResumeStream`、`agent.ResumeStreamEvents` 恢复；Graph 对应支持 `Resume`、`ResumeStream`、`ResumeStreamEvents`。

## 输入与重放约定

- Payload 和恢复输入必须能够编码为 JSON。恢复返回 JSON 基础类型：对象为 `map[string]any`、数组为 `[]any`、数字为 `json.Number`；不会自动还原业务 Go 结构体。需要结构体时显式编解码。`nil` 是合法的 null 输入，与未提供输入不同。
- 恢复重新运行中断节点的 PreHandler 和节点代码，不恢复 Go 调用栈。挂起节点使用进入前的 State 快照，PreHandler 的增量修改不会随每次重放累加。
- 同一节点多次 Interrupt 按顺序匹配。已回答的问题返回保存的输入，未回答的问题产生新的 ID；Payload 必须保持一致。调整分支导致跳过已记录问题或改变 Payload 会报错。
- 节点循环的下一次执行具有新的问题 ID。未知、重复、已回答的 ID 会被拒绝；携带输入只能调用 Resume 系列入口。
- 输入在执行节点前持久化。若后续执行失败，可用不带 WithResume 的 Resume 重试已接受输入，不能重复提交同一个 ID。接受输入后的运行会在节点提交并完成路由后推进检查点到下一节点，清理当前节点的输入记录；整次 Graph 执行完成后保存结果；之后不带输入的 Resume 直接读取结果，再提交人工输入则返回错误。
- 节点前的外部副作用可能重放；将写操作放在交互之后，或使用幂等键。State 快照不会回滚数据库或远程请求。
- PostHandler、路由函数、并行合并器内不支持动态 Interrupt，调用会返回明确错误。嵌套 Graph / 子 Agent 内的顺序节点支持 Interrupt：挂起经执行树传播到根运行，统一返回 `InterruptedError`，从根入口 Resume 恢复；子调用已完成的结果在恢复时直接复用。并行分支（原生 fork 任务、`AddParallelNode` 成员、动态分派成员）节点体内也支持直接调用 Interrupt：每个成员拥有独立的交互日志与入口 State，`AddParallelNode` 成员的待答问题节点身份为 `组名/分支名`；只回答部分成员时其余成员保持等待且不重放，分批回答后全部就绪才合并。工具节点内不同调用各自的 Interrupt 按调用身份归属日志，可分批回答并正确恢复；同一调用内并发发起多个 Interrupt 的顺序仍无法稳定重放，不受支持。
- Routing / GroupChat 下的子 Agent 属于嵌套 Graph：子 Agent 内的 Interrupt 会使整个运行挂起并从根入口恢复，不会被转成模型可见的观察结果，也不会让父级“成功”结束。未配置根检查点时嵌套 Interrupt 仍以 `graph.ErrInterruptUnsupported` 失败。`pkg/subagent` 受管任务内的审批同样作为待答问题递归汇总到根：只回答其中一个任务时该任务先推进，其余任务保持挂起；已完成父任务不重跑，根执行到末尾等待全部任务收敛后才提交最终结果。
- 不再回应挂起的交互时，可用 `state.AbandonRun` 或 Agent 的 `WithAbandonUnfinished` 放弃该运行，见 [统一检查点](checkpoints.md#放弃未完成的运行)。
- 普通无检查点运行不增加 State 编码。配置检查点后，具备交互条件的节点会保存入口快照，并在启动下游前收集输出流以确认是否挂起；节点仍调用 Stream，公开消息增量继续实时发布。
- 统一存储支持历史查询和版本冲突检查；应用仍必须串行化同一会话的运行与恢复请求。不保证并发恢复、崩溃或存储失败时外部操作恰好执行一次。完成标记写入失败后重试也可能重复副作用。
- 外部 `control.RequestInterrupt` 继续表达运行暂停，不要求携带输入；它与交互中断具有不同用途。

## 公共事件流

`StreamEvents` / `ResumeStreamEvents` 在挂起时发布 `lifecycle.phase = "interrupted"`，Graph 生命周期的 `interrupts` 携带待输入的 ID、节点和 Payload。即使 `Config.Lifecycle` 为 false，也会发布一次等待输入通知。

- 不带答案的 `Resume` 按实际进度决定行为：存在可推进的任务或子调用时继续执行；全部仍在等待时返回当前的 `InterruptedError` 与原问题 ID，不空跑节点也不产生新检查点版本。未知、重复或已回答的问题 ID 仍属于输入错误。

挂起时不会发布最终 result；Reader 随后返回同一个 `InterruptedError`。前端据此展示表单或等待状态，而非执行失败。尚未结束的活动会关闭为 canceled，不再保持加载动画；开启生命周期时未结束的节点和 Graph 关闭为 interrupted。

参见 `examples/interaction`（补充信息、选择语言）和 `examples/basic`（工具审批）。

## 本次范围

包含交互记录、持久化、节点重放、输入匹配、查询待输入问题、Agent 入口和公共事件流；嵌套 Graph / 子 Agent 的挂起与恢复由执行树检查点覆盖（见 [统一检查点](checkpoints.md)），并行分支（原生 fork 任务、`AddParallelNode`）内的直接交互同样支持。历史查询由同一检查点提供。任意 State patch、静态断点仍未包含。
