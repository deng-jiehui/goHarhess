# 检查点与中断

Graph 和 Agent 共用 `state.Store`。检查点同时保存会话内容、执行进度和结果，不再分别写入会话存储与执行存储。

## 会话与持久化配置

无状态请求不需要额外配置。要跨轮保存会话，先配置 `state.Store`，然后每次运行指定
稳定且经过鉴权隔离的会话 ID。

```go
contextBuilder, err := contextengine.NewContextBuilder(contextengine.Config{})
if err != nil {
    return err
}
store := state.NewMemoryStore(state.MemoryStoreConfig{TTL: 24 * time.Hour})
assistant, err := agent.New(model,
    agent.WithContextBuilder(contextBuilder),
    agent.WithCheckpointStore(store),
)

output, err := assistant.Invoke(ctx, []schema.Message{
    schema.NewMessage(schema.RoleSystem, schema.NewTextBlock("回答使用中文。")),
    schema.NewMessage(schema.RoleUser, schema.NewTextBlock("记录我的偏好。")),
},
    agent.WithConversationID("tenant-a:user-42:session-7"),
    agent.WithStateValue("locale", "zh-CN"),
)
```

`WithConversationID` 没有 `CheckpointStore` 会返回错误。生产环境使用 Redis 等耐久 Store，
并以 `tenant:user:session` 之类的命名规则隔离会话。State Values 和 Checkpoint 内容必须
可 JSON 序列化；不要写入 client、channel、context、函数、密钥或其他运行时对象。

会话与执行进度统一由 `state.Store` 保存。同一会话的多轮运行共用会话 ID，每次新运行有自己的 RunID；每份检查点有独立 ID 和 ParentID。完整会话与执行状态一起提交，结束时同时保存最终结果。

Graph 的执行快照、游标和版本校验仍由 `pkg/graph` 负责，存储层保留其 JSON 数据。不要将会话 ID 当作执行锁，同会话的运行和恢复应由应用串行化。

## 一个检查点，包含执行进度

对外只有 `state.Checkpoint`：标识、运行状态、会话、执行进度和最终结果都属于这一份记录。`Execution` 保存 State、下一轮任务、分支汇合和人工输入记录，其内容由 Graph 内部解释；存储层不需要了解调度规则。

整份检查点使用一个 `SchemaVersion`，当前为 v3。执行进度没有独立的格式版本、创建时间或公开导出接口。State Codec 的版本仍然保留，用来标识业务 State 的编码方式。

中断后可以直接取得已经成功保存的检查点，并读取待回答的问题：

```go
var paused *graph.InterruptedError
if errors.As(err, &paused) {
    fmt.Println("检查点：", paused.Checkpoint.ID)
    for _, question := range paused.PendingInterrupts() {
        fmt.Println(question.ID, string(question.Payload))
    }
}
```

`paused.Checkpoint` 是保存返回值的独立副本，带有实际的 ID、Version 和 RunID。页面刷新后也可以通过 `store.Load` 读取检查点，再用 Graph 的 `PendingInterrupts` 查询待回答的问题。

导出与解码统一使用 `checkpoint.JSON()` 和 `state.DecodeCheckpoint(data)`，无需另外处理一份恢复对象。`Resume` 从配置的 Store 加载完整检查点，再校验其中的执行进度。格式错误统一返回可用 `errors.Is(err, state.ErrInvalidCheckpoint)` 判断的错误；Graph revision 或 State Codec 不匹配仍有对应的具体错误。

## 配置与恢复

```go
store := state.NewMemoryStore()
option := agent.WithCheckpoint(store, "tenant-a:session-42")
result, err := assistant.Invoke(ctx, input, option)
// 普通执行失败后，可由调用方决定是否重试。
result, err = agent.Resume(ctx, assistant, option)
```

也可以在构造 Agent 时通过 `agent.WithCheckpointStore` 配置存储，调用时只传 `agent.WithConversationID`。这两种方式使用同一套机制。如果同时指定 `WithCheckpoint` 和 `WithConversationID`，两处 ID 必须一致，存储以本次 `WithCheckpoint` 为准。直接使用 Graph 时传入 `graph.WithCheckpoint(store, id)`。

`graph.NewState` 门面与 `Graph.Compile` 也支持把存储绑定在编译期、把会话 ID 推迟到运行期：

```go
app, err := g.Compile(graph.Checkpointer(store))
out, err := app.Invoke(ctx, input, graph.Thread("tenant-a:session-42"))
out, err = app.Resume(ctx, graph.Thread("tenant-a:session-42"))
```

`Thread` 只选择线程 ID；未绑定存储（`Compile` 未传 `Checkpointer` 且本次未给 `WithCheckpoint`）时运行参数校验直接报错。同一次调用同时给 `WithCheckpoint` 与 `Thread` 时，`WithCheckpoint` 的完整配置生效。

新一轮输入使用 `Invoke` 或 `Stream`；继续未完成的运行使用 `Resume` 或 `ResumeStream`。同一会话存在未完成运行时，新一轮输入会以 `state.ErrUnfinishedRun` 被拒绝，避免覆盖尚未完成的工作。需要人工输入的挂起流程仍使用 `WithResume(interruptID, value)`，具体见下文「交互中断与输入恢复」。

## 保存时机与持久化模式

配置存储后，框架会在第一轮开始前、每轮成功提交并确定下一批任务后、人工中断时和运行结束时保存。顺序流程一轮只有一个任务。流式节点必须完成输出消费，才能记为完成。

进度写入的时机由持久化模式决定（参照 LangGraph 的 durability）：

| 模式 | 进度写入 | 适用场景 |
| --- | --- | --- |
| `graph.DurabilityAsync`（默认） | 在后台按顺序写入，下一节点无需等待存储往返 | 大多数交互式应用；节点间不叠加存储延迟 |
| `graph.DurabilitySync` | 写入成功后才开始下一节点 | 节点有昂贵或不可重复的副作用，要求每个节点完成都已落盘 |

```go
// Graph
compiled.Invoke(ctx, input, graph.WithCheckpoint(store, id), graph.WithDurability(graph.DurabilitySync))
// Agent：WithCheckpointDurability 设默认值，WithDurability 按次覆盖
assistant, _ := agent.New(model, agent.WithCheckpointStore(store), agent.WithCheckpointDurability(graph.DurabilitySync))
assistant.Invoke(ctx, input, agent.WithConversationID(id), agent.WithDurability(graph.DurabilityAsync))
```

两种模式的共同保证：

- 人工中断、接受人工输入与运行完成始终同步写入：`InterruptedError` 意味着检查点已落盘，调用方拿到最终结果时完成记录已写好，下一轮输入不会误判为未完成。
- 任何退出路径（包括普通失败）都会先等待排队中的写入完成，因此报错返回后可以立即 `Resume`。
- 写入严格按提交顺序执行；State 编码在调度循环内完成，后台只负责存储写入，不会读到下一节点对 State 的修改。

async 模式允许执行领先于持久化：写入失败在后续提交或退出 flush 时返回；进程崩溃可能丢失尚未写入的轮次。两种情况恢复时都会从上一个成功的检查点重放。初始写入失败时没有可恢复的记录，直接重新 `Invoke` 即可。

例如 A → B → C（sync 模式），B 完成且保存成功后，C 执行失败，恢复会从 C 开始。B 已执行但保存失败时，恢复仍可能重新执行 B。超时和普通取消不会额外保存一份中断记录，但之前成功保存的进度仍可恢复。初始保存之前的失败没有新的恢复点。

Agent 的完整会话保存在检查点 `Messages` 中，配方另存模型工作消息。上下文裁剪、摘要或提示词加工不会覆盖完整会话。未完成记录可能包含本轮已经生成的部分消息，读取者应结合 `Status` 判断运行是否结束。最终结果、完成状态和最终会话内容在同一次写入中提交。

## 交互中断与输入恢复

`graph.Interrupt(ctx, payload)` 用于补充信息、选择方案、人工修订、等待外部系统和审核等场景。Graph 不解释输入的业务含义，工具审批只是一个使用例。

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

高危工具审批使用 `agent.WithBeforeToolBatch`。Hook 在整批 Tool Call 解析完成后、任何 Tool
开始前运行；返回 typed interrupt 时该批次执行数为零。批准后 Resume 会重新运行 Hook：

```go
approval := tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
    if needsApproval(batch) && !isApproved(ctx, batch) {
        return control.RequestInterrupt("等待工具审批", nil)
    }
    return nil
})

assistant, err := agent.New(model,
    agent.WithTools(tools...),
    agent.WithBeforeToolBatch(approval),
)
```

### 输入与重放约定

- Payload 和恢复输入必须能够编码为 JSON。恢复返回 JSON 基础类型：对象为 `map[string]any`、数组为 `[]any`、数字为 `json.Number`；不会自动还原业务 Go 结构体。需要结构体时显式编解码。`nil` 是合法的 null 输入，与未提供输入不同。
- 恢复重新运行中断节点的 PreHandler 和节点代码，不恢复 Go 调用栈。挂起节点使用进入前的 State 快照，PreHandler 的增量修改不会随每次重放累加。
- 同一节点多次 Interrupt 按顺序匹配。已回答的问题返回保存的输入，未回答的问题产生新的 ID；Payload 必须保持一致。调整分支导致跳过已记录问题或改变 Payload 会报错。
- 节点循环的下一次执行具有新的问题 ID。未知、重复、已回答的 ID 会被拒绝；携带输入只能调用 Resume 系列入口。
- 输入在执行节点前持久化。若后续执行失败，可用不带 WithResume 的 Resume 重试已接受输入，不能重复提交同一个 ID。接受输入后的运行会在节点提交并完成路由后推进检查点到下一节点，清理当前节点的输入记录；整次 Graph 执行完成后保存结果；之后不带输入的 Resume 直接读取结果，再提交人工输入则返回错误。
- 节点前的外部副作用可能重放；将写操作放在交互之后，或使用幂等键。State 快照不会回滚数据库或远程请求。
- PostHandler、路由函数、并行合并器内不支持动态 Interrupt，调用会返回明确错误。嵌套 Graph / 子 Agent 内的顺序节点支持 Interrupt：挂起经执行树传播到根运行，统一返回 `InterruptedError`，从根入口 Resume 恢复；子调用已完成的结果在恢复时直接复用。并行分支（原生 fork 任务、动态分派成员）节点体内也支持直接调用 Interrupt：每个成员拥有独立的交互日志与入口 State；只回答部分成员时其余成员保持等待且不重放，分批回答后全部就绪才合并。工具节点内不同调用各自的 Interrupt 按调用身份归属日志，可分批回答并正确恢复；同一调用内并发发起多个 Interrupt 的顺序仍无法稳定重放，不受支持。
- 经 `agent.NewAgentNode` 接入图的子 Agent（如 examples/routing、examples/groupchat 编排）属于嵌套 Graph：子 Agent 内的 Interrupt 会使整个运行挂起并从根入口恢复，不会被转成模型可见的观察结果，也不会让父级"成功"结束。未配置根检查点时嵌套 Interrupt 仍以 `graph.ErrInterruptUnsupported` 失败。`extensions/subagent` 工具内的子 Agent 审批同样作为待答问题沿命名空间执行树汇总到根：同批已完成的兄弟子 Agent 调用结果直接复用，只恢复仍等待的调用。
- 普通无检查点运行不增加 State 编码。配置检查点后，具备交互条件的节点会保存入口快照，并在启动下游前收集输出流以确认是否挂起；节点仍调用 Stream，公开消息增量继续实时发布。
- 统一存储支持历史查询和版本冲突检查；应用仍必须串行化同一会话的运行与恢复请求。不保证并发恢复、崩溃或存储失败时外部操作恰好执行一次。完成标记写入失败后重试也可能重复副作用。
- 外部 `control.RequestInterrupt` 继续表达运行暂停，不要求携带输入；它与交互中断具有不同用途。

### 外部暂停

外部暂停使用 typed Cancel Cause，由应用持有取消权：

```go
runCtx, cancel := context.WithCancelCause(ctx)
go func() {
    <-pauseRequested
    cancel(control.RequestInterrupt("用户暂停", map[string]any{"operator": "user-42"}))
}()

output, err := assistant.Invoke(runCtx, input,
    agent.WithConversationID("tenant-a:user-42:session-7"),
)
if errors.Is(err, graph.ErrInterrupted) {
    output, err = agent.Resume(context.Background(), assistant,
        agent.WithConversationID("tenant-a:user-42:session-7"),
    )
}
```

配置持久化后，运行会逐节点保存。普通取消或 deadline 可从最近成功保存的位置恢复；人工中断还会保存交互记录。Graph 必须成功保存后才返回 `graph.ErrInterrupted`，保存失败会透传 Store 错误。完成后的 Resume 读取已保存结果。

### 公共事件流

`StreamEvents` / `ResumeStreamEvents` 在挂起时发布 `lifecycle.phase = "interrupted"`，Graph 生命周期的 `interrupts` 携带待输入的 ID、节点和 Payload。即使 `Config.Lifecycle` 为 false，也会发布一次等待输入通知。

- 不带答案的 `Resume` 按实际进度决定行为：存在可推进的任务或子调用时继续执行；全部仍在等待时返回当前的 `InterruptedError` 与原问题 ID，不空跑节点也不产生新检查点版本。未知、重复或已回答的问题 ID 仍属于输入错误。

挂起时不会发布最终 result；Reader 随后返回同一个 `InterruptedError`。前端据此展示表单或等待状态，而非执行失败。尚未结束的活动会关闭为 canceled，不再保持加载动画；开启生命周期时未结束的节点和 Graph 关闭为 interrupted。

参见 `examples/interaction`（补充信息、选择语言）和 `examples/basic`（工具审批）。

## 放弃未完成的运行

运行因确定性错误反复失败，或用户不再回应挂起的交互时，可以放弃该运行：

```go
// Graph 或任意 Store 使用方
_, err := state.AbandonRun(ctx, store, conversationID)

// Agent：新输入时自动放弃挂起的运行再开始本轮
result, err := assistant.Invoke(ctx, input, agent.WithConversationID(id), agent.WithAbandonUnfinished())
```

放弃会追加一条 `Status` 为 `abandoned` 的记录：清除执行数据，会话 `Messages` 与 `Values` 回退到该运行开始前（被放弃这一轮的输入和部分输出不再进入会话）。被放弃的记录仍保留在历史中，可通过 `List` 查看。放弃后该运行不能再 `Resume`（返回 `graph.ErrRunAbandoned`），此前的中断 ID 也随之失效。没有未完成运行时返回 `state.ErrNoUnfinishedRun`。放弃同样受版本检查保护，与正在进行的运行并发时返回 `state.ErrConflict`。

已完成的运行再次 Resume 会读取保存的结果，不执行节点；此时不能再提交人工输入。Graph 的状态、最终输出必须能够序列化。最终结果以 JSON 保存，并在读取时解码为 Graph 的输出类型。

恢复输入有两种来源：有 PreHandler 的节点在恢复时由 PreHandler 从 State 重建输入；没有 PreHandler 的顺序节点，其边输入在调度时刻冻结为 JSON 存入下一轮任务（`StepTask.Input`），恢复时按节点静态输入类型解码。输入类型必须能 JSON 往返：接口、`chan`、函数、Reader 等类型在编译期使图失去可恢复性（`ErrGraphNotResumable`）；编码后立即解码比对，嵌套动态值漂移（如 `map[string]any` 里 `int` 变 `float64`）在保存阶段被拒绝。原生 fork 组内的任务不持久化输入，恒由 PreHandler 从分支 State 副本重建。

## 图变更后的恢复

检查点保存进度的整图指纹（图名、拓扑、State Codec 身份与版本等参与编译快照的全部声明）是唯一兼容性判据：指纹与当前编译不一致即拒绝恢复，不再区分变更发生在恢复点闭包内外。根运行返回 `graph.ErrGraphRevisionMismatch`，嵌套子图返回 `graph.ErrResumeIncompatible`。这意味着 State 结构、Codec 版本、节点、边、fork 声明的任何演进都会使既有检查点失效——恢复要求与保存时完全一致的图构建，不提供 State 迁移通道。

恢复可替换 Agent 实例、模型端点和 API Key，但 Graph 名称、拓扑和 State Codec 必须兼容。
检查点不保存这些运行时连接信息。恢复是 at-least-once：中断节点和并行分支可能重放，
副作用必须幂等。框架不提供 Lease、分布式锁、fencing 或 exactly-once；多副本应用在调用
Resume 前自行用 Gateway 锁、队列唯一任务、数据库事务或 Redis SETNX 串行化同一 key。

## 历史与版本

- `ConversationID`：会话标识，跨轮保持不变；普通 Graph 也用它标识一条执行历史。
- `RunID`：本轮运行标识，恢复时保持不变，新一轮输入生成新值。
- `ID`、`ParentID`：本份检查点及上一份检查点的标识。
- `Version`：每次成功写入递增，不能用来统计对话轮数。
- `Status`：`running`、`interrupted`、`completed` 或 `abandoned`。
- `Execution`、`Result`：Graph 编码的执行快照和最终结果。

```go
latest, found, err := store.Load(ctx, conversationID)
record, found, err := store.Get(ctx, conversationID, checkpointID)
page, err := store.List(ctx, conversationID, state.ListOptions{Limit: 20})
older, err := store.List(ctx, conversationID, state.ListOptions{
    Before: page[len(page)-1].ID,
    Limit:  20,
})
```

实际调用时需先检查错误、`found` 和分页是否为空。`List` 按新到旧排列，不包含 `Before` 指定的记录；Limit 为零时默认返回最多 100 条。`Delete(ctx, conversationID)` 删除该会话的最新记录与全部历史。

自定义 Store 必须在一次原子操作中完成版本及当前 ID 校验、追加历史、更新最新指针。失败时不得暴露半份记录；冲突返回 `state.ErrConflict`。保存返回值中的 ID 和 Version 必须用于后续更新。

## Memory 与 Redis

Memory 在同一把锁内提交，适合测试和进程内使用，进程结束后数据不会保留。Redis 实现在 `extensions/state/redis`，用一段 Lua 脚本原子更新同一 Hash 中的记录与指针；TTL 作用于整个会话及历史，每次成功保存都会续期。Redis 故障后的数据保留能力取决于服务端持久化配置。

历史查询可以帮助检查执行过程，但本版不能从旧记录分叉运行，也不支持任意状态修改。同步保存不能替代工具幂等：同一会话的执行和恢复仍需由应用串行化。挂起时已登记进 pending 区的工具结果在节点恢复重放时直接复用（键含批次与运行范围，详见[工具运行时](tool-runtime.md)），但结果登记前崩溃、或非工具路径发出的外部调用仍可能重做。原生分支与动态分派成员按任务粒度保存进度：已完成成员恢复时直接复用结果，挂起成员从自身入口 State 与交互日志重放。两种形态共用同一语义：成员各持局部 State，成员路径上的后续任务沿该成员自己的 State 继续；检查点每轮保存的调度进度与成员 State 快照只是进度持久化，不是把成员状态并回主干。全部成员到齐后，才对各成员终态执行一次合并——静态分支按声明顺序，动态分派按清单顺序；`SetForkMerger` 优先于图级 `SetStepMerger`。

开启检查点后，完成任务的边输出始终编码，值必须满足 JSON 往返稳定；不可往返稳定时保存失败并给出明确诊断（把结果经 PostHandler 收进 State，或换稳定输出类型）。

## 嵌套执行树

根检查点的 `Execution` 保存顶层图的执行进度（frame）：分支任务进度挂在所属 frame 上，每次子图 / 子 Agent 调用登记为父任务下的子执行记录（父任务 ID + 调用身份 + 任务内序号定位）。子执行的进度不内嵌在父记录里——它保存在键带父级前缀的独立命名空间记录中（`父记录键/RunID/childKey`），新一轮运行换 RunID 使旧代子记录自然失效。子调用的输入在登记时编码校验，恢复重放时输入不一致返回 `ErrResumeIncompatible`；已完成的子调用直接复用保存结果，不重新执行。子图内的挂起（含动态 Interrupt）先写入自己的命名空间记录，再把携带子树待答问题的信号向根传播：根记录保存后根入口才返回 `InterruptedError`，待输入问题按各层登记的待答副本递归汇总。并行分支中的子调用按所属分支任务关联；分支节点体内直接发起的 `Interrupt` 也受支持——每个分支任务有独立的交互日志与入口 State，恢复时按自身日志重放，未获答案的分支保持等待不重跑。嵌套深度由记录键路径段数限制，损坏引用、重复身份、非法状态在加载时即被拒绝。挂起的 frame 与任务额外保存当前阻塞位置（等待记录）：区分等待本层问题、等待某次子调用、等待并行任务组——恢复判定以等待记录为准，已回答的旧问题不会误唤醒后来阻塞在其他位置的任务；全部任务完成但尚未合并的检查点允许无输入恢复并完成提交，全部任务仍在等待时无输入的 `Resume` 直接返回原挂起结果且不产生新的保存。

## 接口与格式版本

使用统一的 `state.Store` 保存 `Checkpoint`，从中断结果的 `PendingInterrupts()` 读取待回答问题。

检查点统一为 v3，内置配方 State Codec 仍为 v3。项目尚未投入使用，开发期旧检查点格式与旧进度不提供迁移或兼容承诺。运行恢复应满足当前格式、State Codec 与整图指纹完全一致的要求。
