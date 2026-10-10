# 统一检查点

Graph 和 Agent 共用 `state.Store`。检查点同时保存会话内容、执行进度和结果，不再分别写入会话存储与执行存储。

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

## 图变更后的恢复

检查点保存进度的整图指纹（图名、拓扑、State Codec 身份与版本等参与编译快照的全部声明）是唯一兼容性判据：指纹与当前编译不一致即拒绝恢复，不再区分变更发生在恢复点闭包内外。根运行返回 `graph.ErrGraphRevisionMismatch`，嵌套子图返回 `graph.ErrResumeIncompatible`。这意味着 State 结构、Codec 版本、节点、边、fork 声明的任何演进都会使既有检查点失效——恢复要求与保存时完全一致的图构建，不提供 State 迁移通道。

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

新一轮输入使用 `Invoke` 或 `Stream`；继续未完成的运行使用 `Resume` 或 `ResumeStream`。同一会话存在未完成运行时，新一轮输入会以 `state.ErrUnfinishedRun` 被拒绝，避免覆盖尚未完成的工作。需要人工输入的挂起流程仍使用 `WithResume(interruptID, value)`，具体见 [通用交互中断](resume-input.md)。

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

历史查询可以帮助检查执行过程，但本版不能从旧记录分叉运行，也不支持任意状态修改。同步保存不能替代工具幂等：同一会话的执行和恢复仍需由应用串行化。挂起时已登记进 pending 区的工具结果在节点恢复重放时直接复用（键含批次与运行范围，详见[工具运行时](tool-runtime.md)），但结果登记前崩溃、或非工具路径发出的外部调用仍可能重做。原生分支与动态分派成员按任务粒度保存进度：已完成成员恢复时直接复用结果，挂起成员从自身入口 State 与交互日志重放。两种形态的 State 与合并规则不同：原生分支每轮从公共候选 State 复制给本轮任务、按轮调用 `SetStepMerger`；动态分派成员沿自己的局部 State 继续、本轮结果按清单顺序逐轮并入候选 State。

开启检查点后，完成任务的边输出始终编码，值必须满足 JSON 往返稳定；不可往返稳定时保存失败并给出明确诊断（把结果经 PostHandler 收进 State，或换稳定输出类型）。

## 嵌套执行树

根检查点的 `Execution` 保存顶层图的执行进度（frame）：分支任务进度挂在所属 frame 上，每次子图 / 子 Agent 调用登记为父任务下的子执行记录（父任务 ID + 调用身份 + 任务内序号定位）。子执行的进度不内嵌在父记录里——它保存在键带父级前缀的独立命名空间记录中（`父记录键/RunID/childKey`），新一轮运行换 RunID 使旧代子记录自然失效。子调用的输入在登记时编码校验，恢复重放时输入不一致返回 `ErrResumeIncompatible`；已完成的子调用直接复用保存结果，不重新执行。子图内的挂起（含动态 Interrupt）先写入自己的命名空间记录，再把携带子树待答问题的信号向根传播：根记录保存后根入口才返回 `InterruptedError`，待输入问题按各层登记的待答副本递归汇总。并行分支中的子调用按所属分支任务关联；分支节点体内直接发起的 `Interrupt` 也受支持——每个分支任务有独立的交互日志与入口 State，恢复时按自身日志重放，未获答案的分支保持等待不重跑。嵌套深度由记录键路径段数限制，损坏引用、重复身份、非法状态在加载时即被拒绝。挂起的 frame 与任务额外保存当前阻塞位置（等待记录）：区分等待本层问题、等待某次子调用、等待并行任务组——恢复判定以等待记录为准，已回答的旧问题不会误唤醒后来阻塞在其他位置的任务；全部任务完成但尚未合并的检查点允许无输入恢复并完成提交，全部任务仍在等待时无输入的 `Resume` 直接返回原挂起结果且不产生新的保存。

## 接口与格式版本

使用统一的 `state.Store` 保存 `Checkpoint`，从中断结果的 `PendingInterrupts()` 读取待回答问题。

检查点统一为 v3，内置配方 State Codec 仍为 v3。项目尚未投入使用，开发期旧检查点格式与旧进度不提供迁移或兼容承诺。运行恢复应满足当前格式、State Codec 与整图指纹完全一致的要求。
