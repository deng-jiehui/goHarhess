# 并行与分支

Graph 的并行有两套入口，共用同一调度循环与 `SetMaxConcurrency` 并发上限：

- `AddFork`/`AddJoin` 原生分支把扇出与汇合画进拓扑：成员各持局部 State，
  全部到齐后对成员终态执行一次合并；适合形状固定的并行查询，见
  `examples/branches`。
- `graph.AddDispatch` 由运行到来源节点时的 State 生成任务清单，成员任务携带
  冻结输入、持有局部 State，全部收敛后按清单顺序汇合到 join；清单为空直接进入
  汇合。示例见 `examples/dispatch`。

## 原生并行分支

当一个流程需要同时查两类资料，再等资料齐全后汇总，可以直接把这些步骤画进 Graph。每个步骤仍然是普通节点。

例如：新闻查询走一条短分支，论文查询之后还要下载全文，走另一条长分支。

```text
START ─┬─ news ──────────┬─ summary ─ END
       └─ papers ─ download ─┘
```

节点注册完成后，声明扇出及汇合关系：

```go
g.AddFork(graph.Start, "news", "papers")
g.AddJoin(graph.Start, "summary")
g.AddEdge("news", "summary")
g.AddEdge("papers", "download")
g.AddEdge("download", "summary")
g.AddEdge("summary", graph.End)
```

实际使用时应检查这些调用返回的错误。完整可运行示例见 `examples/branches/main.go`，运行 `go run ./examples/branches` 即可。

`AddFork(from, branches...)` 会添加扇出点到各分支入口的边，不要重复添加这些边。`AddJoin(fork, node)` 表示：这次扇出的所有分支到达 `node` 后，才执行该节点一次。每条分支最终到汇合点的边仍需显式添加。

`AddBranch` 的行为没有改变：按声明顺序选择第一个满足条件的后继，它不表示并行。扇出点本身不能混用普通出边或条件出边；分支内部可以有条件选择，但所有路径必须回到指定汇合点。

## 一轮怎样执行

上面的流程分三轮：第一轮同时执行 `news` 和 `papers`；第二轮执行 `download`，新闻分支保持等待；第三轮执行 `summary`。如果设置 `SetMaxConcurrency(1)`，第一轮的两个任务会依次执行，但仍然读取同一份轮次入口 State，看不到彼此尚未提交的修改。

原生分支图要求配置 State factory，并为普通节点设置 PreHandler。节点输入从 State 重建，跨轮传递的结果由 PostHandler 写入 State，不依赖上一节点的临时输出。开启检查点时，原生分支仍保存完成任务的输出，因此输出值须满足 JSON 往返稳定。

扇出后的成员任务各自持有独立的局部 State：每个成员从扇出点 State 解码出自己的副本，成员路径上的后续节点（如 `papers -> download`）沿该成员自己的局部 State 继续推进，不读公共 State。短分支到达汇合点后保存终态并等待；全部成员到齐后，Graph 才对各成员终态执行一次合并。合并器的签名如下：

```go
func(context.Context, *MyState, []graph.StepResult[*MyState]) error
```

第一个 State 参数是扇出前公共 State 的候选副本。每个 `StepResult` 对应一条分支的**终态**：`TaskID`、`NodeID` 是该成员最后完成的任务，`Branch` 是成员身份，`State` 是成员终态。结果按分支声明顺序提供，不按完成先后排序；短分支提前完成后，其终态仍会进入这次最终合并。

扇出/分派组不做逐轮公共合并；无组的顺序轮仍按轮调用 `SetStepMerger`。按扇出点配置的合并器（`graph.SetForkMerger`，或 `graph.NewState` 门面 `Parallel` 的 merge 参数）优先于图级 `SetStepMerger`。检查点每轮仍会保存调度进度与各成员 State 快照——那是进度持久化，不等于把成员状态并回主干。合并函数只需处理一次终态合并；Graph 不猜测字段变化，也不强制使用字段 reducer。默认 State Codec 使用 JSON；包含特殊动态类型的 State 应配置能深度复制数据的自定义 Codec。

## 失败、取消与恢复

任务、合并或路由失败时，该轮 State 不提交。开启检查点后，`Resume` 复用已经保存的完成任务结果与成员终态，继续未完成任务；全部成员到齐后再执行合并。尚未持久化的执行仍可能重跑，已到达汇合点的短分支不会重跑。外层循环再次经过同一扇出点，会创建新的分支组，不复用上次的到达记录。

统一 Checkpoint v3 的 `Execution` 把 State、累计任务数 `Step`、轮次 `Schedule.Round`、下一批任务及汇合记录保存在同一个检查点中。sync 模式每轮保存成功才推进；默认 async 模式允许下一轮与存储写入重叠，退出前 flush，完成或中断同步保存。进程崩溃可能重跑还没持久化的轮次，外部请求和已发送的流内容也不能回滚。涉及外部写入的节点仍需业务侧保证幂等。

`SetMaxSteps` 限制累计节点任务数，`SetMaxConcurrency` 限制一轮同时运行的任务数。预算不够整轮时不会启动其中一部分。遇到失败或取消后停止领取新任务，并取消执行中任务的 Context；任务本身仍需要响应取消。

## 当前支持范围

- 支持静态扇出、长短分支、分支内部条件选择、全部到齐后汇合，以及汇合之后循环回到扇出点。
- 嵌套扇出、分支内循环、跨分支连线、绕过汇合点、从组外进入汇合点，都会在编译时拒绝。
- 原生分支的分支节点体内可调用 `graph.Interrupt`。各分支保存独立审批现场，支持分批回答、未回答分支保持等待，以及从根入口恢复嵌套子图。PostHandler、路由和合并函数内仍拒绝动态交互。
- 检查点按分支任务记录进度，已完成成员在恢复时不重跑。运行期没有活跃组就到达汇合点会返回 `parallel.join.schedule` 错误而非 panic。
- 未配置图级合并器的旧顺序图保留原 State 和流管线行为，不因这次升级强制增加每个节点的 Codec 复制。原生分支图及配置合并器的顺序图采用上述候选 State 提交规则。

## 动态任务分派

`graph.AddDispatch[I, O, S](g, from, targets, join, handler)` 把 `from` 节点登记为动态扇出点：它完成后由 `handler` 依据本轮入口 State 生成任务清单，框架为每条清单创建独立任务，全部收敛后按清单顺序汇合到 `join`：

```go
err := graph.AddDispatch[string, string](g, "plan", []string{"worker"}, "collect",
    func(_ context.Context, s *workState) ([]graph.DispatchTask, error) {
        var tasks []graph.DispatchTask
        for _, word := range strings.Fields(s.Brief) {
            tasks = append(tasks, graph.Send("worker", word))
        }
        return tasks, nil
    })
```

- `targets` 声明允许的目标节点集合，清单里每项经 `graph.Send(node, input)` 指向其中一个；`input` 是该成员冻结的独立输入。目标到 `join` 的路径仍需显式连线，且路径上除成员入口外的节点都要能从 State 重建输入。
- `from` 不能混用普通出边、条件出边或静态扇出；清单为空时不产生成员任务，直接进入汇合。与静态分支相同，成员各持局部 State、全部到齐后按清单顺序对成员终态执行一次合并，因此分派图必须配置合并器。
- 成员入口任务首次执行使用冻结的 `input`，不经过 PreHandler；同一节点在成员路径上被再次经过时按局部 State 重建输入。与静态分支成员相同，接续节点沿成员自己的局部 State 继续；完成任务的边输出始终编码，值必须 JSON 往返稳定，不能只靠 PostHandler 收进 State。完整示例见 `examples/dispatch`。

检查点按当前格式保存和恢复。项目尚未投入使用，开发期旧格式不提供迁移或兼容承诺。
