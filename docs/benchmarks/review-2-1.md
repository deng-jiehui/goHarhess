# 2.1 消息拷贝优化复核

2026-10-04：第三轮独立复核通过，主体优化与存储隔离修复均已验收，2.1 标记为已完成。下文保留前两轮的证据，最新结果见[第三轮复核](#第三轮复核)，实现与验收结论见[实施方案](../design/archive/架构改进实施方案.md#task-2-1)。

## 第一轮正确性检查（历史记录）

根模块与 extensions 的全量 `go test ./... -count=1`、`go vet ./...` 通过。`go test -race ./pkg/agent ./pkg/state ./pkg/graph ./pkg/contextengine ./pkg/schema ./pkg/llm ./pkg/stream ./internal/benchfixture -count=1` 通过。常规检查使用 Go 1.26.4。

补查将含 `[]string` 字段的结构体及其指针分别放入 `Message.Extra`，保存后修改原输入，或修改 `Save`、`Load`、`Get`、`List` 的返回值，再读取保存的消息。十个场景在改造前全部通过，改造后全部失败：外部修改能改变已保存的数据。根因是持久化使用了宽松结构克隆，未能隔离这些自定义值中的引用。

补查测试副本为 [review_21_isolation_test.go](../../.bench/results/review-21/review_21_isolation_test.go)，前后输出为 [isolation-before.txt](../../.bench/results/review-21/isolation-before.txt) 和 [isolation-after.txt](../../.bench/results/review-21/isolation-after.txt)。正常测试目录中的临时文件已清理。

## 第一轮性能短测

使用 Go 1.24.4、Windows amd64、Ryzen 7 9700X、`cpu=1`，串行采样。六个场景在改造前后分别执行三次，共 36 条结果，下表列中位数。Agent 和 ContextEngine 使用 `-benchtime=1s`；MemoryStore 使用 `-benchtime=20x`，避免计时外的历史初始化耗费大量时间，其耗时仅供参考，重点核对分配量。未使用 benchstat 判定统计显著性。

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
| --- | ---: | ---: | ---: |
| ReAct/model_calls=10/checkpoint=off | 2,454,471→1,622,347 | 1,725,123→1,018,609 | 25,921→14,230 |
| AgentPersist/model_calls=10/checkpoint=off | 2,439,964→1,689,928 | 1,725,322→1,018,810 | 25,924→14,233 |
| AgentPersist/model_calls=10/checkpoint=sync | 9,232,101→3,925,666 | 9,185,458→4,879,530 | 47,819→23,768 |
| ContextTrim/DropOld/turns=100/kind=tool | 2,327,089→1,484,326 | 903,879→811,552 | 11,953→10,900 |
| ContextTrim/DropOld/turns=1000/kind=tool | 22,539,356→14,395,153 | 7,846,614→7,492,121 | 111,863→110,809 |
| MemoryStore/save_history=100 | 9,950→9,255 | 25,312→10,087 | 30→12 |

Agent 持久化的前后样本均保持每次运行 21 条历史记录、21 次保存，最新记录约 66.4KB、累计保存约 598.6KB，分配下降没有减少这些持久化工作。剩余历史存储成本继续在 1.5（后续）的历史存储优化中处理，1.5 的统一检查点已完成。

作者的 `.bench/r21-agent.txt`、`.bench/r21-state.txt`、`.bench/r21-ce.txt` 共有 35 个场景、350 条结果，每场景十次。本轮三次短测独立保存，不与作者采样合并。原有文档中的五十次调用持久化和连续会话增长数字没有出现在这三份输出中，本轮未将其作为验收证据。

本次实际命令如下，均使用 `GOTOOLCHAIN=local`：

```powershell
go test ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^(ReAct|AgentPersist|SessionGrowth)$/^(model_calls=10|requests=10)$/^checkpoint=(off|sync)$' -benchmem -benchtime=1s -count=3 -cpu=1
go test ./pkg/contextengine -run '^$' -bench '^BenchmarkGHFull$/^ContextTrim$/^DropOld$/^turns=(100|1000)$/^kind=tool$' -benchmem -benchtime=1s -count=3 -cpu=1
go test ./pkg/state -run '^$' -bench '^BenchmarkGHFull$/^MemoryStore$/^save_history=100$' -benchmem -benchtime=20x -count=3 -cpu=1
```

第一条筛选实际执行三个 Agent 场景，末段筛选排除了没有 `checkpoint` 路径的 SessionGrowth。原始输出在 `.bench/results/review-21/` 的六份 `before/after-*-cpu1.txt` 中，精确摘要见 [summary.json](../../.bench/results/review-21/summary.json)，环境、命令与源码指纹见 [manifest.json](../../.bench/results/review-21/manifest.json)。

改造前源码由 `baseline-v3-stream` 的 HEAD、补丁和未提交源码归档恢复，再覆盖 `review-23/after-src/engine.go`，对应 2.3 完成、2.1 开始前的状态。改造后在该状态上覆盖本轮 `after-src/` 即可还原；其中保存了本次十三个变更文件，以及核对用的相同基准、fixture 和 Schema 源码。本地 `.bench` 归档被 Git 忽略，临时恢复目录已清理。

## 第二轮复核

2026-10-04：“Clone 优先，JSON 保底”修复提交后的独立复核尚未通过。原隔离测试十个场景全部通过，补查确认四项 P2：后续存储复制仍吞掉 Clone 错误、JSON 回退范围过大、类型化 nil 被误调用，以及嵌套 Clone 返回类型错误被保底掩盖。触发条件和修改建议见[实施方案第二轮复核](../design/archive/架构改进实施方案.md#review-2-1-v2)。

根模块与 extensions 全量 `go test ./... -count=1`、`go vet ./...` 通过；Schema、State、Agent、Graph、ContextEngine、LLM、Stream、BenchFixture 与 Redis Store 的竞态检测通过，使用 Go 1.26.4。补查八个场景均复现对应问题，其中保存和读取的后续克隆失败使用确定性故障注入，确认框架没有正确上报错误，并返回了可改写存储的共享对象。

补查副本见 [review_21_v2_edges_test.go](../../.bench/results/review-21-v2/review_21_v2_edges_test.go)，失败输出见 [supplementary.txt](../../.bench/results/review-21-v2/supplementary.txt)，原十个隔离场景的通过输出见 [original-isolation.txt](../../.bench/results/review-21-v2/original-isolation.txt)。临时测试已清理。

同 Go 1.24.4、`GOTOOLCHAIN=local`、`cpu=1` 下串行复测四个场景，每场景三次，共 12 条结果。Agent 使用 `-benchtime=1s`，MemoryStore 使用 `-benchtime=20x`，口径与上一轮短测一致。下表为本轮中位数，未使用 benchstat 判定统计显著性。

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| ReAct/model_calls=10/checkpoint=off | 1,626,144 | 1,018,609 | 14,230 |
| AgentPersist/model_calls=10/checkpoint=off | 1,619,430 | 1,018,808 | 14,233 |
| AgentPersist/model_calls=10/checkpoint=sync | 3,819,525 | 4,901,623 | 24,228 |
| MemoryStore/save_history=100 | 8,470 | 10,087 | 12 |

正常消息仍保留主体优化的收益；同步持久化的分配相对上一轮略增，B/op 约增加 0.5%、allocs/op 约增加 1.9%，不作为新的验收阻碍。每次运行仍有 21 次保存和 21 条历史记录，持久化工作未减少。

本轮实际命令：

```powershell
go test ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^(ReAct|AgentPersist)$/^model_calls=10$/^checkpoint=(off|sync)$' -benchmem -benchtime=1s -count=3 -cpu=1
go test ./pkg/state -run '^$' -bench '^BenchmarkGHFull$/^MemoryStore$/^save_history=100$' -benchmem -benchtime=20x -count=3 -cpu=1
```

原始输出与摘要保存于 `.bench/results/review-21-v2/`，见 [agent-cpu1.txt](../../.bench/results/review-21-v2/agent-cpu1.txt)、[state-cpu1.txt](../../.bench/results/review-21-v2/state-cpu1.txt)、[summary.json](../../.bench/results/review-21-v2/summary.json) 和 [manifest.json](../../.bench/results/review-21-v2/manifest.json)。按上一轮归档还原源码，再覆盖本轮 `after-src/` 中的五份文件，即可恢复本轮提交状态。

## 第三轮复核

2026-10-04：上轮四项 P2 均已修复，本轮通过，未发现影响 2.1 验收的新问题。根模块与 extensions 全量 `go test ./... -count=1`、`go vet ./...` 通过；Schema、State、Agent、Graph、ContextEngine、LLM、Stream、BenchFixture 与 Redis Store 的竞态检测通过，使用 Go 1.26.4。

此前十八个独立复现场景全部通过。本轮补查十二处消息元数据与输入、`Save/Load/Get/List` 返回值的五个边界，共六十个场景，确认修改外部副本不会改变最新记录或历史。混合元数据里的 `int64` 大整数保留类型和精度，Clone 对象保留具体类型，类型化 nil 可正常保存，强类型数组按约定整体 JSON 保底。另一个场景确认已有版本上的第二次保存复制失败时，最新记录、历史数量和版本不变，后续保存仍可正常提交。

复现源码和输出见 [review_21_isolation_test.go](../../.bench/results/review-21-v3/review_21_isolation_test.go)、[review_21_v2_edges_test.go](../../.bench/results/review-21-v3/review_21_v2_edges_test.go)、[review_21_v3_boundaries_test.go](../../.bench/results/review-21-v3/review_21_v3_boundaries_test.go)、[previous-reproducers.txt](../../.bench/results/review-21-v3/previous-reproducers.txt) 和 [boundary-tests.txt](../../.bench/results/review-21-v3/boundary-tests.txt)。竞态输出见 [race-core.txt](../../.bench/results/review-21-v3/race-core.txt) 与 [race-extensions.txt](../../.bench/results/review-21-v3/race-extensions.txt)，临时测试已从正常测试目录清理。

性能仍使用 Go 1.24.4、`GOTOOLCHAIN=local`、`cpu=1`，串行执行第二轮相同的两条命令，四个场景各三次，共 12 条结果。Agent 使用 `-benchtime=1s`，MemoryStore 使用 `-benchtime=20x`。下表为中位数，未用 benchstat 判定统计显著性。

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| ReAct/model_calls=10/checkpoint=off | 1,622,146 | 1,018,610 | 14,230 |
| AgentPersist/model_calls=10/checkpoint=off | 1,700,395 | 1,018,810 | 14,233 |
| AgentPersist/model_calls=10/checkpoint=sync | 4,010,944 | 4,879,522 | 23,768 |
| MemoryStore/save_history=100 | 10,105 | 10,087 | 12 |

正常消息仍保留主体优化的分配收益；本轮同步持久化分配已回到第一轮优化后的水平。相比改造前的 9.23ms、9.19MB/op、47,819 allocs/op，同步持久化的成本仍明显降低。持久化工作量保持每次运行 21 次保存、21 条历史记录、最新记录 66,437 字节、累计保存 598,627 字节。MemoryStore 固定二十次迭代的耗时会波动，本轮主要确认其分配仍为 10,087 B/op、12 allocs/op。

原始性能输出与摘要见 [agent-cpu1.txt](../../.bench/results/review-21-v3/agent-cpu1.txt)、[state-cpu1.txt](../../.bench/results/review-21-v3/state-cpu1.txt)、[summary.json](../../.bench/results/review-21-v3/summary.json) 和 [manifest.json](../../.bench/results/review-21-v3/manifest.json)。按第二轮的归档链还原源码，再覆盖本轮 `after-src/` 的九份文件，即可恢复本轮提交状态；源码快照已逐份核对 SHA-256，三轮材料保留，2.1 按已完成验收。
