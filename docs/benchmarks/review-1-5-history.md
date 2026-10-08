# 1.5（后续）历史存储优化复核

2026-10-04：第三轮独立复核通过，历史存储优化已完成验收，见[第三轮复核](#第三轮复核)。统一检查点的原交付范围已完成，exit 等扩展另行排期。以下保留历次证据。

## 正确性证据

Go 1.26.4 下，根模块与 extensions 的全量 `go test ./... -count=1`、`go vet ./...` 通过，输出见 [core-tests.txt](../../.bench/results/review-15/core-tests.txt)、[extensions-tests.txt](../../.bench/results/review-15/extensions-tests.txt)、[core-vet.txt](../../.bench/results/review-15/core-vet.txt) 和 [extensions-vet.txt](../../.bench/results/review-15/extensions-vet.txt)。

根模块执行 `go test -race ./pkg/state ./pkg/graph ./pkg/agent ./internal/benchfixture -count=1`。State、Agent、BenchFixture 通过，Graph 失败：三个现有并行恢复测试触发竞态，读发生在新加的 `coord.record.Messages` 会话回填，写发生在后台 writer 替换检查点记录。Redis 常规竞态检测通过。输出见 [race-core.txt](../../.bench/results/review-15/race-core.txt) 和 [race-extensions.txt](../../.bench/results/review-15/race-extensions.txt)。

独立补查覆盖以下场景，失败输出保留为复现证据：

- 会话型 State 使用原生分支图：关闭持久化时通过，开启 sync 或 async 后，分支中的完整历史丢失，两个场景均失败。源码见 [graph 测试](../../.bench/results/review-15/reproducers/pkg/graph/review_15_conversation_test.go)，输出见 [graph-supplementary.txt](../../.bench/results/review-15/graph-supplementary.txt)。
- `MaxHistory=2`，上一轮完成记录之后保存三次当前运行进度，Memory 和 Redis 均无法执行 `AbandonRun`，错误为父记录缺失。
- MemoryStore 保存同一条 512 KiB Opaque 载荷并追加四十次，垃圾回收后存活堆增加 21,998,776 字节，约 21 MiB；只保留一条记录并连续改写二十次会话，仍保留十九份不再引用的旧日志。
- MemoryStore 的后续保存会原地修改已交给锁外读取的归档 map；并发读旧历史和改写会话时触发竞态。
- Redis 完全改写两条消息，零前缀分叉的新日志含四条消息；并发保存与删除不同会话时，新增缓存 map 触发竞态。

存储补查源码见 [MemoryStore 测试](../../.bench/results/review-15/reproducers/pkg/state/review_15_storage_test.go) 与 [Redis 测试](../../.bench/results/review-15/reproducers/extensions/state/redis/review_15_storage_test.go)，输出见 [state-supplementary.txt](../../.bench/results/review-15/state-supplementary.txt)、[redis-supplementary.txt](../../.bench/results/review-15/redis-supplementary.txt)、[race-memory-fork.txt](../../.bench/results/review-15/race-memory-fork.txt) 和 [race-redis-delete.txt](../../.bench/results/review-15/race-redis-delete.txt)。Redis 的另一个旧缓存前缀补查场景通过，没有作为问题报告。临时测试已从正常测试目录清理。

## 性能短测

使用 Go 1.24.4、Windows amd64、Ryzen 7 9700X、`GOTOOLCHAIN=local`、`cpu=1`，串行采样，执行时没有其他测试干扰。四个场景各三次，共十二条结果。下表为中位数，未用 benchstat 判定统计显著性。

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SessionGrowth/requests=10 | 8,376,152 | 9,717,936 | 99,644 |
| SessionGrowth/requests=100 | 940,231,150 | 850,055,732 | 9,024,169 |
| AgentPersist/model_calls=10/checkpoint=sync | 4,677,010 | 5,191,006 | 24,201 |
| AgentPersist/model_calls=50/checkpoint=sync | 105,111,250 | 115,650,142 | 457,166 |

本轮百次请求和五十次模型调用的结果符合作者所报量级。未独立重采长会话的改造前数据，因此不将作者所报 11% / 15% 降幅作为本轮独立前后对照结论。十次模型调用同步持久化的分配相对 2.1 第三轮的 4,879,522 B/op 增加约 6.4%。这项成本变化记录在此，优先处理已复现的正确性与实际存储留存问题。

持久化观测仍来自计时外的 StoreObserver：十次模型调用有 21 次保存和 21 条历史记录，五十次有 101 次保存和 101 条历史记录。`saved_json_bytes/run` 等指标统计完整 API 提交或物化后的记录，不能代表 Store 内部实际去重后的数据量；MemoryStore 的底层切片留存问题由单独的存活内存补查确认。

实际命令：

```powershell
go test ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^SessionGrowth$/^requests=(10|100)$' -benchmem -benchtime=1s -count=3 -cpu=1
go test ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^AgentPersist$/^model_calls=(10|50)$/^checkpoint=sync$' -benchmem -benchtime=1s -count=3 -cpu=1
```

原始数据见 [session-cpu1.txt](../../.bench/results/review-15/session-cpu1.txt)、[persist-cpu1.txt](../../.bench/results/review-15/persist-cpu1.txt) 和 [summary.json](../../.bench/results/review-15/summary.json)。环境、命令、源码指纹与复核结论见 [manifest.json](../../.bench/results/review-15/manifest.json)。本轮保存十四份相关源码快照；按 2.1 第三轮的归档链还原，再覆盖本轮 `after-src/`，即可恢复本轮提交状态。

## 第二轮复核

2026-10-04：原八项实际问题的回归通过，补查确认一项 P1：Redis 前缀缓存失配为零时，把未提交的重试响应当作成功。修改建议见[实施方案第二轮复核](../架构改进实施方案.md#review-1-5-history-v2)。

### 正确性

Go 1.26.4 下，根模块与 extensions 常规全量 `go test ./... -count=1`、`go vet ./...` 通过。原始输出见 [core-tests.txt](../../.bench/results/review-15-v2/core-tests.txt)、[extensions-tests.txt](../../.bench/results/review-15-v2/extensions-tests.txt)、[core-vet.txt](../../.bench/results/review-15-v2/core-vet.txt) 和 [extensions-vet.txt](../../.bench/results/review-15-v2/extensions-vet.txt)。

State、Graph、Agent、BenchFixture 及 Redis Store 的竞态检测通过，含上轮 MemoryStore 并发读取旧历史与分叉保存、Redis 不同会话保存/删除的复现。保存确认的失败场景由独立测试验证，未混入竞态检查的通过结论。实际命令与筛选条件见 [manifest.json](../../.bench/results/review-15-v2/manifest.json)，输出见 [race-core.txt](../../.bench/results/review-15-v2/race-core.txt) 和 [race-extensions.txt](../../.bench/results/review-15-v2/race-extensions.txt)。

补查结果：

- 配方会话 State 在关闭持久化、sync、async 下的原生分支执行均保留完整历史。MemoryStore 无引用日志回收及旧历史并发读取通过。归档 map 不可变的旧属性测试不适用于本轮锁内取日志切片的实现，已跳过。源码与结果见 [Graph 复现](../../.bench/results/review-15-v2/reproducers/pkg/graph/review_15_conversation_test.go)、[Memory 复现](../../.bench/results/review-15-v2/reproducers/pkg/state/review_15_storage_test.go) 和 [core-supplementary.txt](../../.bench/results/review-15-v2/core-supplementary.txt)。
- 一条 512 KiB Opaque 载荷追加保存四十次，GC 后存活堆增量为 1,647,920 字节，约 1.57 MiB；上轮为 21,998,776 字节，约 21 MiB。存活堆测量是进程总堆差值，会有少量测量波动；本次已低于测试的 4 MiB 阈值。
- Memory 与 Redis 在 `MaxHistory=1/2/10` 下的首次放弃、连续放弃、完成后新运行的回退、嵌套 Values 返回值隔离、删除后重建均通过。源码见 [Memory 生命周期测试](../../.bench/results/review-15-v2/reproducers/pkg/state/review_15_v2_rollback_test.go) 与 [Redis 生命周期测试](../../.bench/results/review-15-v2/reproducers/extensions/state/redis/review_15_v2_rollback_test.go)，带竞态检测的输出见 [rollback-memory.txt](../../.bench/results/review-15-v2/rollback-memory.txt) 和 [redis-lifecycle.txt](../../.bench/results/review-15-v2/redis-lifecycle.txt)。
- Redis 零前缀分叉、裁剪后放弃、不同会话保存/删除通过；跨实例改写后的旧缓存补查失败：`Save` 返回版本 3，而 `Load` 仍是版本 2。源码与输出见 [Redis 原复现](../../.bench/results/review-15-v2/reproducers/extensions/state/redis/review_15_storage_test.go) 和 [redis-supplementary.txt](../../.bench/results/review-15-v2/redis-supplementary.txt)。
- Redis TTL 到期或另一实例删除后，旧实例以相同前缀重建会话，`Save` 返回成功，`Load` 却未命中。两个场景均失败，与上一条是同一个 `-0` 状态编码问题。源码见 [缓存失效测试](../../.bench/results/review-15-v2/reproducers/extensions/state/redis/review_15_v2_cache_test.go)，输出保留在 [redis-lifecycle.txt](../../.bench/results/review-15-v2/redis-lifecycle.txt)。

### 性能复测

使用 Go 1.24.4、Windows amd64、Ryzen 7 9700X、`GOTOOLCHAIN=local`、`cpu=1`，沿用首轮的两条实际命令，串行采样四个场景，各三次，共十二条结果。执行期间没有其他测试。中位数如下：

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SessionGrowth/requests=10 | 7,739,992 | 9,748,050 | 99,849 |
| SessionGrowth/requests=100 | 739,189,500 | 854,329,700 | 9,032,719 |
| AgentPersist/model_calls=10/checkpoint=sync | 4,579,248 | 5,194,625 | 24,224 |
| AgentPersist/model_calls=50/checkpoint=sync | 96,379,508 | 115,654,269 | 457,285 |

四个场景的分配量相对首轮增加均不足 0.6%，主体优化保留；本轮耗时较低，但两轮采样非交错配对，且未用 benchstat 判断显著性，不据此宣称进一步提速。十次和五十次模型调用的保存与历史记录数仍分别是 21 和 101。`saved_json_bytes/run` 等指标仍统计完整 API 记录，不代表 Store 内部去重后的实际体积。

原始结果见 [session-cpu1.txt](../../.bench/results/review-15-v2/session-cpu1.txt)、[persist-cpu1.txt](../../.bench/results/review-15-v2/persist-cpu1.txt) 和 [summary.json](../../.bench/results/review-15-v2/summary.json)。

### 归档

本轮的 [manifest.json](../../.bench/results/review-15-v2/manifest.json) 记录环境、命令、结论与筛选条件；[sources.json](../../.bench/results/review-15-v2/sources.json) 保存 260 份源码及模块文件的指纹。`after-src/` 是可独立运行的完整 Go 源码目录，根模块及 extensions 的全量测试已从该目录运行并通过，输出见 [archive-core-tests.txt](../../.bench/results/review-15-v2/archive-core-tests.txt) 和 [archive-extensions-tests.txt](../../.bench/results/review-15-v2/archive-extensions-tests.txt)。直接使用该目录无需拼接旧归档；若覆盖原 HEAD，还需按 [removed-source-files.json](../../.bench/results/review-15-v2/removed-source-files.json) 删除原先的三份 continuation 文件。

六份正式补查测试保存在 `reproducers/`，已从正常测试目录清理。归档没有包含或修改用户的 `.tmp/`，审核没有修改业务实现。首轮材料继续保留。

## 第三轮复核

2026-10-04：通过。上轮 Redis 保存确认问题已修复，未发现影响本项验收的新问题；实现与验收结论见[实施方案](../架构改进实施方案.md#review-1-5-history-v3)。

本轮修改仅涉及 Redis 实现与回归测试。重新运行第二轮两份独立复现，跨实例改写、TTL 到期和另一实例删除后的会话重建均通过；作者新增的四项测试也通过，包括非零前缀失配重试。原零前缀分叉、历史裁剪后放弃、不同会话并发保存/删除继续通过，共十个针对性场景。源码见 [原 Redis 复现](../../.bench/results/review-15-v3/reproducers/extensions/state/redis/review_15_storage_test.go) 和 [缓存失效复现](../../.bench/results/review-15-v3/reproducers/extensions/state/redis/review_15_v2_cache_test.go)，输出见 [redis-supplementary.txt](../../.bench/results/review-15-v3/redis-supplementary.txt)。

Go 1.26.4 下，根模块与 extensions 全量 `go test ./... -count=1`、`go vet ./...` 通过，输出见 [core-tests.txt](../../.bench/results/review-15-v3/core-tests.txt)、[extensions-tests.txt](../../.bench/results/review-15-v3/extensions-tests.txt)、[core-vet.txt](../../.bench/results/review-15-v3/core-vet.txt) 和 [extensions-vet.txt](../../.bench/results/review-15-v3/extensions-vet.txt)。State、Graph、Agent、BenchFixture 及 Redis Store 的竞态检测通过，Redis 包含恢复的独立复现，未跳过保存确认场景；输出见 [race-core.txt](../../.bench/results/review-15-v3/race-core.txt) 和 [race-extensions.txt](../../.bench/results/review-15-v3/race-extensions.txt)。两份修改文件的 gofmt 检查通过。

环境、实际命令、源码指纹与结论见 [manifest.json](../../.bench/results/review-15-v3/manifest.json)。本轮 [sources.json](../../.bench/results/review-15-v3/sources.json) 和 `after-src/` 保存两份修改文件，其余 258 份源码及模块文件与第二轮指纹一致。恢复时以 `review-15-v2/after-src/` 的完整目录为基础，再覆盖本轮两份文件。两份临时测试已归档并清理，用户的 `.tmp/` 及业务实现均未修改。

Agent 与 MemoryStore 基准相关代码未变，本轮未重复此前四个性能场景；第二轮原始数据继续保留，不作为本轮新采样。历史存储优化按已完成验收，后续扩展按需求排期。
