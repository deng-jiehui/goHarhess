# 性能采样记录：baseline-v2 与流式补采

本文汇总方案 2.11 的性能采样。四个流式场景已按原 Go 1.24.4 环境补采，并保存了可恢复的源码归档；其他摘要沿用 baseline-v2 的历史记录，作为分析参考。比较优化前后，应使用同一套 benchmark、fixture、工具链与参数，在同一环境顺序运行。

2.3 上下文裁剪优化已通过独立复核，最新短测见[2.3 性能复核](review-2-3.md)。本文 ContextTrim 表格与热点判断记录优化前的情况，保留为历史参考。

2.1 消息拷贝优化已完成（2026-10-04）。本文 `ReAct`、`AgentPersist`、`SessionGrowth`、`ContextTrim`、`MemoryStore`、`CheckpointCodec` 各行均为优化前代码的 baseline-v2 记录，保留为历史参考；优化后同口径（Go 1.24.4、`cpu=1`、`-benchtime=1s -count=10`）中位对比与原始采样文件见[实施方案 2.1](../design/archive/架构改进实施方案.md#task-2-1)，主要场景耗时下降约 25%–61%，B/op 与 allocs 约减半。

**收尾状态（2026-10-04）**：四个流式场景各 10 次、共 40 条原始记录已补齐，摘要已按原文重算。新补采的源码归档经独立恢复、哈希核对与测试通过，详见[流式正式基线](baseline-v3-stream.md)。旧 baseline-v2 补丁仍无法直接恢复，保留为历史参考；后续优化的前后对照通过新脚本另存归档。2.11 本轮收尾完成，记录见[实施方案](../design/archive/架构改进实施方案.md#completion-2-11-stream)。真实 Redis 组仍未测，按需另行补采。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 历史 Full：2026-10-03；流式补采：2026-10-04 |
| 代码 | HEAD `0d4b4fea` + 工作区改动；流式补采有独立的可恢复源码归档 |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| GOGC / GOMEMLIMIT / CGO | unset / unset / `CGO_ENABLED=1` |
| fixture | `fixture-v2`（`internal/benchfixture`） |
| bench | `bench-v2`（计时边界与正确性守卫修正版） |
| benchstat | 本次未使用（无同口径对比对象） |

原始记录分别保存在 `.bench/results/baseline-v2/` 和 `.bench/results/baseline-v3-stream/`。前者是 99 个场景各 10 次的历史 Full 输出，其中旧流式测量已失效；后者是四个流式场景的补采输出、manifest、摘要、源码和恢复验证记录。具体文件见[流式正式基线](baseline-v3-stream.md#原始记录与复现)。`.bench/results/` 为本地归档，不进入 Git。

## 运行命令

baseline-v2 当时使用的 Full 命令：

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/baseline-v2
```

等价 `go test -p=1` 六包 `^Benchmark(GHFull|MessageTransport)$`，
`-benchmem -count=10`。脚本内置 `bench-validate.ps1` 逐场景核对 99
个预期场景 ×10 采样，缺包、缺场景、少采样或含 FAIL 即失败。
Smoke / Stress 命令见实施方案 2.11；真实 Redis 组需
`GOHARNESS_BENCH_REDIS_ADDR` + `-Redis` 开关，本次未测。

本次只补采受影响的流式场景，使用 Go 1.24.4：

```powershell
go test -p=1 ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^AgentStream$' -benchmem -benchtime=1s -count=10 -cpu=1
```

## 结果摘要（cpu=1，10 次采样中位）

末四行来自本次流式补采，其余行来自 baseline-v2 历史记录。

| 场景 | ns/op | B/op | allocs/op | 备注 |
| --- | --- | --- | --- | --- |
| MessageTransport/delta | 57.6k | 134k | 547 | wire=28.7KB/op |
| MessageTransport/snapshot | 1.13M | 2.37M | 1.0k | wire=2.12MB/op，~19.6× delta |
| MessageClone/100/text | 20k | 36.2k | 701 | 实为 100 条（v1 实为 200 条） |
| MessageClone/100/toolmeta | 159k | 215k | 4.0k | 调用/结果 ID 已配对 |
| MessageClone/1000/toolmeta | 2.20M | 2.17M | 42.2k | 同上 |
| ContextTrim/NoTrim/turns=1000 | 18.0M | 5.06M | 85.1k | 构建基线 |
| ContextTrim/DropOld/100/tool | 45.5M | 11.2M | 175k | 超线性热点 |
| ContextTrim/DropOld/1000/tool | 5.04s | 1.11G | 17.6M | O(T²) 明确 |
| ContextTrim/DropOld/100/cjk | 7.57M | 4.08M | 70.4k | 多字节文本路径 |
| OversizedLatest/64KB | 15.2M | 835k | 1.37k | 超限输入路径 |
| ToolBatch/8/turns=100/off | 182k | 296k | 4.9k | NeedsMessages 零拷贝 |
| ToolBatch/8/turns=100/on | 209k | 558k | 4.9k | 历史视图注入 |
| ToolBatch/32/turns=100/off | 419k | 588k | 8.3k | 并发批次 |
| ToolBatch/8/turns=100/mixed | 199k | 427k | 4.9k | 半数需历史 |
| ToolCallbacks/cb=4/on | 7.53M | 13.6M | 245k | 回调×克隆成本 |
| SequentialGraph/nodes=100 | 288k | 418k | 3.2k | ~2.9μs/节点 |
| SequentialGraph/build_compile/100 | 460k | 514k | 7.1k | 构图+编译（计时外拆分基准） |
| SequentialGraph/compile_only/100 | 350k | 404k | 5.5k | 仅编译 |
| ParallelGraph/forkjoin/8×1KB | 124k | 159k | 999 | max_concurrency 1/8 无差 |
| ParallelGraph/forkjoin/32×1KB | 580k | 706k | 5.8k | |
| PersistedGraph/16×4KB/off | 37.5k | 43.8k | 541 | 构图编译在计时外 |
| PersistedGraph/16×4KB/sync | 854k | 1.94M | 3.5k | 原生 MemoryStore 计时；观测另跑 |
| PersistedGraph/16×4KB/async | 883k | 1.94M | 3.4k | 同上 |
| PersistedGraph/16×64KB/sync | 8.35M | 21.7M | 3.5k | 同上 |
| ResumeGraph/sequential/sync | 107k | 139k | 1.0k | 顺序中段恢复，不重复完成节点 |
| ResumeGraph/parallel/sync | 234k | 276k | 2.2k | 8 分支 7 完成 1 挂起 |
| ResumeGraph/completed/sync | 12.4k | 10.2k | 122 | 读取已完成结果不执行节点 |
| CheckpointCodec/64KB json | 275k | 148k | 8 | 编码 |
| CheckpointCodec/512KB decode | 2.91M | 3.69M | 38 | 解码 |
| MemoryStore/save/history=100 | 9.7k | 25.3k | 30 | 乐观锁保存 |
| MemoryStore/load/history=100 | 229 | 296 | 6 | |
| MemoryStore/list/1000/lim=100 | 27.0k | 94.8k | 608 | 分页 |
| ReAct/calls=10 | 2.67M | 1.85M | 27.9k | 9 次工具执行已计数核对 |
| ReAct/calls=50 | 60.6M | 39.9M | 590k | |
| ReAct/calls=100 | 251M | 160M | 2.31M | 超线性 ~94× |
| SessionGrowth/requests=10 | 25.0M | 21.8M | 229k | 新语义：每请求 1 工具 + 1 回答 |
| SessionGrowth/requests=100 | 2.65s | 2.05G | 21.4M | 同上，与 v1 旧语义不可比 |
| AgentPersist/10/sync | 9.75M | 9.31M | 49.8k | 计时内无观测包装 |
| AgentPersist/50/sync | 219M | 210M | 1.0M | 同上 |
| ResumeAgent/suspend/sync | 1.34M | 1.25M | 11.8k | 8 工具审批恢复：完成 Δ0、等待 Δ1 |
| AgentStream/invoke/16KB | 81,474 | 149,058 | 1,139 | baseline-v3-stream 补采 |
| AgentStream/stream/16KB | 82,197.5 | 149,058 | 1,139 | 同上 |
| AgentStream/events/16KB | 282,519 | 254,865 | 2,769 | 同上；256 次文本增量校验通过 |
| AgentStream/events_lifecycle/16KB | 289,659 | 256,909 | 2,827 | 同上；含生命周期事件 |

持久化观测（StoreObserver，计时外同参独立运行，单次运行）：

| 场景 | saves/run | latest_json | saved_json | history_json |
| --- | --- | --- | --- | --- |
| PersistedGraph/16×4KB | 17 | ~9.0KB | ~152KB | ~152KB (17 条) |
| PersistedGraph/16×64KB | 17 | ~90.9KB | ~1.54MB | ~1.54MB (17 条) |
| AgentPersist/calls=10 | 21 | ~66KB | ~599KB | ~599KB (21 条) |
| AgentPersist/calls=50 | 101 | ~350KB | ~13.7MB | ~13.7MB (101 条) |

> 权威数字以原始输出为准；`saves/run` 语义为一次完整运行的成功保存数。
> `saved_json` 为逻辑保存数据量（MemoryStore 序列化字节），不是磁盘写量。

## 历史基线结论（2.3 优化前）

1. **上下文裁剪超线性是最大热点**：`DropOld` 100 轮工具历史
   45.5ms、1000 轮 5.04s/1.11GB——与 NoTrim（1000 轮 18ms）差
   ~280×。对应 2.3 的 Token 计数缓存与单次遍历改造。
2. **会话/轮次累积呈平方曲线**：ReAct 10→100 次模型调用耗时
   2.67ms→251ms（约 94 倍）。`SessionGrowth/100` 新语义下
   2.65s/2.05GB，与会话内消息数平方一致；支撑 2.1 消息拷贝与
   1.5 历史存储优化的优先级判断。
3. **持久化保存量随历史放大**：`saves/run=101` 时单次运行的逻辑保存
   数据量约 13.7MB，最新记录约 350KB。计时内已剔除构图与观测包装，
   持久化本身成本即为上表 ns/op。
4. **快照传输 vs 增量**：snapshot 比 delta 慢 ~19.6×、B/op ~18×；
   流式保持增量正确，且公共事件流逐片发布已核实。
5. **NeedsMessages 生效**：8 工具 100 轮 off 182μs → on 209μs，
   按需注入只 +15%；但 `ToolCallbacks/cb=4/history=on` 达 7.5ms
   ——回调克隆视图是下一层成本（对应 2.6）。

## 回归流程

- 变更前后各跑 `-Profile Full -Count 10`，`benchstat` 比较同名场景；
- 提示线：`ns/op` +10% 且显著，或 `B/op`/`allocs/op` +10% 进入复查；
  旧值为零时报绝对增量；
- 正确性失败、缺少预期场景、运行错误即失败（`bench-validate.ps1`
  逐项核对场景与采样数）；
- 修改输入语义、计时范围或 fixture 版本 → 新基线，不能沿用对比；
- manifest 记录 HEAD、dirty、fixture/bench 版本与 Go 工具链；
  工作区脏时归档 `workdir.patch`（git 直写二进制补丁）、
  `workdir-src/`（未跟踪源码按相对路径）与 `workdir-fingerprints.txt`，
  本次 `baseline-v3-stream` 补采已验证可恢复源码、核对全部 100 个
  归档源码文件指纹，并跑通相关测试和流式检查；旧 `baseline-v2`
  归档仍无法直接还原，后续前后对照需另存新脚本生成的归档；
- 当前仓库无 CI：本文档的命令即回归入口，性能数字只做提示，
  不声称已纳入阻断流程。

## 历史记录（baseline-v2，2026-10-03）

baseline-v2 的原始 Full 输出有 99 个场景各 10 次，非流式摘要保留作分析参考。其流式测量包含替身的无用全文拼接和一次计时内预检查，不能作为修复后流式性能的基线；旧 events 原始中位数为 550,923 ns/op、2,472,192.5 B/op，已由本次补采替换。旧补丁仍报 `corrupt patch at line 27`，缺少未跟踪源码内容，不声称能还原旧采样。

## 历史记录（baseline-v1，2026-10-03 首轮）

v1 数据因测量口径问题作废，仅留档：

- `MessageClone/100/text` 54k ns 实测为 200 条消息；v2 修正为 20k ns。
- `AgentStream/events` 当时只观察到一次最终快照；v2 已逐片发布。
  v2 首轮还有替身的无用全文拼接与计时内预检查成本，当前修正后的
  流式数据以 baseline-v3-stream 原始记录为准。
- `PersistedGraph`/`AgentPersist` 的 ns/op 含构图、编译与 StoreObserver
  包装；v2 拆分后 sync 16×4KB 为 854k（v1 记 1.09M）。两次采样的差值
  还可能包含运行波动，不能全部归因于被剔除的构建与观测成本。
- `SessionGrowth` v1 为每请求最多 8 次工具调用的旧语义，与 v2
  （1 工具 + 1 回答）不可比。
