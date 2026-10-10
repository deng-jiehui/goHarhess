# 阶段四基准：引擎收敛（pre-slim 对照）

2026-10-10 在阶段四收口态上按《轻量化改良方案》性能验收规则跑基准。
因阶段四改动集中在 Graph 引擎（并行统一、检查点简化、命名空间嵌套记录、
受管任务删除），本轮采样范围为 **Graph 侧 24 个场景子集**（
ParallelGraph / PersistedGraph / ResumeGraph / SequentialGraph），
与 `pre-slim` Full 基线同名场景逐项比较。结论：**通过**——24 项全部
在 ±10% 阈值内或为改善，0 项超阈；恢复与并行路径改善显著
（ns/op 最大 -56.7%，B/op 最大 -46.8%，allocs/op 最大 -55.2%）。

阶段四改动：删除 `AddParallelNode`（`parallelnode` 场景名保留、改走
forkjoin 同一路径）、条目化契约改整图指纹、嵌套执行树改命名空间独立
记录、受管任务与会话子 Agent 移出核心。均为删除与换径，无新增热路径
开销；恢复路径不再聚合嵌套执行树，完成态检查点恢复显著变快。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-10 |
| 代码 | `de7ac02` 加工作区文档改动（基准二进制于 `de7ac02` 内容编译） |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| 采样范围 | Graph 侧 24 个场景，每个 10 次，共 240 条样本 |

## 运行命令

```powershell
go test -p=1 ./pkg/graph -run '^$' `
  -bench 'BenchmarkGHFull/(ParallelGraph|ResumeGraph|PersistedGraph|SequentialGraph)' `
  -benchmem -benchtime=1s -count=10 -cpu=1
```

原始输出归档在 [raw/phase4/graph-subset.txt](raw/phase4/graph-subset.txt)；
基线原始数据见 [raw/pre-slim/core-cpu1.txt](raw/pre-slim/core-cpu1.txt)。
另跑 Smoke 组 14 场景 ×5 样本于 `.bench/results/phase4-smoke/`（该组无
pre-slim 同名基线，仅作冒烟通过记录）。

## 对照结论（中位数，Δ 相对 pre-slim）

| 场景 | 基线 ns/op | 本轮 ns/op | Δns | 基线 B/op | 本轮 B/op | ΔB | 基线 allocs | 本轮 allocs | Δa |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ParallelGraph/forkjoin/branches=2/1K | 45,606 | 48,710 | +6.8% | 59,541 | 58,933 | -1.0% | 345 | 332 | -3.8% |
| ParallelGraph/forkjoin/branches=32/1K | 582,660 | 416,361 | -28.5% | 708,150 | 533,774 | -24.6% | 5,792 | 2,596 | -55.2% |
| ParallelGraph/forkjoin/branches=8/mc=1 | 158,996 | 108,197 | -31.9% | 159,469 | 152,284 | -4.5% | 1,015 | 808 | -20.4% |
| ParallelGraph/forkjoin/branches=8/mc=8 | 161,114 | 109,417 | -32.1% | 160,255 | 153,070 | -4.5% | 1,022 | 815 | -20.3% |
| ParallelGraph/forkjoin/branches=8/1K | 125,163 | 118,465 | -5.4% | 159,343 | 152,158 | -4.5% | 1,003 | 796 | -20.6% |
| ParallelGraph/forkjoin/branches=8/64K | 3,980,793 | 4,123,311 | +3.6% | 7,183,848 | 7,176,664 | -0.1% | 1,016 | 809 | -20.4% |
| ParallelGraph/parallelnode/branches=2/1K | 71,498 | 46,730 | -34.6% | 93,848 | 58,933 | -37.2% | 497 | 332 | -33.2% |
| ParallelGraph/parallelnode/branches=32/1K | 714,209 | 398,872 | -44.2% | 946,336 | 533,774 | -43.6% | 4,232 | 2,596 | -38.7% |
| ParallelGraph/parallelnode/branches=8/mc=1 | 249,655 | 108,223 | -56.7% | 263,110 | 152,284 | -42.1% | 1,274 | 808 | -36.6% |
| ParallelGraph/parallelnode/branches=8/mc=8 | 242,256 | 109,817 | -54.7% | 263,898 | 153,070 | -42.0% | 1,281 | 815 | -36.4% |
| ParallelGraph/parallelnode/branches=8/1K | 198,511 | 120,365 | -39.4% | 262,577 | 152,158 | -42.1% | 1,257 | 796 | -36.7% |
| ParallelGraph/parallelnode/branches=8/64K | 7,408,633 | 3,830,766 | -48.3% | 13,247,096 | 7,176,677 | -45.8% | 1,281 | 809 | -36.8% |
| PersistedGraph/4K/checkpoint=async | 971,076 | 612,283 | -36.9% | 1,542,396 | 1,269,553 | -17.7% | 3,264 | 2,090 | -36.0% |
| PersistedGraph/4K/checkpoint=off | 44,991 | 32,954 | -26.8% | 45,474 | 41,026 | -9.8% | 573 | 536 | -6.5% |
| PersistedGraph/4K/checkpoint=sync | 960,706 | 614,209 | -36.1% | 1,541,020 | 1,268,206 | -17.7% | 3,291 | 2,117 | -35.7% |
| PersistedGraph/64K/checkpoint=async | 9,375,875 | 7,571,197 | -19.2% | 17,931,412 | 17,321,354 | -3.4% | 3,295 | 2,121 | -35.6% |
| PersistedGraph/64K/checkpoint=off | 50,992 | 33,349 | -34.6% | 106,918 | 102,468 | -4.2% | 573 | 536 | -6.5% |
| PersistedGraph/64K/checkpoint=sync | 9,655,214 | 7,590,425 | -21.4% | 17,898,217 | 17,299,557 | -3.3% | 3,330 | 2,157 | -35.2% |
| ResumeGraph/completed_result/sync | 16,355 | 5,933 | -63.7% | 10,369 | 5,904 | -43.1% | 121 | 77 | -36.4% |
| ResumeGraph/completed_result/async | 16,229 | 5,779 | -64.4% | 10,369 | 5,904 | -43.1% | 121 | 77 | -36.4% |
| ResumeGraph/parallel_suspend/sync | 272,996 | 169,688 | -37.8% | 227,548 | 179,707 | -21.0% | 2,212 | 1,820 | -17.7% |
| ResumeGraph/parallel_suspend/async | 276,786 | 167,254 | -39.6% | 227,564 | 179,723 | -21.0% | 2,211 | 1,819 | -17.7% |
| ResumeGraph/sequential_suspend/sync | 115,618 | 55,144 | -52.3% | 107,412 | 57,147 | -46.8% | 930 | 674 | -27.5% |
| ResumeGraph/sequential_suspend/async | 115,022 | 52,160 | -54.7% | 107,459 | 57,194 | -46.8% | 926 | 670 | -27.6% |

## 判定

- 确定性指标 `B/op`、`allocs/op`：0 项超阈，全部一致或改善。
- `ns/op`：两项在阈内正向波动（forkjoin/branches=2 +6.8%、
  forkjoin/64K +3.6%），无超阈项，无需复测。
- 改善来源：恢复路径不再序列化/聚合嵌套执行树；`AddParallelNode`
  展开包装层消失（`parallelnode` 场景现为 forkjoin 等价路径）；
  受管任务簿记从超步路径移除。

未采样的 75 个场景（MessageClone、ToolBatch、ContextTrim、ReAct、
AgentPersist、AgentStream、CheckpointCodec、MemoryStore 等）在阶段四
无相关代码路径变更，按前阶段（phase1–3 全量）结论沿用；阶段五如需
可补跑一次 Full 组归档。
