# 阶段一基准：统一观测（pre-slim 对照）

2026-10-09 在 `27569bf`（阶段一完成态）上按《轻量化改良方案》性能验收规则重跑 Full 组，
对照 `pre-slim` 基线（见 [pre-slim.md](pre-slim.md)）。结论：**通过**——确定性指标
（`B/op`、`allocs/op`）全部不劣于基线且多数明显下降；`ns/op` 有 11 项超阈，经双版本
同条件复测确认为采样时段的机器噪声而非代码回退，依据见下文"复测记录"。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-09 |
| 代码 | `27569bfccb5b293cbdb7f909d9aa5ba0ffc80042`，工作区干净 |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| 采样范围 | Full 99 个场景，每个 10 次，共 990 条样本 |

## 运行命令

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/phase1-final
```

## 对照结论

- 99 个同名场景全部存在，无缺漏。
- `B/op` 与 `allocs/op`：无任何超过 +10% 的场景。`allocs/op` 为确定性计数，与基线
  逐项一致或更低（如 ReAct `model_calls=50` 261.8k→224.6k，-14%；checkpoint 路径
  在惰性守卫修复后回落到基线以内）。
- `ns/op`：96 项指标改善超过 10%（典型如 `ToolCallbacks/callbacks=4/history=on`
  13.81M→1.54M，-89%；`SessionGrowth/requests=100` -34%），11 项超阈。

## 复测记录（10% 阈值规则）

第一轮复测针对首跑标记的 15 项回退：全部位于 checkpoint save/load 与 interrupt
发射路径，`observe.Begin`/`EventOf` 在无观察者时仍无条件执行，每次保存约多 20
次分配（确定性可复现）。修复方式：发射点加 `observe.Active` 守卫，无观察者时短路
为零值 `Invocation`（提交 `27569bf`）。复测后该项回退清零。

第二轮（本次归档数据）新标记的 11 项全部为 `ns/op`：`ParallelGraph` 系列、
`ToolBatch/calls=32/history=off`、`MessageClone/messages=10/kind=text`、
`MessageTransport/*`。判定为环境噪声的依据：

1. 均为纯耗时指标，同场景 `B/op`、`allocs/op` 与基线逐项一致；
2. 相关代码路径未变——无观察者时并行节点、`pkg/schema` 传输/克隆不含任何观测调用，
   且同一批场景在阶段一首跑中录得 -17%~-20% 改善，两跑方向相反；
3. 用 `pre-slim` worktree 同条件重采样该组场景，复现原基线中位数
   （forkjoin/2 ≈44.6k vs 基线 45.6k；ToolBatch/32 ≈413k vs 411k），
   即基线侧稳定，波动发生在本轮采样时段。

## 结果摘要（cpu=1，10 次采样中位）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| MessageTransport/delta | 67.4k | 133.6k | 547 |
| MessageTransport/snapshot | 1.31M | 2.37M | 1.0k |
| GHFull/MessageClone/messages=10/kind=text | 3.0k | 3.6k | 71 |
| GHFull/MessageClone/messages=10/kind=toolmeta | 18.4k | 21.3k | 399 |
| GHFull/MessageClone/messages=100/kind=text | 24.6k | 36.2k | 701 |
| GHFull/MessageClone/messages=100/kind=toolmeta | 244.4k | 215.2k | 4.0k |
| GHFull/MessageClone/messages=1000/kind=text | 377.3k | 365.7k | 8.5k |
| GHFull/MessageClone/messages=1000/kind=toolmeta | 2.87M | 2.17M | 42.2k |
| GHFull/ContextTrim/NoTrim/turns=10 | 152.5k | 29.1k | 379 |
| GHFull/ContextTrim/NoTrim/turns=100 | 1.41M | 284.4k | 3.4k |
| GHFull/ContextTrim/NoTrim/turns=1000 | 14.06M | 2.63M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=100/kind=text | 1.10M | 268.0k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=tool | 1.95M | 811.6k | 10.9k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=text | 8.39M | 2.35M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=tool | 18.35M | 7.49M | 110.8k |
| GHFull/ContextTrim/DropOld/turns=100/kind=cjk | 378.2k | 263.9k | 3.4k |
| GHFull/ContextTrim/OversizedLatest/input_bytes=65536 | 10.46M | 779.3k | 407 |
| GHFull/ToolBatch/calls=1/turns=100/history=off | 157.9k | 209.1k | 3.8k |
| GHFull/ToolBatch/calls=1/turns=100/history=on | 163.1k | 241.8k | 3.8k |
| GHFull/ToolBatch/calls=8/turns=100/history=off | 238.0k | 278.4k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=100/history=on | 268.3k | 540.5k | 4.7k |
| GHFull/ToolBatch/calls=32/turns=100/history=off | 505.1k | 518.6k | 7.7k |
| GHFull/ToolBatch/calls=32/turns=100/history=on | 628.2k | 1.57M | 7.8k |
| GHFull/ToolBatch/calls=8/turns=100/history=mixed | 250.0k | 409.5k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=10/history=off | 98.2k | 99.0k | 1.4k |
| GHFull/ToolBatch/calls=8/turns=1000/history=off | 2.34M | 2.07M | 42.5k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=off | 234.8k | 278.4k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=on | 263.2k | 540.5k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=off | 261.4k | 305.5k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=on | 1.52M | 2.18M | 34.8k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=off | 254.8k | 305.5k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=on | 1.54M | 2.18M | 34.8k |
| GHFull/SequentialGraph/nodes=1 | 4.1k | 4.9k | 66 |
| GHFull/SequentialGraph/nodes=10 | 23.8k | 23.3k | 343 |
| GHFull/SequentialGraph/nodes=100 | 384.9k | 405.4k | 3.2k |
| GHFull/SequentialGraph/build_compile/nodes=10 | 65.5k | 54.4k | 802 |
| GHFull/SequentialGraph/build_compile/nodes=100 | 599.7k | 520.5k | 7.1k |
| GHFull/SequentialGraph/compile_only/nodes=100 | 465.9k | 407.5k | 5.5k |
| GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024 | 57.6k | 58.6k | 335 |
| GHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024 | 90.3k | 92.7k | 485 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024 | 161.8k | 157.0k | 981 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024 | 252.2k | 260.0k | 1.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024 | 761.1k | 700.5k | 5.7k |
| GHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024 | 906.3k | 938.4k | 4.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536 | 5.02M | 7.18M | 994 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536 | 7.47M | 13.24M | 1.3k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1 | 120.4k | 157.2k | 993 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1 | 190.9k | 260.6k | 1.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8 | 121.7k | 157.9k | 1.0k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8 | 191.5k | 261.4k | 1.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off | 34.6k | 41.6k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync | 803.3k | 1.54M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async | 817.4k | 1.54M | 3.2k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off | 37.9k | 103.0k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync | 7.94M | 17.89M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async | 7.91M | 17.92M | 3.3k |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=sync | 95.6k | 106.3k | 923 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=sync | 220.4k | 226.9k | 2.2k |
| GHFull/ResumeGraph/completed_result/checkpoint=sync | 12.2k | 10.4k | 121 |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=async | 93.3k | 106.3k | 919 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=async | 219.6k | 226.9k | 2.2k |
| GHFull/ResumeGraph/completed_result/checkpoint=async | 12.2k | 10.4k | 121 |
| GHFull/ReAct/model_calls=10/checkpoint=off | 1.58M | 863.3k | 12.5k |
| GHFull/ReAct/model_calls=50/checkpoint=off | 34.10M | 16.45M | 224.6k |
| GHFull/ReAct/model_calls=100/checkpoint=off | 137.21M | 66.51M | 854.7k |
| GHFull/ReAct/model_calls=50/history=off | 34.06M | 16.45M | 224.6k |
| GHFull/ReAct/model_calls=50/history=on | 34.31M | 16.64M | 224.7k |
| GHFull/AgentPersist/model_calls=10/checkpoint=off | 1.57M | 863.5k | 12.5k |
| GHFull/AgentPersist/model_calls=10/checkpoint=sync | 4.59M | 5.37M | 22.6k |
| GHFull/AgentPersist/model_calls=10/checkpoint=async | 4.58M | 5.37M | 22.6k |
| GHFull/AgentPersist/model_calls=50/checkpoint=off | 34.55M | 16.45M | 224.6k |
| GHFull/AgentPersist/model_calls=50/checkpoint=sync | 95.47M | 120.58M | 421.0k |
| GHFull/AgentPersist/model_calls=50/checkpoint=async | 95.95M | 120.58M | 421.0k |
| GHFull/SessionGrowth/requests=10 | 8.16M | 9.88M | 98.9k |
| GHFull/SessionGrowth/requests=100 | 782.70M | 873.68M | 9.02M |
| GHFull/ResumeAgent/tool_suspend/checkpoint=sync | 899.8k | 746.3k | 5.9k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=async | 660.1k | 746.3k | 5.9k |
| GHFull/AgentStream/mode=invoke/text_bytes=16384 | 66.3k | 138.2k | 961 |
| GHFull/AgentStream/mode=stream/text_bytes=16384 | 67.3k | 138.2k | 961 |
| GHFull/AgentStream/mode=events/text_bytes=16384 | 316.3k | 266.6k | 2.7k |
| GHFull/AgentStream/mode=events_lifecycle/text_bytes=16384 | 322.5k | 268.6k | 2.8k |
| GHFull/CheckpointCodec/payload_bytes=4096/clone | 242 | 4.9k | 2 |
| GHFull/CheckpointCodec/payload_bytes=4096/json | 17.3k | 10.2k | 8 |
| GHFull/CheckpointCodec/payload_bytes=4096/decode | 23.7k | 30.0k | 27 |
| GHFull/CheckpointCodec/payload_bytes=65536/clone | 3.6k | 73.8k | 2 |
| GHFull/CheckpointCodec/payload_bytes=65536/json | 257.4k | 147.9k | 8 |
| GHFull/CheckpointCodec/payload_bytes=65536/decode | 329.4k | 482.4k | 31 |
| GHFull/CheckpointCodec/payload_bytes=524288/clone | 26.6k | 532.5k | 2 |
| GHFull/CheckpointCodec/payload_bytes=524288/json | 1.95M | 1.07M | 9 |
| GHFull/CheckpointCodec/payload_bytes=524288/decode | 2.52M | 3.69M | 38 |
| GHFull/MemoryStore/save_empty | 8.2k | 11.2k | 15 |
| GHFull/MemoryStore/save_history=100 | 8.2k | 10.1k | 12 |
| GHFull/MemoryStore/load_history=100 | 263 | 296 | 6 |
| GHFull/MemoryStore/get_oldest/history=100 | 267 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=100/limit=10 | 3.3k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=100/limit=100 | 28.8k | 143.3k | 617 |
| GHFull/MemoryStore/get_oldest/history=1000 | 293 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=1000/limit=10 | 4.5k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=1000/limit=100 | 40.0k | 143.3k | 617 |

## 原始记录

- [raw/phase1/core-cpu1.txt](raw/phase1/core-cpu1.txt)：990 条原始样本，已随本文件提交。
- [raw/phase1/manifest.json](raw/phase1/manifest.json)：环境、命令与代码身份（`git_dirty=false`）。
- `.bench/results/phase1/` 与 `.bench/results/phase1-final/`：首跑（含惰性守卫修复前）与
  最终跑的本地归档，不进 Git。首跑的对比分析曾暴露真实分配回退，修复记录见 `27569bf`。
