# 阶段二基准：图门面与 Agent 收敛（pre-slim 对照）

2026-10-09 在阶段二收口态上按《轻量化改良方案》性能验收规则重跑 Full 组，
对照 `pre-slim` 基线（见 [pre-slim.md](pre-slim.md)）。结论：**通过**——确定性
指标（`B/op`、`allocs/op`）与基线逐项一致或改善，无一超过 +10% 阈值；`ns/op`
有 2 项超阈，经同条件复测确认为采样噪声，依据见下文"复测记录"。

阶段二改动（`b4808e6`、`eedd987`、`ec48966`、`7073e79` 及后续复核修复）
为 API 面收敛：`graph.NewState` 门面、单次求值路由边、组级合并器（全体
到达后一次性执行）、`Checkpointer`/`Thread`、`tool.New`、
`agent.New(model, opts...)` 与 Agent 接口缩减；第三轮复核后为成员局部态
增加活指针复用，原生分支热路径较基线进一步下降。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-09 |
| 代码 | `eedd987` 加阶段二复核修复工作区（manifest 记录 `git_dirty=true`，补丁已随 raw 归档） |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| 采样范围 | Full 99 个场景，每个 10 次，共 990 条样本 |

## 运行命令

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/phase2
```

## 对照结论

- 99 个同名场景全部存在，无缺漏。统计口径：同名场景按 10 次采样取中位数后逐项
  比较，阈值 ±10%。
- `B/op` 与 `allocs/op`：99 项全部与基线一致或在基线以内，0 项超阈。Agent 收敛
  未改变执行路径，分配曲线与阶段一持平。
- `ns/op`：87 项改善超过 10%，2 项超阈（下述），其余在阈值内波动。

## 复测记录（10% 阈值规则）

本轮新标记 2 项，均为纯耗时指标。按门槛要求补做双侧同条件 10 次复测
（2026-10-10，`pre-slim` 基线 worktree 与阶段二收口态 `7073e79` 各 10 次，
`-p=1 -benchmem -benchtime=1s -count=10 -cpu=1`，原始输出归档在
[raw/phase2/rerun/](raw/phase2/rerun/)）：

| 场景 | 基线 Full 中位 | 阶段二 Full 中位 | 基线复测中位（×10） | 候选复测中位（×10） | 判定 |
| --- | ---: | ---: | ---: | ---: | --- |
| `MessageTransport/snapshot` | 1.070M | 1.236M（+15.5%） | 1.051M | 1.093M（+4.0%） | 噪声 |
| `GHFull/MessageClone/messages=10/kind=text` | 2.06k | 2.84k（+37.9%） | 1968 | 1962（-0.3%） | 噪声 |

判定依据：

1. 两项的 `B/op`、`allocs/op` 与基线逐项一致，无新增分配；
2. 相关代码路径未变——`pkg/schema` 的传输/克隆不含阶段二任何改动；
3. 双侧各 10 次同条件复测中位差 +4.0% 与 -0.3%，均落在 ±10% 阈值内；
   Full 组的单样本抖动（15.5%/37.9%）未在复测中复现，判定为采样噪声。

## 原生分支开销复测（第三轮复核后）

第三轮复核标记 `forkjoin`（原生分支）族耗时/分配超阈：根因是成员局部态
每轮无条件 `snapshotRuntimeState`/`restoreRuntimeState` 编解码。修复为
`GroupMember` 增加未导出活指针：无检查点运行时直接复用内存态，仅
`setup.configured` 时才编码快照供持久化；汇合时优先读活指针。

修复后按门槛双侧各 10 次同条件复测（`pre-slim` worktree 与修复态，
`-p=1 -benchmem -benchtime=1s -count=10 -cpu=1`，原始输出归档
[raw/phase2/rerun/pre-slim-forkjoin.txt](raw/phase2/rerun/pre-slim-forkjoin.txt) 与
[raw/phase2/rerun/phase2-forkjoin.txt](raw/phase2/rerun/phase2-forkjoin.txt)）：

| 场景 | 基线中位 ns/op | 候选中位 ns/op | 差值 | B/op 差值 | allocs/op 差值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `forkjoin/branches=2/state_bytes=1024` | 45,336 | 45,495 | +0.4% | -0.7% | -3.5% |
| `forkjoin/branches=8/state_bytes=1024` | 158,046 | 116,515 | -26.3% | -4.3% | -20.5% |
| `forkjoin/branches=8/state_bytes=65536` | 4,014,360 | 3,983,697 | -0.8% | -0.1% | -20.3% |
| `forkjoin/branches=8/max_concurrency=1` | 127,347 | 115,298 | -9.5% | -4.3% | -20.3% |
| `forkjoin/branches=8/max_concurrency=8` | 129,249 | 115,671 | -10.5% | -4.2% | -20.2% |
| `forkjoin/branches=32/state_bytes=1024` | 682,927 | 398,141 | -41.7% | -24.5% | -55.2% |

六项全部在 ±10% 阈值内且多数显著优于基线：活指针复用同时消除了旧模型每轮
批内合并的重复工作，分支越多收益越大。检查点路径行为不变（`configured` 时
仍编码快照并随调度持久化，恢复走快照解码）。

## 结果摘要（cpu=1，10 次采样中位数）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| GHFull/AgentPersist/model_calls=10/checkpoint=async | 4.82M | 5.37M | 22.6k |
| GHFull/AgentPersist/model_calls=10/checkpoint=off | 1.63M | 865.1k | 12.5k |
| GHFull/AgentPersist/model_calls=10/checkpoint=sync | 4.88M | 5.37M | 22.6k |
| GHFull/AgentPersist/model_calls=50/checkpoint=async | 101.08M | 120.59M | 421k |
| GHFull/AgentPersist/model_calls=50/checkpoint=off | 37.08M | 16.46M | 224.6k |
| GHFull/AgentPersist/model_calls=50/checkpoint=sync | 101.09M | 120.60M | 421k |
| GHFull/AgentStream/mode=events/text_bytes=16384 | 316.4k | 266.6k | 2.7k |
| GHFull/AgentStream/mode=events_lifecycle/text_bytes=16384 | 324.5k | 268.6k | 2.8k |
| GHFull/AgentStream/mode=invoke/text_bytes=16384 | 66.2k | 138.2k | 961 |
| GHFull/AgentStream/mode=stream/text_bytes=16384 | 66.1k | 138.2k | 961 |
| GHFull/CheckpointCodec/payload_bytes=4096/clone | 247 | 4.9k | 2 |
| GHFull/CheckpointCodec/payload_bytes=4096/decode | 23.6k | 30k | 27 |
| GHFull/CheckpointCodec/payload_bytes=4096/json | 17.2k | 10.2k | 8 |
| GHFull/CheckpointCodec/payload_bytes=524288/clone | 27.8k | 532.5k | 2 |
| GHFull/CheckpointCodec/payload_bytes=524288/decode | 2.60M | 3.69M | 38 |
| GHFull/CheckpointCodec/payload_bytes=524288/json | 2.06M | 1.07M | 9 |
| GHFull/CheckpointCodec/payload_bytes=65536/clone | 3.7k | 73.8k | 2 |
| GHFull/CheckpointCodec/payload_bytes=65536/decode | 341.7k | 482.4k | 31 |
| GHFull/CheckpointCodec/payload_bytes=65536/json | 261.4k | 147.9k | 8 |
| GHFull/ContextTrim/DropOld/turns=100/kind=cjk | 338.1k | 263.9k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=text | 930.3k | 268k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=tool | 1.61M | 811.6k | 10.9k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=text | 6.87M | 2.35M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=tool | 14.66M | 7.49M | 110.8k |
| GHFull/ContextTrim/NoTrim/turns=10 | 126.6k | 29.1k | 379 |
| GHFull/ContextTrim/NoTrim/turns=100 | 1.16M | 284.4k | 3.4k |
| GHFull/ContextTrim/NoTrim/turns=1000 | 12.17M | 2.63M | 37.5k |
| GHFull/ContextTrim/OversizedLatest/input_bytes=65536 | 8.94M | 779.3k | 407 |
| GHFull/MemoryStore/get_oldest/history=100 | 277 | 296 | 6 |
| GHFull/MemoryStore/get_oldest/history=1000 | 307 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=100/limit=10 | 3.5k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=100/limit=100 | 30.1k | 143.3k | 617 |
| GHFull/MemoryStore/list_latest/history=1000/limit=10 | 4.6k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=1000/limit=100 | 42.4k | 143.3k | 617 |
| GHFull/MemoryStore/load_history=100 | 278 | 296 | 6 |
| GHFull/MemoryStore/save_empty | 8.8k | 11.2k | 15 |
| GHFull/MemoryStore/save_history=100 | 8.7k | 10.1k | 12 |
| GHFull/MessageClone/messages=10/kind=text | 2.8k | 3.6k | 71 |
| GHFull/MessageClone/messages=10/kind=toolmeta | 16.6k | 21.3k | 399 |
| GHFull/MessageClone/messages=100/kind=text | 22k | 36.2k | 701 |
| GHFull/MessageClone/messages=100/kind=toolmeta | 167.7k | 215.2k | 4k |
| GHFull/MessageClone/messages=1000/kind=text | 288.2k | 365.7k | 8.5k |
| GHFull/MessageClone/messages=1000/kind=toolmeta | 2.28M | 2.17M | 42.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024 | 46.4k | 58.6k | 335 |
| GHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024 | 605k | 700.5k | 5.7k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1 | 128.9k | 157.2k | 993 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8 | 133.6k | 157.9k | 1k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024 | 128k | 157k | 981 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536 | 4.09M | 7.18M | 994 |
| GHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024 | 73.2k | 92.7k | 485 |
| GHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024 | 733.9k | 938.4k | 4.2k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1 | 206k | 260.6k | 1.3k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8 | 203.1k | 261.4k | 1.3k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024 | 202.4k | 260k | 1.2k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536 | 7.67M | 13.24M | 1.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async | 862.1k | 1.54M | 3.2k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off | 37.9k | 41.6k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync | 850k | 1.54M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async | 8.35M | 17.93M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off | 40.1k | 103k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync | 8.27M | 17.89M | 3.3k |
| GHFull/ReAct/model_calls=10/checkpoint=off | 1.62M | 864.9k | 12.5k |
| GHFull/ReAct/model_calls=100/checkpoint=off | 144.44M | 66.53M | 854.7k |
| GHFull/ReAct/model_calls=50/checkpoint=off | 35.59M | 16.46M | 224.6k |
| GHFull/ReAct/model_calls=50/history=off | 35.71M | 16.46M | 224.6k |
| GHFull/ReAct/model_calls=50/history=on | 35.57M | 16.65M | 224.7k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=async | 680.5k | 746.5k | 5.9k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=sync | 685k | 746.5k | 5.9k |
| GHFull/ResumeGraph/completed_result/checkpoint=async | 13.7k | 10.4k | 121 |
| GHFull/ResumeGraph/completed_result/checkpoint=sync | 13.4k | 10.4k | 121 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=async | 233.7k | 226.9k | 2.2k |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=sync | 239k | 226.9k | 2.2k |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=async | 100.1k | 106.3k | 919 |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=sync | 101.8k | 106.3k | 923 |
| GHFull/SequentialGraph/build_compile/nodes=10 | 45.5k | 54.5k | 803 |
| GHFull/SequentialGraph/build_compile/nodes=100 | 418.8k | 520.6k | 7.1k |
| GHFull/SequentialGraph/compile_only/nodes=100 | 343k | 407.6k | 5.5k |
| GHFull/SequentialGraph/nodes=1 | 3.4k | 4.9k | 66 |
| GHFull/SequentialGraph/nodes=10 | 19.9k | 23.3k | 343 |
| GHFull/SequentialGraph/nodes=100 | 286.2k | 405.4k | 3.2k |
| GHFull/SessionGrowth/requests=10 | 8.56M | 9.88M | 98.9k |
| GHFull/SessionGrowth/requests=100 | 813.36M | 873.70M | 9.02M |
| GHFull/ToolBatch/calls=1/turns=100/history=off | 138.1k | 209.2k | 3.8k |
| GHFull/ToolBatch/calls=1/turns=100/history=on | 175.9k | 242k | 3.8k |
| GHFull/ToolBatch/calls=32/turns=100/history=off | 379.2k | 524.2k | 7.7k |
| GHFull/ToolBatch/calls=32/turns=100/history=on | 507.5k | 1.57M | 7.8k |
| GHFull/ToolBatch/calls=8/turns=10/history=off | 77.2k | 100.4k | 1.4k |
| GHFull/ToolBatch/calls=8/turns=100/history=mixed | 200k | 410.9k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=100/history=off | 198.6k | 279.8k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=100/history=on | 211.3k | 542k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=1000/history=off | 1.81M | 2.08M | 42.5k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=off | 181.1k | 279.8k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=on | 208.9k | 542k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=off | 197.6k | 306.9k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=on | 1.21M | 2.18M | 34.8k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=off | 206.7k | 306.9k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=on | 1.24M | 2.18M | 34.8k |
| MessageTransport/delta | 62.2k | 133.6k | 547 |
| MessageTransport/snapshot | 1.24M | 2.37M | 1k |

## 原始记录

- [raw/phase2/core-cpu1.txt](raw/phase2/core-cpu1.txt)：990 条原始样本，已随本文件提交。
- [raw/phase2/manifest.json](raw/phase2/manifest.json)：环境、命令与代码身份
  （`git_dirty=true`，工作区补丁见 manifest 引用的 `workdir.patch` 归档）。
- `.bench/results/phase2/`：本地归档，不进 Git。
