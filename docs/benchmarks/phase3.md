# 阶段三基准：低风险瘦身（pre-slim 对照）

2026-10-09 在阶段三收口态上按《轻量化改良方案》性能验收规则跑 Full 组，
对照 `pre-slim` 基线（见 [pre-slim.md](pre-slim.md)）。结论：**通过**——确定性
指标（`B/op`、`allocs/op`）与基线逐项一致或改善，无一超过 +10% 阈值；`ns/op`
有 10 项超阈，经双侧同条件各 10 次复测全部回落到阈值内，判定为采样噪声，
依据见下文"复测记录"。

阶段三改动（`16cd900`）为删除与外移：`pkg/retrieval` 移至
`extensions/retrieval`，删除 `httpclient.Service`/`tool.NewHTTPService`、
旧并行检查点格式迁移、已弃用的 `llm.DeltaAccumulator`；Agent 运行前置抽为
共享 `runPrelude`，`agentrecipe` 运行时 State 移除冗余 `Input` 字段（线格式
保留兼容）。无热路径行为变化。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-09 |
| 代码 | `16cd900`（manifest 记录 `git_dirty=true`，仅文档改动，补丁已随 raw 归档） |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD64 Family 26 Model 68 |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| 采样范围 | Full 99 个场景，每个 10 次，共 990 条样本 |

## 运行命令

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/phase3
```

## 对照结论

- 99 个同名场景全部存在，无缺漏。统计口径：同名场景按 10 次采样取中位数后逐项
  比较，阈值 ±10%。
- `B/op` 与 `allocs/op`：0 项超阈；多项因阶段二起引入的成员活指针复用显著改善
  （forkjoin `branches=32` 为 -24.5%/-55.2%）。
- `ns/op`：21 项改善超过 10%，10 项超阈（下述），其余在阈值内波动。

## 复测记录（10% 阈值规则）

本轮新标记 10 项，均为纯耗时指标（`B/op`/`allocs/op` 全部在阈内或为改善）。
按门槛要求补做双侧同条件各 10 次复测（`pre-slim` 基线 worktree 与阶段三收口态
`16cd900`，`-p=1 -benchmem -benchtime=1s -count=10 -cpu=1`，原始输出归档在
[raw/phase3/rerun/](raw/phase3/rerun/)）：

| 场景 | 基线 Full 中位 | 阶段三 Full 中位 | 基线复测中位（×10） | 候选复测中位（×10） | 判定 |
| --- | ---: | ---: | ---: | ---: | --- |
| `MessageTransport/snapshot` | 1.070M | 1.614M（+50.8%） | 1.409M | 1.437M（+2.0%） | 噪声 |
| `MessageTransport/delta` | 58.2k | 83.8k（+44.0%） | 76.3k | 76.8k（+0.7%） | 噪声 |
| `GHFull/MessageClone/messages=10/kind=text` | 2.06k | 3.64k（+76.8%） | 3213 | 3323（+3.4%） | 噪声 |
| `GHFull/MessageClone/messages=10/kind=toolmeta` | 25.9k | 30.5k（+17.8%） | 19.8k | 20.1k（+1.6%） | 噪声 |
| `GHFull/ToolBatch/calls=32/turns=100/history=off` | 429.2k | 519.0k（+20.9%） | 523.0k | 481.8k（-7.9%） | 噪声 |
| `GHFull/ResumeGraph/parallel_suspend/checkpoint=async` | 276.8k | 313.2k（+13.2%） | 292.9k | 308.3k（+5.3%） | 噪声 |
| `GHFull/ResumeGraph/parallel_suspend/checkpoint=sync` | 273.0k | 306.3k（+12.2%） | 296.4k | 301.1k（+1.6%） | 噪声 |
| `GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024` | 45.6k | 61.5k（+34.8%） | 57.1k | 57.2k（+0.1%） | 噪声 |
| `GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024` | 125.2k | 152.8k（+22.1%） | 158.5k | 145.4k（-8.2%） | 噪声 |
| `GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536` | 3.98M | 5.15M（+29.3%） | 4.89M | 4.83M（-1.4%） | 噪声 |

判定依据：

1. 十项的 `B/op`、`allocs/op` 与基线逐项一致或改善，无新增分配；
2. 相关代码路径未变——`pkg/schema` 传输/克隆、`pkg/tool` 批处理均无阶段三
   改动；图族场景改动仅限删除迁移分支，无新增运行时工作；
3. 双侧各 10 次同条件复测中位差均落在 ±10% 阈值内（最大 +5.3%）；基线复测
   值本身高于冻结基线（如 `snapshot` 1.41M 对 1.07M），说明本轮 Full 采样
   环境整体偏慢，超阈为环境性噪声而非回归。

## 结果摘要（cpu=1，10 次采样中位数）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| GHFull/AgentPersist/model_calls=10/checkpoint=async | 5.91M | 5.10M | 22.0k |
| GHFull/AgentPersist/model_calls=10/checkpoint=off | 1.95M | 845.1k | 12.2k |
| GHFull/AgentPersist/model_calls=10/checkpoint=sync | 5.92M | 5.10M | 22.0k |
| GHFull/AgentPersist/model_calls=50/checkpoint=async | 120.89M | 115.02M | 411.1k |
| GHFull/AgentPersist/model_calls=50/checkpoint=off | 42.59M | 15.70M | 219.4k |
| GHFull/AgentPersist/model_calls=50/checkpoint=sync | 120.87M | 115.01M | 411.1k |
| GHFull/AgentStream/mode=events/text_bytes=16384 | 387.3k | 260.3k | 2.7k |
| GHFull/AgentStream/mode=events_lifecycle/text_bytes=16384 | 396.1k | 262.3k | 2.7k |
| GHFull/AgentStream/mode=invoke/text_bytes=16384 | 81.5k | 134.9k | 961 |
| GHFull/AgentStream/mode=stream/text_bytes=16384 | 81.5k | 134.9k | 961 |
| GHFull/CheckpointCodec/payload_bytes=4096/clone | 294 | 4.8k | 2 |
| GHFull/CheckpointCodec/payload_bytes=4096/decode | 28.0k | 29.3k | 27 |
| GHFull/CheckpointCodec/payload_bytes=4096/json | 20.6k | 9.9k | 8 |
| GHFull/CheckpointCodec/payload_bytes=524288/clone | 31.3k | 520.0k | 2 |
| GHFull/CheckpointCodec/payload_bytes=524288/decode | 2.92M | 3.52M | 38 |
| GHFull/CheckpointCodec/payload_bytes=524288/json | 2.37M | 1.02M | 9 |
| GHFull/CheckpointCodec/payload_bytes=65536/clone | 4.3k | 72.0k | 2 |
| GHFull/CheckpointCodec/payload_bytes=65536/decode | 392.1k | 471.1k | 31 |
| GHFull/CheckpointCodec/payload_bytes=65536/json | 308.7k | 144.5k | 8 |
| GHFull/ContextTrim/DropOld/turns=100/kind=cjk | 409.5k | 257.7k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=text | 1.11M | 261.7k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=tool | 2.00M | 792.5k | 10.6k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=text | 8.58M | 2.24M | 36.7k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=tool | 18.40M | 7.15M | 108.2k |
| GHFull/ContextTrim/NoTrim/turns=10 | 163.6k | 28.4k | 379 |
| GHFull/ContextTrim/NoTrim/turns=100 | 1.47M | 277.7k | 3.4k |
| GHFull/ContextTrim/NoTrim/turns=1000 | 14.12M | 2.51M | 36.7k |
| GHFull/ContextTrim/OversizedLatest/input_bytes=65536 | 10.68M | 761.0k | 407 |
| GHFull/MemoryStore/get_oldest/history=100 | 360 | 296 | 6 |
| GHFull/MemoryStore/get_oldest/history=1000 | 393 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=100/limit=10 | 4.3k | 15.2k | 71 |
| GHFull/MemoryStore/list_latest/history=100/limit=100 | 38.2k | 140.0k | 617 |
| GHFull/MemoryStore/list_latest/history=1000/limit=10 | 5.7k | 15.2k | 71 |
| GHFull/MemoryStore/list_latest/history=1000/limit=100 | 51.7k | 140.0k | 617 |
| GHFull/MemoryStore/load_history=100 | 356 | 296 | 6 |
| GHFull/MemoryStore/save_empty | 10.3k | 10.9k | 15 |
| GHFull/MemoryStore/save_history=100 | 9.9k | 9.8k | 12 |
| GHFull/MessageClone/messages=10/kind=text | 3.6k | 3.5k | 71 |
| GHFull/MessageClone/messages=10/kind=toolmeta | 29.8k | 20.8k | 399 |
| GHFull/MessageClone/messages=100/kind=text | 35.3k | 35.3k | 701 |
| GHFull/MessageClone/messages=100/kind=toolmeta | 273.1k | 210.2k | 3.9k |
| GHFull/MessageClone/messages=1000/kind=text | 393.7k | 357.1k | 8.3k |
| GHFull/MessageClone/messages=1000/kind=toolmeta | 3.00M | 2.07M | 41.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024 | 60.0k | 57.7k | 333 |
| GHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024 | 519.2k | 522.4k | 2.5k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1 | 140.1k | 149.1k | 809 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8 | 141.8k | 149.8k | 816 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024 | 149.3k | 149.0k | 797 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536 | 4.91M | 6.84M | 810 |
| GHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024 | 68.0k | 66.6k | 401 |
| GHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024 | 563.1k | 534.1k | 2.7k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1 | 152.4k | 159.1k | 914 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8 | 153.8k | 159.9k | 921 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024 | 163.3k | 158.6k | 897 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536 | 4.88M | 7.12M | 911 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async | 1.01M | 1.47M | 3.2k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off | 41.9k | 40.6k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync | 1,009.7k | 1.47M | 3.2k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async | 9.70M | 17.11M | 3.2k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off | 47.7k | 100.6k | 537 |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync | 9.84M | 17.07M | 3.2k |
| GHFull/ReAct/model_calls=10/checkpoint=off | 1.95M | 844.9k | 12.2k |
| GHFull/ReAct/model_calls=100/checkpoint=off | 169.92M | 63.45M | 834.7k |
| GHFull/ReAct/model_calls=50/checkpoint=off | 42.26M | 15.70M | 219.4k |
| GHFull/ReAct/model_calls=50/history=off | 42.18M | 15.70M | 219.4k |
| GHFull/ReAct/model_calls=50/history=on | 42.53M | 15.88M | 219.4k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=async | 860.8k | 728.0k | 5.7k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=sync | 841.8k | 728.0k | 5.7k |
| GHFull/ResumeGraph/completed_result/checkpoint=async | 17.4k | 10.1k | 121 |
| GHFull/ResumeGraph/completed_result/checkpoint=sync | 15.1k | 10.1k | 121 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=async | 305.9k | 229.0k | 2.2k |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=sync | 299.1k | 228.9k | 2.2k |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=async | 118.6k | 103.9k | 919 |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=sync | 120.8k | 103.8k | 923 |
| GHFull/SequentialGraph/build_compile/nodes=10 | 59.3k | 53.2k | 803 |
| GHFull/SequentialGraph/build_compile/nodes=100 | 557.9k | 508.4k | 6.9k |
| GHFull/SequentialGraph/compile_only/nodes=100 | 474.2k | 398.0k | 5.3k |
| GHFull/SequentialGraph/nodes=1 | 4.3k | 4.8k | 66 |
| GHFull/SequentialGraph/nodes=10 | 23.9k | 22.7k | 343 |
| GHFull/SequentialGraph/nodes=100 | 350.3k | 395.9k | 3.2k |
| GHFull/SessionGrowth/requests=10 | 10.32M | 9.39M | 96.6k |
| GHFull/SessionGrowth/requests=100 | 988.62M | 832.57M | 8.61M |
| GHFull/ToolBatch/calls=1/turns=100/history=off | 172.8k | 204.3k | 3.7k |
| GHFull/ToolBatch/calls=1/turns=100/history=on | 180.9k | 236.3k | 3.7k |
| GHFull/ToolBatch/calls=32/turns=100/history=off | 506.8k | 511.9k | 7.5k |
| GHFull/ToolBatch/calls=32/turns=100/history=on | 688.5k | 1.50M | 7.6k |
| GHFull/ToolBatch/calls=8/turns=10/history=off | 105.6k | 98.1k | 1.3k |
| GHFull/ToolBatch/calls=8/turns=100/history=mixed | 269.4k | 401.2k | 4.6k |
| GHFull/ToolBatch/calls=8/turns=100/history=off | 247.9k | 273.2k | 4.6k |
| GHFull/ToolBatch/calls=8/turns=100/history=on | 279.8k | 529.2k | 4.6k |
| GHFull/ToolBatch/calls=8/turns=1000/history=off | 2.41M | 1.98M | 41.5k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=off | 244.9k | 273.2k | 4.6k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=on | 280.5k | 529.2k | 4.6k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=off | 262.0k | 299.7k | 4.8k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=on | 1.56M | 2.08M | 33.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=off | 277.1k | 299.7k | 4.8k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=on | 1.58M | 2.08M | 33.9k |
| MessageTransport/delta | 81.8k | 130.4k | 547 |
| MessageTransport/snapshot | 1.54M | 2.26M | 1.0k |

## 原始记录

- [raw/phase3/core-cpu1.txt](raw/phase3/core-cpu1.txt)：990 条原始样本，已随本文件提交。
- [raw/phase3/manifest.json](raw/phase3/manifest.json)：环境、命令与代码身份
  （`git_dirty=true`，工作区补丁见 manifest 引用的 `workdir.patch` 归档）。
- [raw/phase3/rerun/](raw/phase3/rerun/)：双侧复测原始输出（`pre-slim-*` 为基线
  worktree，`phase3-*` 为候选态）。
- `.bench/results/phase3/`：本地归档，不进 Git。
