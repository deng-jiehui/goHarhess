# 轻量化前基线：pre-slim

2026-10-08 在 `pre-slim` tag（`2335b1a`）上按《轻量化改良方案》阶段零第 7 项重跑的 Full 基线。
取代 `baseline.md` 中"HEAD `0d4b4fea` 加工作区改动"的旧记录——该记录早于执行树、受管任务、
会话子 Agent 等后续改动，不代表轻量化之前的状态。后续阶段的性能对照以本文为准，
退化阈值按方案待决策 5 取 10%。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-08 |
| 代码 | `pre-slim` tag = `2335b1a900aaa758acc30df35f755e3bde123f7b`，工作区干净 |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| GOGC / GOMEMLIMIT / CGO | unset / unset / `CGO_ENABLED=1` |
| fixture / bench | `fixture-v2` / `bench-v2`（`internal/benchfixture`） |
| 采样范围 | Full 99 个场景，每个 10 次，共 990 条样本 |

## 运行命令

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/pre-slim
```

`bench-validate.ps1` 已逐场景核对 99 × 10 采样完整、无 FAIL。

## 结果摘要（cpu=1，10 次采样中位）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| MessageTransport/delta | 58.2k | 133.6k | 547 |
| MessageTransport/snapshot | 1.07M | 2.37M | 1.0k |
| GHFull/MessageClone/messages=10/kind=text | 2.1k | 3.6k | 71 |
| GHFull/MessageClone/messages=10/kind=toolmeta | 25.9k | 21.3k | 399 |
| GHFull/MessageClone/messages=100/kind=text | 35.5k | 36.2k | 701 |
| GHFull/MessageClone/messages=100/kind=toolmeta | 274.4k | 215.2k | 4.0k |
| GHFull/MessageClone/messages=1000/kind=text | 417.1k | 365.7k | 8.5k |
| GHFull/MessageClone/messages=1000/kind=toolmeta | 3.20M | 2.17M | 42.2k |
| GHFull/ContextTrim/NoTrim/turns=10 | 168.0k | 29.1k | 379 |
| GHFull/ContextTrim/NoTrim/turns=100 | 1.54M | 284.4k | 3.4k |
| GHFull/ContextTrim/NoTrim/turns=1000 | 15.34M | 2.63M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=100/kind=text | 1.21M | 268.0k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=tool | 2.13M | 811.6k | 10.9k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=text | 9.31M | 2.35M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=tool | 20.33M | 7.49M | 110.8k |
| GHFull/ContextTrim/DropOld/turns=100/kind=cjk | 428.8k | 263.9k | 3.4k |
| GHFull/ContextTrim/OversizedLatest/input_bytes=65536 | 11.36M | 779.3k | 407 |
| GHFull/ToolBatch/calls=1/turns=100/history=off | 176.0k | 211.2k | 3.9k |
| GHFull/ToolBatch/calls=1/turns=100/history=on | 179.1k | 244.0k | 3.9k |
| GHFull/ToolBatch/calls=8/turns=100/history=off | 267.6k | 295.9k | 4.9k |
| GHFull/ToolBatch/calls=8/turns=100/history=on | 311.5k | 558.1k | 4.9k |
| GHFull/ToolBatch/calls=32/turns=100/history=off | 429.2k | 588.7k | 8.3k |
| GHFull/ToolBatch/calls=32/turns=100/history=on | 742.2k | 1.64M | 8.4k |
| GHFull/ToolBatch/calls=8/turns=100/history=mixed | 283.2k | 427.0k | 4.9k |
| GHFull/ToolBatch/calls=8/turns=10/history=off | 115.9k | 116.5k | 1.5k |
| GHFull/ToolBatch/calls=8/turns=1000/history=off | 2.51M | 2.09M | 42.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=off | 266.5k | 299.0k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=on | 1.58M | 2.17M | 34.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=off | 288.0k | 337.6k | 5.3k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=on | 4.36M | 5.42M | 94.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=off | 358.3k | 453.4k | 6.3k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=on | 13.81M | 15.18M | 274.6k |
| GHFull/SequentialGraph/nodes=1 | 4.7k | 5.4k | 72 |
| GHFull/SequentialGraph/nodes=10 | 30.3k | 25.8k | 367 |
| GHFull/SequentialGraph/nodes=100 | 440.5k | 428.1k | 3.4k |
| GHFull/SequentialGraph/build_compile/nodes=10 | 69.6k | 54.4k | 802 |
| GHFull/SequentialGraph/build_compile/nodes=100 | 625.9k | 520.5k | 7.1k |
| GHFull/SequentialGraph/compile_only/nodes=100 | 472.4k | 407.5k | 5.5k |
| GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024 | 45.6k | 59.5k | 345 |
| GHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024 | 71.5k | 93.8k | 497 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024 | 125.2k | 159.3k | 1.0k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024 | 198.5k | 262.6k | 1.3k |
| GHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024 | 582.7k | 708.2k | 5.8k |
| GHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024 | 714.2k | 946.3k | 4.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536 | 3.98M | 7.18M | 1.0k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536 | 7.41M | 13.25M | 1.3k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1 | 159.0k | 159.5k | 1.0k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1 | 249.7k | 263.1k | 1.3k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8 | 161.1k | 160.3k | 1.0k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8 | 242.3k | 263.9k | 1.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off | 45.0k | 45.5k | 573 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync | 960.7k | 1.54M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async | 971.1k | 1.54M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off | 51.0k | 106.9k | 573 |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync | 9.66M | 17.90M | 3.3k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async | 9.38M | 17.93M | 3.3k |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=sync | 115.6k | 107.4k | 930 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=sync | 273.0k | 227.5k | 2.2k |
| GHFull/ResumeGraph/completed_result/checkpoint=sync | 16.4k | 10.4k | 121 |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=async | 115.0k | 107.5k | 926 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=async | 276.8k | 227.6k | 2.2k |
| GHFull/ResumeGraph/completed_result/checkpoint=async | 16.2k | 10.4k | 121 |
| GHFull/ReAct/model_calls=10/checkpoint=off | 2.03M | 1.02M | 14.3k |
| GHFull/ReAct/model_calls=50/checkpoint=off | 43.68M | 19.33M | 261.8k |
| GHFull/ReAct/model_calls=100/checkpoint=off | 173.50M | 77.46M | 999.1k |
| GHFull/ReAct/model_calls=50/history=off | 42.86M | 19.33M | 261.8k |
| GHFull/ReAct/model_calls=50/history=on | 43.61M | 19.52M | 261.9k |
| GHFull/AgentPersist/model_calls=10/checkpoint=off | 2.06M | 1.02M | 14.3k |
| GHFull/AgentPersist/model_calls=10/checkpoint=sync | 5.95M | 5.52M | 24.4k |
| GHFull/AgentPersist/model_calls=10/checkpoint=async | 6.04M | 5.52M | 24.4k |
| GHFull/AgentPersist/model_calls=50/checkpoint=off | 53.75M | 19.33M | 261.8k |
| GHFull/AgentPersist/model_calls=50/checkpoint=sync | 147.68M | 123.50M | 458.1k |
| GHFull/AgentPersist/model_calls=50/checkpoint=async | 150.92M | 123.50M | 458.1k |
| GHFull/SessionGrowth/requests=10 | 12.50M | 10.01M | 100.2k |
| GHFull/SessionGrowth/requests=100 | 1.19s | 875.00M | 9.04M |
| GHFull/ResumeAgent/tool_suspend/checkpoint=sync | 1.02M | 765.9k | 6.1k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=async | 1.01M | 765.9k | 6.1k |
| GHFull/AgentStream/mode=invoke/text_bytes=16384 | 102.3k | 142.4k | 995 |
| GHFull/AgentStream/mode=stream/text_bytes=16384 | 103.2k | 142.4k | 995 |
| GHFull/AgentStream/mode=events/text_bytes=16384 | 413.9k | 248.2k | 2.6k |
| GHFull/AgentStream/mode=events_lifecycle/text_bytes=16384 | 431.2k | 250.2k | 2.7k |
| GHFull/CheckpointCodec/payload_bytes=4096/clone | 316.9 | 4.9k | 2 |
| GHFull/CheckpointCodec/payload_bytes=4096/json | 23.3k | 10.2k | 8 |
| GHFull/CheckpointCodec/payload_bytes=4096/decode | 30.8k | 30.0k | 27 |
| GHFull/CheckpointCodec/payload_bytes=65536/clone | 4.6k | 73.8k | 2 |
| GHFull/CheckpointCodec/payload_bytes=65536/json | 346.8k | 147.9k | 8 |
| GHFull/CheckpointCodec/payload_bytes=65536/decode | 435.7k | 482.4k | 31 |
| GHFull/CheckpointCodec/payload_bytes=524288/clone | 33.2k | 532.5k | 2 |
| GHFull/CheckpointCodec/payload_bytes=524288/json | 2.71M | 1.07M | 9 |
| GHFull/CheckpointCodec/payload_bytes=524288/decode | 3.26M | 3.69M | 38 |
| GHFull/MemoryStore/save_empty | 11.3k | 11.2k | 15 |
| GHFull/MemoryStore/save_history=100 | 10.6k | 10.1k | 12 |
| GHFull/MemoryStore/load_history=100 | 350.7 | 296 | 6 |
| GHFull/MemoryStore/get_oldest/history=100 | 385.0 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=100/limit=10 | 4.5k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=100/limit=100 | 38.3k | 143.3k | 617 |
| GHFull/MemoryStore/get_oldest/history=1000 | 415.7 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=1000/limit=10 | 5.4k | 15.6k | 71 |
| GHFull/MemoryStore/list_latest/history=1000/limit=100 | 54.0k | 143.3k | 617 |

## 原始记录

- [raw/pre-slim/core-cpu1.txt](raw/pre-slim/core-cpu1.txt)：990 条原始样本，已随本文件提交。
- [raw/pre-slim/manifest.json](raw/pre-slim/manifest.json)：环境、命令与代码身份（`git_dirty=false`）。
- `.bench/results/pre-slim/`：同一组文件的本地归档位置，不进 Git。

真实 Redis 组（`GOHARNESS_BENCH_REDIS_ADDR` + `-Redis`）本次未测，与旧基线口径一致。
