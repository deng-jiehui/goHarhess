# 阶段六前基线：pre-stage6

2026-10-12 在工作区 `81e9168`（含第零波压测工具与基准用例改动，指纹与补丁随 manifest 归档）上，
按阶段六《实施顺序》第零波 W0-1 建立的 Full 核心基线与真实 Redis 组基线。
后续各波的性能对照以本文为准，比较与复测规则沿用 10% 阈值（与 `pre-slim` 相同口径、
相同机器与工具链）。W0-2 至 W0-5 的测量结果与 G0 决策摘要也记录在此。

## 环境

| 项 | 值 |
| --- | --- |
| 日期 | 2026-10-12 |
| 代码 | HEAD `81e9168250cb2876315eecedfcce7a2e2e705086`，工作区有压测相关改动（`workdir.patch`/`workdir-fingerprints.txt` 归档） |
| Go | go1.24.4 windows/amd64 |
| OS / 架构 | Windows / amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core |
| GOMAXPROCS | 1（`-cpu=1` 固定） |
| GOGC / GOMEMLIMIT / CGO | unset / unset / `CGO_ENABLED=1` |
| fixture / bench | `fixture-v2` / `bench-v2` |
| 采样范围 | Full 102 个场景 × 10 次 = 1020 条样本；Redis 组 12 场景 × 10 次 + `save_log_compare` 2 场景 × 10 次 |
| Redis | 7.2.7（WSL2 源码编译），`127.0.0.1:6379` |

## 运行命令

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/pre-stage6
$env:GOHARNESS_BENCH_REDIS_ADDR='127.0.0.1:6379'
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -Redis -OutputDirectory .bench/results/pre-stage6-redis
```

两组均由 `bench-validate.ps1` 逐场景核对采样完整、无 FAIL。原始输出在
`.bench/results/pre-stage6/` 与 `.bench/results/pre-stage6-redis/`。
注：WSL 内执行 Windows Go 时，`GOHARNESS_BENCH_REDIS_ADDR` 须经 `WSLENV` 传递。

## Full 结果摘要（cpu=1，10 次采样中位）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| GHFull/AgentPersist/model_calls=10/checkpoint=async | 4.81M | 5.21M | 22.2k |
| GHFull/AgentPersist/model_calls=10/checkpoint=off | 1.68M | 865.7k | 12.5k |
| GHFull/AgentPersist/model_calls=10/checkpoint=sync | 4.89M | 5.21M | 22.2k |
| GHFull/AgentPersist/model_calls=50/checkpoint=async | 107.67M | 120.12M | 418.9k |
| GHFull/AgentPersist/model_calls=50/checkpoint=off | 36.75M | 16.46M | 224.6k |
| GHFull/AgentPersist/model_calls=50/checkpoint=sync | 108.47M | 120.12M | 418.9k |
| GHFull/AgentStream/mode=events/text_bytes=16384 | 329.8k | 266.5k | 2.7k |
| GHFull/AgentStream/mode=events_lifecycle/text_bytes=16384 | 338.0k | 268.6k | 2.8k |
| GHFull/AgentStream/mode=invoke/text_bytes=16384 | 66.3k | 138.1k | 960 |
| GHFull/AgentStream/mode=stream/text_bytes=16384 | 66.6k | 138.1k | 960 |
| GHFull/CheckpointCodec/payload_bytes=4096/clone | 293 | 4.9k | 2 |
| GHFull/CheckpointCodec/payload_bytes=4096/decode | 28.3k | 30.0k | 27 |
| GHFull/CheckpointCodec/payload_bytes=4096/json | 19.3k | 10.1k | 8 |
| GHFull/CheckpointCodec/payload_bytes=524288/clone | 29.1k | 532.5k | 2 |
| GHFull/CheckpointCodec/payload_bytes=524288/decode | 3.05M | 3.69M | 38 |
| GHFull/CheckpointCodec/payload_bytes=524288/json | 2.26M | 1.07M | 9 |
| GHFull/CheckpointCodec/payload_bytes=65536/clone | 4.2k | 73.8k | 2 |
| GHFull/CheckpointCodec/payload_bytes=65536/decode | 399.1k | 482.4k | 31 |
| GHFull/CheckpointCodec/payload_bytes=65536/json | 289.5k | 147.9k | 8 |
| GHFull/ContextTrim/DropOld/turns=100/kind=cjk | 319.3k | 263.9k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=text | 939.0k | 268.0k | 3.4k |
| GHFull/ContextTrim/DropOld/turns=100/kind=tool | 1.67M | 811.6k | 10.9k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=text | 7.14M | 2.35M | 37.5k |
| GHFull/ContextTrim/DropOld/turns=1000/kind=tool | 14.80M | 7.49M | 110.8k |
| GHFull/ContextTrim/NoTrim/turns=10 | 129.8k | 29.1k | 379 |
| GHFull/ContextTrim/NoTrim/turns=100 | 1.20M | 284.4k | 3.4k |
| GHFull/ContextTrim/NoTrim/turns=1000 | 11.84M | 2.63M | 37.5k |
| GHFull/ContextTrim/OversizedLatest/input_bytes=65536 | 8.97M | 779.3k | 407 |
| GHFull/MemoryStore/get_oldest/history=100 | 285 | 296 | 6 |
| GHFull/MemoryStore/get_oldest/history=1000 | 291 | 296 | 6 |
| GHFull/MemoryStore/list_latest/history=100/limit=10 | 3.7k | 14.3k | 71 |
| GHFull/MemoryStore/list_latest/history=100/limit=100 | 30.4k | 126.9k | 617 |
| GHFull/MemoryStore/list_latest/history=1000/limit=10 | 4.5k | 14.3k | 71 |
| GHFull/MemoryStore/list_latest/history=1000/limit=100 | 34.3k | 126.9k | 617 |
| GHFull/MemoryStore/load_history=100 | 282 | 296 | 6 |
| GHFull/MemoryStore/save_empty | 10.1k | 11.2k | 15 |
| GHFull/MemoryStore/save_history=100 | 9.3k | 10.1k | 12 |
| GHFull/MessageClone/messages=10/kind=text | 2.8k | 3.6k | 71 |
| GHFull/MessageClone/messages=10/kind=toolmeta | 20.4k | 21.3k | 399 |
| GHFull/MessageClone/messages=100/kind=text | 28.5k | 36.2k | 701 |
| GHFull/MessageClone/messages=100/kind=toolmeta | 199.9k | 215.2k | 4.0k |
| GHFull/MessageClone/messages=1000/kind=text | 290.5k | 365.7k | 8.5k |
| GHFull/MessageClone/messages=1000/kind=toolmeta | 2.35M | 2.17M | 42.2k |
| GHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024 | 48.1k | 59.0k | 332 |
| GHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024 | 419.1k | 533.8k | 2.6k |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1 | 122.2k | 152.5k | 808 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8 | 122.4k | 153.3k | 815 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024 | 121.0k | 152.2k | 796 |
| GHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536 | 4.43M | 7.18M | 809 |
| GHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024 | 47.4k | 59.0k | 332 |
| GHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024 | 416.6k | 533.8k | 2.6k |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1 | 122.6k | 152.5k | 808 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8 | 123.2k | 153.3k | 815 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024 | 122.5k | 152.2k | 796 |
| GHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536 | 4.36M | 7.18M | 809 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async | 706.2k | 1.27M | 2.1k |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off | 35.8k | 41.4k | 536 |
| GHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync | 706.2k | 1.27M | 2.1k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async | 8.80M | 17.33M | 2.1k |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off | 39.2k | 102.9k | 536 |
| GHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync | 8.87M | 17.30M | 2.2k |
| GHFull/ReAct/model_calls=10/checkpoint=off | 1.70M | 865.5k | 12.5k |
| GHFull/ReAct/model_calls=100/checkpoint=off | 146.77M | 66.53M | 854.7k |
| GHFull/ReAct/model_calls=50/checkpoint=off | 36.26M | 16.46M | 224.6k |
| GHFull/ReAct/model_calls=50/history=off | 36.42M | 16.46M | 224.6k |
| GHFull/ReAct/model_calls=50/history=on | 36.42M | 16.65M | 224.7k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=async | 672.7k | 726.1k | 5.8k |
| GHFull/ResumeAgent/tool_suspend/checkpoint=sync | 681.1k | 726.1k | 5.8k |
| GHFull/ResumeGraph/completed_result/checkpoint=async | 6.7k | 5.9k | 77 |
| GHFull/ResumeGraph/completed_result/checkpoint=sync | 6.6k | 5.9k | 77 |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=async | 193.7k | 179.7k | 1.8k |
| GHFull/ResumeGraph/parallel_suspend/checkpoint=sync | 189.6k | 179.7k | 1.8k |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=async | 59.8k | 57.3k | 670 |
| GHFull/ResumeGraph/sequential_suspend/checkpoint=sync | 60.9k | 57.2k | 674 |
| GHFull/SequentialGraph/build_compile/nodes=10 | 35.6k | 40.1k | 615 |
| GHFull/SequentialGraph/build_compile/nodes=100 | 325.1k | 389.0k | 5.5k |
| GHFull/SequentialGraph/compile_only/nodes=100 | 256.0k | 279.3k | 3.9k |
| GHFull/SequentialGraph/nodes=1 | 3.4k | 4.8k | 65 |
| GHFull/SequentialGraph/nodes=10 | 18.7k | 23.2k | 342 |
| GHFull/SequentialGraph/nodes=100 | 278.7k | 404.5k | 3.2k |
| GHFull/SessionGrowth/requests=10 | 8.62M | 9.58M | 98.0k |
| GHFull/SessionGrowth/requests=100 | 856.52M | 870.33M | 9.01M |
| GHFull/StructuredOutput/construct | 51.5k | 44.7k | 772 |
| GHFull/StructuredOutput/decode/fields=3 | 3.2k | 3.8k | 76 |
| GHFull/StructuredOutput/decode/fields=9_children=16 | 78.4k | 64.4k | 1.7k |
| GHFull/ToolBatch/calls=1/turns=100/history=off | 122.4k | 209.2k | 3.8k |
| GHFull/ToolBatch/calls=1/turns=100/history=on | 123.4k | 242.0k | 3.8k |
| GHFull/ToolBatch/calls=32/turns=100/history=off | 394.9k | 524.2k | 7.7k |
| GHFull/ToolBatch/calls=32/turns=100/history=on | 509.2k | 1.57M | 7.8k |
| GHFull/ToolBatch/calls=8/turns=10/history=off | 77.6k | 100.4k | 1.4k |
| GHFull/ToolBatch/calls=8/turns=100/history=mixed | 200.9k | 410.9k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=100/history=off | 185.1k | 279.8k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=100/history=on | 208.0k | 542.0k | 4.7k |
| GHFull/ToolBatch/calls=8/turns=1000/history=off | 1.86M | 2.08M | 42.5k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=off | 182.6k | 279.8k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=0/history=on | 211.7k | 542.0k | 4.7k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=off | 201.3k | 306.9k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=1/history=on | 1.19M | 2.18M | 34.8k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=off | 200.9k | 306.9k | 4.9k |
| GHFull/ToolCallbacks/calls=8/callbacks=4/history=on | 1.20M | 2.18M | 34.8k |
| MessageTransport/delta | 72.1k | 133.6k | 547 |
| MessageTransport/snapshot | 1.22M | 2.37M | 1.0k |

## Redis 组结果摘要（cpu=1，10 次采样中位）

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| GHRedis/RedisGraphRun/nodes=16/checkpoint=async | 6.86M | 1.40M | 2.6k |
| GHRedis/RedisGraphRun/nodes=16/checkpoint=sync | 6.92M | 1.40M | 2.6k |
| GHRedis/RedisStore/list/history=100/limit=10 | 2.69M | 360.7k | 404 |
| GHRedis/RedisStore/list/history=100/limit=100 | 27.01M | 3.59M | 4.0k |
| GHRedis/RedisStore/list/history=1000/limit=10 | 2.74M | 360.7k | 404 |
| GHRedis/RedisStore/list/history=1000/limit=100 | 27.78M | 3.59M | 4.0k |
| GHRedis/RedisStore/load_payload_bytes=4096 | 272.2k | 35.2k | 38 |
| GHRedis/RedisStore/load_payload_bytes=524288 | 3.56M | 4.23M | 54 |
| GHRedis/RedisStore/load_payload_bytes=65536 | 623.1k | 556.5k | 43 |
| GHRedis/RedisStore/save_log_compare/history=100 | 649.4k | 107.5k | 1.4k |
| GHRedis/RedisStore/save_log_compare/history=1000 | 44.07M | 921.5k | 14.5k |
| GHRedis/RedisStore/save_payload_bytes=4096 | 326.5k | 16.2k | 39 |
| GHRedis/RedisStore/save_payload_bytes=524288 | 18.96M | 2.13M | 54 |
| GHRedis/RedisStore/save_payload_bytes=65536 | 43.19M | 222.8k | 40 |

## W0-2 压测工具

`extensions/examples/loadtest`：假模型（可配置延迟）、会话数、并发数、同会话争用比例、
工具数量、历史长度、`none|memory|redis` 存储、`async|sync` durability、Redis PING 探针、
`-cpuprofile`/`-memprofile`/`-pprofaddr`，JSON 结果输出。run 与 seed 两种模式。
会话争用行为已验证：同会话热请求恰一个成功，其余为 `ErrConflict`/`ErrUnfinishedRun`。

## W0-3 `RedisStore.logs` 内存增长（seed 模式实测）

| 会话数 | 历史 | 留存/会话 |
| ---: | ---: | ---: |
| 2000 | 50 | 7.4KB |
| 2000 | 100 | 10.8KB |
| 10000 | 100 | 7.5KB |
| 2000 | 500 | 36.5KB |
| 500 | 1000 | 73.1KB |

结论：留存随历史长度近似线性，约 **56B/条 + 2KB 固定**（sha1 摘要与视图结构）。
支持 E1（LRU 上限）；`LogCacheSize` 初始值 1024 会话可接受（约 10–40MB），维持。

## W0-4 Redis 保存脚本剖析

- 服务端 evalsha（commandstats 均值）：**0.86ms@历史200、3.75ms@历史1000**，约 3.7µs/条逐条 `sha1hex` 比对 + 约 1ms 固定（两次 `cjson.decode` 记录、`order`/`epochs` 编解码）。SLOWLOG 证实 evalsha 条目为毫秒级。
- 客户端 `Store.Save` 全程（`save_log_compare` 基准则为）：**0.53ms@历史100、44ms@历史1000**——客户端侧 O(历史) 成本（PersistClone、全量 marshal/sha1/hex、参数装配）才是主要项，约 44µs/条。
- 阻塞证据：历史 1000、并发 8 下 evalsha 合计占 Redis 事件循环约 93%，PING 探针 p50 8–11ms、p99 27–31ms；历史 200 下 PING p50≈0、p99 6–8ms。

## W0-5 七项核查

1. `observe.Slog`：**不带** trace/span ID（只用 SpanID 算缩进）→ 并入 W1-1/W2-1。
2. `httpclient.Pool`：MaxIdleConns=100、MaxIdleConnsPerHost=20、总连接不限；并发 8–32 无瓶颈证据，维持。
3. `control.RequestInterrupt`：在途运行可恢复中断**成立**（有检查点时；无存储返回 `ErrCheckpointRequired`）；`-race` 下中断/恢复测试通过。
4. pprof 三项（memory Store、并发 8、历史 100、2000 请求）：State·消息克隆 cum 40.9%（→W2-11）、`snapshotRuntimeState` 10.2%（→W2-12）、stream publish 1.2%（占比小，总体方案第二节标为关闭）。
5. 事件 Agent 名称：**不带**（run 事件 `Name` 是配方名 react/basic）→ 并入 W1-1/W2-1 的 `agent` 标签。
6. 子 Agent 工具：受运行时默认 30s、5 分钟硬上限约束；自声明超时只能向小覆盖。
7. 公共事件流：子 Agent 事件**透传**（常开、无开关，与设计期望的默认关不同 → W2-9 决定）。

## G0 决策

- **E2：做 a+b**（去两次记录 decode + 命中视图时跳逐条比对）；c 暂不做。客户端 O(历史) 成本归 W2-11。
- `LogCacheSize` 初始值 1024、`MaxWorkers` 硬上限 4096：**维持**。
- **W4-7 `exit` 持久化**：维持条件项暂不实施——历史≤200 保存不构成瓶颈；历史≥500 的写放大由 E2 先处理。
- `Drain` 可行性成立，以检查点配置为前置。
