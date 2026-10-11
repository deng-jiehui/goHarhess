# 结构化输出基准：4.4 实现（pre-slim 对照）

验收日期：2026-10-11。代码：`0eece88`。

Full 原始记录包含 102 个场景，每个十次，共 1020 条样本；其中 99 个旧场景与固定 pre-slim 基线逐项比较，3 个新增 StructuredOutput 场景作为独立成本记录。校验器确认场景完整、采样次数齐全，manifest 记录 clean 的 `0eece88`。

首轮对照有 4 个旧场景耗时超过 10% 门槛，已按方案做同条件双侧各十次复测。复测期间机器自身出现可测漂移（同一 pre-slim 源码两轮基线中位数差 1.9%~9.1%），两个场景回到门槛内或可解释区间，`CheckpointCodec/4096/clone` 与 `MemoryStore/list_latest` 的残留差异保留在下表，不做超出门槛依据的结论。

## 环境与采样

| 项目 | 条件 |
| --- | --- |
| Full 候选提交 | `0eece8843c1ae7c3cd779c01ffda8ceed25229f3` |
| 固定基线 | `pre-slim`：`2335b1a900aaa758acc30df35f755e3bde123f7b` |
| 工具链 | Go 1.24.4，Windows/amd64 |
| 机器 | AMD Ryzen 7 9700X 8-Core |
| 参数 | Full、Count=10、Benchtime=1s、Cpu=1 |
| 运行时 | CGO=1，GOGC、GOMEMLIMIT 均未设置 |
| 场景与 fixture | bench-v2、fixture-v2 |

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/structured-full
```

原始数据归档 `raw/structured-output/`，双侧复测原始数据归档 `raw/structured-output/rerun/`。复测基线从 `pre-slim` 标签 `git archive` 提取到 `.bench/pre-slim-source`，两侧在同一环境顺序运行。

## 新场景独立成本

三个新场景没有基线，单列为本次新增能力的成本记录，不与旧数字比较：

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `StructuredOutput/construct` | 42,304 | 29,029 | 588 |
| `StructuredOutput/decode/fields=3` | 4,300 | 3,816 | 75 |
| `StructuredOutput/decode/fields=9_children=16` | 99,538 | 64,395 | 1,741 |

构造成本含 Schema 推导、严格模式规范化与 Resolve 编译，只在组件构造期支付一次；解码行覆盖小结构体（3 字段）与嵌套大结构体（9 字段、16 子对象）的校验加类型化解码路径。

## 首轮超标项与双侧复测

四个旧场景的 ns/op 首轮超过 10% 门槛，B/op 与 allocs/op 全部持平：

| 场景 | 冻结基线 | 本轮 Full | base 复测 | cand 复测 | 复测差异 | base 二次复测 | 基线侧漂移 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `MessageClone/10/text` | 2,059 | 3,038 | 2,611 | 2,919 | +11.8% | 2,848 | +9.1% |
| `CheckpointCodec/4096/clone` | 317 | 370 | 295 | 436 | +47.7% | 310 | +4.9% |
| `CheckpointCodec/65536/json` | 346,773 | 382,999 | 316,941 | 372,774 | +17.6% | 339,688 | +7.2% |
| `MemoryStore/list_latest/100/10` | 4,478 | 5,465 | 4,738 | 5,969 | +26.0% | 4,827 | +1.9% |

复测命令（两侧相同，pre-slim 侧在 `git archive` 提取目录执行）：

```powershell
go test -p=1 ./pkg/schema -run '^$' -bench '^BenchmarkGHFull$/^MessageClone$/^messages=10$/^kind=text$' -benchmem -benchtime=1s -count=10 -cpu=1
go test -p=1 ./pkg/state -run '^$' -bench '^BenchmarkGHFull$/^(CheckpointCodec|MemoryStore)$' -benchmem -benchtime=1s -count=10 -cpu=1
```

判读依据：

- 四个场景都在 `pkg/schema`/`pkg/state`。本次改动（`4688565`、`0eece88`）只触及 `pkg/llm`、`pkg/agent` 测试、文档与示例，`git diff ac81986..HEAD` 在这两个包内只有新增测试文件；自阶段四验收提交 `ac81986` 以来这些路径的源码逐字节相同。
- `pre-slim..HEAD` 对 `pkg/state`/`pkg/schema` 的全部差异是删除 70 行死代码，不涉及克隆、编解码或列表读取热路径。
- 全部超标项的 B/op 与 allocs/op 与基线完全一致（`MemoryStore` B/op 反而 -8.3%），说明两侧执行的工作量相同，差异只在耗时。
- 同一份 pre-slim 源码的两次基线复测自身漂移 1.9%~9.1%；`MessageClone` 的 cand 值（2,919）落在二次基线（2,848）的噪声范围内，`65536/json` 相对二次基线差 9.7%。
- `4096/clone`（300ns 量级）与 `list_latest`（4.7µs 量级）的 cand/base 残留差异（约 +40%/+24%）未被漂移完全覆盖。两个场景在阶段四采样时均快于基线（234ns、3,962ns），同一份代码在两轮之间没有变化；该量级微基准对代码布局与机器状态高度敏感，现有证据支持测量噪声而非本批改动引入的回归，如实记录残留数字供复核。

## 修复后复测（2026-10-11，`634f0c3`）

审核要求修复后按相同条件重新双侧采样。本轮对 `pkg/schema`、`pkg/state` 的全部 24 个 GHFull 场景做两次成对采样：第二轮为包级顺序采样（基线侧 `run.log` 完成后再跑候选侧），第三轮改为逐场景交错采样（每个场景基线、候选相邻执行，消除长时间漂移偏差）。机器采样时基本空闲（总 CPU ~2%），两侧均为 Go 1.24.4、`-benchtime=1s -count=10 -cpu=1`。原始数据见 `raw/structured-output/rerun2/` 与 `raw/structured-output/rerun3/`。

交错采样的中位数对比（全部 24 场景）：

- 首轮四个超标项全部回落到门槛内：`MessageClone/10/text` +1.3%、`CheckpointCodec/4096/clone` +5.5%、`CheckpointCodec/65536/json` +9.3%、`MemoryStore/list_latest/100/10` +0.7%。
- 20 个场景在 ±10% 内；`MemoryStore/list_latest/1000/limit=100` 反而 -21.9%。
- 仍有 4 个场景耗时超阈：`CheckpointCodec` 三个负载档位的 `decode`（+12.1%/+13.5%/+13.8%）与 `MessageClone/messages=1000/kind=text`（+10.1%），B/op 与 allocs/op 全部持平。

残留项取证：`DecodeCheckpoint` 在两侧逐字节相同，`benchfixture` 负载生成未变，`Checkpoint` 结构体只少一个 `SubAgents` 字段，工作量严格不增。对 `524288/decode` 各采 3 秒 CPU profile：两侧前四帧同为 `encoding/json` 扫描器（`stateInString`/`readValue`/`checkValid`/`skip`，合计约 90%），goHarness 自有函数零自耗时；差异集中在 `Validate → json.Valid` 的 stdlib 扫描帧（1.17s vs 1.02s），即同一函数对字节相同输入的耗时差——属于每个二进制固定的链接布局/对齐效应，不是语义回归。`MessageClone` 场景代码自 `pre-slim` 以来仅有删除未用类型的 26 行差异，分配指标持平。

## 结论

102 个场景覆盖校验通过，新增能力成本已单独记录。功能修复后重新双侧采样：首轮超标项全部达标；`CheckpointCodec/decode` 与 `MessageClone/1000/text` 仍有 10%~14% 的 ns/op 差异，剖面证明为同一代码、同一输入、同一 stdlib 热循环上的二进制布局效应，与本次改动无因果路径。按约定门槛如实保留残留记录，性能项维持待验收，供独立复核裁定。若仍要求门槛内，建议在确认安静的环境重新提取基线整轮对跑，而不是叠加局部复测。

## 数据模型接入前后对比（2026-10-11，通用数据模型实施方案）

改造前基线在 `8cde546` 采集，改造后在 `ea19dfe`（+文档改动）采集，条件相同（Go 1.24.4、`-benchtime=1s -count=10 -cpu=1`、`.bench/go-cache`）。原始数据见 `.bench/results/datamodel/`（`llm-before/after`、`tool-before/after`、`model-after`、`rerun-*`）。

首轮中位数对比超阈 5 项，按约定做同条件成对复测（worktree 双测试二进制、逐场景交错、各侧 n≥10）：

| 场景 | 首轮 | 成对复测 | 结论 |
| --- | --- | --- | --- |
| `StructuredOutput/construct` | +77.2% | **+50.4%** | 真实回归：构造多一次 Schema 重解析（冻结隔离）与公共类型检查；一次性成本 |
| `StructuredOutput/decode/fields=3` | +15.3% | **+13.8%** | 真实回归：3.4µs 微场景上约 +460ns 固定开销；fields=9+children=16 同轮仅 +1.7% |
| `ToolBatch/calls=1/turns=100/history=on` | +16.5% | −2.8% | 漂移 |
| `ToolBatch/calls=32/turns=100/history=on` | +22.6% | −0.6% | 漂移 |
| `ToolBatch/calls=8/turns=10/history=off` | +10.2% | +0.1% | 漂移 |

新增 `BenchmarkModelConstruct`（~112µs，一次构造）与 `BenchmarkModelDecode/{small,nested}`（~2.9µs/~31µs）。

按约定门槛如实保留：`construct` 与 `decode/fields=3` 两项 ns/op 超阈未达标，性能项维持待验收；工具侧运行期无回归（B/op、allocs/op 全部持平）。
