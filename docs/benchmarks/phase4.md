# 阶段四基准：引擎收敛（pre-slim 对照）

验收日期：2026-10-10。代码：`ac81986`。**性能验收通过。**

Full 原始记录包含 99 个场景，每个十次，共 990 条样本。与固定 pre-slim 基线逐项比较，分配字节数和分配次数没有超标；只有十条文本消息克隆的耗时首轮增加 22.1%，按方案做同条件双侧各十次复测后，差异为 -0.6%，三个指标均满足既定门槛。

本次使用已经生成的当前提交 Full 记录，独立核验场景完整性、环境与原始中位数，并现场补做超标项复测；没有重复运行完整 Full。代码与功能修复复核的源码一致。

## 环境与采样

| 项目 | 条件 |
| --- | --- |
| Full 候选提交 | `ac819864e8e323e562307679ed5ee2409acdbfc7` |
| 固定基线 | `pre-slim`：`2335b1a900aaa758acc30df35f755e3bde123f7b` |
| 工具链 | Go 1.24.4，Windows/amd64 |
| 机器 | AMD Ryzen 7 9700X 8-Core |
| 参数 | Full、Count=10、Benchtime=1s、Cpu=1 |
| 运行时 | CGO=1，GOGC、GOMEMLIMIT 均未设置 |
| 场景与 fixture | bench-v2、fixture-v2 |

```powershell
./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 -OutputDirectory .bench/results/phase4-full
```

两版 Full 的环境字段一致；候选 manifest 记录 clean 的 `ac81986`。本轮未改变采样代码。复测基线从 pre-slim 标签重新提取，254 个源码、测试及模块文件与该标签逐一核对一致；两侧顺序运行。

## 比较口径

93 个场景保留原来的工作量与计时范围，按三个指标分别判定。六个 `entry=parallelnode` 场景对应已删除的 AddParallelNode，当前入口改用 Fork/Join；它们单列为替代实现的成本参照，不能解释为旧接口仍然存在。这六项也没有超标；Fork/Join 自身原有场景继续参与正式回归判断。

Agent 基准相对 pre-slim 的构造入口变化来自阶段二 API 适配，任务、模型与计时范围相同。新增的子 Agent 扩展使用单独场景记录成本，不拿新数字与语义不同的旧受管任务作性能比较。

## 首轮超标项与双侧复测

| 指标 | 冻结基线 Full | 阶段四 Full | 基线复测 | 候选复测 | 复测变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| ns/op | 2,058.5 | 2,513.5 | 2,073.0 | 2,061.5 | -0.55% |
| B/op | 3,568.0 | 3,568.0 | 3,568.0 | 3,568.0 | +0.00% |
| allocs/op | 71.0 | 71.0 | 71.0 | 71.0 | +0.00% |

场景：`MessageClone/messages=10/kind=text`。复测命令在 pre-slim 源码目录和当前仓库根目录分别执行：

```powershell
go test -p=1 ./pkg/schema -run '^$' -bench '^BenchmarkGHFull$/^MessageClone$/^messages=10$/^kind=text$' -benchmem -benchtime=1s -count=10 -cpu=1
```

两侧十次中位数回到门槛内，且分配指标相同，因此按既定复测规则通过。完整原始数据和计算结果保留，首轮的超标数字没有删去。

## 子 Agent 扩展的独立成本

使用本地确定性模型：父 ReAct 通过一个工具执行一次子 Agent，子模型真实调用一次，返回后父模型结束。构建在计时外预热；结果解析与检查计入运行成本。会话场景每次创建独立会话，包含父图、子图检查点和子 Agent 会话记录的保存；一次性场景关闭检查点。两者职责不同，表格用于说明各自成本，不据此判断一个实现比另一个好。

| 场景 | ns/op 中位数 | B/op 中位数 | allocs/op 中位数 |
| --- | ---: | ---: | ---: |
| ephemeral | 139,067.0 | 127,875.0 | 2,061.0 |
| session_spawn | 536,162.0 | 347,965.5 | 3,798.0 |

两组均十次、Benchtime=1s、Cpu=1，每次均核对一个真实子模型调用和正确最终结果。数字不包含远程模型服务延迟，也不代表所有业务图的耗时。

## 原始记录

- [Full 990 条样本](raw/phase4/core-cpu1.txt)与[环境清单](raw/phase4/manifest.json)。
- [逐项中位数与判定](raw/phase4/comparison.json)及[环境、代码核对](raw/phase4/environment-check.json)。
- [双侧复测](raw/phase4/rerun/)及其中的环境记录。
- [子 Agent 成本样本](raw/phase4/subagent-cost.txt)、[环境与场景说明](raw/phase4/subagent-cost-manifest.json)、[独立基准源码](raw/phase4/subagent-benchmark.go.txt)。
- 早期的[二十四项图侧子集](raw/phase4/graph-subset.txt)保留为历史记录；阶段四本次结论以 Full 和双侧复测为准。
- [最终验收记录](../../.bench/results/review-slim-stage4-final/verification.md)。
