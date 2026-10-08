# 流式性能基线：baseline-v3-stream

2026-10-04 按 2.11 第三轮复核要求补采四个 `AgentStream` 场景。这次使用本机缓存中的 Go 1.24.4，沿用原来的单核、`1s`、10 次采样参数，替换此前没有对应原始记录的四行流式摘要。其他场景没有重测，继续在[总记录](baseline.md)中注明为 baseline-v2 的历史参考。

## 环境与工作量

| 项目 | 本次取值 |
| --- | --- |
| 日期 | 2026-10-04 |
| Go | go1.24.4 windows/amd64，`GOTOOLCHAIN=local` |
| 系统 | Windows，版本 10.0.26100.0，amd64 |
| CPU | AMD Ryzen 7 9700X 8-Core Processor |
| GOMAXPROCS | 1，由 `-cpu=1` 固定 |
| GOGC / GOMEMLIMIT / CGO | unset / unset / `CGO_ENABLED=1` |
| 代码 | HEAD `0d4b4fea716c1b68ea932eae5606b268a31fc957` + 采样前归档的工作区改动 |
| fixture / bench | `fixture-v2` / `bench-v2`，具体流式计时修正保存在源码快照中 |
| 采样范围 | Full 中的四个 AgentStream 场景，每个 10 次，共 40 条 |
| 输入 | 16 KiB 文本，256 次文本增量；四种模式均得到相同最终正文 |

模型替身不再做未使用的全文拼接，公共流预检查在计时外运行。正式循环只测执行和完整消费，Agent 构造也在计时外。每次预检查核对消息起止、文本增量数、重建正文以及恰好一次最终结果。

## 本次结果

下表为原始十次采样的中位数，偶数个样本取中间两项的平均值。

| 模式 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| invoke | 81,474 | 149,058 | 1,139 |
| stream | 82,197.5 | 149,058 | 1,139 |
| events | 282,519 | 254,865 | 2,769 |
| events_lifecycle | 289,659 | 256,909 | 2,827 |

原来的 77k / 79k / 324k / 292k ns/op 四行不再作为当前摘要，统一以本次可追溯的采样为准。修复前的流式测量包含替身拼接和预检查成本，不能用两次耗时的差值直接判断框架提速比例。

## 原始记录与复现

本次归档位于 `.bench/results/baseline-v3-stream/`：

- [agent-stream-cpu1.txt](../../.bench/results/baseline-v3-stream/agent-stream-cpu1.txt)：四个场景的全部 40 条原始样本。
- [manifest.json](../../.bench/results/baseline-v3-stream/manifest.json)：实际命令、工具链路径、环境和代码身份。
- [summary.json](../../.bench/results/baseline-v3-stream/summary.json)：逐场景样本数和精确中位数。
- `workdir.patch`、`workdir-src/`、`workdir-fingerprints.txt`：Git 直接输出的补丁、未跟踪源码及 SHA256 指纹；`workdir-status.txt` 保留当时状态。
- `collect.ps1`：本次采样与归档流程；`verification.json`、`restore-tests.txt`、`restore-stream-check.txt` 保存恢复验证结果，`checksums.sha256` 用于核对归档文件。

这些是本地结果产物，`.bench/results/` 已被 Git 忽略。采样源码在运行前保存，后续更新报告不会改动这份源码快照。CPU 名取自原始输出，系统信息取自本机 OSVersion，取值来源在 manifest 中说明。

实际 Go 参数如下。执行前确认 `go version` 为 `go1.24.4 windows/amd64`；本次通过 manifest 中记录的缓存工具链路径调用，并设置 `GOTOOLCHAIN=local`。

```powershell
go test -p=1 ./pkg/agent -run '^$' -bench '^BenchmarkGHFull$/^AgentStream$' -benchmem -benchtime=1s -count=10 -cpu=1
```

恢复验证已完成：按 manifest 的 HEAD 在独立临时目录解出源码，检查并应用 `workdir.patch`，再覆盖 `workdir-src/`。100 个归档源码文件与指纹一致。恢复后的 `go test -p=1 ./internal/benchfixture ./pkg/agent -count=1` 通过，四种 Full 流式模式及 Stress 的 64 KiB 流也以固定 `1x` 通过检查。短测没有混入上表的正式数据，临时恢复目录已清理。

后续优化应保留新的前后采样与对应源码归档，使用同一工具链、输入和计时范围比较。本次不是新的六包 Full 全量采样，也不包含真实 Redis 性能数据。
