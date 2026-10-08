# 2.3 上下文裁剪优化复核

2026-10-04 独立复核通过。逐轮扣减替代了循环内的消息展开、深拷贝和全量计数，裁剪结果及输入隔离保持一致。本记录保存审核补查与性能短测，不替代六包 Full 的完整回归采样。

## 正确性与计数工作量

从 `.bench/results/baseline-v3-stream/` 的源码归档恢复改造前实现，与新实现对照了 800 组输入。覆盖空历史、固定前缀、普通和工具历史、中文与 emoji、超长最新输入、推理块、媒体、并行工具调用，以及严格／修复两种角色策略、角色合并开关和两种文本计数器。输出消息、全部统计字段及错误一致。

计数器调用次数：100 轮工具历史为 2,418 次，1,000 轮为 24,018 次，约增加 9.9 倍。这个检查直接确认计数工作量随历史长度接近线性。另行检查 16 次并发调用和修改结果中的正文、嵌套元数据：输入、其他调用和再次调用的结果均未受影响。

根模块与 extensions 的全量 `go test ./... -count=1 -timeout=120s`、`go vet ./...` 通过；ContextEngine、Schema、Agent 的竞态检测通过。常规及竞态检查使用 Go 1.26.4，临时补查测试和恢复目录已清理。

## 本轮性能短测

使用原 Go 1.24.4、`fixture-v2`、`bench-v2`，执行 `-benchtime=1s -count=3 -cpu=1`。九个场景各三次，共 27 条结果；下表为三次采样的中位数。

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| NoTrim/turns=10 | 179,906 | 39,538 | 551 |
| NoTrim/turns=100 | 1,664,335 | 374,407 | 4,874 |
| NoTrim/turns=1000 | 17,143,657 | 3,514,965 | 55,062 |
| DropOld/turns=100/kind=text | 1,389,493 | 321,828 | 4,174 |
| DropOld/turns=100/kind=tool | 2,401,354 | 903,880 | 11,953 |
| DropOld/turns=1000/kind=text | 11,411,712 | 2,537,159 | 38,268 |
| DropOld/turns=1000/kind=tool | 22,311,123 | 7,846,617 | 111,863 |
| DropOld/turns=100/kind=cjk | 396,345 | 308,958 | 4,006 |
| OversizedLatest/input_bytes=65536 | 8,987,830 | 782,498 | 439 |

1,000 轮工具历史的裁剪约为 22.3ms、7.85MB/op；100 轮约为 2.40ms，十倍历史量对应约 9.3 倍耗时。结合计数器补查，确认原来的重复全量扫描已消除。旧历史基线的同名场景约为 5.04s、1.11GB/op，足以说明改善量级；本轮没有通过 benchstat 判定统计显著性，不用两组采样推断精确提速比例。

作者此前十次自测的 28.1ms 与本轮三次复测的 22.3ms 分别保留在各自记录中，不合并为同一批样本。剩余的线性消息克隆成本继续在 2.1 中处理。

原始输出为 [after-context-cpu1.txt](../../.bench/results/review-23/after-context-cpu1.txt)，命令和源码指纹在 [after-manifest.json](../../.bench/results/review-23/after-manifest.json)，精确摘要在 [after-summary.json](../../.bench/results/review-23/after-summary.json)，审核结果在 `review.json`。`after-src/` 保留本次引擎、基准和 fixture 源码核对副本；本地结果目录已被 Git 忽略。

本次实际命令如下，工具链路径记录在 manifest 中，并使用 `GOTOOLCHAIN=local`：

```powershell
go test -p=1 ./pkg/contextengine -run '^$' -bench '^BenchmarkGHFull$/^ContextTrim$' -benchmem -benchtime=1s -count=3 -cpu=1
```
