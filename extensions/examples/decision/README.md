# 决策模型集成示例（实验）

`extensions/decision` 的端到端演示，全程脚本化假模型，离线可跑：

```bash
go run .
```

## 演示内容

1. **分诊路由**：`decision.Node` 在 State 图里问一个 `choice` 问题，
   `decision.Route(answer, minConfidence, targets, fallback)` 把低置信度
   回答落到预声明的兜底目标——路由目标仍受编译期校验。
2. **工具筛查**：`decision.ToolGuard` 中间件对非只读工具先问 `noul`
   问题，PTrue 低于阈值经 `graph.Interrupt` 挂起；批准后经
   `agent.Resume` 恢复，工具只执行一次。

## 接入真实服务

把假模型换成 `decision.NewHTTP(decision.HTTPConfig{Endpoint: ...})`
（信封字段名可配的厂商中立 JSON 客户端）。红线与阈值校准建议见
`extensions/decision/doc.go` 与 `docs/extensions.md` 的决策模型一节。

注意：恢复时筛查中间件随节点重放，决策模型会再问一次同一问题；
本示例用 `NewFuncModel` 返回恒定答案保证中断载荷一致。
