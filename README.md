# goHarness

这是一个参考 `qwen_agent` 分层思想搭建的 Go 版 LLM 应用开发框架骨架。

当前先完成底层内核，不追求功能完整：

- `pkg/schema`: 统一消息、内容、多模态内容项、函数调用 schema
- `pkg/llm`: 模型接口、模型注册表、生成配置、测试用 mock model
- `pkg/tool`: 工具接口、工具注册表、JSON 参数解析和 OpenAI 风格函数定义
- `pkg/agent`: Basic、FnCall、Assistant、GroupChat 等 agent 基础形态
- `pkg/memory`: RAG/文件记忆能力的抽象入口
- `pkg/framework`: LLM 应用层，负责 agent 注册、默认 agent 选择和运行

## 运行示例

```bash
go run ./examples/basic
```

## 验证

```bash
go test ./...
```

## 后续方向

- 接入真实模型 provider，例如 DashScope / OpenAI compatible
- 完善 streaming delta、重试、缓存、截断和观测能力
- 补充 MCP、RAG、文档解析和并行工具执行
- 增加配置驱动的应用装配方式
