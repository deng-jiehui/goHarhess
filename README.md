# goHarness

goHarness 是一个面向 LLM 应用的 Go 框架，提供统一的模型、Agent、工具、检索、状态持久化和可观测性执行契约。

## 项目特点

- 统一的 `runnable.Runnable[I, O]` 执行接口
- Basic、ReAct、Routing 和 GroupChat Agent
- OpenAI-compatible 模型适配、SSE 流式输出和推理内容标准化
- 工具参数校验、并发调度、Middleware、身份信息和托管 HTTP 客户端
- Token 裁切、Prompt Cache 对齐和上下文构建
- 会话 Checkpoint、断点续跑和 Redis 持久化扩展
- 分块、批量索引、混合检索和重排
- Callback 生命周期和运行时错误隔离

## 环境要求

- Go 1.24 或更高版本
- Windows 下运行 race 检测需要可用的 MinGW-w64 GCC

## 快速开始

运行内置示例：

```powershell
go run ./examples/basic
```

示例会创建一个带 `echo` 工具的 Agent，并输出模型和工具执行后的消息。

## 模块结构

| 目录 | 说明 |
| --- | --- |
| `pkg/schema` | 消息、内容、多模态内容和函数调用数据结构 |
| `pkg/runnable` | Runnable、流式、批处理和选项计划 |
| `pkg/llm` | LLM 模型接口、Provider 注册和 OpenAI-compatible 适配器 |
| `pkg/agent` | Basic、ReAct、Routing 和 GroupChat Agent |
| `pkg/tool` | 工具接口、参数解析、并发策略和 ToolsNode |
| `pkg/contextengine` | Token 预算、上下文裁切和 Prompt Cache 对齐 |
| `pkg/state` | Checkpoint、断点续跑和状态版本控制 |
| `pkg/retrieval` | 分块、索引、检索和重排 |
| `pkg/framework` | 应用注册和统一运行入口 |
| `internal/toolruntime` | 框架内部的工具执行、超时、重试和安全控制 |
| `extensions` | Cache、Redis、MCP 等可选实现，单独维护 Go module |

## 可选扩展

核心模块只包含 provider-neutral 的接口和内存实现。Cache、Redis、MCP 等扩展位于嵌套的 `extensions` 模块中：

```powershell
Push-Location extensions
go test ./...
Pop-Location
```

详细说明见 [extensions 文档](docs/extensions.md) 和 [MCP 工具文档](docs/mcp-tools.md)。

## 核心执行契约

所有可执行组件都基于 `runnable.Runnable[I, O]`：

- Agent 和应用：`Runnable[[]schema.Message, []schema.Message]`
- LLM：`llm.Model`，处理 `llm.Request` 和 `llm.Response`
- 工具：`tool.Tool`，处理 `tool.CallInput` 和 `tool.Result`
- 检索、Embedding、Rerank 和向量数据库：各自的强类型 Runnable 契约

Graph 是唯一的编排引擎。流式和批处理是叠加在同一执行契约上的可选能力，不会引入另一套调用接口。

详见 [执行模型](docs/execution-model.md)、[工具运行时](docs/tool-runtime.md) 和 [上下文管理](docs/context-management.md)。

## 验证

根模块：

```powershell
go test -count=1 ./...
go vet ./...
```

扩展模块：

```powershell
Push-Location extensions
go test -count=1 ./...
go vet ./...
Pop-Location
```

Windows 下运行 race 检测时，将 MinGW-w64 的 bin 目录加入 `PATH`：

```powershell
$env:Path = 'D:\Program Files\msys64\ucrt64\bin;' + $env:Path
go test -race -count=1 ./...
```
