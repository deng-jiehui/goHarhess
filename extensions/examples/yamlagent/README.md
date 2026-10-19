# 声明式（YAML）Agent 示例

演示把 YAML 声明映射到 `agent.New` 的 Option 装配，离线可跑：

```bash
go run .
```

```yaml
name: helpdesk
version: "1.0"
instructions: 你是客服助手，数据库问题交给专家。
model: main
max_llm_calls: 4
tools: [echo]
agents:
  db_expert: {model: main, tools: [echo], description: 数据库问题专家}
```

映射关系：

| YAML | agent.New |
| --- | --- |
| `name`/`instructions` | `WithName` / `WithSystemMessage` |
| `version` | `WithDefinitionVersion`（声明变了，旧挂起运行拒绝恢复） |
| `model`/`tools` | 注册表查名——**未知名报错，不放行** |
| `agents:` | 递归装配后经 `subagent.AsTool` 挂成专家工具 |
| `max_llm_calls`/`max_tokens`/`timeout` | 对应 Option，向下收紧到封顶值 |

框架不内置 YAML 加载器：加载、校验、租户白名单都在应用层。
不可信输入的最小护栏清单见 `main.go` 末尾 `untrustedInputRules`
（注册表约束、参数封顶、指令长度、版本必填、MCP 严格模式）。
