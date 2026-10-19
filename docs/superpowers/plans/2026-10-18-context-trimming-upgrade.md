# 上下文剪切升级实施方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在不删除完整会话历史、不破坏工具调用协议的前提下，让 Context Engine 按实际模型请求预算保留更有价值的内容，并能处理嵌套工具结果和工具定义过大的场景。

**Architecture:** 保留现有 `pkg/contextengine` 作为唯一上下文组装入口。剪切前先把消息按用户轮次和工具调用/结果关系组成不可拆分的保留单元，再按“必保留、近期、历史”优先级逐步执行删除、推理块移除、工具结果递归截断和普通文本截断。Context Engine 同时接收本次 `llm.Request.Functions`，把工具定义纳入同一份预算；完整历史仍由 Checkpoint 保存。框架提供可选的 `HistoryCompressor` 接口，压缩器只改写本次请求中的旧历史，不改变 Checkpoint，也不暗中调用模型生成摘要。

**Tech Stack:** Go、`pkg/contextengine`、`pkg/schema`、`pkg/agent`、现有 `TokenCounter`/媒体估算器、Go test/benchmark。

## Global Constraints

- 完整会话历史仍由 `state.Checkpoint.Messages` 保存，剪切只影响当前模型请求。
- 工具调用及其结果必须成对保留；不能通过截断 `FunctionToolCall.Arguments` 生成非法 JSON。
- 当前轮输入不能被整轮丢弃；可按现有兼容行为截断其中的文本，无法满足预算时返回可识别的 `BudgetError`。
- Pinned 继续表示必保留前缀；不新增第二套系统提示配置入口。
- 默认不引入隐藏的摘要模型调用，避免改变 `MaxLLMCalls`、费用、取消和检查点语义。
- 历史压缩器只处理上一轮及更早的历史；当前输入、工具定义、Pinned 和动态提醒由 Context Engine 单独管理。
- 保留现有 `ContextBuilder`、`RequestBuilder`、`WithContextBuilder` API；新增字段使用命名字段，不改变已有调用的语义。
- 注释使用中文；不覆盖工作区已有的无关修改。

---

### Task 0: 增加动态 System Reminder 尾部

**Files:**
- Modify: `pkg/contextengine/engine.go`
- Modify: `pkg/agent/base.go`
- Modify: `pkg/llm/model.go`（如需增加 Provider 能力声明）
- Test: `pkg/contextengine/engine_test.go`
- Test: `pkg/agent/context_state_test.go`
- Modify: `docs/context-management.md`

**Interfaces:**
- `contextengine.Request` 增加本次调用专用的 `SystemReminder *Reminder`；它只属于当前模型请求，不进入 `Checkpoint.Messages`。
- 新增 `Reminder` 类型，至少包含渲染文本和可选的结构化元数据；默认渲染为单条带 `context_kind=system_reminder` 标记的消息。
- 动态提醒由 Agent 在每次 LLM 调用前根据当前运行状态生成。提醒内容只允许放入当前时间、阶段、剩余预算、审批状态、工具/工件可用性等短信息，不能把原始工具结果重新塞回提醒。
- Context Engine 将提醒放在最终工作集尾部，单独计数并排除在稳定前缀和 Prompt Cache breakpoint 之外。
- Provider-neutral 层保留提醒标识；Provider 若不支持尾部 system 消息，则将其渲染为带明确边界标记的普通文本消息。提醒只是模型提示，不是权限边界，工具权限仍由工具运行时强制执行。

- [x] **Step 1: 添加失败测试**

覆盖以下场景：

1. 最终消息顺序是稳定前缀、保留对话、当前输入、工具结果、System Reminder。
2. 连续两次模型调用的时间、剩余预算或审批状态变化时，Reminder 也随之变化。
3. Reminder 不会出现在 Checkpoint 的完整会话历史中，也不会在恢复后重复累积。
4. Reminder token 计入总预算；可选提醒字段超预算时先省略，必需字段无法容纳时返回 `BudgetError`。

- [x] **Step 2: 运行失败测试**

```text
go test ./pkg/contextengine ./pkg/agent -run 'Reminder|Context' -count=1
```

预期：当前实现没有 Reminder 输入和尾部标识，新增断言失败。

- [x] **Step 3: 实现动态提醒边界**

Agent 在 `prepareLLMRequest` 阶段创建本次调用的提醒快照，Context Engine 负责：

1. 克隆并校验提醒内容；
2. 把提醒作为最后一个工作集单元；
3. 在裁剪历史之前预留提醒 token；
4. 不把提醒并入 Pinned，不保存到会话消息；
5. 在输出统计中记录 Reminder token 和被省略的可选字段。

提醒文本采用固定的开始/结束标记，动态值按结构化字段转义，防止工具结果或用户输入伪装成框架指令。

- [x] **Step 4: 运行测试**

```text
go test ./pkg/contextengine ./pkg/agent ./pkg/llm -count=1
```

预期：提醒始终位于工作集尾部，预算统计正确，Checkpoint、Resume 和 Prompt Cache 行为不回归。

- [ ] **Step 5: 提交**

```text
git add pkg/contextengine pkg/agent pkg/llm docs/context-management.md
git commit -m "feat(context): add per-call system reminder"
```

---

### Task 1: 把工具定义纳入上下文预算

**Files:**
- Modify: `pkg/contextengine/engine.go`
- Modify: `pkg/contextengine/token.go`
- Modify: `pkg/agent/base.go`
- Test: `pkg/contextengine/engine_test.go`
- Test: `pkg/agent/options_boundary_test.go`

**Interfaces:**
- `contextengine.Request` 新增 `Functions []schema.FunctionDefinition`。
- `contextengine.Stats` 新增 `FunctionDefinitionTokens int` 和 `RequestTokens int`；保留既有 `InputTokens`、`OutputTokens` 的含义。
- 新增内部 `countFunctionDefinitions([]schema.FunctionDefinition, TokenCounter) int`，使用确定性的 `json.Marshal` 结果估算工具定义 token。
- `agent.base.prepareLLMRequest` 将即将发送的 `functions` 传入 Context Engine。

- [x] **Step 1: 添加失败测试**

在 `pkg/contextengine/engine_test.go` 增加一个带大参数 Schema 的函数定义，断言：

```go
result, err := assembler.Invoke(ctx, contextengine.Request{
    Functions: []schema.FunctionDefinition{largeDefinition},
    Input: []schema.Message{schema.UserMessage("hi")},
})
if err == nil {
    t.Fatal("expected function definitions to consume the context budget")
}
var budgetErr *contextengine.BudgetError
if !errors.As(err, &budgetErr) {
    t.Fatalf("error = %v, want BudgetError", err)
}
```

在 Agent 测试中让自定义 Context Builder 记录 `request.Functions`，断言它收到的函数定义与模型请求一致。

- [x] **Step 2: 运行失败测试**

运行：

```text
go test ./pkg/contextengine ./pkg/agent -run 'Function|Budget' -count=1
```

预期：新增测试失败，因为当前 Request 没有工具定义字段且预算未包含函数定义。

- [x] **Step 3: 实现最小改动**

在 `Request` 加入 `Functions`，在 `Invoke` 中先计算函数定义 token。预算检查和最终 `RequestTokens` 使用：

```text
request tokens = 函数定义 tokens + 稳定前缀 tokens + 消息 tokens
```

函数定义属于模型调用契约，不能被 Context Engine 静默删除；仅当完整请求超预算时返回 `BudgetError`，并在 `Reason` 中说明是工具定义或必保留内容超限。

- [x] **Step 4: 运行测试**

```text
go test ./pkg/contextengine ./pkg/agent -count=1
```

预期：通过；旧的无函数定义场景统计保持不变，带函数定义场景的 `RequestTokens` 不超过预算。

- [ ] **Step 5: 提交**

```text
git add pkg/contextengine pkg/agent
git commit -m "feat(context): include tool definitions in context budget"
```

---

### Task 2: 建立不可拆分的上下文保留单元

**Files:**
- Create: `pkg/contextengine/retention.go`
- Modify: `pkg/contextengine/engine.go`
- Test: `pkg/contextengine/engine_test.go`

**Interfaces:**
- 新增内部 `contextUnit`，至少包含消息切片、来源（Pinned/History/Input）、轮次序号、token 成本和工具调用关系。
- 新增公开的轻量配置：

```go
type RetentionConfig struct {
    DropReasoning bool
    TrimToolResults bool
    TrimHistoricalText bool
}
```

- `Config` 增加 `Retention RetentionConfig`；零值沿用当前行为：删除旧轮、移除推理块、保留最新文本尾部。
- 工具调用和结果的配对关系只在内部计算，不修改 `schema.Message` 公共结构。

- [x] **Step 1: 添加失败测试**

覆盖以下场景：

1. 同一轮含多个并行 FunctionToolCall 和 FunctionToolResult，裁剪时不会留下孤立调用或孤立结果。
2. 旧轮有长工具结果时，删除操作以完整工具交换为单位。
3. 当前轮输入含工具调用时，不能通过删除整轮来解决预算。
4. Pinned 超预算仍返回 BudgetError。

- [x] **Step 2: 运行失败测试**

```text
go test ./pkg/contextengine -run 'Atomic|Retention|Budget' -count=1
```

预期：新增的原子性断言至少有一项失败。

- [x] **Step 3: 实现分组与保留顺序**

将归一化后的消息划分为：

1. Pinned 和 System 前缀：必保留。
2. History 中的完整用户轮次：按从旧到新编号。
3. Input 所在的当前轮：不可整轮删除。

对含工具调用的轮次建立调用 ID 索引。删除旧轮时整组删除；绝不只删除 Assistant 调用或只删除 Tool 结果。保留最终消息顺序，不按优先级重排消息。

- [x] **Step 4: 运行测试**

```text
go test ./pkg/contextengine -count=1
```

预期：所有消息顺序、角色修复、并行调用和现有裁剪测试通过。

- [ ] **Step 5: 提交**

```text
git add pkg/contextengine
git commit -m "feat(context): retain atomic conversation units"
```

---

### Task 3: 增加递归的安全压缩动作

**Files:**
- Modify: `pkg/contextengine/engine.go`
- Modify: `pkg/contextengine/token.go`
- Test: `pkg/contextengine/engine_test.go`
- Test: `pkg/contextengine/media_estimate_test.go`

**Interfaces:**
- 新增内部递归函数，对 `ContentBlock` 和 `FunctionToolResult.ContentBlocks` 进行安全裁剪。
- `Stats` 增加 `TruncatedToolResults int`、`DroppedReasoningBlocks int`、`DroppedMediaBlocks int`。
- 新增内部文本策略：普通最新输入保留尾部；历史工具结果和普通历史文本采用“头部 + 尾部”策略，中间以固定标记替代。

- [x] **Step 1: 添加失败测试**

覆盖：

1. 长文本嵌套在 FunctionToolResult 中时可以递归截断，结果仍通过 `schema.Message.Validate`。
2. FunctionToolCall 的 Arguments 不被截断，仍是合法 JSON；如果固定开销本身超预算，返回 BudgetError。
3. 历史媒体块可按完整块删除并记录 `DroppedMediaBlocks`；当前输入媒体不能被半截。
4. Opaque JSON 和不可拆分的媒体块不能伪造截断结果。

- [x] **Step 2: 运行失败测试**

```text
go test ./pkg/contextengine -run 'Nested|ToolResult|Media|Opaque' -count=1
```

预期：当前实现无法裁剪嵌套工具结果，至少一项失败。

- [x] **Step 3: 实现按成本递进的裁剪**

预算超限时依次执行：

1. 删除旧历史轮次中的低价值可选块（先推理块，再历史媒体块）。
2. 递归截断旧历史的 ToolResult 文本，保留调用 ID、工具名、错误标记和嵌套块类型。
3. 截断历史普通文本。
4. 按现有兼容语义截断当前轮文本并保留尾部。
5. 若剩余内容只有函数调用参数、Pinned、当前媒体或其他固定开销，返回 BudgetError。

每次动作都重新计算受影响单元的 token 差额，不能退回到按整个消息列表反复全量计数的 O(T²) 实现。

- [x] **Step 4: 运行测试**

```text
go test ./pkg/contextengine ./pkg/schema -count=1
```

预期：通过；所有输出消息可验证，工具调用/结果关系不被破坏。

- [ ] **Step 5: 提交**

```text
git add pkg/contextengine pkg/schema
git commit -m "feat(context): recursively trim safe content blocks"
```

---

### Task 4: 统一 Agent 调用链与统计结果

**Files:**
- Modify: `pkg/agent/base.go`
- Modify: `pkg/contextengine/engine.go`
- Test: `pkg/agent/context_state_test.go`
- Test: `pkg/agent/unified_checkpoint_test.go`

**Interfaces:**
- Agent 每次模型调用都将完整会话历史、当前输入和当前模型函数定义交给同一个 Context Builder。
- `contextengine.HistoryCompressor` 接收上一轮及更早的 `[]schema.Message`，返回只用于本次模型请求的历史视图；`agent.WithHistoryCompressor` 只配置默认 Context Builder。
- Checkpoint 仍保存完整消息；Context Builder 的裁剪结果不回写 Checkpoint。
- Agent 的模型请求仍只承载 `Messages` 和 `Functions`，不把裁剪选项混进 `llm.Request`。
- 观测事件可在已有 metadata 中记录 `context.request_tokens`、`context.dropped_turns`、`context.truncated_tool_results`；不把完整历史写入观测。

- [x] **Step 1: 添加失败测试**

1. Agent 的自定义 Context Builder 能同时看到 History/Input 和 Functions。
2. 一次模型调用的 checkpoint 仍含完整历史，而模型实际收到的是裁剪后的工作集。
3. 多轮调用每次重新计算函数定义和消息预算，不能复用上轮裁剪后的消息。

- [x] **Step 2: 运行失败测试**

```text
go test ./pkg/agent -run 'Context|History|Function' -count=1
```

预期：当前 Builder 未收到 Functions 的测试失败。

- [x] **Step 3: 实现调用链**

在 `prepareLLMRequest` 构造 `contextengine.Request` 时填充 `Functions: functions`。Context Builder 返回的 Stats 只用于观测和诊断，不写回会话。对自定义 Builder，保持它可以选择忽略新字段；默认 Builder 执行完整预算治理。

Agent 同时保存本轮输入边界：默认 Builder 分别收到上一轮历史、本轮输入以及本轮已经产生的模型/工具消息，历史压缩器只作用于上一轮历史。配方 State 将本轮输入边界随执行 State 保存，恢复时继续使用同一拆分，避免压缩后的消息污染 Checkpoint。

- [x] **Step 4: 运行测试**

```text
go test ./pkg/agent ./pkg/contextengine -count=1
```

预期：通过，且旧的 checkpoint、Resume、流式路径不回归。

**历史压缩接口补充实现（2026-10-18）**：新增 `HistoryCompressor`、函数适配器和
`agent.WithHistoryCompressor`。Agent 的默认 Context Builder 现在能区分上一轮历史与本轮
工作集；压缩结果不会回写 Checkpoint。新增两轮会话回归，验证压缩器收到真实历史、模型不再
收到被压缩的旧消息、当前输入仍保留，且 Checkpoint 保存完整原文。配方 State codec 升至
v5 以保存本轮输入边界；替换默认 Context Builder 时仍由自定义实现自行接入压缩器。

- [ ] **Step 5: 提交**

```text
git add pkg/agent pkg/contextengine
git commit -m "feat(agent): assemble context with complete request budget"
```

---

### Task 5: 基准、文档和回归验收

**Files:**
- Modify: `pkg/contextengine/engine_benchmark_test.go`（如不存在则创建）
- Modify: `docs/context-management.md`
- Modify: `docs/design/archive/架构改进实施方案.md`（新增上下文剪切升级条目）
- Modify: `docs/design/archive/架构审查与改进报告.md`（只记录状态和方案引用）

**Interfaces:**
- 基准场景固定为：短历史、100 轮历史、长工具结果、32 个工具定义、媒体输入、当前轮超限。
- 记录 ns/op、B/op、allocs/op、最终请求 token、裁剪统计。
- 验收阈值沿用仓库已有规则：同名场景中位数增加超过 10% 才触发复测；正确性失败直接不通过。

- [x] **Step 1: 添加基准与文档测试**

基准必须检查每次结果满足：

```text
RequestTokens <= BudgetTokens
所有输出消息 Validate() 成功
工具调用和结果 ID 配对
```

文档示例说明“完整历史”和“本次工作集”的区别，并明确工具定义也占上下文预算。

- [x] **Step 2: 运行验证**

```text
go test ./pkg/contextengine ./pkg/agent ./pkg/schema -count=1
go vet ./pkg/contextengine ./pkg/agent ./pkg/schema
go test ./... -count=1
go vet ./...
```

按仓库既有脚本补跑 Context Engine、Agent 和 extensions 相关竞态检测；使用 Go 1.24.4 基准工具链保存原始结果和摘要。

- [x] **Step 3: 更新两份文档**

实施方案写实现细节、接口和测试证据；审查报告只写“已完成/未完成”和方案链接，不复制实施过程。

- [ ] **Step 4: 提交**

```text
git add pkg/contextengine pkg/agent docs/context-management.md docs/design/archive/架构改进实施方案.md docs/design/archive/架构审查与改进报告.md
git commit -m "docs(context): record context trimming upgrade and verification"
```

---

### Task 6：工具结果工件化

**Files:**
- Create: `pkg/tool/artifact.go`
- Test: `pkg/tool/artifact_test.go`
- Modify: `pkg/tool/node.go`
- Modify: `pkg/agent/base.go`
- Modify: `pkg/agent/config_options.go`
- Modify: `docs/context-management.md`

工具结果超过配置阈值时，先把完整的 JSON Block 列表写入 `ArtifactStore`，再把
工具结果替换为包含 `artifact_id`、大小、校验摘要和短预览的文本卡片。卡片本身
进入模型工作集和 Checkpoint，完整结果只通过 `Read` 按范围读取。保存失败返回
`ErrArtifactStore` 框架错误，不能伪造成功卡片。提供本地目录实现和可选的
`read_artifact` 只读工具，生产环境可以替换为对象存储。

- [x] **Step 1: 添加工件存储回归测试**

覆盖目录读写、按范围读取、过大结果变成卡片，以及保存失败不继续执行。

- [x] **Step 2: 接入 ToolsNode 和 Agent 配置**

新增 `ArtifactStore`、`ArtifactThresholdBytes`，保留零值关闭外置的兼容行为。

- [x] **Step 3: 运行工具、Agent 和扩展回归**

```text
go test ./pkg/tool ./pkg/agent ./extensions/...
```

实现记录（2026-10-18）：`FileArtifactStore` 对同一 ID 的相同内容保存采用幂等复用，
不同内容拒绝覆盖；ToolsNode 在工件保存失败时返回 `ErrArtifactStore`，不会把失败
伪装成成功卡片。根模块与 extensions 的定向回归已通过，最终全量测试、`go vet`
和相关竞态检测作为本轮验收的一部分执行。

---

## 暂不纳入本次升级的内容

- 框架自动调用 LLM 生成历史摘要。它会额外消耗模型调用次数，并要求把摘要作为新的持久化状态处理；本阶段先提供安全的规则化压缩和完整历史保留。
- 根据工具名称自动猜测哪些工具定义可以删除。工具定义属于模型调用契约，缺失时可能导致模型继续调用不可用工具；如需动态工具集合，由应用在调用前显式传入。
- Provider 专属精确 tokenizer。继续使用现有可注入 `TokenCounter`，需要精确计费时由应用提供对应 tokenizer。

