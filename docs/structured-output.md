# 结构化输出

`llm.StructuredOutput[T]` 让模型输出经过 JSON Schema 校验后解码为明确的 Go
结构体 T，可以作为普通 Runnable 挂进 Graph。第一版只支持原生 JSON Schema
输出；工具调用形式、提示词降级和自动纠错不在本次范围内——校验失败直接返回
`llm.ErrInvalidStructuredOutput` 错误。

整体分两层：

- `llm.ResponseFormat` + `llm.WithResponseFormat`：类型化的模型输出格式，
  作为 `llm.Options` 经 `runnable.Option` 下发给模型；Provider 负责映射成
  自己的线路字段（OpenAI-compatible 映射为 `response_format: {type: "json_schema", ...}`）。
- `llm.StructuredOutput[T]`：构造期推导严格模式 Schema 并编译校验器，
  `Decode(messages)` 选择最后一条 assistant 消息做校验解码；同时实现
  `Runnable[[]schema.Message, T]` 的四种形式，可直接 `AddNode` 进图。

## 定义目标类型与构造组件

```go
type Decision struct {
    Approved bool   `json:"approved"`
    Reason   string `json:"reason"`
}

output, err := llm.NewStructuredOutput[Decision](
    llm.WithOutputName("decision"),
    llm.WithOutputDescription("审批结论"),
)
```

T 必须是结构体（顶层指针、标量、切片、map 在构造期拒绝）。字段按 `json`
标签命名；`jsonschema` 标签沿用工具参数的描述用法。名称限定
`[A-Za-z0-9_-]{1,64}`，缺省由类型名规范化推导（`Decision` → `decision`，
匿名类型 → `structured_output`）。

### Schema 规则（严格模式）

- 对象禁止额外字段，所有导出字段进入 `required`——包括嵌套对象。
- `omitempty`/`omitzero` 只允许指针字段：规范化后仍 required，但允许 `null`。
  非指针字段带这些标签在构造期报错（提示改成指针或移除标签）——
  「可以为 null」与「可以缺失」是两种不同语义。
- 需要可空字段时用 `*T`、`[]T` 等可空类型；非指针字段收到 `null` 会被拒绝。
- 实现了 `json.Marshaler`/`Unmarshaler`/`TextMarshaler`/`TextUnmarshaler`
  的自定义编解码类型（如 `json.RawMessage` 字段）在构造期拒绝：推导出的
  Schema 与实际编解码不一致，无法准确约束。顶层与容器元素同样检查，
  不是只查直接字段。
- 字段级 `map` 在构造期拒绝：动态键无法展开成 strict 要求的封闭属性集，
  改用显式结构体字段。
- `json:",string"` 标签与 `json.Number` 字段在构造期拒绝：线路形态与推导
  类型不一致（前者按字符串包裹数字，后者相反）。带显式 JSON 名的匿名
  内嵌字段同样拒绝。
- 接口字段（含 `any`、`[]any`）在构造期拒绝：推导为不限制约束的
  Schema，strict 要求每个字段可约束。
- `time.Time`、`slog.Level`、`big.Int` 等标准库类型按已知 Schema 保留。

`Format()` 每次返回独立副本（`Strict=true`），组件构造完成后只读，可并发
复用到多个请求。Schema 推导、冻结编译、单值解析与严格解码由内部共享
内核 `internal/jsonvalidation` 承担，与 `pkg/datamodel`、工具参数走同一
实现；消息选择、refusal 与截断处理等 strict 政策仍归本组件。

## 能力声明

类型化格式只在显式声明 `JSONSchema` 能力的端点上启用；未声明时模型调用
直接返回 `llm.ErrResponseFormatUnsupported`，不会静默降级成 JSON mode 或
提示词模式：

```go
model, err := llm.New(llm.Config{
    ModelType:    "openai-compatible",
    Model:        "gpt-4o",
    APIKey:       key,
    // Capabilities 为 nil 时采用默认声明（JSONSchema 默认关闭）；
    // 非 nil 使用完整内容，包括显式全 false。
    Capabilities: &llm.Capabilities{Streaming: true, ToolCalls: true, JSONSchema: true},
})
```

注意默认能力变化：`OpenAICompatibleConfig.Capabilities` 零值/工厂未声明时
保留 Streaming/ToolCalls 默认，但 `JSONSchema` 默认为 **false**。需要时按
实际端点显式声明——框架不按模型名猜测，也不保证服务一定接受（HTTP 拒绝
仍按模型调用错误返回）。

`WithResponseFormat` 与构造配置或单次 `GenerateConfig["response_format"]`
同时存在时返回 `llm.ErrResponseFormatConflict`（在发 HTTP 请求之前）。

手写 `ResponseFormat` 在发请求前统一校验：`Name` 必须匹配
`[A-Za-z0-9_-]{1,64}`，`Schema` 必须是合法 JSON 且根类型包含 `object`
（`type:["object","null"]` 这类可空联合同样接受）；`Strict=true` 时递归
要求每个对象节点 `additionalProperties=false` 且 `required` 覆盖全部
属性——`anyOf`/`oneOf`/`allOf` 分支、`items`/`prefixItems`/
`patternProperties`/`additionalProperties` 子 Schema 与 `$defs`/
`definitions` 都在递归范围内，组合分支无法绕行 strict 规则。组件生成的
格式与手写格式走同一条校验路径，不合法的格式在发请求之前报错。

## 用法一：单次回答 / Basic Agent

```go
messages, err := assistant.Invoke(ctx, input,
    llm.WithResponseFormat(output.Format()))
if err != nil {
    return err
}
decision, err := output.Decode(messages)
```

直接调用模型时同样传 `WithResponseFormat`，然后把 `response.Messages` 交给
`Decode`。`WithResponseFormat(nil)` 清除格式；未设置时保留普通消息输出，
不增加 Schema 编译或校验成本。

## 用法二：挂成普通图节点

无 State 的最小接线：

```go
g := graph.NewGraph[[]schema.Message, Decision]("decision")
_ = g.AddNode("answer", assistant)
_ = g.AddNode("decode", output)
_ = g.AddEdge(graph.Start, "answer")
_ = g.AddEdge("answer", "decode")
_ = g.AddEdge("decode", graph.End)
compiled, _ := g.Compile()

decision, err := compiled.Invoke(ctx, input,
    runnable.ForNode("answer", llm.WithResponseFormat(output.Format())))
```

解码组件 `OptionTypes()` 返回空集：广播的模型 Option 不会进入 decode 节点；
它的四个方法入口也会拒绝任何非空配置或定向选项。

需要持久化时按常规 State 接线（示例见
[`examples/structured-output`](../examples/structured-output/main.go)）：

- State 保存原任务、Agent 全部输出、整理模型消息和最终 Decision；
- `work` 节点 PostHandler 把 Agent 输出写入 State；
- `organize` 节点（`llm.NewChatModelNode`）PreHandler 从 State 组装
  `llm.Request`——明确不给 `Functions`，整理模型只生成结构化答案；
- `decode` 节点 PreHandler 读整理消息、PostHandler 成功后写 `State.Decision`；
- `WithCheckpoint`/`Checkpointer` 沿用现有接口；恢复落在 decode 时复用
  已保存的模型输出，不重复调用模型。

## 用法三：带业务工具的 Agent

推荐显式组成 `业务 Agent → 最终整理模型 → decode`，格式 Option 只定向给
整理模型节点。这条路径多一次模型调用：业务工具和审批由前面的 Agent 完成，
格式化失败不会重新调用这些工具。业务需要纠错时，自行声明有次数限制的
整理节点循环，循环只重新生成格式化答案。

## 消息选择与错误行为

解码流程固定为：最后一条 assistant 消息 → 检查是否为完整最终回答 → 提取
文本 → 解析唯一 JSON 值 → Schema 校验 → 严格解码进 T。

| 场景 | 行为 |
| --- | --- |
| 无 assistant 消息 | `ErrInvalidStructuredOutput` |
| 最后一条 assistant 仍带工具调用 | 错误（不回退到更早的回答） |
| 只有 reasoning、没有文本 | 错误 |
| `Message.Extra["llm.refusal"]` 非空 | 错误（拒绝回答） |
| `ResponseMeta.FinishReason` 为 `length`/`content_filter` | 错误（不完整） |
| Markdown 围栏、JSON 前后散文、第二个 JSON 值 | 错误（不做猜测性截取） |
| 缺 required 字段、未知字段、类型错误、数值溢出、`null` 给非可空字段 | 错误 |
| `0`、`false`、空字符串等合法值 | 正常解码（不因等于零值被拒） |

错误统一包装 `llm.ErrInvalidStructuredOutput`，可用 `errors.Is` 分类并保留
底层链；失败返回 T 零值，不交付部分填充结构。无 `ResponseMeta` 的自定义
模型按完整文本正常校验。

流式语义：`Stream` 成功交付一次 T 后结束；`Collect`/`Transform` 等待输入
流正常结束后才解码，输入尾部错误不会被吞；关闭输出 Reader 会释放上游。
解码失败不会在公共事件流中产生 `stream.KindResult`。

## 缓存身份

`extensions/cache/llm` 的缓存身份包含 `ResponseFormat` 的名称、Schema
字节与 Strict——同一 Schema 命中，任一变化产生不同键；`WithResponseFormat`
在创建与应用时都复制格式，调用方修改副本不污染已冻结的身份。命中缓存的
响应仍由解码组件照常校验。

## Provider 说明

当前仅 OpenAI-compatible 族（`openai-compatible`、`openai`、`deepseek`、
`dashscope`、`qwen`）映射线路格式：

```json
{"response_format": {"type": "json_schema", "json_schema": {
    "name": "decision", "description": "审批结论",
    "strict": true, "schema": {...}}}}
```

拒绝回答的 `refusal` 字段被规范化保存到 `Message.Extra["llm.refusal"]`，
Invoke 与 Stream 使用相同规则。

## 相关文档

- [通用数据模型](datamodel.md)
- [模型](llm-providers.md)
- [Agent](agent.md)
- [检查点与中断](checkpoints-interrupts.md)
- [Cookbook](cookbook.md)
