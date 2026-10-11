# 通用数据模型

`pkg/datamodel` 把普通 Go 类型编译为可复用的 JSON 数据模型：由类型推导
JSON Schema 并在构造期冻结编译，对外部 JSON 输入提供校验、严格解码与
业务校验。它服务于普通 JSON 数据（API 入参、配置、外部载荷），不调用
模型、不管理运行或检查点。

```go
type Ticket struct {
    ID       int64    `json:"id" jsonschema:"ticket identifier"`
    Title    string   `json:"title"`
    Priority string   `json:"priority,omitempty"`
    Tags     []string `json:"tags,omitempty"`
}

model, err := datamodel.New[Ticket]()
if err != nil { panic(err) }
ticket, err := model.DecodeJSON([]byte(`{"id":1,"title":"t"}`))
```

## 构造期行为

`New[T]` 依次完成三件事，任一失败都返回错误而不是延后到运行期：

1. **表达性检查**：推导 Schema 与 `encoding/json` 解码不一致的写法
   在构造期拒绝——自定义 `Marshaler`/`Unmarshaler`/`TextMarshaler`/
   `TextUnmarshaler` 类型、`json.Number` 字段、`,string` 标签、带显式
   `json` 名的内嵌字段。标准库白名单（`time.Time`、`slog.Level`、
   `big.Int/Rat/Float`）与库推导一致，允许使用。
2. **Schema 推导**：字段名按 `json` 标签；无 `omitempty`/`omitzero` 的
   字段进入 `required`；结构体 `additionalProperties:false`；`jsonschema`
   标签提供描述；指针/切片字段产生 nullable/数组表达。`[]byte` 按
   `encoding/json` 的 base64 字符串形态约束（`["null","string"]`），
   不接受整数数组；`[N]byte` 数组仍按整数数组。
3. **冻结编译**：Schema 序列化后重新解析并编译校验器——发送方、本地
   校验与之后编辑用的都是同一份字节。

构造完成的 `Model[T]` 只读，可并发复用。

## 校验与解码

| 方法 | 行为 |
|---|---|
| `JSONSchema()` | 返回冻结 Schema 字节的独立副本 |
| `ValidateJSON(data)` | 只校验：输入必须是恰好一个 JSON 值（尾部第二值或文本同样拒绝）且通过 Schema |
| `DecodeJSON(data)` | 校验后按 unknown-fields-strict 解码进 T，再执行业务校验 |

失败统一包裹 `ErrInvalidData`（`errors.Is` 可分类），并返回 T 零值；
错误消息尽量指到具体字段或路径。解码以 `json.Number` 保留数字精度：
`int64` 与 `any` 字段不会因中间转 `float64` 丢精度。

## 选项与派生

```go
model, err := datamodel.New[Ticket](
    datamodel.WithSchema[Ticket](func(s *jsonschema.Schema) error {
        s.Properties["id"].Minimum = jsonschema.Ptr(1.0)
        return nil
    }),
    datamodel.WithValidator(func(t Ticket) error {
        if t.Priority == "high" && t.Title == "" {
            return errors.New("high-priority ticket requires a title")
        }
        return nil
    }),
)
```

- `WithSchema(edit)`：推导之后、冻结之前应用编辑；编辑返回错误或
  结果无法编译，构造失败。
- `WithValidator(check)`：Schema 校验与解码都通过后按注册顺序执行，
  返回错误即整体失败。
- `model.WithSchema(edit)`：在一份 Schema 副本上编辑后重新编译，
  返回新 `Model[T]`，原模型不受影响。

## `any` 字段与业务检查的边界

- `any` 字段按依赖的开放 Schema 处理：校验不做约束，解码按 JSON 值
  原样进入 `any`（数字保留为 `json.Number`）。它与泛型约束无关——
  `Model[T]` 的 T 就是解码目标类型本身。业务需要约束 `any` 内容时，
  用 `WithSchema` 在 Schema 中显式表达，或改用具体类型。
- 业务检查函数只在本地执行，不会自动变成 Schema 约束或发给模型的
  规则。「结束时间不能早于开始时间」这类判断不会因为注册了 Go 函数
  就出现在 `JSONSchema()` 中；发送侧能表达的约束仍需 `WithSchema`
  显式配置。

## 与其他层的关系

- **结构化输出**（`llm.StructuredOutput[T]`）：在更严的 strict 政策下
  复用同一编译/解码内核，另加消息选择、refusal 与截断处理，见
  [structured-output.md](structured-output.md)。
- **工具参数**：`tool.New` 的参数 Schema 推导与严格解码也复用同一
  内核，但注册快照、错误类型与执行语义不变。

完整示例见 `examples/datamodel`。
