// examples/datamodel 演示通用数据模型：
//  1. datamodel.New[T] 由 Go 类型推导 JSON Schema 并冻结编译，一次构造
//     可并发复用。
//  2. ValidateJSON 只校验，DecodeJSON 校验后严格解码为 T；错误统一按
//     ErrInvalidData 分类。
//  3. WithSchema 选项在编译前编辑 Schema，WithValidator 追加业务校验；
//     Model.WithSchema 派生新模型且不改动原模型。
//
// 示例不调用模型，不需要 API Key。
package main

import (
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/goharness/goharness/pkg/datamodel"
)

// Ticket 是普通业务输入：json 标签定字段名与可选性，jsonschema 标签定
// 描述；其余规则随 Go 类型推导。
type Ticket struct {
	ID       int64    `json:"id" jsonschema:"ticket identifier"`
	Title    string   `json:"title"`
	Priority string   `json:"priority,omitempty"`
	Tags     []string `json:"tags,omitempty"`
}

func main() {
	// 构造期编译：Schema 与校验器一次成型，运行期只做值匹配。
	model, err := datamodel.New[Ticket]()
	if err != nil {
		panic(err)
	}
	fmt.Println("schema:", string(model.JSONSchema()))

	// 校验 + 解码：Schema 通过后再严格解码进 Ticket。
	raw := `{"id":9007199254740993,"title":"网关 502","priority":"high","tags":["prod"]}`
	ticket, err := model.DecodeJSON([]byte(raw))
	if err != nil {
		panic(err)
	}
	fmt.Printf("decoded: id=%d title=%s priority=%s tags=%v\n",
		ticket.ID, ticket.Title, ticket.Priority, ticket.Tags)

	// 失败统一按 ErrInvalidData 分类，成功路径返回 T 零值。
	for _, bad := range []string{
		`{"id":1}`,                // 缺必填 title
		`{"id":1,"title":"t","x":1}`, // 未知字段
		`{"id":1,"title":"t"} {}`,    // 尾部第二个 JSON 值
	} {
		if _, err := model.DecodeJSON([]byte(bad)); errors.Is(err, datamodel.ErrInvalidData) {
			fmt.Println("rejected:", bad)
		}
	}

	// 业务校验在 Schema 与解码之后执行。
	checked, err := datamodel.New[Ticket](datamodel.WithValidator(func(t Ticket) error {
		if t.Priority == "high" && t.Title == "" {
			return errors.New("high-priority ticket requires a title")
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}
	if _, err := checked.DecodeJSON([]byte(`{"id":2,"title":"","priority":"high"}`)); err != nil {
		fmt.Println("business rule:", err)
	}

	// 派生模型收紧约束，原模型不受影响。
	strict, err := model.WithSchema(func(s *jsonschema.Schema) error {
		s.Properties["id"].Minimum = jsonschema.Ptr(1.0)
		return nil
	})
	if err != nil {
		panic(err)
	}
	if _, err := strict.DecodeJSON([]byte(`{"id":0,"title":"t"}`)); err != nil {
		fmt.Println("derived model rejected id=0")
	}
	if _, err := model.DecodeJSON([]byte(`{"id":0,"title":"t"}`)); err != nil {
		panic("original model must stay unchanged")
	}
	fmt.Println("original model still accepts id=0")
}
